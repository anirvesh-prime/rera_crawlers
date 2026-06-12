from __future__ import annotations

import logging
import os
import random
import re
import ssl
import threading
import time
from typing import Any

import httpx
import siphash24

from core.config import settings
from core.logger import CrawlerLogger


_UINT64_MASK = (1 << 64) - 1
_HTTP_CLIENTS_LOCK = threading.Lock()
_HTTP_CLIENTS: dict[tuple[bool | int, ...], httpx.Client] = {}


def _project_hash_seed() -> bytes:
    seed_text = os.environ.get("PYTHONHASHSEED", "0")
    try:
        seed = int(seed_text)
    except ValueError:
        logging.warning("Invalid PYTHONHASHSEED %r; defaulting project-key seed to 0", seed_text)
        seed = 0
    return seed.to_bytes(16, byteorder="little", signed=False)


def generate_project_key(registration_number: str) -> str:
    """Generate a deterministic unsigned project key from the stripped registration number."""
    key_string = registration_number.strip()
    int_hash = siphash24.siphash24(key_string.encode("utf-8"), key=_project_hash_seed()).intdigest()
    return str(int_hash & _UINT64_MASK)


def random_delay(min_s: float = 1.0, max_s: float = 3.0) -> None:
    scaled_min, scaled_max = get_scaled_delay_range(min_s, max_s)
    if scaled_max <= 0:
        return
    time.sleep(random.uniform(scaled_min, scaled_max))


def get_scaled_delay_range(min_s: float, max_s: float) -> tuple[float, float]:
    """Apply the global crawl delay scale while preserving a valid range."""
    scale = max(settings.CRAWL_DELAY_SCALE, 0.0)
    scaled_min = max(min_s * scale, 0.0)
    scaled_max = max(max_s * scale, 0.0)
    if scaled_min > scaled_max:
        scaled_min, scaled_max = scaled_max, scaled_min
    return scaled_min, scaled_max


def get_legacy_ssl_context() -> ssl.SSLContext:
    """
    Returns an SSL context that accepts old government sites running TLS 1.0/1.1
    with unsafe legacy renegotiation (e.g. WB HIRA, Telangana, Uttarakhand).

    ``ssl.OP_LEGACY_SERVER_CONNECT`` was only added in Python 3.12 / OpenSSL 3.x.
    On older stacks (e.g. OpenSSL 1.1.1k) the constant is absent, so we fall back
    to the raw OpenSSL bit ``SSL_OP_ALLOW_UNSAFE_LEGACY_RENEGOTIATION = 0x00040000``
    which has the same effect on all 1.x releases.
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.set_ciphers("DEFAULT:@SECLEVEL=0")
    # 0x00040000 == SSL_OP_ALLOW_UNSAFE_LEGACY_RENEGOTIATION (OpenSSL 1.1.1)
    ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x00040000)
    return ctx


def get_random_ua() -> str:
    return random.choice(settings.user_agents)


def _verify_cache_key(verify: bool | ssl.SSLContext) -> bool | int:
    if isinstance(verify, ssl.SSLContext):
        return id(verify)
    return verify


def _get_shared_http_client(*, verify: bool | ssl.SSLContext) -> httpx.Client:
    key = (_verify_cache_key(verify),)
    with _HTTP_CLIENTS_LOCK:
        client = _HTTP_CLIENTS.get(key)
        if client is not None and not client.is_closed:
            return client

        client = httpx.Client(
            verify=verify,
            limits=httpx.Limits(
                max_connections=settings.HTTP_MAX_CONNECTIONS,
                max_keepalive_connections=settings.HTTP_MAX_KEEPALIVE_CONNECTIONS,
            ),
        )
        _HTTP_CLIENTS[key] = client
        return client


def close_shared_http_clients() -> None:
    with _HTTP_CLIENTS_LOCK:
        clients = list(_HTTP_CLIENTS.values())
        _HTTP_CLIENTS.clear()
    for client in clients:
        try:
            client.close()
        except Exception:
            pass


def safe_get(
    url: str,
    retries: int = 3,
    delay: float = 3.0,
    headers: dict | None = None,
    params: dict | None = None,
    logger: CrawlerLogger | None = None,
    timeout: float | httpx.Timeout = 30.0,
    verify: bool = True,
    client: httpx.Client | None = None,
) -> httpx.Response | None:
    """GET with retry/backoff.  Pass `client` to reuse an existing connection pool.

    The timeout escalates with each attempt: attempt 1 uses ``timeout`` as-is,
    attempt 2 uses ``timeout * 2``, attempt 3 uses ``timeout * 3``, etc.
    With the default of 30 s that gives 30 s → 60 s → 90 s across three tries.
    """
    _headers = {"User-Agent": get_random_ua(), **(headers or {})}
    last_exc: Exception | None = None
    attempt = 0
    for attempt in range(1, retries + 1):
        attempt_timeout = timeout * attempt if isinstance(timeout, (int, float)) else timeout
        try:
            if client is not None:
                resp = client.get(url, headers=_headers, params=params, timeout=attempt_timeout)
            else:
                pooled_client = _get_shared_http_client(verify=verify)
                resp = pooled_client.get(
                    url,
                    headers=_headers,
                    params=params,
                    timeout=attempt_timeout,
                    follow_redirects=True,
                )
            resp.raise_for_status()
            return resp
        except httpx.HTTPStatusError as e:
            last_exc = e
            if logger:
                logger.warning(
                    f"GET attempt {attempt}/{retries} failed (timeout={attempt_timeout}s): {e}",
                    url=url,
                )
            # 4xx errors are definitive client errors — no point retrying
            if e.response.status_code < 500:
                break
            if attempt < retries:
                time.sleep(delay * attempt)
        except Exception as e:
            last_exc = e
            if logger:
                logger.warning(
                    f"GET attempt {attempt}/{retries} failed (timeout={attempt_timeout}s): {e}",
                    url=url,
                )
            if attempt < retries:
                time.sleep(delay * attempt)
    if logger and last_exc is not None:
        logger.error(f"GET failed after {attempt} attempt(s): {last_exc}", url=url)
    return None


def safe_post(
    url: str,
    data: Any = None,
    json_data: Any = None,
    retries: int = 3,
    delay: float = 3.0,
    headers: dict | None = None,
    logger: CrawlerLogger | None = None,
    timeout: float | httpx.Timeout = 30.0,
    verify: bool = True,
    client: httpx.Client | None = None,
) -> httpx.Response | None:
    """POST with retry/backoff.  Pass `client` to reuse an existing connection pool.

    The timeout escalates with each attempt: attempt 1 uses ``timeout`` as-is,
    attempt 2 uses ``timeout * 2``, attempt 3 uses ``timeout * 3``, etc.
    With the default of 30 s that gives 30 s → 60 s → 90 s across three tries.
    """
    _headers = {"User-Agent": get_random_ua(), **(headers or {})}
    last_exc: Exception | None = None
    attempt = 0
    for attempt in range(1, retries + 1):
        attempt_timeout = timeout * attempt if isinstance(timeout, (int, float)) else timeout
        try:
            if client is not None:
                resp = client.post(
                    url,
                    data=data,
                    json=json_data,
                    headers=_headers,
                    timeout=attempt_timeout,
                )
            else:
                pooled_client = _get_shared_http_client(verify=verify)
                resp = pooled_client.post(
                    url,
                    data=data,
                    json=json_data,
                    headers=_headers,
                    timeout=attempt_timeout,
                    follow_redirects=True,
                )
            resp.raise_for_status()
            return resp
        except httpx.HTTPStatusError as e:
            last_exc = e
            if logger:
                logger.warning(
                    f"POST attempt {attempt}/{retries} failed (timeout={attempt_timeout}s): {e}",
                    url=url,
                )
            # 4xx errors are definitive client errors — no point retrying
            if e.response.status_code < 500:
                break
            if attempt < retries:
                time.sleep(delay * attempt)
        except Exception as e:
            last_exc = e
            if logger:
                logger.warning(
                    f"POST attempt {attempt}/{retries} failed (timeout={attempt_timeout}s): {e}",
                    url=url,
                )
            if attempt < retries:
                time.sleep(delay * attempt)
    if logger and last_exc is not None:
        logger.error(f"POST failed after {attempt} attempt(s): {last_exc}", url=url)
    return None


def download_response(
    url: str,
    *,
    method: str = "GET",
    retries: int = 3,
    delay: float = 3.0,
    headers: dict | None = None,
    params: dict | None = None,
    data: Any = None,
    json_data: Any = None,
    logger: CrawlerLogger | None = None,
    timeout: float | httpx.Timeout = 30.0,
    total_timeout: float = 60.0,
    max_bytes: int = 50 * 1024 * 1024,
    verify: bool = True,
    follow_redirects: bool = True,
    client: httpx.Client | None = None,
) -> httpx.Response | None:
    """Download a response body with a hard wall-clock timeout and size cap.

    Both ``timeout`` (per-request connect/read, when a plain float) and
    ``total_timeout`` (wall-clock download limit) escalate with each attempt:
    attempt 1 uses the base values as-is, attempt 2 doubles them, attempt 3
    triples them, etc.  With the defaults that gives:
      attempt 1 → timeout=30 s, total_timeout=60 s
      attempt 2 → timeout=60 s, total_timeout=120 s
      attempt 3 → timeout=90 s, total_timeout=180 s
    """
    _headers = {"User-Agent": get_random_ua(), **(headers or {})}
    last_exc: Exception | None = None
    method = method.upper()
    attempt = 0

    for attempt in range(1, retries + 1):
        # Scale timeouts linearly with attempt number (30→60→90 pattern).
        attempt_timeout: float | httpx.Timeout = (
            timeout * attempt if isinstance(timeout, (int, float)) else timeout
        )
        attempt_total_timeout = total_timeout * attempt
        try:
            if client is not None:
                response = _stream_download_response(
                    client=client,
                    method=method,
                    url=url,
                    headers=_headers,
                    params=params,
                    data=data,
                    json_data=json_data,
                    timeout=attempt_timeout,
                    total_timeout=attempt_total_timeout,
                    max_bytes=max_bytes,
                    follow_redirects=follow_redirects,
                )
            else:
                pooled_client = _get_shared_http_client(verify=verify)
                response = _stream_download_response(
                    client=pooled_client,
                    method=method,
                    url=url,
                    headers=_headers,
                    params=params,
                    data=data,
                    json_data=json_data,
                    timeout=attempt_timeout,
                    total_timeout=attempt_total_timeout,
                    max_bytes=max_bytes,
                    follow_redirects=follow_redirects,
                )
            return response
        except httpx.HTTPStatusError as exc:
            last_exc = exc
            if logger:
                logger.warning(
                    f"{method} attempt {attempt}/{retries} failed"
                    f" (timeout={attempt_timeout}s, total={attempt_total_timeout}s): {exc}",
                    url=url,
                )
            if exc.response.status_code < 500:
                break
            if attempt < retries:
                time.sleep(delay * attempt)
        except Exception as exc:
            last_exc = exc
            if logger:
                logger.warning(
                    f"{method} attempt {attempt}/{retries} failed"
                    f" (timeout={attempt_timeout}s, total={attempt_total_timeout}s): {exc}",
                    url=url,
                )
            if attempt < retries:
                time.sleep(delay * attempt)

    if logger and last_exc is not None:
        logger.error(f"{method} failed after {attempt} attempt(s): {last_exc}", url=url)
    return None


def _stream_download_response(
    *,
    client: httpx.Client,
    method: str,
    url: str,
    headers: dict,
    params: dict | None,
    data: Any,
    json_data: Any,
    timeout: float | httpx.Timeout,
    total_timeout: float,
    max_bytes: int,
    follow_redirects: bool,
) -> httpx.Response:
    deadline_hit = threading.Event()

    with client.stream(
        method,
        url,
        headers=headers,
        params=params,
        data=data,
        json=json_data,
        timeout=timeout,
        follow_redirects=follow_redirects,
    ) as response:
        response.raise_for_status()

        def _abort_on_deadline() -> None:
            deadline_hit.set()
            try:
                response.close()
            except Exception:
                pass

        timer = threading.Timer(total_timeout, _abort_on_deadline)
        timer.start()
        try:
            chunks: list[bytes] = []
            total_bytes = 0
            for chunk in response.iter_bytes(chunk_size=65536):
                if deadline_hit.is_set():
                    raise TimeoutError(
                        f"Download exceeded {total_timeout}s total limit"
                    )
                chunks.append(chunk)
                total_bytes += len(chunk)
                if total_bytes > max_bytes:
                    raise ValueError(
                        f"Document too large (>{max_bytes // (1024 * 1024)} MB)"
                    )
        finally:
            timer.cancel()

        return httpx.Response(
            status_code=response.status_code,
            headers=response.headers,
            content=b"".join(chunks),
            request=response.request,
            history=response.history,
            extensions=response.extensions,
        )


# ── Selenium session (shared across Selenium-based crawlers) ──────────────────

class SeleniumResponse:
    """Minimal response-like object exposing .text / .content (drop-in for httpx.Response parsers)."""
    __slots__ = ("text", "content", "url", "status_code", "headers", "raise_for_status")

    def __init__(self, text: str = "", content: bytes = b"", url: str = "",
                 status_code: int = 200, headers: dict | None = None):
        self.text = text
        self.content = content
        self.url = url
        self.status_code = status_code
        self.headers: dict = headers or {}
        # Optional httpx-style ``raise_for_status`` callback — populated by the
        # ``_ClientAdapter`` shims in the migrated crawlers when needed.
        self.raise_for_status = None  # type: ignore[assignment]

    def __bool__(self) -> bool:
        return bool(self.text or self.content)


class SeleniumSession:
    """Reusable headless-Chrome session for legacy government sites.

    Provides ``get()`` (page fetch) and ``download()`` (binary fetch via the
    browser's ``fetch()`` API so cookies/TLS-trust match the rendered context).
    Designed as a drop-in for crawlers that currently use ``safe_get`` /
    ``download_response`` — both methods return a ``SeleniumResponse`` exposing
    ``.text`` and ``.content``.

    Driver is created lazily on first ``get``/``download`` call and torn down
    when the context manager exits.  Re-entrant within a single thread.
    """

    # Default page-load timeout kept BELOW the urllib3 read timeout (120s) so
    # chromedriver aborts the navigation cleanly and we get a WebDriverException
    # (which triggers driver.quit()) instead of a poisoned ReadTimeoutError.
    DEFAULT_PAGE_LOAD_TIMEOUT = 90.0
    DEFAULT_SCRIPT_TIMEOUT    = 90.0
    # urllib3 read timeout for the python -> chromedriver localhost socket.
    # Set to (page_load_timeout + buffer) so chromedriver always trips first.
    DEFAULT_CLIENT_TIMEOUT    = 180.0

    def __init__(
        self,
        *,
        ignore_certificate_errors: bool = True,
        page_load_timeout: float = DEFAULT_PAGE_LOAD_TIMEOUT,
        script_timeout: float = DEFAULT_SCRIPT_TIMEOUT,
        client_timeout: float = DEFAULT_CLIENT_TIMEOUT,
        window_size: str = "1920,1080",
        eager: bool = True,
        block_images: bool = True,
        extra_chrome_args: tuple[str, ...] = (),
    ):
        self.ignore_certificate_errors = ignore_certificate_errors
        self.page_load_timeout = page_load_timeout
        self.script_timeout = script_timeout
        self.client_timeout = max(client_timeout, page_load_timeout + 30.0)
        self.window_size = window_size
        self.eager = eager
        self.block_images = block_images
        self.extra_chrome_args = tuple(extra_chrome_args)
        self._driver = None
        self._lock = threading.Lock()

    # ── lifecycle ────────────────────────────────────────────────────────────
    def __enter__(self) -> "SeleniumSession":
        return self

    def __exit__(self, *args):
        self.quit()

    def _create_driver(self):
        # Imports kept lazy so projects that don't use Selenium needn't install it.
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options as _ChromeOptions
        from selenium.webdriver.chrome.service import Service as _ChromeService
        from webdriver_manager.chrome import ChromeDriverManager

        opts = _ChromeOptions()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-sync")
        opts.add_argument("--disable-translate")
        opts.add_argument("--disable-default-apps")
        opts.add_argument("--disable-background-networking")
        opts.add_argument("--disable-renderer-backgrounding")
        opts.add_argument("--disable-backgrounding-occluded-windows")
        opts.add_argument("--disable-features=Translate,OptimizationHints,MediaRouter")
        opts.add_argument("--metrics-recording-only")
        opts.add_argument("--mute-audio")
        opts.add_argument("--no-first-run")
        opts.add_argument("--no-default-browser-check")
        opts.add_argument(f"--window-size={self.window_size}")
        for arg in self.extra_chrome_args:
            opts.add_argument(arg)
        if self.ignore_certificate_errors:
            opts.add_argument("--ignore-certificate-errors")
            opts.add_argument("--ignore-ssl-errors=yes")
            opts.add_argument("--allow-insecure-localhost")
            opts.set_capability("acceptInsecureCerts", True)
        # Eager: return as soon as the DOM is parsed (don't wait for images,
        # iframes, ads, analytics beacons). Cuts page-load time 30-70% on
        # heavy government pages without losing any data BeautifulSoup needs.
        if self.eager:
            opts.set_capability("pageLoadStrategy", "eager")
        prefs: dict = {
            "profile.managed_default_content_settings.images":       2 if self.block_images else 0,
            "profile.managed_default_content_settings.notifications": 2,
            "profile.managed_default_content_settings.media_stream":  2,
            "profile.default_content_setting_values.geolocation":     2,
            "profile.default_content_setting_values.cookies":         1,
            "credentials_enable_service":                             False,
            "profile.password_manager_enabled":                       False,
        }
        opts.add_experimental_option("prefs", prefs)
        opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        service = _ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)
        # Sync urllib3's read timeout for the localhost->chromedriver socket so
        # chromedriver's page_load_timeout trips first (clean abort + quit())
        # instead of a poisoned ReadTimeoutError leaving a half-loaded session.
        # The selenium 4.x class-level RemoteConnection.set_timeout has a
        # version-dependent bug; instead reach into the instance's client_config.
        try:
            cmd = getattr(driver, "command_executor", None)
            cfg = getattr(cmd, "_client_config", None) or getattr(cmd, "client_config", None)
            if cfg is not None and hasattr(cfg, "timeout"):
                cfg.timeout = self.client_timeout
        except Exception:
            pass
        driver.set_page_load_timeout(self.page_load_timeout)
        driver.set_script_timeout(self.script_timeout)
        # Inject the XHR / fetch counter via CDP so it's installed *before*
        # any subsequent navigation runs the page's own scripts.  This is
        # what makes ``wait_for_load_state("networkidle")`` work reliably on
        # Angular / React SPAs that fire their first AJAX in `ngOnInit`.
        try:
            driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {"source": self._XHR_TRACKER_JS_INIT},
            )
        except Exception:
            pass
        return driver

    # Same body as SeleniumPageAdapter._XHR_TRACKER_JS but kept here so the
    # session can pre-install it without a circular reference.
    _XHR_TRACKER_JS_INIT = """
        if (!window.__augNetInstalled) {
            window.__augNetInstalled = true;
            window.__augNetPending = 0;
            const _OXHR = window.XMLHttpRequest;
            window.XMLHttpRequest = function () {
                const xhr = new _OXHR();
                xhr.addEventListener('loadstart', () => { window.__augNetPending++; });
                const _done = () => { window.__augNetPending = Math.max(0, window.__augNetPending - 1); };
                xhr.addEventListener('loadend', _done);
                xhr.addEventListener('error', _done);
                xhr.addEventListener('abort', _done);
                return xhr;
            };
            const _OFETCH = window.fetch;
            if (_OFETCH) {
                window.fetch = function () {
                    window.__augNetPending++;
                    return _OFETCH.apply(this, arguments).finally(() => {
                        window.__augNetPending = Math.max(0, window.__augNetPending - 1);
                    });
                };
            }
        }
    """

    def driver(self):
        with self._lock:
            if self._driver is None:
                self._driver = self._create_driver()
            return self._driver

    def quit(self) -> None:
        with self._lock:
            if self._driver is not None:
                try:
                    self._driver.quit()
                except Exception:
                    pass
                self._driver = None

    # ── HTTP-like API ────────────────────────────────────────────────────────
    def get(
        self,
        url: str,
        *,
        retries: int = 3,
        delay: float = 3.0,
        page_load_timeout: float | None = None,
        logger: CrawlerLogger | None = None,
        **_ignored,
    ) -> SeleniumResponse | None:
        """Fetch a page via the browser; returns ``SeleniumResponse`` (``.text``) or None.

        Extra keyword arguments (``params``, ``headers``, ``verify``, ``timeout``,
        ``client``) are accepted-and-ignored so this is a drop-in replacement
        for ``safe_get`` call sites.  Use ``urllib.parse.urlencode`` to fold
        ``params`` into the URL before calling if needed.
        """
        from selenium.common.exceptions import WebDriverException
        last_exc: Exception | None = None
        timeout = page_load_timeout if page_load_timeout is not None else self.page_load_timeout
        for attempt in range(1, retries + 1):
            try:
                driver = self.driver()
                driver.set_page_load_timeout(timeout)
                driver.get(url)
                return SeleniumResponse(text=driver.page_source or "", url=url)
            except WebDriverException as exc:
                last_exc = exc
                if logger:
                    logger.warning(
                        f"Selenium GET attempt {attempt}/{retries} failed: {exc.__class__.__name__}",
                        url=url,
                    )
                self.quit()  # reset on hard errors so retry starts clean
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if logger:
                    logger.warning(
                        f"Selenium GET attempt {attempt}/{retries} failed: {exc.__class__.__name__}",
                        url=url,
                    )
                # Recycle on any error (incl. urllib3 ReadTimeoutError) so the
                # next retry starts with a fresh driver instead of waiting on a
                # half-loaded chromedriver session.
                self.quit()
            if attempt < retries:
                time.sleep(delay * attempt)
        if logger and last_exc is not None:
            logger.error(f"Selenium GET failed after {retries} attempt(s): {last_exc}", url=url)
        return None

    # ── In-browser fetch helpers (POST / JSON) ───────────────────────────────
    _FETCH_JS = (
        "const url = arguments[0];"
        "const init = arguments[1] || {};"
        "const done = arguments[arguments.length - 1];"
        "init.credentials = init.credentials || 'include';"
        "fetch(url, init)"
        " .then(r => r.text().then(t => done({ok: r.ok, status: r.status, text: t})))"
        " .catch(e => done({ok: false, status: 0, error: String(e)}));"
    )

    def fetch(
        self,
        url: str,
        *,
        method: str = "GET",
        headers: dict | None = None,
        data: str | bytes | None = None,
        json_body: dict | list | None = None,
        origin_url: str | None = None,
        retries: int = 3,
        delay: float = 3.0,
        script_timeout: float | None = None,
        logger: CrawlerLogger | None = None,
    ) -> SeleniumResponse | None:
        """Issue an arbitrary HTTP request via the browser's ``fetch()`` API.

        Uses the same cookies + TLS-trust as the rendered context so legacy
        government SSL chains and CSRF-cookie-gated endpoints work without
        a separate httpx client.  Returns a ``SeleniumResponse`` whose
        ``.text`` holds the response body (always decoded text — use
        :meth:`download` for binary bodies).
        """
        import json as _json
        from urllib.parse import urlsplit, urlencode
        from selenium.common.exceptions import WebDriverException

        timeout = script_timeout if script_timeout is not None else self.script_timeout
        init: dict = {"method": method.upper()}
        hdrs = dict(headers or {})
        if json_body is not None:
            init["body"] = _json.dumps(json_body)
            hdrs.setdefault("Content-Type", "application/json")
        elif data is not None:
            if isinstance(data, str):
                init["body"] = data
            elif isinstance(data, (bytes, bytearray)):
                init["body"] = bytes(data).decode("utf-8", "replace")
            elif isinstance(data, dict) or (
                isinstance(data, (list, tuple))
                and all(isinstance(x, (list, tuple)) and len(x) == 2 for x in data)
            ):
                # Form-encode dict / sequence-of-pairs payloads.
                init["body"] = urlencode(data, doseq=True)
                hdrs.setdefault("Content-Type", "application/x-www-form-urlencoded")
            else:
                init["body"] = str(data)
        if hdrs:
            init["headers"] = hdrs

        target_origin = origin_url
        if not target_origin:
            parts = urlsplit(url)
            target_origin = f"{parts.scheme}://{parts.netloc}/" if parts.scheme and parts.netloc else None

        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                driver = self.driver()
                driver.set_script_timeout(timeout)
                try:
                    current = driver.current_url or ""
                except Exception:
                    current = ""
                if target_origin and not current.startswith(target_origin.rstrip("/")):
                    try:
                        driver.get(target_origin)
                    except WebDriverException:
                        pass
                result = driver.execute_async_script(self._FETCH_JS, url, init)
                if result and result.get("ok"):
                    return SeleniumResponse(
                        text=result.get("text", ""),
                        url=url,
                        status_code=int(result.get("status") or 200),
                    )
                if logger:
                    logger.warning(
                        f"Selenium fetch attempt {attempt}/{retries} failed: status={(result or {}).get('status')}",
                        url=url,
                    )
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if logger:
                    logger.warning(
                        f"Selenium fetch attempt {attempt}/{retries} errored: {exc.__class__.__name__}",
                        url=url,
                    )
                self.quit()
            if attempt < retries:
                time.sleep(delay * attempt)
        if logger and last_exc is not None:
            logger.error(f"Selenium fetch failed after {retries} attempt(s): {last_exc}", url=url)
        return None

    def post(self, url: str, **kw) -> SeleniumResponse | None:
        """Drop-in shim for ``safe_post`` call sites — issues an in-browser POST."""
        return self.fetch(url, method="POST", **kw)

    def get_json(self, url: str, **kw):
        """Convenience: fetch ``url`` (defaulting to GET) and return the parsed JSON body."""
        import json as _json
        resp = self.fetch(url, **kw)
        if not resp or not resp.text:
            return None
        try:
            return _json.loads(resp.text)
        except (ValueError, _json.JSONDecodeError):
            return None

    _DOWNLOAD_JS = (
        "const url = arguments[0];"
        "const init = arguments[1] || {};"
        "const done = arguments[arguments.length - 1];"
        "init.credentials = init.credentials || 'include';"
        "fetch(url, init)"
        " .then(r => r.arrayBuffer().then(buf => ({ok: r.ok, status: r.status, ct: r.headers.get('content-type') || '', buf})))"
        " .then(({ok, status, ct, buf}) => {"
        "   const bytes = new Uint8Array(buf);"
        "   let bin = '';"
        "   const chunk = 0x8000;"
        "   for (let i = 0; i < bytes.length; i += chunk) {"
        "     bin += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));"
        "   }"
        "   done({ok: ok, status: status, ct: ct, b64: btoa(bin)});"
        " })"
        " .catch(e => done({ok: false, status: 0, error: String(e)}));"
    )

    def download(
        self,
        url: str,
        *,
        method: str = "GET",
        data: str | bytes | None = None,
        headers: dict | None = None,
        retries: int = 3,
        delay: float = 3.0,
        script_timeout: float | None = None,
        origin_url: str | None = None,
        logger: CrawlerLogger | None = None,
        **_ignored,
    ) -> SeleniumResponse | None:
        """Download a binary resource via the browser's ``fetch()`` API.

        Uses session cookies + the browser's TLS trust store so legacy SSL
        chains that ``httpx`` rejects still work.  Drop-in for
        ``download_response`` call sites — extra kwargs (``timeout``,
        ``verify``, ``client``, ``method``, ``params``, ``data``,
        ``json_data``, ``total_timeout``, ``max_bytes``, ``follow_redirects``)
        are accepted-and-ignored.

        Pass ``origin_url`` to prime the document.origin / cookies for the
        target host before issuing the fetch (e.g. when the crawler hasn't
        visited any page on that host yet).  Defaults to the URL's origin.
        """
        import base64 as _b64
        from urllib.parse import urlsplit
        from selenium.common.exceptions import WebDriverException

        timeout = script_timeout if script_timeout is not None else self.script_timeout
        target_origin = origin_url
        if not target_origin:
            parts = urlsplit(url)
            target_origin = f"{parts.scheme}://{parts.netloc}/" if parts.scheme and parts.netloc else None

        init: dict = {"method": method.upper()}
        hdrs = dict(headers or {})
        if data is not None:
            from urllib.parse import urlencode as _urlencode
            if isinstance(data, str):
                init["body"] = data
            elif isinstance(data, (bytes, bytearray)):
                init["body"] = bytes(data).decode("utf-8", "replace")
            elif isinstance(data, dict) or (
                isinstance(data, (list, tuple))
                and all(isinstance(x, (list, tuple)) and len(x) == 2 for x in data)
            ):
                init["body"] = _urlencode(data, doseq=True)
                hdrs.setdefault("Content-Type", "application/x-www-form-urlencoded")
            else:
                init["body"] = str(data)
        if hdrs:
            init["headers"] = hdrs

        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                driver = self.driver()
                driver.set_script_timeout(timeout)
                # Ensure cookies / origin are established for the target host.
                try:
                    current = driver.current_url or ""
                except Exception:
                    current = ""
                if target_origin and not current.startswith(target_origin.rstrip("/")):
                    try:
                        driver.get(target_origin)
                    except WebDriverException:
                        pass  # priming failure is non-fatal; the fetch may still work
                result = driver.execute_async_script(self._DOWNLOAD_JS, url, init)
                if result and result.get("ok"):
                    ct = (result.get("ct") or "").lower()
                    resp = SeleniumResponse(content=_b64.b64decode(result["b64"]), url=url,
                                            status_code=int(result.get("status") or 200))
                    # Expose response headers in a minimal form for callers that
                    # inspect content-type to distinguish binary vs HTML responses.
                    resp.headers = {"content-type": ct}  # type: ignore[attr-defined]
                    return resp
                if logger:
                    logger.warning(
                        f"Selenium download attempt {attempt}/{retries} failed: status={(result or {}).get('status')}",
                        url=url,
                    )
            except WebDriverException as exc:
                last_exc = exc
                if logger:
                    logger.warning(
                        f"Selenium download attempt {attempt}/{retries} errored: {exc.__class__.__name__}",
                        url=url,
                    )
                self.quit()
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if logger:
                    logger.warning(
                        f"Selenium download attempt {attempt}/{retries} errored: {exc.__class__.__name__}",
                        url=url,
                    )
            if attempt < retries:
                time.sleep(delay * attempt)
        if logger and last_exc is not None:
            logger.error(f"Selenium download failed after {retries} attempt(s): {last_exc}", url=url)
        return None


# ── Selenium-compatible adapter over a SeleniumSession ──────────────────────
# Supports the subset of Selenium Page/Locator/Keyboard APIs used by the
# state RERA crawlers so they can be migrated from Selenium to Selenium with
# minimal call-site changes.

class SeleniumTimeout(Exception):
    """Raised when a SeleniumPageAdapter wait operation exceeds its timeout."""


def _ms_to_s(ms: float | int | None) -> float:
    return float(ms) / 1000.0 if ms is not None else 30.0


_HAS_TEXT_RE = re.compile(r":has-text\(['\"]?([^'\")]+)['\"]?\)")


def _split_has_text(selector: str, has_text: str | None) -> tuple[str, str | None]:
    """Strip Selenium-style ``:has-text("…")`` from *selector* and merge it
    with any explicit ``has_text`` kwarg.

    Selenium supports ``:has-text(...)`` as a pseudo-class on CSS selectors;
    Selenium doesn't.  We extract the literal text and surface it via the
    ``has_text`` filter applied at iteration time.
    """
    m = _HAS_TEXT_RE.search(selector)
    if not m:
        return selector, has_text
    text = m.group(1)
    cleaned = _HAS_TEXT_RE.sub("", selector).strip().rstrip(",")
    # Combine multiple :has-text() values into a single concatenated string
    # (rare in practice — the migrated crawlers only use one at a time).
    combined = text if has_text is None else f"{has_text} {text}"
    return cleaned or "*", combined


_TEXT_ENGINE_RE = re.compile(r"^\s*text\s*=\s*(.+?)\s*$", re.DOTALL)


def _parse_text_selector(selector: str) -> tuple[str, bool] | None:
    """Parse a Playwright ``text=`` engine selector.

    Returns ``(text, exact)`` when *selector* uses the ``text=`` engine, else
    ``None``. A quoted value (``text="Foo"`` / ``text='Foo'``) requests an exact,
    whitespace-trimmed match; an unquoted value matches a case-insensitive
    substring — mirroring Playwright's behaviour closely enough for the tab/link
    clicks the migrated crawlers rely on.
    """
    m = _TEXT_ENGINE_RE.match(selector)
    if not m:
        return None
    val = m.group(1).strip()
    if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
        return val[1:-1], True
    return val, False


def _xpath_literal(value: str) -> str:
    """Quote *value* as an XPath 1.0 string literal (handles embedded quotes)."""
    if '"' not in value:
        return f'"{value}"'
    if "'" not in value:
        return f"'{value}'"
    parts = value.split('"')
    return "concat(" + ', \'"\', '.join(f'"{p}"' for p in parts) + ")"


def _glob_to_regex(pattern: str) -> "re.Pattern":
    """Convert a Playwright URL glob to a regex (``**`` spans ``/``, ``*`` does not)."""
    out = ["^"]
    i, n = 0, len(pattern)
    while i < n:
        c = pattern[i]
        if c == "*":
            if i + 1 < n and pattern[i + 1] == "*":
                out.append(".*")  # ** — match across path separators
                i += 2
            else:
                out.append("[^/]*")  # * — match within a path segment
                i += 1
        elif c == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(c))
            i += 1
    out.append("$")
    return re.compile("".join(out))


def _url_matches(pattern: Any, url: str) -> bool:
    """Test *url* against a Playwright-style URL matcher (callable / regex / glob / exact)."""
    if callable(pattern):
        try:
            return bool(pattern(url))
        except Exception:
            return False
    if hasattr(pattern, "search"):  # compiled regex
        return bool(pattern.search(url))
    text = str(pattern)
    if any(ch in text for ch in "*?"):
        return bool(_glob_to_regex(text).match(url))
    return url == text


def _find_by_text(root: Any, text: str, exact: bool) -> list:
    """Find Selenium elements whose visible text matches *text*.

    Candidates are ordered innermost-first (shortest normalized text) and, within
    an equal-length tie, natively-clickable elements (``a`` / ``button`` /
    ``role=tab``) sort ahead of their wrappers. This matters for SPA tabs whose
    label lives in both an ``<li class="nav-item">`` and the inner
    ``<a class="nav-link">`` — only a click dispatched on the anchor fires the
    framework's handler, so the anchor must win.
    """
    from selenium.webdriver.common.by import By

    if exact:
        xpath = f".//*[normalize-space(.)={_xpath_literal(text)}]"
    else:
        lit = _xpath_literal(text.lower())
        xpath = (
            ".//*[contains(translate(normalize-space(.), "
            "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), "
            f"{lit})]"
        )
    elems = root.find_elements(By.XPATH, xpath)
    elems.sort(key=lambda e: (len((e.text or "").strip()), _element_click_rank(e)))
    return elems


def _element_click_rank(element: Any) -> int:
    """Rank an element for click preference (lower sorts first).

    Natively-clickable elements (anchors, buttons, anything with a ``tab`` /
    ``button`` / ``link`` role or an ``onclick`` handler) rank ahead of inert
    containers so a ``text=`` selector resolves to the element that actually
    handles the click.
    """
    try:
        tag = (element.tag_name or "").lower()
    except Exception:
        return 3
    if tag in ("a", "button"):
        return 0
    try:
        if (element.get_attribute("role") or "").lower() in ("tab", "button", "link"):
            return 0
        if element.get_attribute("onclick"):
            return 0
    except Exception:
        pass
    return 1


class _SeleniumLocator:
    """Thin Selenium-Locator-style wrapper around CSS selectors.

    Supports the subset used by the migrated crawlers: ``count``, ``first``,
    ``click``, ``text_content``, ``inner_text``, ``get_attribute``, ``all``,
    ``locator`` (chained), ``wait_for``, ``fill``, ``is_visible``.
    """

    def __init__(self, driver, selector: str, has_text: str | None = None, parent=None):
        # Selectors that embed Selenium's ``:has-text(...)`` pseudo-class
        # are split into a pure CSS portion and a separate text filter so the
        # underlying Selenium ``find_elements`` call can succeed.
        selector, has_text = _split_has_text(selector, has_text)
        self._driver = driver
        self._selector = selector
        self._has_text = has_text
        self._parent = parent  # WebElement or None (root = document)

    # ── chaining ─────────────────────────────────────────────────────────────
    def locator(self, selector: str, has_text: str | None = None) -> "_SeleniumLocator":
        # Convert the current locator's first element into a parent root.
        elem = self._first_element(timeout_ms=1000)
        return _SeleniumLocator(self._driver, selector, has_text=has_text, parent=elem)

    @property
    def first(self) -> "_SeleniumLocator":
        return self  # `.first` is implicit in our access pattern; .click() already takes first

    # ── enumeration ──────────────────────────────────────────────────────────
    def _all_elements(self):
        from selenium.webdriver.common.by import By
        root = self._parent if self._parent is not None else self._driver
        elements = root.find_elements(By.CSS_SELECTOR, self._selector)
        if self._has_text is not None:
            t = self._has_text
            elements = [e for e in elements if t in (e.text or "")]
        return elements

    def count(self) -> int:
        return len(self._all_elements())

    def all(self) -> list["_SeleniumLocator"]:
        elems = self._all_elements()
        out: list[_SeleniumLocator] = []
        for el in elems:
            loc = _SeleniumLocator(self._driver, "*", parent=el)
            loc._cached_element = el  # type: ignore[attr-defined]
            out.append(loc)
        return out

    # ── single-element access ────────────────────────────────────────────────
    def _first_element(self, timeout_ms: int = 5000):
        cached = getattr(self, "_cached_element", None)
        if cached is not None:
            return cached
        from selenium.webdriver.support.ui import WebDriverWait
        deadline = time.monotonic() + _ms_to_s(timeout_ms)
        last_exc: Exception | None = None
        while time.monotonic() < deadline:
            try:
                elems = self._all_elements()
                if elems:
                    return elems[0]
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
            time.sleep(0.2)
        raise SeleniumTimeout(
            f"locator({self._selector!r}, has_text={self._has_text!r}) "
            f"not found within {timeout_ms}ms: {last_exc}"
        )

    def click(self, timeout: int = 5000) -> None:
        el = self._first_element(timeout_ms=timeout)
        el.click()

    def fill(self, value: str, *, timeout: int = 5000) -> None:
        el = self._first_element(timeout_ms=timeout)
        el.clear()
        el.send_keys(value)

    def is_visible(self) -> bool:
        try:
            el = self._first_element(timeout_ms=500)
            return bool(el.is_displayed())
        except Exception:
            return False

    def text_content(self, timeout: int = 5000) -> str:
        el = self._first_element(timeout_ms=timeout)
        return el.text or ""

    inner_text = text_content

    def get_attribute(self, name: str, timeout: int = 5000) -> str | None:
        el = self._first_element(timeout_ms=timeout)
        return el.get_attribute(name)

    def wait_for(self, state: str = "visible", timeout: int = 5000) -> None:
        deadline = time.monotonic() + _ms_to_s(timeout)
        while time.monotonic() < deadline:
            elems = self._all_elements()
            if state in ("visible", "attached"):
                if elems and elems[0].is_displayed():
                    return
            elif state in ("hidden", "detached"):
                if not elems or not elems[0].is_displayed():
                    return
            time.sleep(0.2)
        raise SeleniumTimeout(
            f"locator({self._selector!r}).wait_for(state={state!r}) "
            f"exceeded {timeout}ms"
        )


class _SeleniumKeyboard:
    """Tiny adapter exposing ``press(key)`` over the Selenium driver."""

    _MAP = {
        "Escape": "\ue00c", "Enter": "\ue007", "Tab": "\ue004",
        "ArrowDown": "\ue015", "ArrowUp": "\ue013",
        "ArrowLeft": "\ue012", "ArrowRight": "\ue014",
        "Backspace": "\ue003", "Delete": "\ue017",
    }

    def __init__(self, driver):
        self._driver = driver

    def press(self, key: str) -> None:
        from selenium.webdriver.common.action_chains import ActionChains
        seq = self._MAP.get(key, key)
        ActionChains(self._driver).send_keys(seq).perform()


class _SeleniumElement:
    """Selenium ElementHandle-compatible wrapper around a Selenium WebElement.

    Exposes the subset of element-level methods used by the migrated crawlers:
    ``screenshot``, ``text_content`` / ``inner_text``, ``inner_html``,
    ``get_attribute``, ``click``, ``fill``, ``query_selector``,
    ``query_selector_all``.  Attribute access falls back to the underlying
    Selenium WebElement for anything not explicitly overridden.
    """

    def __init__(self, element):
        self._el = element

    # ── content ──────────────────────────────────────────────────────────────
    def text_content(self) -> str:
        return self._el.text or ""

    inner_text = text_content

    def inner_html(self) -> str:
        return self._el.get_attribute("innerHTML") or ""

    def get_attribute(self, name: str) -> str | None:
        return self._el.get_attribute(name)

    # ── interactions ─────────────────────────────────────────────────────────
    def click(self, *, timeout: int | None = None) -> None:
        self._el.click()

    def fill(self, value: str) -> None:
        self._el.clear()
        self._el.send_keys(value)

    # ── screenshots ──────────────────────────────────────────────────────────
    def screenshot(self, *, path: str | None = None, **_kw) -> bytes:
        data = self._el.screenshot_as_png
        if path:
            with open(path, "wb") as fh:
                fh.write(data)
        return data

    # ── descendants ──────────────────────────────────────────────────────────
    def query_selector(self, selector: str):
        from selenium.webdriver.common.by import By
        elems = self._el.find_elements(By.CSS_SELECTOR, selector)
        return _SeleniumElement(elems[0]) if elems else None

    def query_selector_all(self, selector: str):
        from selenium.webdriver.common.by import By
        elems = self._el.find_elements(By.CSS_SELECTOR, selector)
        return [_SeleniumElement(e) for e in elems]

    # ── delegate everything else to the underlying WebElement ────────────────
    def __getattr__(self, name: str):
        return getattr(self._el, name)


class SeleniumPageAdapter:
    """Selenium Page-compatible adapter wrapping a SeleniumSession driver.

    Supports the subset of Selenium APIs used by the migrated state RERA
    crawlers: navigation, content, evaluate, query_selector(_all), locator,
    wait_for_selector / function / timeout / load_state, fill, click, reload,
    title, request.post (in-browser fetch), context.new_page (returns self).
    """

    def __init__(self, session: "SeleniumSession"):
        self._sess = session
        self._driver = session.driver()
        self.keyboard = _SeleniumKeyboard(self._driver)

    # ── navigation ───────────────────────────────────────────────────────────
    def goto(self, url: str, *, timeout: int | None = None,
             wait_until: str | None = None, **_kw) -> None:
        prev = self._sess.page_load_timeout
        if timeout is not None:
            self._driver.set_page_load_timeout(_ms_to_s(timeout))
        try:
            self._driver.get(url)
        finally:
            self._driver.set_page_load_timeout(prev)

    def reload(self, timeout: int | None = None, **_kw) -> None:
        self._driver.refresh()

    @property
    def url(self) -> str:
        return self._driver.current_url or ""

    @property
    def title(self) -> str:
        return self._driver.title or ""

    def content(self) -> str:
        return self._driver.page_source or ""

    def close(self) -> None:
        pass  # session lifecycle owned by the caller

    # ── scripts ──────────────────────────────────────────────────────────────
    def evaluate(self, script: str, *args):
        # Selenium wraps top-level expressions like "() => ..."; convert to
        # Selenium's execute_script (which expects a body, not an arrow IIFE).
        s = script.strip()
        if s.startswith("(") and "=>" in s:
            # () => EXPR  or  (a, b) => { ... } — wrap as (FN)(...arguments)
            wrapped = f"return ({script}).apply(null, arguments);"
        elif s.startswith("function"):
            wrapped = f"return ({script}).apply(null, arguments);"
        else:
            wrapped = script if "return" in s else f"return ({script});"
        return self._driver.execute_script(wrapped, *args)

    def evaluate_handle(self, script: str, *args):
        return self.evaluate(script, *args)

    # ── selectors ────────────────────────────────────────────────────────────
    def _find_elements(self, selector: str) -> list:
        """Resolve a Playwright-style *selector* to a list of WebElements.

        Supports the ``text=`` engine and an ``xpath=`` prefix in addition to
        plain CSS (with Selenium's ``:has-text(...)`` pseudo-class). Centralising
        this keeps ``click`` / ``wait_for_selector`` / ``query_selector*`` in
        sync so a ``text=`` tab click no longer reaches Selenium as invalid CSS.
        """
        from selenium.webdriver.common.by import By
        text_sel = _parse_text_selector(selector)
        if text_sel is not None:
            return _find_by_text(self._driver, *text_sel)
        if selector.startswith("xpath="):
            return self._driver.find_elements(By.XPATH, selector[len("xpath="):])
        sel, has_text = _split_has_text(selector, None)
        elems = self._driver.find_elements(By.CSS_SELECTOR, sel)
        if has_text:
            elems = [e for e in elems if has_text in (e.text or "")]
        return elems

    def query_selector(self, selector: str):
        elems = self._find_elements(selector)
        return _SeleniumElement(elems[0]) if elems else None

    def query_selector_all(self, selector: str):
        return [_SeleniumElement(e) for e in self._find_elements(selector)]

    def locator(self, selector: str, has_text: str | None = None) -> _SeleniumLocator:
        return _SeleniumLocator(self._driver, selector, has_text=has_text)

    # ── waits ────────────────────────────────────────────────────────────────
    def wait_for_selector(self, selector: str, *, timeout: int = 30_000,
                          state: str = "visible") -> None:
        deadline = time.monotonic() + _ms_to_s(timeout)
        while time.monotonic() < deadline:
            elems = self._find_elements(selector)
            if state in ("attached",):
                if elems:
                    return
            elif state in ("visible",):
                if elems and elems[0].is_displayed():
                    return
            elif state in ("hidden",):
                if not elems or not any(e.is_displayed() for e in elems):
                    return
            elif state in ("detached",):
                if not elems:
                    return
            time.sleep(0.2)
        raise SeleniumTimeout(
            f"wait_for_selector({selector!r}, state={state!r}) exceeded {timeout}ms"
        )

    def wait_for_function(self, script: str, *, arg: Any = None,
                          timeout: int = 30_000, **_kw) -> None:
        # Forward *arg* into the in-page function (Playwright passes it as the
        # first parameter). Dropping it leaves arrow params undefined, which
        # silently breaks predicates like ``(selector) => …`` / ``(re) => …``.
        deadline = time.monotonic() + _ms_to_s(timeout)
        while time.monotonic() < deadline:
            try:
                result = (self.evaluate(script) if arg is None
                          else self.evaluate(script, arg))
                if result:
                    return
            except Exception:
                pass
            time.sleep(0.2)
        raise SeleniumTimeout(
            f"wait_for_function exceeded {timeout}ms"
        )

    def wait_for_load_state(self, state: str = "load", *, timeout: int = 30_000) -> None:
        deadline = time.monotonic() + _ms_to_s(timeout)
        if state == "domcontentloaded":
            target = ("interactive", "complete")
            quiet_ms = 0
        elif state == "networkidle":
            target = ("complete",)
            quiet_ms = 1500  # require a 1.5 s in-flight-fetch quiet window
        else:  # "load"
            target = ("complete",)
            quiet_ms = 0
        # Install a one-shot XHR / fetch counter so we can approximate
        # Selenium's "networkidle" semantics.
        if state == "networkidle":
            try:
                self._driver.execute_script(self._XHR_TRACKER_JS)
            except Exception:
                pass
        last_active = time.monotonic()
        while time.monotonic() < deadline:
            try:
                rs = self._driver.execute_script("return document.readyState")
                if rs in target:
                    if state != "networkidle":
                        return
                    pending = self._driver.execute_script(
                        "return window.__augNetPending || 0"
                    )
                    if pending == 0:
                        if (time.monotonic() - last_active) * 1000 >= quiet_ms:
                            return
                    else:
                        last_active = time.monotonic()
            except Exception:
                pass
            time.sleep(0.2)

    def wait_for_url(self, url, *, timeout: int = 30_000,
                     wait_until: str | None = None, **_kw) -> None:
        # Poll ``current_url`` until it matches *url* (Playwright accepts a glob
        # string, a compiled regex, or a predicate). Globs follow Playwright's
        # rules: ``**`` spans ``/`` while ``*`` does not.
        deadline = time.monotonic() + _ms_to_s(timeout)
        while time.monotonic() < deadline:
            try:
                current = self._driver.current_url or ""
                if _url_matches(url, current):
                    return
            except Exception:
                pass
            time.sleep(0.2)
        raise SeleniumTimeout(
            f"wait_for_url({url!r}) exceeded {timeout}ms"
        )

    _XHR_TRACKER_JS = """
        if (!window.__augNetInstalled) {
            window.__augNetInstalled = true;
            window.__augNetPending = 0;
            const _OXHR = window.XMLHttpRequest;
            window.XMLHttpRequest = function () {
                const xhr = new _OXHR();
                xhr.addEventListener('loadstart', () => { window.__augNetPending++; });
                const _done = () => { window.__augNetPending = Math.max(0, window.__augNetPending - 1); };
                xhr.addEventListener('loadend', _done);
                xhr.addEventListener('error', _done);
                xhr.addEventListener('abort', _done);
                return xhr;
            };
            const _OFETCH = window.fetch;
            if (_OFETCH) {
                window.fetch = function () {
                    window.__augNetPending++;
                    return _OFETCH.apply(this, arguments).finally(() => {
                        window.__augNetPending = Math.max(0, window.__augNetPending - 1);
                    });
                };
            }
        }
    """

    def wait_for_timeout(self, timeout: int) -> None:
        time.sleep(_ms_to_s(timeout))

    # ── interactions ─────────────────────────────────────────────────────────
    def fill(self, selector: str, value: str, *, timeout: int = 30_000) -> None:
        from selenium.webdriver.common.by import By
        deadline = time.monotonic() + _ms_to_s(timeout)
        while time.monotonic() < deadline:
            elems = self._driver.find_elements(By.CSS_SELECTOR, selector)
            if elems:
                elems[0].clear()
                elems[0].send_keys(value)
                return
            time.sleep(0.2)
        raise SeleniumTimeout(f"fill({selector!r}) — element not found")

    def click(self, selector: str, *, timeout: int = 30_000) -> None:
        deadline = time.monotonic() + _ms_to_s(timeout)
        last_exc: Exception | None = None
        while time.monotonic() < deadline:
            for el in self._find_elements(selector):
                if not el.is_displayed():
                    continue
                # Mirror Playwright's actionability: scroll the target into the
                # viewport centre first. SPA tabs frequently render under a
                # sticky header at negative Y, where a native click is rejected
                # with "element click intercepted".
                try:
                    self._driver.execute_script(
                        "arguments[0].scrollIntoView({block:'center',"
                        "inline:'center'});", el,
                    )
                except Exception:
                    pass
                try:
                    el.click()
                    return
                except Exception as exc:
                    last_exc = exc
                    # An overlay/header intercepted the hit-test — dispatch the
                    # click directly via JS, which bypasses the obscuring layer.
                    try:
                        self._driver.execute_script("arguments[0].click();", el)
                        return
                    except Exception as exc2:
                        last_exc = exc2
            time.sleep(0.2)
        raise SeleniumTimeout(
            f"click({selector!r}) — element not found: {last_exc}"
        )

    # ── selector-array evaluation (Selenium eval_on_selector_all) ──────────
    def eval_on_selector_all(self, selector: str, js: str, *args):
        """Run *js* across all elements matching *selector*.

        *js* is a Selenium-style expression of the form ``"els => …"`` or
        ``"(els, arg) => …"``; we convert it to a Selenium IIFE that receives
        the NodeList as ``arguments[0]`` and any caller-supplied args after.
        """
        wrapped = (
            "return ("
            + js
            + ").apply(null, ["
            + "Array.from(document.querySelectorAll(arguments[0]))"
            + (", arguments[1]" if args else "")
            + "]);"
        )
        return self._driver.execute_script(wrapped, selector, *args)

    # ── context / request shims ──────────────────────────────────────────────
    class _Request:
        def __init__(self, sess):
            self._s = sess

        def post(self, url: str, *, headers=None, data=None, form=None, **_kw):
            body = data if data is not None else form
            return self._s.fetch(url, method="POST", data=body, headers=headers)

        def get(self, url: str, *, headers=None, **_kw):
            return self._s.get(url)

    @property
    def request(self) -> "SeleniumPageAdapter._Request":
        return SeleniumPageAdapter._Request(self._sess)

    class _ExpectPopup:
        """Context manager matching Selenium's ``ctx.expect_page()`` /
        ``page.expect_popup()``.  Captures the URL of any new browser window
        opened during the ``with`` block and exposes a ``.value`` attribute
        whose ``.url`` and ``.close()`` mimic the Selenium Page surface.
        """

        class _PopupHandle:
            def __init__(self, url: str, driver, original_handle: str):
                self.url = url
                self._driver = driver
                self._orig = original_handle

            def wait_for_load_state(self, *args, **kwargs):
                pass

            def content(self) -> str:
                return self._driver.page_source or ""

            def close(self) -> None:
                # Close the popup window and return to the original.
                try:
                    self._driver.close()
                except Exception:
                    pass
                try:
                    self._driver.switch_to.window(self._orig)
                except Exception:
                    pass

        def __init__(self, driver, timeout_ms: int = 15_000):
            self._driver = driver
            self._timeout_ms = timeout_ms
            self._before: set[str] = set()
            self._orig_handle: str = ""
            self.value: "SeleniumPageAdapter._ExpectPopup._PopupHandle | None" = None

        def __enter__(self):
            self._before = set(self._driver.window_handles)
            self._orig_handle = self._driver.current_window_handle
            return self

        def __exit__(self, exc_type, exc, tb):
            if exc is not None:
                return False
            deadline = time.monotonic() + _ms_to_s(self._timeout_ms)
            new_handle: str | None = None
            while time.monotonic() < deadline:
                handles = set(self._driver.window_handles)
                new = handles - self._before
                if new:
                    new_handle = next(iter(new))
                    break
                time.sleep(0.1)
            if not new_handle:
                raise SeleniumTimeout(
                    f"expect_page: no new window appeared within {self._timeout_ms}ms"
                )
            self._driver.switch_to.window(new_handle)
            url = self._driver.current_url or ""
            self.value = SeleniumPageAdapter._ExpectPopup._PopupHandle(
                url, self._driver, self._orig_handle,
            )
            return False

    def expect_popup(self, *, timeout: int = 15_000):
        return SeleniumPageAdapter._ExpectPopup(self._driver, timeout)

    class _Context:
        def __init__(self, outer: "SeleniumPageAdapter"):
            self._outer = outer

        def new_page(self):
            return self._outer  # SeleniumSession has only one browsing context

        def expect_page(self, *, timeout: int = 15_000):
            return SeleniumPageAdapter._ExpectPopup(self._outer._driver, timeout)

        @property
        def browser(self):
            return self

    @property
    def context(self) -> "SeleniumPageAdapter._Context":
        return SeleniumPageAdapter._Context(self)


def page_adapter(session: "SeleniumSession") -> SeleniumPageAdapter:
    """Public factory for SeleniumPageAdapter — drops in for ``new_page()`` calls."""
    return SeleniumPageAdapter(session)
