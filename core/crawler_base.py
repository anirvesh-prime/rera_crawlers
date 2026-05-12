from __future__ import annotations

import logging
import os
import random
import ssl
import threading
import time
from typing import Any

import httpx
import siphash24
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page

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
    timeout: float = 30.0,
    verify: bool = True,
    client: httpx.Client | None = None,
) -> httpx.Response | None:
    """GET with retry/backoff.  Pass `client` to reuse an existing connection pool."""
    _headers = {"User-Agent": get_random_ua(), **(headers or {})}
    last_exc: Exception | None = None
    attempt = 0
    for attempt in range(1, retries + 1):
        try:
            if client is not None:
                resp = client.get(url, headers=_headers, params=params, timeout=timeout)
            else:
                pooled_client = _get_shared_http_client(verify=verify)
                resp = pooled_client.get(
                    url,
                    headers=_headers,
                    params=params,
                    timeout=timeout,
                    follow_redirects=True,
                )
            resp.raise_for_status()
            return resp
        except httpx.HTTPStatusError as e:
            last_exc = e
            if logger:
                logger.warning(f"GET attempt {attempt}/{retries} failed: {e}", url=url)
            # 4xx errors are definitive client errors — no point retrying
            if e.response.status_code < 500:
                break
            if attempt < retries:
                time.sleep(delay * attempt)
        except Exception as e:
            last_exc = e
            if logger:
                logger.warning(f"GET attempt {attempt}/{retries} failed: {e}", url=url)
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
    timeout: float = 30.0,
    verify: bool = True,
    client: httpx.Client | None = None,
) -> httpx.Response | None:
    """POST with retry/backoff.  Pass `client` to reuse an existing connection pool."""
    _headers = {"User-Agent": get_random_ua(), **(headers or {})}
    last_exc: Exception | None = None
    attempt = 0
    for attempt in range(1, retries + 1):
        try:
            if client is not None:
                resp = client.post(
                    url,
                    data=data,
                    json=json_data,
                    headers=_headers,
                    timeout=timeout,
                )
            else:
                pooled_client = _get_shared_http_client(verify=verify)
                resp = pooled_client.post(
                    url,
                    data=data,
                    json=json_data,
                    headers=_headers,
                    timeout=timeout,
                    follow_redirects=True,
                )
            resp.raise_for_status()
            return resp
        except httpx.HTTPStatusError as e:
            last_exc = e
            if logger:
                logger.warning(f"POST attempt {attempt}/{retries} failed: {e}", url=url)
            # 4xx errors are definitive client errors — no point retrying
            if e.response.status_code < 500:
                break
            if attempt < retries:
                time.sleep(delay * attempt)
        except Exception as e:
            last_exc = e
            if logger:
                logger.warning(f"POST attempt {attempt}/{retries} failed: {e}", url=url)
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
    """Download a response body with a hard wall-clock timeout and size cap."""
    _headers = {"User-Agent": get_random_ua(), **(headers or {})}
    last_exc: Exception | None = None
    method = method.upper()
    attempt = 0

    for attempt in range(1, retries + 1):
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
                    timeout=timeout,
                    total_timeout=total_timeout,
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
                    timeout=timeout,
                    total_timeout=total_timeout,
                    max_bytes=max_bytes,
                    follow_redirects=follow_redirects,
                )
            return response
        except httpx.HTTPStatusError as exc:
            last_exc = exc
            if logger:
                logger.warning(
                    f"{method} attempt {attempt}/{retries} failed: {exc}",
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
                    f"{method} attempt {attempt}/{retries} failed: {exc}",
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


class PlaywrightSession:
    """Context manager that provides a reusable Playwright browser session."""

    def __init__(self, headless: bool = True, ignore_https_errors: bool = False):
        self.headless = headless
        self.ignore_https_errors = ignore_https_errors
        self._playwright = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None

    def __enter__(self) -> "PlaywrightSession":
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=self.headless)
        self._context = self._browser.new_context(
            user_agent=get_random_ua(),
            ignore_https_errors=self.ignore_https_errors,
        )
        return self

    def __exit__(self, *args):
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()

    def new_page(self, ua: str | None = None) -> Page:
        if self._context is None:
            raise RuntimeError(
                "PlaywrightSession.new_page() called outside of a 'with' block "
                "(context is None — use 'with PlaywrightSession() as session:')"
            )
        page = self._context.new_page()
        if ua:
            page.set_extra_http_headers({"User-Agent": ua})
        return page

    def fetch_page(self, url: str, wait_selector: str | None = None, timeout: int = 30000) -> Page:
        page = self.new_page()
        page.goto(url, timeout=timeout)
        if wait_selector:
            page.wait_for_selector(wait_selector, timeout=timeout)
        else:
            page.wait_for_load_state("networkidle", timeout=timeout)
        return page
