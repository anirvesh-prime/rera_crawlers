from __future__ import annotations

import logging
import os
import random
import ssl
import time
from typing import Any

import httpx
import siphash24
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page

from core.config import settings
from core.logger import CrawlerLogger


_UINT64_MASK = (1 << 64) - 1


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
    time.sleep(random.uniform(min_s, max_s))


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
    for attempt in range(1, retries + 1):
        try:
            if client is not None:
                resp = client.get(url, headers=_headers, params=params)
            else:
                with httpx.Client(timeout=timeout, follow_redirects=True, verify=verify) as c:
                    resp = c.get(url, headers=_headers, params=params)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            if logger:
                logger.warning(f"GET attempt {attempt}/{retries} failed: {e}", url=url)
            if attempt < retries:
                time.sleep(delay * attempt)
    if logger and last_exc is not None:
        logger.error(f"GET failed after {retries} attempts: {last_exc}", url=url)
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
    for attempt in range(1, retries + 1):
        try:
            if client is not None:
                resp = client.post(url, data=data, json=json_data, headers=_headers)
            else:
                with httpx.Client(timeout=timeout, follow_redirects=True, verify=verify) as c:
                    resp = c.post(url, data=data, json=json_data, headers=_headers)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            if logger:
                logger.warning(f"POST attempt {attempt}/{retries} failed: {e}", url=url)
            if attempt < retries:
                time.sleep(delay * attempt)
    if logger and last_exc is not None:
        logger.error(f"POST failed after {retries} attempts: {last_exc}", url=url)
    return None


class PlaywrightSession:
    """Context manager that provides a reusable Playwright browser session."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._playwright = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None

    def __enter__(self) -> "PlaywrightSession":
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=self.headless)
        self._context = self._browser.new_context(user_agent=get_random_ua())
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
