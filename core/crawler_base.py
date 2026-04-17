from __future__ import annotations

import hashlib
import random
import ssl
import time
from typing import Any

import httpx
from playwright.sync_api import sync_playwright, Page, Browser

from core.config import settings
from core.logger import CrawlerLogger


def generate_project_key(state_code: str, registration_number: str) -> str:
    """Key is the registration number itself — human-readable and directly identifiable."""
    return registration_number.strip()


def random_delay(min_s: float = 1.0, max_s: float = 3.0) -> None:
    time.sleep(random.uniform(min_s, max_s))


def get_legacy_ssl_context() -> ssl.SSLContext:
    """
    Returns an SSL context that accepts old government sites running TLS 1.0/1.1
    with unsafe legacy renegotiation (e.g. WB HIRA, Telangana).
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.set_ciphers("DEFAULT:@SECLEVEL=0")
    ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0)
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
) -> httpx.Response | None:
    _headers = {"User-Agent": get_random_ua(), **(headers or {})}
    for attempt in range(1, retries + 1):
        try:
            with httpx.Client(timeout=timeout, follow_redirects=True, verify=verify) as client:
                resp = client.get(url, headers=_headers, params=params)
                resp.raise_for_status()
                return resp
        except Exception as e:
            if logger:
                logger.warning(f"GET attempt {attempt}/{retries} failed: {e}", url=url)
            if attempt < retries:
                time.sleep(delay * attempt)
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
) -> httpx.Response | None:
    _headers = {"User-Agent": get_random_ua(), **(headers or {})}
    for attempt in range(1, retries + 1):
        try:
            with httpx.Client(timeout=timeout, follow_redirects=True, verify=verify) as client:
                resp = client.post(url, data=data, json=json_data, headers=_headers)
                resp.raise_for_status()
                return resp
        except Exception as e:
            if logger:
                logger.warning(f"POST attempt {attempt}/{retries} failed: {e}", url=url)
            if attempt < retries:
                time.sleep(delay * attempt)
    return None


class PlaywrightSession:
    """Context manager that provides a reusable Playwright browser session."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._playwright = None
        self._browser: Browser | None = None

    def __enter__(self) -> "PlaywrightSession":
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=self.headless)
        return self

    def __exit__(self, *args):
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()

    def new_page(self, ua: str | None = None) -> Page:
        context = self._browser.new_context(user_agent=ua or get_random_ua())
        return context.new_page()

    def fetch_page(self, url: str, wait_selector: str | None = None, timeout: int = 30000) -> Page:
        page = self.new_page()
        page.goto(url, timeout=timeout)
        if wait_selector:
            page.wait_for_selector(wait_selector, timeout=timeout)
        else:
            page.wait_for_load_state("networkidle", timeout=timeout)
        return page
