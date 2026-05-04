#!/usr/bin/env python3
"""
Quick listing + pagination smoke-test for every RERA crawler.

Does NOT do a deep crawl — only verifies that:
  1. The listing page is reachable and returns ≥1 projects.
  2. Where the crawler paginates, page 2 also returns ≥1 projects.

Run: python test_listing_pagination.py
"""
from __future__ import annotations

import os, sys, traceback, time

if os.environ.get("PYTHONHASHSEED") != "0":
    os.execvpe(sys.executable, [sys.executable, *sys.argv],
               {**os.environ, "PYTHONHASHSEED": "0"})

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bs4 import BeautifulSoup
from core.crawler_base import safe_get, safe_post

# ── Minimal no-op logger (no DB, no file I/O) ─────────────────────────────────
class _L:
    def info(self, m, **k):    print(f"      [i] {m}")
    def warning(self, m, **k): print(f"      [!] {m}")
    def error(self, m, **k):   print(f"      [E] {m}")
    def debug(self, m, **k):   pass
    def set_project(self, **k): pass
    def clear_project(self): pass
    def close(self): pass
    def log_run_key_summary(self, **k): pass

LOG = _L()

# ── Result accumulator ────────────────────────────────────────────────────────
_results: list[dict] = []

def _record(state: str, crawler: str, check: str, count: int = 0, note: str = ""):
    icon = {"ok": "✅", "fail": "❌", "warn": "⚠️ "}.get(state, "?")
    _results.append(dict(state=state, crawler=crawler, check=check, count=count, note=note))
    cnt_str = f"({count} items)" if count else ""
    note_str = f"  — {note}" if note else ""
    print(f"  {icon} [{crawler}] {check} {cnt_str}{note_str}")

def _run(crawler: str, fn):
    try:
        fn(crawler)
    except Exception as exc:
        _record("fail", crawler, "EXCEPTION", note=traceback.format_exc(limit=3).strip().splitlines()[-1])

# ═══════════════════════════════════════════════════════════════════════════════
# Per-crawler test functions
# ═══════════════════════════════════════════════════════════════════════════════

def _andhra_pradesh(c):
    from sites.andhra_pradesh_rera import _fetch_listing, _parse_listing_rows
    soup = _fetch_listing(LOG)
    if not soup:
        return _record("fail", c, "listing fetch", note="None returned")
    rows = _parse_listing_rows(soup)
    _record("ok" if rows else "fail", c, "listing page", len(rows))

def _assam(c):
    from sites.assam_rera import _fetch_listing
    rows = _fetch_listing(LOG)
    _record("ok" if rows else "fail", c, "listing page", len(rows))

def _bihar(c):
    # Bihar listing is HTTP-reachable; Playwright is needed for detail popup URLs only
    from sites.bihar_rera import LISTING_URL, _parse_page_rows, _has_next_page
    resp = safe_get(LISTING_URL, retries=2, logger=LOG)
    if not resp:
        return _record("fail", c, "listing fetch", note="HTTP error")
    soup = BeautifulSoup(resp.text, "lxml")
    rows = _parse_page_rows(soup)
    _record("ok" if rows else "fail", c, "listing page 1", len(rows))
    has_next = _has_next_page(soup, 1)
    _record("ok" if has_next else "warn", c, "pagination (has page 2)", note="" if has_next else "no next-page link")

def _chhattisgarh(c):
    from sites.chhattisgarh_rera import LISTING_URL, _parse_listing_map_data, _get
    resp = _get(LISTING_URL, LOG)
    if not resp:
        return _record("fail", c, "listing fetch", note="HTTP error")
    rows = _parse_listing_map_data(resp.text)
    _record("ok" if rows else "fail", c, "listing page", len(rows))

def _delhi(c):
    from sites.delhi_rera import LISTING_URL, _parse_listing_page, _has_next_page, _get_listing_response
    resp = _get_listing_response(LISTING_URL, LOG)
    if not resp:
        return _record("fail", c, "listing fetch", note="HTTP error")
    rows = _parse_listing_page(resp.text)
    _record("ok" if rows else "fail", c, "listing page 0", len(rows))
    has_next = _has_next_page(resp.text)
    _record("ok" if has_next else "warn", c, "pagination (has page 1)", note="" if has_next else "no next-page link")
    if has_next:
        resp2 = _get_listing_response(f"{LISTING_URL}?page=1", LOG)
        rows2 = _parse_listing_page(resp2.text) if resp2 else []
        _record("ok" if rows2 else "fail", c, "listing page 1", len(rows2))

def _goa(c):
    # Goa requires Playwright + captcha — just verify the home page responds
    from sites.goa_rera import BASE_URL
    resp = safe_get(BASE_URL, retries=2, logger=LOG, timeout=30)
    _record("ok" if resp else "warn", c, "home page reachable (Playwright+captcha required for full listing)",
            note="" if resp else "unreachable")

def _gujarat(c):
    # Gujarat is Playwright-only; verify base URL responds
    from sites.gujarat_rera import BASE_URL
    resp = safe_get(BASE_URL, retries=2, logger=LOG, timeout=30)
    _record("ok" if resp else "warn", c, "base URL reachable (Playwright required for full listing)",
            note="" if resp else "unreachable")

def _haryana(c):
    from sites.haryana_rera import LISTING_URLS, _fetch_listing
    for url in LISTING_URLS:
        rows = _fetch_listing(url, LOG)
        label = "Gurugram" if "2" in url else "Panchkula"
        _record("ok" if rows else "fail", c, f"listing ({label})", len(rows))

def _jharkhand(c):
    from sites.jharkhand_rera import LISTING_URL, _parse_listing_rows, _has_next_page
    resp = safe_get(LISTING_URL, retries=2, logger=LOG, timeout=30)
    if not resp:
        return _record("fail", c, "listing fetch", note="HTTP error")
    soup = BeautifulSoup(resp.text, "lxml")
    rows = _parse_listing_rows(soup)
    _record("ok" if rows else "fail", c, "listing page 1", len(rows))
    has_next = _has_next_page(soup, 1)
    _record("ok" if has_next else "warn", c, "pagination (has page 2)", note="" if has_next else "no next-page link")
    if has_next:
        resp2 = safe_get(f"{LISTING_URL}?page=2", retries=2, logger=LOG, timeout=30)
        rows2 = _parse_listing_rows(BeautifulSoup(resp2.text, "lxml")) if resp2 else []
        _record("ok" if rows2 else "fail", c, "listing page 2", len(rows2))

def _karnataka(c):
    from sites.karnataka_rera import DISTRICTS, _post_listing, _extract_listing_rows
    district = DISTRICTS[0]  # Test first district only
    html = _post_listing(district, 0, LOG)
    if not html:
        return _record("fail", c, "listing POST page 0", note="HTTP error")
    rows = _extract_listing_rows(html, district)
    _record("ok" if rows else "fail", c, f"listing district={district!r} start=0", len(rows))
    if rows:  # test next page offset
        next_start = len(rows)
        html2 = _post_listing(district, next_start, LOG)
        rows2 = _extract_listing_rows(html2, district) if html2 else []
        _record("ok" if rows2 else "warn", c, f"pagination start={next_start}",
                len(rows2), note="" if rows2 else "no items on page 2")

def _kerala(c):
    from sites.kerala_rera import _get_explore_page, _parse_explore_cards, _get_total_pages
    soup1 = _get_explore_page(1, LOG)
    if not soup1:
        return _record("fail", c, "listing page 1 fetch", note="HTTP error")
    cards1 = _parse_explore_cards(soup1)
    total = _get_total_pages(soup1)
    _record("ok" if cards1 else "fail", c, f"listing page 1 (total={total})", len(cards1))
    if total > 1:
        soup2 = _get_explore_page(2, LOG)
        cards2 = _parse_explore_cards(soup2) if soup2 else []
        _record("ok" if cards2 else "fail", c, "listing page 2", len(cards2))

def _madhya_pradesh(c):
    from sites.madhya_pradesh_rera import _fetch_listing
    rows = _fetch_listing(LOG)
    _record("ok" if rows else "fail", c, "listing page (single AJAX)", len(rows))

def _maharashtra(c):
    from sites.maharashtra_rera import LISTING_URL, _parse_cards, _get_total_pages, _url_for_page
    resp = safe_get(LISTING_URL, retries=2, logger=LOG, timeout=30)
    if not resp:
        return _record("fail", c, "listing fetch", note="HTTP error")
    soup = BeautifulSoup(resp.text, "lxml")
    cards = _parse_cards(soup)
    total = _get_total_pages(soup)
    _record("ok" if cards else "fail", c, f"listing page 0 (total≈{total})", len(cards))
    if total > 1:
        resp2 = safe_get(_url_for_page(1), retries=2, logger=LOG, timeout=30)
        cards2 = _parse_cards(BeautifulSoup(resp2.text, "lxml")) if resp2 else []
        _record("ok" if cards2 else "fail", c, "listing page 1", len(cards2))

def _odisha(c):
    # Odisha is Playwright-only; verify base URL responds
    from sites.odisha_rera import LISTING_URL
    resp = safe_get(LISTING_URL, retries=2, logger=LOG, timeout=30)
    _record("ok" if resp else "warn", c, "listing URL reachable (Playwright required for full listing)",
            note="" if resp else "unreachable")

def _pondicherry(c):
    from sites.pondicherry_rera import LISTING_URL, _parse_listing_cards
    resp = safe_get(LISTING_URL, verify=False, retries=2, logger=LOG, timeout=30)
    if not resp:
        return _record("fail", c, "listing fetch", note="HTTP error")
    soup = BeautifulSoup(resp.text, "lxml")
    cards = _parse_listing_cards(soup)
    _record("ok" if cards else "fail", c, "listing page", len(cards))

def _punjab(c):
    # Punjab uses a captcha; just check the listing URL is reachable
    from sites.punjab_rera import LISTING_URL
    resp = safe_get(LISTING_URL, retries=2, logger=LOG, timeout=30)
    _record("ok" if resp else "warn", c, "listing URL reachable (captcha required for full listing)",
            note="" if resp else "unreachable")

def _rajasthan(c):
    # Rajasthan uses Playwright; verify base URL
    from sites.rajasthan_rera import BASE_URL, LISTING_PAGE_URL
    resp = safe_get(LISTING_PAGE_URL, retries=2, logger=LOG, timeout=30)
    _record("ok" if resp else "warn", c, "listing URL reachable (Playwright required for full listing)",
            note="" if resp else "unreachable")

def _tamil_nadu(c):
    from sites.tamil_nadu_rera import _get_year_listing_urls, _parse_year_listing
    urls = _get_year_listing_urls(LOG)
    _record("ok" if urls else "fail", c, f"year listing URLs discovered", len(urls))
    if urls:
        rows = _parse_year_listing(urls[0], LOG)
        _record("ok" if rows else "fail", c, f"listing page ({urls[0].split('/')[-1]})", len(rows))
        if len(urls) > 1:
            rows2 = _parse_year_listing(urls[1], LOG)
            _record("ok" if rows2 else "warn", c, f"listing page ({urls[1].split('/')[-1]})",
                    len(rows2), note="" if rows2 else "empty")

def _telangana(c):
    # Telangana uses Playwright; verify search URL responds
    from sites.telangana_rera import SEARCH_URL
    resp = safe_get(SEARCH_URL, retries=2, logger=LOG, timeout=30)
    _record("ok" if resp else "warn", c, "search URL reachable (Playwright required for full listing)",
            note="" if resp else "unreachable")

def _tripura(c):
    from sites.tripura_rera import LISTING_URL, _parse_listing_rows, _has_next_page, _get
    resp = _get(LISTING_URL, LOG, params={"startFrom": "0"})
    if not resp:
        return _record("fail", c, "listing fetch startFrom=0", note="HTTP error")
    soup = BeautifulSoup(resp.text, "lxml")
    rows = _parse_listing_rows(soup)
    _record("ok" if rows else "fail", c, "listing startFrom=0", len(rows))
    has_next = _has_next_page(soup, 0)
    _record("ok" if has_next else "warn", c, "pagination (has next page)", note="" if has_next else "no next-page link")
    if has_next:
        page_size = len(rows) if rows else 10
        resp2 = _get(LISTING_URL, LOG, params={"startFrom": str(page_size)})
        rows2 = _parse_listing_rows(BeautifulSoup(resp2.text, "lxml")) if resp2 else []
        _record("ok" if rows2 else "fail", c, f"listing startFrom={page_size}", len(rows2))

def _west_bengal(c):
    from sites.west_bengal_rera import LISTING_URL, _parse_listing_rows, _get
    resp = _get(LISTING_URL, LOG)
    if not resp:
        return _record("fail", c, "listing fetch", note="HTTP error")
    soup = BeautifulSoup(resp.text, "lxml")
    rows = _parse_listing_rows(soup)
    _record("ok" if rows else "fail", c, "listing page", len(rows))

# ═══════════════════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════════════════

CRAWLERS = [
    ("andhra_pradesh",  _andhra_pradesh),
    ("assam",           _assam),
    ("bihar",           _bihar),
    ("chhattisgarh",    _chhattisgarh),
    ("delhi",           _delhi),
    ("goa",             _goa),
    ("gujarat",         _gujarat),
    ("haryana",         _haryana),
    ("jharkhand",       _jharkhand),
    ("karnataka",       _karnataka),
    ("kerala",          _kerala),
    ("madhya_pradesh",  _madhya_pradesh),
    ("maharashtra",     _maharashtra),
    ("odisha",          _odisha),
    ("pondicherry",     _pondicherry),
    ("punjab",          _punjab),
    ("rajasthan",       _rajasthan),
    ("tamil_nadu",      _tamil_nadu),
    ("telangana",       _telangana),
    ("tripura",         _tripura),
    ("west_bengal",     _west_bengal),
]

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("  RERA Crawler — Listing & Pagination Smoke Test")
    print("=" * 70 + "\n")

    for name, fn in CRAWLERS:
        print(f"\n── {name.upper().replace('_',' ')} ─────────────────────────────────────────")
        t0 = time.monotonic()
        _run(name, fn)
        print(f"     ({time.monotonic()-t0:.1f}s)")

    # ── Summary ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    ok   = [r for r in _results if r["state"] == "ok"]
    fail = [r for r in _results if r["state"] == "fail"]
    warn = [r for r in _results if r["state"] == "warn"]
    print(f"  ✅ OK   : {len(ok)}")
    print(f"  ⚠️  WARN : {len(warn)}  (captcha/Playwright — reachability only checked)")
    print(f"  ❌ FAIL : {len(fail)}")
    if fail:
        print("\n  Failed checks:")
        for r in fail:
            print(f"    ❌ [{r['crawler']}] {r['check']}  {r['note']}")
    if warn:
        print("\n  Warnings:")
        for r in warn:
            print(f"    ⚠️  [{r['crawler']}] {r['check']}  {r['note']}")
    print("=" * 70 + "\n")
