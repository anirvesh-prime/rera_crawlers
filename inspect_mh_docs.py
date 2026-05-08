#!/usr/bin/env python3
"""
Live inspector for Maharashtra RERA document section.
Visits a known project detail page, solves the CAPTCHA,
intercepts all network requests, and dumps document-related API calls + HTML.

Usage:
    python inspect_mh_docs.py [cert_id]

Default cert_id: 61699  (PP1190002502346 — VISION CITY 3)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

DETAIL_BASE = "https://maharerait.maharashtra.gov.in"
CERT_ID = sys.argv[1] if len(sys.argv) > 1 else "61699"
URL = f"{DETAIL_BASE}/public/project/view/{CERT_ID}"

_CAPTCHA_INTERCEPT = """
(function() {
    var origFillText = CanvasRenderingContext2D.prototype.fillText;
    window.__captchaTexts = [];
    CanvasRenderingContext2D.prototype.fillText = function(text, x, y, maxWidth) {
        if (typeof text === 'string' && text.length > 0 && text.trim().length > 0)
            window.__captchaTexts.push(text);
        return origFillText.apply(this, arguments);
    };
})();
"""

api_calls: list[dict] = []
doc_calls: list[dict] = []

def on_request(req):
    url = req.url
    if any(k in url for k in ("document", "download", "dms", "/doc", "file", "upload", "attachment")):
        entry = {"method": req.method, "url": url, "post": req.post_data}
        doc_calls.append(entry)
    api_calls.append({"method": req.method, "url": url})

def on_response(resp):
    url = resp.url
    if any(k in url for k in ("document", "download", "dms", "/doc", "file", "upload", "attachment")):
        try:
            body = resp.text()
            print(f"\n[DOC RESPONSE] {url}\n  Status: {resp.status}\n  Body(200): {body[:400]}")
        except Exception:
            pass

def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
        )
        ctx.add_init_script(_CAPTCHA_INTERCEPT)
        page = ctx.new_page()
        page.on("request", on_request)
        page.on("response", on_response)

        print(f"[*] Loading: {URL}")
        page.goto(URL, timeout=60_000)

        # Try canvas interception for CAPTCHA
        for attempt in range(1, 4):
            print(f"[*] CAPTCHA attempt {attempt}/3")
            try:
                page.wait_for_selector("canvas", timeout=20_000)
            except Exception:
                print("  [!] No canvas found — refreshing")
                page.reload(timeout=45_000)
                continue

            captcha_texts = page.evaluate("() => window.__captchaTexts || []")
            captcha_value = "".join(captcha_texts).strip()
            if not captcha_value:
                # Try OCR fallback via canvas screenshot
                print("  [!] Canvas interception empty — trying reload")
                page.reload(timeout=45_000)
                continue

            print(f"  [+] Captcha value: {captcha_value!r}")
            page.fill("input[name='captcha']", captcha_value)
            page.click("button.next")
            try:
                page.wait_for_selector("label.form-label, .col-md-4 .f-s-15", timeout=12_000)
                print("  [+] CAPTCHA accepted — Angular content loaded")
                break
            except Exception:
                print("  [!] Angular content not loaded — refreshing")
                page.reload(timeout=45_000)

        # Wait for full render
        try:
            page.wait_for_load_state("networkidle", timeout=25_000)
        except Exception:
            print("[!] networkidle timeout — continuing")

        # Extra wait for Angular document section to render
        page.wait_for_timeout(3000)

        # Screenshot the full page
        ss_path = Path("screenshots/mh_inspect_docs.png")
        ss_path.parent.mkdir(exist_ok=True)
        page.screenshot(path=str(ss_path), full_page=True)
        print(f"\n[+] Screenshot saved: {ss_path}")

        # Dump HTML
        html = page.content()
        html_path = Path("screenshots/mh_inspect_docs.html")
        html_path.write_text(html, encoding="utf-8")
        print(f"[+] HTML saved: {html_path}")

        # Try to find document download buttons and trigger one click to see API
        doc_btns = page.query_selector_all("button[ng-click*='document'], button[ng-click*='download'], a[ng-click*='document'], button[onclick*='document']")
        print(f"\n[+] Found {len(doc_btns)} document buttons via ng-click")

        # Look for download links in Angular attrs
        all_links = page.query_selector_all("a, button")
        for el in all_links[:200]:
            try:
                txt = el.inner_text().strip()
                href = el.get_attribute("href") or ""
                ng = el.get_attribute("ng-click") or el.get_attribute("(click)") or ""
                if any(k in (txt+href+ng).lower() for k in ("download", "document", "view", "pdf")):
                    print(f"  [LINK] text={txt!r:40} href={href!r:50} ng={ng!r}")
            except Exception:
                pass

        browser.close()

    print(f"\n=== DOCUMENT-RELATED API CALLS ({len(doc_calls)}) ===")
    for c in doc_calls:
        print(f"  {c['method']} {c['url']}")
        if c['post']:
            print(f"    POST: {c['post'][:200]}")

    # Find API call patterns
    print(f"\n=== ALL API CALLS (filtered to /api) ===")
    for c in api_calls:
        if "/api/" in c["url"] or "rest" in c["url"] or ".json" in c["url"]:
            print(f"  {c['method']} {c['url']}")

    out = {"cert_id": CERT_ID, "doc_calls": doc_calls,
           "api_calls": [c for c in api_calls if "/api/" in c["url"] or "rest" in c["url"]]}
    Path("screenshots/mh_inspect_docs.json").write_text(json.dumps(out, indent=2))
    print("\n[+] Results saved to screenshots/mh_inspect_docs.json")

if __name__ == "__main__":
    main()
