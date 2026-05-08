#!/usr/bin/env python3
"""
Live inspector for Maharashtra RERA document section.
Visits a known project detail page, solves the CAPTCHA (via OCR service),
intercepts ALL network requests, and dumps document-related API calls + HTML.

Usage:
    python inspect_mh_docs.py [cert_id]

Default cert_id: 61699  (PP1190002502346 — VISION CITY 3)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright
from core.captcha_solver import solve_captcha_from_page, wait_for_captcha_canvas

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

all_requests: list[dict] = []
all_responses: list[dict] = []

def on_request(req):
    all_requests.append({"method": req.method, "url": req.url, "post": req.post_data})

def on_response(resp):
    url = resp.url
    entry = {"status": resp.status, "url": url, "body": None}
    if any(k in url.lower() for k in ("document", "download", "dms", "/doc/", "file", "upload", "attachment", "/api/", "project")):
        try:
            ct = resp.headers.get("content-type", "")
            if "json" in ct or "text" in ct:
                entry["body"] = resp.text()[:600]
        except Exception:
            pass
    all_responses.append(entry)

def main():
    ss_dir = Path("screenshots")
    ss_dir.mkdir(exist_ok=True)

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

        # Screenshot before CAPTCHA solve
        page.screenshot(path=str(ss_dir / "mh_inspect_pre_captcha.png"))
        print(f"[+] Pre-captcha screenshot saved")

        captcha_solved = False
        for attempt in range(1, 4):
            print(f"[*] CAPTCHA attempt {attempt}/3")
            canvas_ready = wait_for_captcha_canvas(page, "canvas", timeout_ms=20_000)
            if not canvas_ready:
                print("  [!] Canvas not ready — refreshing")
                page.reload(timeout=45_000)
                continue

            # Try OCR solver first
            captcha_value = solve_captcha_from_page(page, selectors=["canvas"], captcha_source="eprocure")
            if captcha_value:
                print(f"  [+] OCR solved: {captcha_value!r}")
            else:
                # Fallback: canvas text interception
                captcha_texts = page.evaluate("() => window.__captchaTexts || []")
                captcha_value = "".join(captcha_texts).strip()
                if captcha_value:
                    print(f"  [+] Canvas interception: {captcha_value!r}")

            if not captcha_value:
                print("  [!] Both OCR and canvas interception failed — refreshing")
                page.reload(timeout=45_000)
                continue

            page.fill("input[name='captcha']", captcha_value)
            page.click("button.next")
            try:
                page.wait_for_selector("label.form-label, .col-md-4 .f-s-15", timeout=12_000)
                print("  [+] CAPTCHA accepted — Angular content loaded")
                captcha_solved = True
                break
            except Exception:
                print("  [!] Angular not loaded after submit — refreshing")
                page.reload(timeout=45_000)

        if not captcha_solved:
            print("[!] All CAPTCHA attempts failed — saving page state anyway")

        # Wait for full Angular render + network idle
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            print("[!] networkidle timeout — continuing")
        page.wait_for_timeout(4000)

        # Screenshot after Angular loaded
        page.screenshot(path=str(ss_dir / "mh_inspect_post_captcha.png"), full_page=True)
        print(f"[+] Post-captcha screenshot saved")

        # Save full HTML
        html = page.content()
        (ss_dir / "mh_inspect_docs.html").write_text(html, encoding="utf-8")
        print(f"[+] Full HTML saved: screenshots/mh_inspect_docs.html ({len(html)} chars)")

        # ── Inspect all links / buttons for document patterns ────────────────
        print("\n=== DOCUMENT LINKS & BUTTONS ON PAGE ===")
        elements = page.query_selector_all("a, button")
        for el in elements[:300]:
            try:
                txt = (el.inner_text() or "").strip()[:60]
                href = el.get_attribute("href") or ""
                ng_click = el.get_attribute("ng-click") or el.get_attribute("(click)") or ""
                data_id = el.get_attribute("data-id") or el.get_attribute("data-doc-id") or ""
                all_attrs = txt + href + ng_click + data_id
                if any(k in all_attrs.lower() for k in ("download", "document", "pdf", "view doc", "dms")):
                    print(f"  text={txt!r:45} href={href!r:55} ng={ng_click!r:60} data-id={data_id!r}")
            except Exception:
                pass

        # ── Try clicking each "View" / "Download" button to trigger API calls ─
        print("\n[*] Attempting to click document download buttons to expose API calls...")
        btns = page.query_selector_all("button, a")
        clicks = 0
        for btn in btns[:300]:
            try:
                txt = (btn.inner_text() or "").strip().lower()
                ng = (btn.get_attribute("ng-click") or btn.get_attribute("(click)") or "").lower()
                if any(k in (txt + ng) for k in ("download", "view doc", "document")):
                    print(f"  [CLICK] {txt!r}")
                    btn.click(timeout=3000)
                    page.wait_for_timeout(2000)
                    clicks += 1
                    if clicks >= 3:
                        break
            except Exception:
                pass

        page.wait_for_timeout(3000)
        browser.close()

    # ── Report ──────────────────────────────────────────────────────────────
    doc_reqs = [r for r in all_requests if any(
        k in r["url"].lower() for k in ("document", "download", "dms", "/doc/", "attachment", "upload")
    )]
    api_reqs = [r for r in all_requests if any(
        k in r["url"] for k in ("/api/", "rest", ".json", "/project/", "/public/")
    )]
    doc_resps = [r for r in all_responses if r.get("body")]

    print(f"\n=== DOCUMENT-RELATED REQUESTS ({len(doc_reqs)}) ===")
    for r in doc_reqs:
        print(f"  {r['method']} {r['url']}")
        if r["post"]:
            print(f"    POST body: {r['post'][:300]}")

    print(f"\n=== API / PROJECT REQUESTS ({len(api_reqs)}) ===")
    for r in api_reqs[:40]:
        print(f"  {r['method']} {r['url']}")
        if r["post"]:
            print(f"    POST body: {r['post'][:300]}")

    print(f"\n=== RESPONSES WITH BODIES ({len(doc_resps)}) ===")
    for r in doc_resps[:20]:
        print(f"  [{r['status']}] {r['url']}")
        print(f"    {r['body'][:300]}")

    out = {
        "cert_id": CERT_ID,
        "captcha_solved": captcha_solved,
        "doc_requests": doc_reqs,
        "api_requests": api_reqs[:60],
        "responses_with_bodies": doc_resps[:30],
    }
    Path("screenshots/mh_inspect_docs.json").write_text(json.dumps(out, indent=2))
    print("\n[+] Full results saved to screenshots/mh_inspect_docs.json")

if __name__ == "__main__":
    main()
