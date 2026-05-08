#!/usr/bin/env python3
"""
Find the actual DMS document download URL by:
1. Solving CAPTCHA on the detail page
2. Clicking document download buttons and intercepting the real download requests
3. Also brute-force guessing the DMS service endpoint variations
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from urllib.parse import quote

import httpx
from playwright.sync_api import sync_playwright
from core.captcha_solver import solve_captcha_from_page, wait_for_captcha_canvas

BASE    = "https://maharerait.maharashtra.gov.in"
CERT_ID = sys.argv[1] if len(sys.argv) > 1 else "61699"
URL     = f"{BASE}/public/project/view/{CERT_ID}"

# A known DMS ref from the getMigratedDocuments call
TEST_DMS_REF  = "bda2f887-44e5-4860-b844-6168977ec4f3;0.1"
TEST_FILENAME = "FORM 1 Architect Cert.pdf"

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

captured = {"token": None, "download_requests": [], "all_requests": []}

def on_request(req):
    url = req.url
    captured["all_requests"].append({"method": req.method, "url": url, "post": req.post_data})
    if any(k in url.lower() for k in ("download", "dms", "view", "pdf", "document", "stream")):
        captured["download_requests"].append({"method": req.method, "url": url, "post": req.post_data})
        print(f"  [REQ] {req.method} {url}")

def on_response(resp):
    url = resp.url
    if "authenticatePublic" in url:
        try:
            data = json.loads(resp.text())
            tok = (data.get("responseObject") or {}).get("accessToken")
            if tok:
                captured["token"] = tok
        except Exception:
            pass

def main():
    ss_dir = Path("screenshots")
    ss_dir.mkdir(exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)  # headless=False to debug
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

        for attempt in range(1, 4):
            canvas_ready = wait_for_captcha_canvas(page, "canvas", timeout_ms=20_000)
            if not canvas_ready:
                page.reload(timeout=45_000); continue
            captcha_value = solve_captcha_from_page(page, selectors=["canvas"], captcha_source="eprocure")
            if not captcha_value:
                captcha_texts = page.evaluate("() => window.__captchaTexts || []")
                captcha_value = "".join(captcha_texts).strip()
            if not captcha_value:
                page.reload(timeout=45_000); continue
            print(f"[+] Captcha: {captcha_value!r}")
            page.fill("input[name='captcha']", captcha_value)
            page.click("button.next")
            try:
                page.wait_for_selector("label.form-label, .col-md-4 .f-s-15", timeout=12_000)
                print("[+] CAPTCHA accepted")
                break
            except Exception:
                page.reload(timeout=45_000)

        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass
        page.wait_for_timeout(3000)

        # Click Document Library
        try:
            page.click("text=Document Library", timeout=8000)
            page.wait_for_timeout(3000)
            print("[+] Clicked Document Library")
        except Exception as e:
            print(f"  [!] No Document Library link: {e}")

        # Find and click any "View" or "Download" button on the doc tables
        print("\n[*] Looking for document action buttons...")
        btns = page.query_selector_all("button, a")
        clicked = 0
        for btn in btns:
            try:
                txt = (btn.inner_text() or "").strip()
                if txt.lower() in ("view", "download", "open"):
                    print(f"  [CLICK] {txt!r}")
                    with page.expect_popup() as popup_info:
                        btn.click(timeout=5000)
                    popup = popup_info.value
                    popup_url = popup.url
                    print(f"  [POPUP] URL: {popup_url}")
                    popup.close()
                    clicked += 1
                    if clicked >= 3:
                        break
            except Exception as e:
                try:
                    txt2 = (btn.inner_text() or "").strip()
                    if txt2.lower() in ("view", "download", "open"):
                        btn.click(timeout=3000)
                        page.wait_for_timeout(2000)
                        clicked += 1
                        if clicked >= 3:
                            break
                except Exception:
                    pass

        page.wait_for_timeout(3000)

        # Dump Angular source to find download function
        ng_source = page.evaluate("""
        () => {
            // Look for ng-click handlers with download/dms references
            const results = [];
            document.querySelectorAll('[ng-click], [data-ng-click]').forEach(el => {
                const v = el.getAttribute('ng-click') || el.getAttribute('data-ng-click') || '';
                if (v.toLowerCase().includes('download') || v.toLowerCase().includes('dms') || v.toLowerCase().includes('document')) {
                    results.push({tag: el.tagName, text: el.innerText.trim().substring(0,50), ng: v.substring(0,200)});
                }
            });
            return results;
        }
        """)
        print(f"\n=== NG-CLICK DOCUMENT ELEMENTS ({len(ng_source)}) ===")
        for el in ng_source:
            print(f"  <{el['tag']}> {el['text']!r} ng={el['ng']!r}")

        browser.close()

    print(f"\n=== DOWNLOAD-RELATED REQUESTS CAPTURED ({len(captured['download_requests'])}) ===")
    for r in captured["download_requests"]:
        print(f"  {r['method']} {r['url']}")
        if r["post"]:
            print(f"    POST: {r['post'][:300]}")

    token = captured["token"]
    if not token:
        print("[!] No auth token — cannot test download URLs")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Origin": BASE,
        "Referer": URL,
    }

    # Brute-force DMS download URL patterns
    print(f"\n[*] Brute-forcing DMS download endpoints with ref: {TEST_DMS_REF!r}")
    ref_encoded = quote(TEST_DMS_REF, safe="")
    ref_plain   = TEST_DMS_REF

    dms_service = f"{BASE}/api/maha-rera-dms-service"
    pub_service = f"{BASE}/api/maha-rera-public-view-project-registration-service/public/projectregistartion"

    patterns = [
        ("GET",  f"{dms_service}/dms/downloadDocument/{ref_encoded}",         None),
        ("GET",  f"{dms_service}/dms/downloadDocument/{ref_plain}",            None),
        ("GET",  f"{dms_service}/dms/viewDocument/{ref_encoded}",              None),
        ("GET",  f"{dms_service}/dms/viewDocument/{ref_plain}",                None),
        ("GET",  f"{dms_service}/dms/document/{ref_encoded}",                  None),
        ("GET",  f"{dms_service}/dms/getFile?dmsRefNo={ref_encoded}",          None),
        ("GET",  f"{dms_service}/dms/download?dmsRefNo={ref_encoded}",         None),
        ("POST", f"{dms_service}/dms/downloadDocument",                        {"dmsRefNo": ref_plain}),
        ("POST", f"{dms_service}/dms/viewDocument",                            {"dmsRefNo": ref_plain}),
        ("POST", f"{pub_service}/downloadDocumentByDmsRef",                    {"dmsRefNo": ref_plain, "projectId": CERT_ID}),
        ("POST", f"{pub_service}/downloadDocument",                            {"dmsRefNo": ref_plain, "projectId": CERT_ID}),
        ("POST", f"{pub_service}/getDocumentByDmsRefNo",                       {"dmsRefNo": ref_plain}),
    ]

    with httpx.Client(timeout=15, follow_redirects=False) as client:
        for method, pat_url, body in patterns:
            try:
                if method == "GET":
                    r = client.get(pat_url, headers=headers)
                else:
                    r = client.post(pat_url, json=body, headers=headers)
                ct = r.headers.get("content-type", "?")
                loc = r.headers.get("location", "")
                print(f"  {r.status_code} [{method}] ct={ct:35} {pat_url}")
                if r.status_code in (200, 302) and ("pdf" in ct or "octet" in ct or loc):
                    print(f"    *** HIT! content-type={ct} location={loc} len={len(r.content)}")
                elif r.status_code not in (404,):
                    print(f"    body: {r.text[:200]}")
            except Exception as e:
                print(f"  ERR [{method}] {pat_url}: {e}")

    out = Path("screenshots/mh_download_url_results.json")
    out.write_text(json.dumps({
        "cert_id": CERT_ID,
        "test_dms_ref": TEST_DMS_REF,
        "download_requests": captured["download_requests"],
        "all_api_requests": [r for r in captured["all_requests"] if "/api/" in r["url"]],
    }, indent=2, default=str))
    print(f"\n[+] Saved to {out}")

if __name__ == "__main__":
    main()
