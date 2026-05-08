#!/usr/bin/env python3
"""
Direct API inspection: Authenticate via MH RERA public API, then call all
document endpoints and discover the DMS download URL pattern.

Usage:
    python inspect_mh_doc_api.py [cert_id]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx
from playwright.sync_api import sync_playwright
from core.captcha_solver import solve_captcha_from_page, wait_for_captcha_canvas

BASE   = "https://maharerait.maharashtra.gov.in"
CERT_ID = sys.argv[1] if len(sys.argv) > 1 else "61699"
URL    = f"{BASE}/public/project/view/{CERT_ID}"

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

captured = {"token": None, "responses": []}

def on_response(resp):
    url = resp.url
    if "authenticatePublic" in url:
        try:
            data = json.loads(resp.text())
            tok = (data.get("responseObject") or {}).get("accessToken")
            if tok:
                captured["token"] = tok
                print(f"  [+] Auth token captured: {tok[:60]}...")
        except Exception:
            pass
    if any(k in url for k in ("getUploaded", "getMigrated", "dms", "download")):
        try:
            body = resp.text()
            captured["responses"].append({"url": url, "body": body[:2000]})
        except Exception:
            pass

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
        page.on("response", on_response)

        print(f"[*] Loading: {URL}")
        page.goto(URL, timeout=60_000)

        for attempt in range(1, 4):
            print(f"[*] CAPTCHA attempt {attempt}/3")
            canvas_ready = wait_for_captcha_canvas(page, "canvas", timeout_ms=20_000)
            if not canvas_ready:
                print("  [!] Canvas not ready — refreshing")
                page.reload(timeout=45_000); continue
            captcha_value = solve_captcha_from_page(page, selectors=["canvas"], captcha_source="eprocure")
            if not captcha_value:
                captcha_texts = page.evaluate("() => window.__captchaTexts || []")
                captcha_value = "".join(captcha_texts).strip()
            if not captcha_value:
                print("  [!] Solve failed — refreshing")
                page.reload(timeout=45_000); continue
            print(f"  [+] Captcha: {captcha_value!r}")
            page.fill("input[name='captcha']", captcha_value)
            page.click("button.next")
            try:
                page.wait_for_selector("label.form-label, .col-md-4 .f-s-15", timeout=12_000)
                print("  [+] CAPTCHA accepted")
                break
            except Exception:
                page.reload(timeout=45_000)

        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass
        page.wait_for_timeout(4000)

        # Click Document Library to trigger doc API calls
        try:
            page.click("text=Document Library", timeout=5000)
            page.wait_for_timeout(4000)
        except Exception:
            print("  [!] Could not click Document Library")

        browser.close()

    print(f"\n[+] Auth token: {'CAPTURED' if captured['token'] else 'MISSING'}")

    if not captured["token"]:
        print("[!] Cannot proceed without auth token")
        return

    token = captured["token"]
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": BASE,
        "Referer": f"{BASE}/public/project/view/{CERT_ID}",
    }

    # Fetch all document endpoints
    doc_endpoints = [
        ("getMigratedDocuments",        {"projectId": CERT_ID}),
        ("getUploadedDocuments_tech",   {"projectId": CERT_ID, "documentSectionName": "Project_Technical", "documentTypeId": [10,11,12,13,52]}),
        ("getUploadedDocuments_legal",  {"projectId": CERT_ID, "documentSectionName": "Project_Legal", "documentTypeId": [27]}),
        ("getUploadedDocuments_fin",    {"projectId": CERT_ID, "documentSectionNmae": "Project_Finance", "documentTypeId": [26]}),
        ("getUploadedDocuments_other",  {"projectId": CERT_ID, "documentTypeId": [28, 30, 51]}),
        ("getUploadedDocuments_arch",   {"projectId": CERT_ID, "documentSectionName": "Project_Technical", "documentTypeId": [15]}),
        ("getUploadedDocuments_eng",    {"projectId": CERT_ID, "documentSectionName": "Project_Technical", "documentTypeId": [16]}),
        ("getStatusForProjectPreview",  {"projectId": CERT_ID}),
    ]
    base_url = f"{BASE}/api/maha-rera-public-view-project-registration-service/public/projectregistartion"

    all_dms_refs: list[dict] = []
    results: dict = {}

    with httpx.Client(timeout=30) as client:
        for name, body in doc_endpoints:
            ep_name = name.split("_")[0] if "_" in name else name
            ep_url = f"{base_url}/{ep_name}"
            try:
                r = client.post(ep_url, json=body, headers=headers)
                data = r.json()
                results[name] = data
                print(f"\n[{name}] status={r.status_code} msg={data.get('message')}")
                obj = data.get("responseObject") or []
                if isinstance(obj, list):
                    for item in obj[:3]:
                        print(f"  item keys: {list(item.keys())}")
                        # Collect DMS refs
                        for k, v in item.items():
                            if "dms" in k.lower() or "refno" in k.lower():
                                print(f"    DMS field: {k} = {v!r}")
                                if v and ";" in str(v):
                                    all_dms_refs.append({"source": name, "field": k, "ref": str(v),
                                                         "filename": item.get("documentFileName") or item.get("fileName") or ""})
                elif isinstance(obj, dict):
                    for k, v in obj.items():
                        if "dms" in k.lower() or "refno" in k.lower():
                            print(f"    DMS field: {k} = {v!r}")
            except Exception as e:
                print(f"[{name}] ERROR: {e}")

    # Try to find the download URL pattern using one DMS ref
    print(f"\n=== DMS REFS FOUND: {len(all_dms_refs)} ===")
    for ref in all_dms_refs:
        print(f"  [{ref['source']}] {ref['field']}={ref['ref']!r}  file={ref['filename']!r}")

    if all_dms_refs:
        test_ref = all_dms_refs[0]["ref"]
        print(f"\n[*] Testing download URL patterns with ref: {test_ref!r}")
        dms_patterns = [
            f"{BASE}/api/maha-rera-dms-service/dms/downloadDocument?dmsRefNo={test_ref}",
            f"{BASE}/api/maha-rera-dms-service/dms/viewDocument?dmsRefNo={test_ref}",
            f"{BASE}/api/maha-rera-dms-service/dms/getDocument?dmsRefNo={test_ref}",
            f"{BASE}/api/maha-rera-dms-service/dms/download/{test_ref}",
        ]
        with httpx.Client(timeout=15, follow_redirects=True) as client:
            for pat in dms_patterns:
                try:
                    r = client.get(pat, headers=headers)
                    ct = r.headers.get("content-type", "")
                    print(f"  {r.status_code} {ct:40} {pat}")
                except Exception as e:
                    print(f"  ERR {pat}: {e}")

    out_path = Path("screenshots/mh_doc_api_results.json")
    out_path.write_text(json.dumps({"cert_id": CERT_ID, "dms_refs": all_dms_refs, "results": results}, indent=2, default=str))
    print(f"\n[+] Full results saved to {out_path}")

if __name__ == "__main__":
    main()
