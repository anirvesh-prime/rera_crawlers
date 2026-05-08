#!/usr/bin/env python3
"""
Debug script: capture the Telangana CAPTCHA image and show exactly
what is sent to the solver endpoint and what comes back.
"""
import base64
import json
import socket
import sys

SEARCH_URL = "https://rerait.telangana.gov.in/SearchList/Search"
CAPTCHA_HOST = "captcha.hawker.news"
CAPTCHA_PORT = 4444


def raw_send_receive(image_source: str, captcha_source: str = "model_captcha") -> dict:
    """Send to solver and return the full raw response dict."""
    payload = {"image_source": image_source, "captcha_source": captcha_source}
    serialized = json.dumps(payload)
    client = socket.socket()
    client.connect((CAPTCHA_HOST, CAPTCHA_PORT))
    client.settimeout(180)
    # Send length-prefixed message
    client.send(b"%d\n" % len(serialized))
    client.sendall(serialized.encode())
    # Read length-prefixed response
    length_str = b""
    while True:
        ch = client.recv(1)
        if ch == b"\n" or not ch:
            break
        length_str += ch
    total = int(length_str)
    buf = bytearray(total)
    view = memoryview(buf)
    offset = 0
    while total - offset > 0:
        n = client.recv_into(view[offset:], total - offset)
        offset += n
    client.close()
    return json.loads(bytes(buf))


N_SAMPLES = 5


def capture_and_solve(page, attempt: int) -> dict:
    """Reload the page, capture the CAPTCHA, call the solver, return result dict."""
    print(f"\n[{attempt}/{N_SAMPLES}] Reloading page …")
    page.reload(wait_until="domcontentloaded", timeout=120_000)

    page.wait_for_selector("#captchaImage", state="visible", timeout=20_000)
    page.wait_for_function(
        "() => { const img = document.querySelector('#captchaImage'); "
        "return img && img.complete && img.naturalWidth > 0; }",
        timeout=15_000,
    )
    print(f"[{attempt}/{N_SAMPLES}] Stabilisation wait (3.5 s) …")
    page.wait_for_timeout(3_500)

    captcha_el = page.query_selector("#captchaImage")
    if not captcha_el:
        return {"attempt": attempt, "error": "#captchaImage not found"}

    png_bytes = captcha_el.screenshot()
    print(f"[{attempt}/{N_SAMPLES}] Raw screenshot : {len(png_bytes)} bytes")

    sent_b64 = base64.b64encode(png_bytes).decode()
    full_data_url = "data:image/png;base64," + sent_b64

    print(f"[{attempt}/{N_SAMPLES}] Sending full-size image ({len(png_bytes)} bytes, no resize) …")
    try:
        response = raw_send_receive(full_data_url, captcha_source="model_captcha")
    except Exception as exc:
        response = {"image_text": None, "error": str(exc)}

    image_text = response.get("image_text")
    print(f"[{attempt}/{N_SAMPLES}] solver returned : {image_text!r}")

    return {
        "attempt": attempt,
        "sent_to_endpoint": {
            "host": CAPTCHA_HOST,
            "port": CAPTCHA_PORT,
            "captcha_source": "model_captcha",
            "image_base64": sent_b64,
            "raw_screenshot_bytes": len(png_bytes),
            "note": "full-size screenshot, no resize",
        },
        "raw_response": response,
        "image_text": image_text,
    }


def main():
    from playwright.sync_api import sync_playwright

    results = []
    print(f"→ Launching Playwright and navigating to {SEARCH_URL}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=120_000)

        for i in range(1, N_SAMPLES + 1):
            result = capture_and_solve(page, i)
            results.append(result)

        browser.close()

    out_path = "debug_telangana_captcha_result.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{'=' * 72}")
    print("SUMMARY")
    print(f"{'=' * 72}")
    for r in results:
        txt = r.get("image_text") or r.get("error", "—")
        print(f"  [{r['attempt']}/{N_SAMPLES}] {txt}")
    print(f"\n→ All results written to {out_path}")


if __name__ == "__main__":
    main()
