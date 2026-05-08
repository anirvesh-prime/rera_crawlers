"""
Telangana RERA — end-to-end step-by-step diagnostic.

Runs each stage of the crawler in isolation with full intermediate value
logging so every success/failure can be seen clearly.
"""
from __future__ import annotations
import base64, struct, sys, time, textwrap
sys.path.insert(0, ".")

# ─── helpers ──────────────────────────────────────────────────────────────────
def hdr(title: str):
    bar = "=" * 70
    print(f"\n{bar}\n  STEP: {title}\n{bar}")

def ok(msg: str):   print(f"  ✅  {msg}")
def warn(msg: str): print(f"  ⚠️   {msg}")
def err(msg: str):  print(f"  ❌  {msg}")
def info(msg: str): print(f"  ℹ️   {msg}")

def png_dims(b: bytes) -> tuple[int,int]:
    return struct.unpack(">II", b[16:20] + b[20:24])

# ─── imports ──────────────────────────────────────────────────────────────────
hdr("0 — Module imports")
try:
    from core.captcha_solver import captcha_to_text, connect_client, send_data, receive_data
    from core.crawler_base import safe_get, PlaywrightSession
    from sites.telangana_rera import (
        SEARCH_URL, BASE_URL,
        _solve_captcha, _submit_search, _parse_listing_rows,
        _get_total_pages, _goto_next_page,
        _decode_data_cert, _build_cert_url, _build_preview_url,
        _extract_print_preview_url,
        _scrape_print_preview,
        _CERT_CHAR_D, _PREVIEW_CHAR_D,
    )
    from bs4 import BeautifulSoup
    ok("All imports succeeded")
    info(f"SEARCH_URL = {SEARCH_URL}")
except Exception as e:
    err(f"Import failed: {e}")
    sys.exit(1)

# ─── STEP 1: Sentinel / SSL ───────────────────────────────────────────────────
hdr("1 — Sentinel: HTTPS GET to search page (SSL bypass)")
t0 = time.time()
resp = safe_get(SEARCH_URL, retries=2, verify=False)
elapsed = time.time() - t0
if resp:
    lower = resp.text.lower()
    has_captcha = "captchaimage" in lower or "captcha" in lower
    ok(f"HTTP {resp.status_code} in {elapsed:.2f}s — page length {len(resp.text):,} chars")
    if has_captcha:
        ok("CAPTCHA element found in page HTML — site structure intact")
    else:
        err("CAPTCHA element NOT found in page HTML")
else:
    err(f"GET failed after retries ({elapsed:.2f}s)")
    sys.exit(1)

# ─── STEP 2–N: Playwright session ─────────────────────────────────────────────
hdr("2 — Launch Playwright browser (headless, ignore_https_errors=True)")
info("Opening PlaywrightSession …")

with PlaywrightSession(headless=True, ignore_https_errors=True) as browser:
    page = browser.new_page()
    page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=120_000)
    ok(f"Browser open — current URL: {page.url}")

    # ── STEP 3: CAPTCHA image element ─────────────────────────────────────────
    hdr("3 — Wait for #captchaImage to be visible and fully loaded")
    try:
        page.wait_for_selector("#captchaImage", state="visible", timeout=15_000)
        page.wait_for_function(
            "() => { const i=document.querySelector('#captchaImage'); return i&&i.complete&&i.naturalWidth>0; }",
            timeout=15_000,
        )
        el = page.query_selector("#captchaImage")
        src = el.get_attribute("src") if el else "(none)"
        nat_w = page.evaluate("document.querySelector('#captchaImage').naturalWidth")
        nat_h = page.evaluate("document.querySelector('#captchaImage').naturalHeight")
        ok(f"#captchaImage found — src={src!r}")
        ok(f"Natural size reported by browser: {nat_w}×{nat_h} px")
    except Exception as e:
        err(f"Waiting for #captchaImage failed: {e}")
        sys.exit(1)

    # ── STEP 4: element.screenshot() ──────────────────────────────────────────
    hdr("4 — Playwright element.screenshot() → PNG bytes")
    try:
        png = el.screenshot()
        w, h = png_dims(png)
        raw_b64 = base64.b64encode(png).decode()
        ok(f"Screenshot captured: {len(png):,} bytes, {w}×{h} px")
        ok(f"Base64 length: {len(raw_b64)} chars")
        # save for inspection
        with open("/tmp/tg_diag_captcha.png", "wb") as f: f.write(png)
        import shutil; shutil.copy("/tmp/tg_diag_captcha.png",
            "/Users/anirvesharcot/Desktop/PrimeNumbers/rera_crawlers/tg_diag_captcha.png")
        ok("Saved to tg_diag_captcha.png (desktop + /tmp)")
    except Exception as e:
        err(f"element.screenshot() failed: {e}")
        sys.exit(1)

    # ── STEP 5: Solver raw response ────────────────────────────────────────────
    hdr("5 — CAPTCHA solver: raw socket call (maharera model, native res)")
    try:
        client = connect_client("captcha.hawker.news", 60)
        payload = {"image_source": raw_b64, "captcha_source": "maharera"}
        send_data(client, payload)
        raw_resp = receive_data(client)
        client.close()
        ok(f"Solver raw response: {raw_resp}")
        solved_text = (raw_resp.get("image_text") or "").strip()
        if solved_text:
            ok(f"Solver returned text: {solved_text!r}")
        else:
            warn("Solver returned EMPTY text for this CAPTCHA image")
    except Exception as e:
        err(f"Solver socket call failed: {e}")
        solved_text = ""

    # ── STEP 6: Full _solve_captcha() helper ──────────────────────────────────
    hdr("6 — _solve_captcha() helper (as used by real crawler)")
    class FakeLogger:
        def info(self,m,**k):  info(f"[logger.info]  {m}")
        def warning(self,m,**k): warn(f"[logger.warn]  {m}")
        def error(self,m,**k):  err(f"[logger.error] {m}")
    fake_log = FakeLogger()
    captcha_answer = _solve_captcha(page, fake_log)
    if captcha_answer:
        ok(f"_solve_captcha() → {captcha_answer!r}")
    else:
        warn("_solve_captcha() → None (solver empty/failed)")

    # ── STEP 7: Manual form submission with screenshot after each attempt ──────
    hdr("7 — Manual CAPTCHA-solve → submit → screenshot (up to 5 attempts)")
    import shutil as _sh

    def save_ss(page, tag: str):
        path = f"/tmp/tg_step7_{tag}.png"
        page.screenshot(path=path, full_page=True)
        desk = f"/Users/anirvesharcot/Desktop/PrimeNumbers/rera_crawlers/tg_step7_{tag}.png"
        _sh.copy(path, desk)
        info(f"Screenshot saved → tg_step7_{tag}.png")

    from sites.telangana_rera import _CAPTCHA_MAX_TRIES, _CAPTCHA_SELECTORS, _NAV_TIMEOUT_MS

    submission_success = False
    for attempt in range(1, _CAPTCHA_MAX_TRIES + 1):
        info(f"--- Attempt {attempt}/{_CAPTCHA_MAX_TRIES} ---")

        # navigate fresh
        page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)

        # solve captcha
        captcha_answer = _solve_captcha(page, fake_log)
        info(f"Solver returned: {captcha_answer!r}")
        if not captcha_answer:
            warn("Skipping attempt — empty solver result")
            continue

        # find input
        captcha_input = None
        for sel in ("input[name*='captcha' i]","input[id*='captcha' i]",
                    "input[placeholder*='captcha' i]","#txtCaptcha","input[type='text']"):
            try:
                captcha_input = page.wait_for_selector(sel, timeout=3_000)
                if captcha_input:
                    info(f"CAPTCHA input found via selector: {sel!r}")
                    break
            except Exception:
                pass
        if not captcha_input:
            warn("Could not find CAPTCHA input field")
            continue

        captcha_input.fill(captcha_answer)
        info(f"Typed {captcha_answer!r} into CAPTCHA field")

        # click Search
        for btn_sel in ("input[value*='Search' i]","button[type='submit']",
                        "input[type='submit']","#btnSearch"):
            try:
                btn = page.query_selector(btn_sel)
                if btn:
                    info(f"Clicking search button ({btn_sel})")
                    btn.click()
                    break
            except Exception:
                pass

        # wait up to 60s for table
        try:
            page.wait_for_selector("table", timeout=60_000)
            ok(f"Results TABLE appeared on attempt {attempt}!")
            save_ss(page, f"a{attempt}_SUCCESS")
            submission_success = True
            break
        except Exception:
            warn(f"No table after 60s — capturing page state")
            page_text = page.inner_text("body")[:600].replace("\n", " ")
            info(f"Page body text snippet: {page_text!r}")
            # look for specific error patterns
            lower = page_text.lower()
            if "captcha is not valid" in lower or "invalid captcha" in lower:
                warn("→ Server says: CAPTCHA IS NOT VALID (wrong answer)")
            elif "no records found" in lower:
                warn("→ Server says: NO RECORDS FOUND (search ran, no data)")
            elif "captcha" in lower:
                warn(f"→ CAPTCHA-related text found in page")
            save_ss(page, f"a{attempt}_fail")

    if not submission_success:
        err(f"All {_CAPTCHA_MAX_TRIES} attempts exhausted — form never returned a table")
        sys.exit(1)
    ok("Form submission succeeded — proceeding to parse")

    # ── STEP 8: Parse listing rows ─────────────────────────────────────────────
    hdr("8 — _parse_listing_rows() — parse first-page HTML")
    html = page.content()
    rows = _parse_listing_rows(html)
    total_pages = _get_total_pages(page)
    ok(f"Total pages reported: {total_pages}")
    ok(f"Rows parsed from page 1: {len(rows)}")
    if rows:
        r0 = rows[0]
        info(f"Row[0] app_id={r0['app_id']!r}  project_id={r0['project_id']!r}")
        info(f"Row[0] listing_texts={r0['listing_texts']}")
        info(f"Row[0] print_preview_url={r0['print_preview_url']!r}")
        info(f"Row[0] cert_url={r0['cert_url']!r}")
    else:
        err("No rows parsed — listing parse failed")
        sys.exit(1)

    # ── STEP 9: Navigate to PrintPreview for row[0] ───────────────────────────
    hdr("9 — Navigate to PrintPreview detail page for first row")
    from sites.telangana_rera import _fetch_print_preview_html, _scrape_print_preview
    pp_url = rows[0]["print_preview_url"]
    info(f"PrintPreview URL: {pp_url}")
    t0 = time.time()
    try:
        pp_html = _fetch_print_preview_html(page, pp_url, fake_log)
        elapsed = time.time() - t0
        if pp_html:
            ok(f"PrintPreview HTML fetched: {len(pp_html):,} chars in {elapsed:.1f}s")
        else:
            err(f"PrintPreview HTML empty after {elapsed:.1f}s")
            sys.exit(1)
    except Exception as e:
        err(f"_fetch_print_preview_html() raised: {e}")
        sys.exit(1)

    # ── STEP 10: Parse PrintPreview detail ────────────────────────────────────
    hdr("10 — _scrape_print_preview() — parse detail HTML")
    try:
        soup = BeautifulSoup(pp_html, "lxml")
        detail = _scrape_print_preview(soup, rows[0])
        ok(f"Detail parsed — {len(detail)} top-level keys")
        for k, v in detail.items():
            if isinstance(v, str):
                info(f"  {k}: {v[:120]!r}")
            elif isinstance(v, list):
                info(f"  {k}: [{len(v)} items]")
            else:
                info(f"  {k}: {v}")
    except Exception as e:
        err(f"_scrape_print_preview() raised: {e}")
        import traceback; traceback.print_exc()

print("\n" + "=" * 70)
print("  DIAGNOSTIC COMPLETE")
print("=" * 70)
