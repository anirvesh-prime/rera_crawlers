"""
Inspect the live MH RERA sentinel project page to understand why
acknowledgement_no, professional_information, project_cost_detail,
and promoter_contact_details are missing from the fresh scrape.

Runs Playwright, solves the CAPTCHA, then dumps:
  1. The HTML snippet for each relevant section
  2. What the parser actually extracts
"""
import base64, json, re, sys
sys.path.insert(0, ".")

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from core.captcha_solver import captcha_to_text, solve_captcha_from_page, wait_for_captcha_canvas
from core.logger import CrawlerLogger

CERT_ID  = "61699"
DETAIL_BASE = "https://maharerait.maharashtra.gov.in"
URL = f"{DETAIL_BASE}/public/project/view/{CERT_ID}"

_CAPTCHA_INTERCEPT_SCRIPT = """
(function() {
    var origFillText = CanvasRenderingContext2D.prototype.fillText;
    window.__captchaTexts = [];
    CanvasRenderingContext2D.prototype.fillText = function(text, x, y, maxWidth) {
        if (typeof text === 'string' && text.length > 0 && text.trim().length > 0) {
            window.__captchaTexts.push(text);
        }
        return origFillText.apply(this, arguments);
    };
})();
"""

logger = CrawlerLogger(site_id="mh_inspect", run_id=0)

def solve_captcha(page):
    for attempt in range(1, 11):
        print(f"[captcha] attempt {attempt}")
        wait_for_captcha_canvas(page, "canvas", timeout_ms=20_000, logger=logger)
        texts = page.evaluate("() => window.__captchaTexts || []")
        val = "".join(texts).strip() or None
        if not val:
            canvas_el = page.query_selector("canvas")
            if canvas_el:
                img_bytes = canvas_el.screenshot()
                img_b64 = base64.b64encode(img_bytes).decode()
                val = captcha_to_text(f"data:image/png;base64,{img_b64}", default_captcha_source="model_captcha").strip() or None
        if not val:
            page.reload(timeout=45_000); continue
        print(f"[captcha] value={val!r}")
        page.fill("input[name='captcha']", val)
        page.click("button.next")
        try: page.wait_for_load_state("networkidle", timeout=10_000)
        except: pass
        try:
            invalid = page.query_selector("h2:text('Captcha is not valid.')")
            if invalid and invalid.is_visible():
                print("[captcha] invalid — reloading")
                try: page.click("button.confirm", timeout=3_000)
                except: pass
                page.reload(timeout=45_000); continue
        except: pass
        try:
            page.wait_for_selector("label.bg-blue.f-w-700", timeout=20_000)
            print("[captcha] SOLVED — Angular loaded")
            return True
        except: pass
        try:
            page.wait_for_selector("label.form-label, .col-md-4 .f-s-15", timeout=5_000)
            print("[captcha] SOLVED — fallback")
            return True
        except: pass
        page.reload(timeout=45_000)
    return False

def dump_section(soup, label):
    el = soup.find(["h5","div","b","h4"], string=lambda t: t and label.lower() in t.strip().lower())
    if not el:
        print(f"\n[SECTION] '{label}' — NOT FOUND in HTML")
        return
    # Walk up to nearest white-box / card-body
    container = None
    for ancestor in el.parents:
        if ancestor.name != "div": continue
        cls = ancestor.get("class") or []
        if any(c in cls for c in ("white-box","card-body","wh")):
            container = ancestor; break
    if not container:
        container = el.find_parent("div")
    print(f"\n[SECTION] '{label}' found — container classes={container.get('class') if container else 'N/A'}")
    print(container.get_text(separator=" | ", strip=True)[:500] if container else "(none)")

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 800, "height": 800},
    )
    ctx.add_init_script(_CAPTCHA_INTERCEPT_SCRIPT)
    page = ctx.new_page()
    page.goto(URL, timeout=45_000)

    if not solve_captcha(page):
        print("CAPTCHA FAILED — cannot inspect page")
        browser.close()
        sys.exit(1)

    try: page.wait_for_load_state("networkidle", timeout=15_000)
    except: pass
    try: page.wait_for_selector("label.bg-blue.f-w-700", timeout=20_000)
    except: pass
    try: page.wait_for_load_state("networkidle", timeout=10_000)
    except: pass

    html = page.content()
    with open("screenshots/mh_sentinel_live.html", "w") as f:
        f.write(html)
    print("\n[INFO] Full HTML saved to screenshots/mh_sentinel_live.html")

    soup = BeautifulSoup(html, "lxml")

    # ── 1. Acknowledgement No ────────────────────────────────────────────────
    print("\n=== ACKNOWLEDGEMENT NO ===")
    ack_labels = [el for el in soup.select(".col-md-4 .f-s-15, label.form-label")
                  if "acknowledgement" in el.get_text(strip=True).lower()]
    print(f"Found {len(ack_labels)} elements with 'acknowledgement' text:")
    for el in ack_labels:
        parent = el.find_parent()
        val = parent.select_one(".f-w-700") if parent else None
        print(f"  tag={el.name} text={el.get_text(strip=True)!r}  value={val.get_text(strip=True) if val else 'NOT FOUND'}")

    # ── 2. Project Cost / Construction Cost ──────────────────────────────────
    print("\n=== PROJECT COST ===")
    cost_labels = [el for el in soup.select("label.form-label:not(.col-4)")
                   if any(k in el.get_text(strip=True).lower() for k in
                          ("cost","construction cost","project cost"))]
    print(f"Found {len(cost_labels)} cost-related labels:")
    for el in cost_labels:
        parent = el.find_parent()
        val = parent.select_one(".f-w-700") if parent else None
        print(f"  text={el.get_text(strip=True)!r}  value={val.get_text(strip=True) if val else 'NOT FOUND'}")

    # ── 3. Promoter Contact (phone/email) ────────────────────────────────────
    print("\n=== PROMOTER CONTACT ===")
    contact_labels = [el for el in soup.select("label.form-label.col-4")
                      if any(k in el.get_text(strip=True).lower() for k in ("phone","mobile","email"))]
    print(f"Found {len(contact_labels)} phone/email labels:")
    for el in contact_labels:
        row = el.find_parent("div", class_="row")
        val = row.select_one(".col-8 .f-w-700") if row else None
        print(f"  text={el.get_text(strip=True)!r}  value={val.get_text(strip=True) if val else 'NOT FOUND'}")

    # ── 4. Professional Information table ────────────────────────────────────
    print("\n=== PROFESSIONAL INFORMATION ===")
    prof_tables = [t for t in soup.select("table")
                   if any("professional name" in th.get_text(strip=True).lower() for th in t.select("thead th"))]
    print(f"Found {len(prof_tables)} 'Professional Name' table(s)")
    for t in prof_tables:
        headers = [th.get_text(strip=True) for th in t.select("thead th")]
        rows = t.select("tbody tr")
        print(f"  headers={headers}  rows={len(rows)}")
        for r in rows[:3]:
            print(f"    {[td.get_text(strip=True) for td in r.select('td')]}")

    # ── Section containers ────────────────────────────────────────────────────
    for sec in ["Project Address Details", "Promoter Details",
                "Promoter Official Communication Address", "Bank Details",
                "Project Professionals", "Designated Partner"]:
        dump_section(soup, sec)

    browser.close()

print("\n[DONE]")
