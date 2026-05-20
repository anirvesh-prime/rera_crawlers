import re
from playwright.sync_api import sync_playwright

BASE_URL = "https://gujrera.gujarat.gov.in"
js_url = f"{BASE_URL}/35-es2015.31d9aa7052d65aae0694.js"

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    ctx2 = browser.new_context(ignore_https_errors=True)
    page = ctx2.new_page()
    page.goto(f"{BASE_URL}/", timeout=30_000, wait_until="domcontentloaded")
    js = page.evaluate(f"""async () => {{
        const r = await fetch('{js_url}');
        return await r.text();
    }}""")
    ctx2.close()
    browser.close()

# Find all calls to approvedProjectList
for m in re.finditer(r'approvedProjectList\([^)]{0,200}\)', js):
    ctx = js[max(0, m.start()-300):m.end()+100]
    print(f"Call: {m.group()}")
    print(f"Context: ...{ctx}...")
    print()

# Also look for the component that handles registered-project-listing
idx = js.find("registered-project-listing")
while idx >= 0:
    snippet = js[max(0, idx-100):idx+500]
    # Only show if it's near API code
    if "approvedProjectList" in snippet or "get-district" in snippet:
        print(f"Near registered-project-listing (pos {idx}):")
        print(f"  {snippet}")
        print()
    idx = js.find("registered-project-listing", idx+1)
