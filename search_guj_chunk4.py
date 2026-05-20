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

# Find where getChangedData is called (initial load)
for m in re.finditer(r'getChangedData\([^)]{0,300}\)', js):
    ctx = js[max(0, m.start()-300):m.end()+50]
    print(f"getChangedData call: {m.group()}")
    print(f"Context: ...{ctx[-400:]}...")
    print()

# Find ngOnInit for the listing component
idx = js.find("registered-project-listing", 5000)
if idx > 0:
    # Look for the class/component around this
    snippet = js[max(0, idx-3000):idx+100]
    # Find ngOnInit
    oi = snippet.rfind("ngOnInit")
    if oi >= 0:
        print(f"ngOnInit near listing route:")
        print(snippet[oi:oi+600])
