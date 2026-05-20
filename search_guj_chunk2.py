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

# Find surrounding context for get-district-wise-projects (without "list")
idx = js.find("get-district-wise-projects/")
if idx >= 0:
    snippet = js[max(0, idx-200):idx+300]
    print("Context around 'get-district-wise-projects/':")
    print(f"  {snippet}")
    print()

# Find surrounding context for get-district-wise-projectlist
idx2 = js.find("get-district-wise-projectlist/")
if idx2 >= 0:
    snippet2 = js[max(0, idx2-200):idx2+400]
    print("Context around 'get-district-wise-projectlist/':")
    print(f"  {snippet2}")
    print()

# Try the projects vs projectlist difference
print("\nAll occurrences of get-district-wise:")
for m in re.finditer(r'get-district-wise-[a-z]+[^"\']*', js):
    ctx = js[max(0, m.start()-50):m.end()+50]
    print(f"  ...{ctx}...")
    print()
