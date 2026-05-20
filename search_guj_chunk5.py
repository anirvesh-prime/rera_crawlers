import re
from playwright.sync_api import sync_playwright

BASE_URL = "https://gujrera.gujarat.gov.in"
js_url = f"{BASE_URL}/main-es2015.7702a0d11649ed3b4e00.js"

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

# Find projectOutstanding definition
for m in re.finditer(r'projectOutstanding[^,;)]{0,100}', js):
    ctx = js[max(0, m.start()-50):m.end()+50]
    print(f"  {ctx}")

# Also look at how the listing component calls the API with pagination
# Find size/pageSize/pagination params
for m in re.finditer(r'pageSize|pageNo|pagination|totalRecord|totalPage', js):
    ctx = js[max(0, m.start()-100):m.end()+100]
    if 'district' in ctx.lower() or 'project' in ctx.lower():
        print(f"  pagination context: {ctx}")
