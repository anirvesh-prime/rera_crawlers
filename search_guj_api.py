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

# Find all dashboard API paths
patterns3 = re.findall(r'"([^"]*dashboard[^"]*)"', js, re.IGNORECASE)
print("dashboard API paths:")
for p in list(dict.fromkeys(patterns3))[:20]:
    print(f"  {p}")

# Also look for project listing APIs
patterns4 = re.findall(r'"([^"]*project[^"]*list[^"]*)"', js, re.IGNORECASE)
print("\nproject list API paths:")
for p in list(dict.fromkeys(patterns4))[:20]:
    print(f"  {p}")

# Look for all GET/POST API patterns in the angular service
patterns5 = re.findall(r'"([^"]*project_reg[^"]*)"', js, re.IGNORECASE)
print("\nproject_reg API paths:")
for p in list(dict.fromkeys(patterns5))[:20]:
    print(f"  {p}")
