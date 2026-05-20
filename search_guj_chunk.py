import re
from playwright.sync_api import sync_playwright

BASE_URL = "https://gujrera.gujarat.gov.in"
# The lazy chunk that likely has the registered-project-listing component
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

# Find the API path for district-wise listing
patterns = re.findall(r'["\']([^"\']*get-district[^"\']*)["\']', js)
print("get-district patterns:")
for p in list(dict.fromkeys(patterns))[:15]:
    print(f"  {p}")

# Look for all public API endpoints
patterns2 = re.findall(r'["\']([^"\']*public[^"\']{3,60})["\']', js)
print("\npublic API paths:")
for p in list(dict.fromkeys(patterns2))[:20]:
    print(f"  {p}")

# Look for "registered" keyword
patterns3 = re.findall(r'["\']([^"\']*registered[^"\']{3,80})["\']', js)
print("\nregistered API/route patterns:")
for p in list(dict.fromkeys(patterns3))[:15]:
    print(f"  {p}")

# Look for getTableService or getProjectService patterns  
patterns4 = re.findall(r'getTableService|getProjectService|districtwise|getDistrictList', js)
print(f"\nService function calls: {list(dict.fromkeys(patterns4))}")
