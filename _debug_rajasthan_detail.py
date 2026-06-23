#!/usr/bin/env python3
"""
Diagnostic script: navigate to the Rajasthan RERA sentinel project detail page
and dump the rendered HTML structure to understand what changed.

Run from the project root:
    python _debug_rajasthan_detail.py
"""
import sys, os, json, time
sys.path.insert(0, os.path.dirname(__file__))

from core.crawler_base import SeleniumSession, page_adapter
from bs4 import BeautifulSoup

DETAIL_URL = "https://rera.rajasthan.gov.in/ProjectDetail?id=A4LeF1gt6aXRt85tGnk3AQ"
LISTING_URL = "https://rera.rajasthan.gov.in/ProjectList?status=3"
OUT_FILE = "/tmp/rajasthan_detail_debug.html"
OUT_LINKS = "/tmp/rajasthan_detail_links.json"


def main():
    print(f"[DEBUG] Starting Selenium session")
    sess = SeleniumSession(ignore_certificate_errors=True)
    page = page_adapter(sess)

    print(f"[DEBUG] Navigating to: {DETAIL_URL}")
    page.goto(DETAIL_URL, timeout=60_000)

    # Wait for Angular to render
    print("[DEBUG] Waiting 8s for Angular to render...")
    page.wait_for_timeout(8_000)

    # Try to click any inactive tabs
    print("[DEBUG] Attempting to click tabs...")
    tab_selectors = [
        "div.tab:not(.selected)",
        "li.nav-item a.nav-link:not(.active)",
        "[role='tab'][aria-selected='false']",
        ".mat-tab-label:not(.mat-tab-label-active)",
        "mat-tab-header .mat-tab-label",
    ]
    for sel in tab_selectors:
        try:
            tabs = page.locator(sel).all()
            for tab in tabs[:5]:
                try:
                    tab.click(timeout=3_000)
                    page.wait_for_timeout(1_000)
                    print(f"  Clicked tab with selector: {sel}")
                except Exception:
                    pass
        except Exception:
            pass

    page.wait_for_timeout(2_000)

    # Dump all links
    all_links = page.evaluate("""() => {
        return Array.from(document.querySelectorAll('a[href]')).map(a => ({
            text: a.textContent.trim().substring(0, 80),
            href: a.href
        })).filter(l => l.href.length > 0);
    }""")
    print(f"\n[DEBUG] Found {len(all_links)} links on page:")
    viewproject_links = [l for l in all_links if 'ViewProject' in l['href'] or 'viewproject' in l['href'].lower()]
    print(f"  ViewProject links: {len(viewproject_links)}")
    for l in viewproject_links:
        print(f"    {l}")

    print(f"\n  Sample of ALL links (first 30):")
    for l in all_links[:30]:
        print(f"    href={l['href'][:100]}  text={l['text'][:50]}")

    with open(OUT_LINKS, 'w') as f:
        json.dump(all_links, f, indent=2)
    print(f"\n[DEBUG] All links saved to {OUT_LINKS}")

    # Dump div.details structure
    details = page.evaluate("""() => {
        return Array.from(document.querySelectorAll('div.details')).map(d => ({
            label: d.querySelector('.label') ? d.querySelector('.label').textContent.trim() : '',
            value: d.querySelector('.value') ? d.querySelector('.value').textContent.trim().substring(0, 100) : '',
            hasLink: !!d.querySelector('a[href]'),
            linkHref: d.querySelector('a[href]') ? d.querySelector('a[href]').href : ''
        }));
    }""")
    print(f"\n[DEBUG] Found {len(details)} div.details elements:")
    for d in details[:20]:
        print(f"  label={d['label']!r}  value={d['value'][:60]!r}  hasLink={d['hasLink']}  linkHref={d['linkHref'][:80]!r}")

    # Save full HTML
    html = page.content()
    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\n[DEBUG] Full HTML ({len(html)} bytes) saved to {OUT_FILE}")

    # Parse with BS4 to check div.details count
    soup = BeautifulSoup(html, 'lxml')
    all_divs_details = soup.find_all('div', class_='details')
    print(f"[DEBUG] BS4 found {len(all_divs_details)} div.details")

    # Check for common alternative structures
    print("\n[DEBUG] Checking for common Angular/Angular Material structures:")
    print(f"  mat-tab-group: {len(soup.find_all('mat-tab-group'))}")
    print(f"  mat-tab-header: {len(soup.find_all('mat-tab-header'))}")
    print(f"  mat-card: {len(soup.find_all('mat-card'))}")
    print(f"  table elements: {len(soup.find_all('table'))}")
    print(f"  tr elements: {len(soup.find_all('tr'))}")

    # Check what buttons/links might lead to ViewProject
    print("\n[DEBUG] Buttons on page:")
    buttons = page.evaluate("""() => {
        return Array.from(document.querySelectorAll('button')).map(b => ({
            text: b.textContent.trim().substring(0, 80),
            type: b.type,
            onclick: b.getAttribute('onclick') || ''
        }));
    }""")
    for b in buttons[:20]:
        print(f"  text={b['text']!r}  type={b['type']!r}  onclick={b['onclick']!r}")

    sess.quit()
    print("\n[DEBUG] Done.")


if __name__ == "__main__":
    main()
