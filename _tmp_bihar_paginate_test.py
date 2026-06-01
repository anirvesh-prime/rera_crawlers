"""Temporary harness to reproduce Bihar RERA pagination beyond ~20 projects.

Exercises only _collect_listing_pages (the Selenium listing walker), bypassing
the DB/sentinel/details phases. Run with the venv python.
"""
import os
os.environ.setdefault("CRAWLER_TESTER", "true")

from core.logger import CrawlerLogger
from sites import bihar_rera


def main():
    logger = CrawlerLogger("bihar_rera", run_id=0)
    try:
        # Walk enough pages to need pagination past page 2 (capture popups too,
        # since that's the real-world path that seems to break).
        pages = bihar_rera._collect_listing_pages(
            logger,
            max_items=30,
            max_pages=None,
            capture_detail_urls=True,
        )
    finally:
        bihar_rera._quit_driver()

    print("\n==== RESULT ====")
    total_rows = sum(len(p["rows"]) for p in pages)
    total_urls = sum(1 for p in pages for u in p["detail_urls"] if u)
    print(f"pages collected: {len(pages)}")
    print(f"total rows: {total_rows}")
    print(f"total detail urls: {total_urls}")
    for p in pages:
        print(f"  page {p['page']}: rows={len(p['rows'])} "
              f"urls={sum(1 for u in p['detail_urls'] if u)}")


if __name__ == "__main__":
    main()
