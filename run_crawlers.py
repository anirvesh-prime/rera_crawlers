#!/usr/bin/env python3
"""
RERA Crawlers Orchestrator — runs all enabled states.

Usage:
    python run_crawlers.py                      # runs all states
    python run_crawlers.py --site kerala_rera   # runs one state
    python run_crawlers.py --mode weekly_deep   # explicit mode (default: weekly_deep)
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Must be set before any import that uses hash()
os.environ.setdefault("PYTHONHASHSEED", "0")

from core.config import settings
from core.db import insert_crawl_run, update_crawl_run, insert_crawl_error
from core.logger import CrawlerLogger
from sites_config import SITES

_SEP = "=" * 64


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RERA Crawlers Orchestrator")
    parser.add_argument(
        "--mode",
        choices=["daily_light", "weekly_deep"],
        default="weekly_deep",
        help="Crawl mode (default: weekly_deep)",
    )
    parser.add_argument(
        "--site",
        default=None,
        help="Run only this site_id, e.g. kerala_rera (default: all enabled)",
    )
    return parser.parse_args()


def run_site(site_cfg: dict, mode: str) -> dict:
    site_id = site_cfg["id"]
    run_id  = insert_crawl_run(site_id, mode)
    logger  = CrawlerLogger(site_id, run_id)
    logger.info(f"Starting {mode} crawl", site=site_id, run_id=run_id)

    counts = {
        "projects_found":   0,
        "projects_new":     0,
        "projects_updated": 0,
        "projects_skipped": 0,
        "documents_uploaded": 0,
        "error_count":      0,
    }

    t0 = time.monotonic()
    try:
        module = importlib.import_module(site_cfg["crawler_module"])
        result = module.run(site_cfg, run_id, mode)
        counts.update(result)
        update_crawl_run(run_id, "completed", counts, notes=None)
        logger.info("Crawl completed", **counts)
    except Exception as exc:
        counts["error_count"] += 1
        update_crawl_run(run_id, "failed", counts)
        insert_crawl_error(run_id, site_id, "CRAWLER_EXCEPTION", str(exc))
        logger.error(f"Crawl failed: {exc}")

    elapsed = time.monotonic() - t0
    return {"site_id": site_id, "run_id": run_id, "elapsed_s": round(elapsed, 1), **counts}


def _fmt_row(result: dict) -> str:
    return (
        f"  found={result['projects_found']:<5} "
        f"new={result['projects_new']:<5} "
        f"updated={result['projects_updated']:<5} "
        f"skipped={result['projects_skipped']:<5} "
        f"docs={result['documents_uploaded']:<5} "
        f"errors={result['error_count']:<4} "
        f"({result['elapsed_s']}s)"
    )


def ensure_playwright_browsers():
    """Install Playwright Chromium if it isn't already present."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            p.chromium.launch(headless=True).close()
    except Exception:
        print("Playwright browser not found — installing chromium...")
        result = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            check=False,
        )
        if result.returncode != 0:
            print("[WARNING] playwright install chromium failed — Odisha crawl may error.")
        else:
            print("Playwright chromium installed successfully.")


def main():
    args = parse_args()
    Path(settings.LOG_DIR).mkdir(parents=True, exist_ok=True)
    ensure_playwright_browsers()

    sites = [s for s in SITES if s["enabled"]]
    if args.site:
        sites = [s for s in sites if s["id"] == args.site]
        if not sites:
            print(f"[ERROR] Site '{args.site}' not found or not enabled.")
            return

    started = datetime.now(timezone.utc)
    print(f"\n{_SEP}")
    print(f"  RERA Crawler Orchestrator")
    print(f"  Mode   : {args.mode}")
    print(f"  Host   : {socket.gethostname()}")
    print(f"  States : {', '.join(s['id'] for s in sites)}")
    print(f"  Started: {started.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{_SEP}\n")

    summary = []
    for site_cfg in sites:
        print(f"→ [{site_cfg['id']}]  {site_cfg['name']}")
        result = run_site(site_cfg, args.mode)
        summary.append(result)
        print(f"{_fmt_row(result)}\n")

    # Totals
    totals = {k: sum(r.get(k, 0) for r in summary)
              for k in ("projects_found", "projects_new", "projects_updated",
                        "projects_skipped", "documents_uploaded", "error_count", "elapsed_s")}

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    summary_path = Path(settings.LOG_DIR) / f"{ts}_orchestrator_summary.json"
    summary_path.write_text(
        json.dumps({"mode": args.mode, "started": started.isoformat(),
                    "sites": summary, "totals": totals}, indent=2, default=str)
    )

    print(f"{_SEP}")
    print(f"  TOTALS  {_fmt_row({'site_id': 'all', 'run_id': 0, **totals})}")
    print(f"  Summary: {summary_path}")
    print(f"{_SEP}\n")


if __name__ == "__main__":
    main()
