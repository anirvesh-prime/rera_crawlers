#!/usr/bin/env python3
"""
RERA Crawlers Orchestrator — runs all enabled states in parallel.

Usage:
    python run_crawlers.py                      # runs all states in parallel
    python run_crawlers.py --site kerala_rera   # runs one state
    python run_crawlers.py --site kerala_rera --site bihar_rera
    python run_crawlers.py --site kerala_rera,bihar_rera
    python run_crawlers.py --item-limit 5       # cap each crawler to 5 projects
    python run_crawlers.py --no-item-limit      # override env and run unlimited
    python run_crawlers.py --mode weekly_deep   # explicit mode (default: weekly_deep)
    python run_crawlers.py --sequential         # disable parallel execution
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
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

# Must be set before any import that uses hash()
os.environ.setdefault("PYTHONHASHSEED", "0")

from core.config import settings
from core.db import insert_crawl_run, update_crawl_run, insert_crawl_error
from core.logger import CrawlerLogger
from sites_config import select_sites

_SEP = "=" * 64


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("item limit must be greater than 0")
    return parsed


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
        "--sites",
        action="append",
        default=[],
        help=(
            "Run only the selected site ids. Repeat the flag or pass a comma-separated "
            "list, e.g. --site kerala_rera --site bihar_rera or --site kerala_rera,bihar_rera. "
            "When omitted, all enabled sites run."
        ),
    )
    item_limit_group = parser.add_mutually_exclusive_group()
    item_limit_group.add_argument(
        "--item-limit",
        type=_positive_int,
        default=None,
        help="Cap each selected crawler to this many projects for the current run.",
    )
    item_limit_group.add_argument(
        "--no-item-limit",
        action="store_true",
        default=False,
        help="Ignore CRAWL_ITEM_LIMIT from env/config and run without an item limit for this run.",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        default=False,
        help="Run sites one-by-one instead of in parallel (useful for debugging)",
    )
    return parser.parse_args()


def _worker_init() -> None:
    """Initialiser run inside every worker process before it executes any task.
    Re-applies PYTHONHASHSEED=0 so that generate_project_key() stays deterministic
    even on platforms that use 'spawn' rather than 'fork' for new processes.
    """
    os.environ["PYTHONHASHSEED"] = "0"


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


def ensure_playwright_browsers(sites: list[dict]) -> None:
    """Install Playwright Chromium only when a selected site requires it."""
    if not any(site["crawler_type"] == "playwright" for site in sites):
        return

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


def apply_runtime_overrides(args: argparse.Namespace) -> int:
    """Apply CLI overrides to runtime settings and child worker environment."""
    if args.no_item_limit:
        os.environ.pop("CRAWL_ITEM_LIMIT", None)
        settings.CRAWL_ITEM_LIMIT = 0
        return settings.CRAWL_ITEM_LIMIT

    if args.item_limit is not None:
        os.environ["CRAWL_ITEM_LIMIT"] = str(args.item_limit)
        settings.CRAWL_ITEM_LIMIT = args.item_limit

    return settings.CRAWL_ITEM_LIMIT


def main():
    args = parse_args()
    Path(settings.LOG_DIR).mkdir(parents=True, exist_ok=True)
    item_limit = apply_runtime_overrides(args)

    sites, unknown_sites, disabled_sites = select_sites(args.site)
    if unknown_sites:
        print(f"[ERROR] Unknown site id(s): {', '.join(unknown_sites)}")
        return
    if not sites:
        print("[ERROR] No sites selected to run.")
        return

    ensure_playwright_browsers(sites)

    parallel = not args.sequential and len(sites) > 1

    started = datetime.now(timezone.utc)
    print(f"\n{_SEP}")
    print(f"  RERA Crawler Orchestrator")
    print(f"  Mode      : {args.mode}")
    print(f"  Execution : {'parallel' if parallel else 'sequential'}")
    print(f"  Workers   : {len(sites)}")
    print(f"  Item Limit: {item_limit or 'unlimited'}")
    print(f"  Host      : {socket.gethostname()}")
    print(f"  States    : {', '.join(s['id'] for s in sites)}")
    print(f"  Started   : {started.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if disabled_sites:
        print(f"  Disabled  : {', '.join(disabled_sites)} (explicitly selected)")
    print(f"{_SEP}\n")

    summary = []

    if parallel:
        print(f"→ Launching {len(sites)} crawlers in parallel...\n")
        with ProcessPoolExecutor(max_workers=len(sites), initializer=_worker_init) as executor:
            futures = {
                executor.submit(run_site, site_cfg, args.mode): site_cfg["id"]
                for site_cfg in sites
            }
            for future in as_completed(futures):
                site_id = futures[future]
                try:
                    result = future.result()
                    summary.append(result)
                    print(f"✓ [{site_id}] finished")
                    print(f"{_fmt_row(result)}\n")
                except Exception as exc:
                    print(f"✗ [{site_id}] worker process crashed: {exc}\n")
    else:
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
    summary_path = Path(settings.LOG_DIR) / "orchestrator" / f"{ts}_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
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
