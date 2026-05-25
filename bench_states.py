#!/usr/bin/env python3
"""
Benchmark the seven slow target RERA crawlers in test mode.

Runs each site individually via run_crawlers.py --test --sequential with a
bounded MAX_PAGES + CRAWL_ITEM_LIMIT so wall-clock comparisons are repeatable
between before/after refactor runs.

Usage:
    venv/bin/python bench_states.py [--item-limit 10] [--max-pages 2]
                                    [--site kerala_rera ...]
                                    [--output bench_results.json]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

DEFAULT_SITES = [
    "kerala_rera",
    "odisha_rera",
    "rajasthan_rera",
    "bihar_rera",
    "maharashtra_rera",
    "karnataka_rera",
    "telangana_rera",
]

REPO_ROOT = Path(__file__).resolve().parent
PYTHON = str(REPO_ROOT / "venv" / "bin" / "python")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--item-limit", type=int, default=10)
    p.add_argument("--max-pages", type=int, default=2)
    p.add_argument("--site", action="append", default=[])
    p.add_argument("--output", default="bench_results.json")
    p.add_argument(
        "--timeout",
        type=int,
        default=900,
        help="Hard wall-clock timeout per state in seconds (default 900).",
    )
    return p.parse_args()


def run_one(site_id: str, item_limit: int, max_pages: int, timeout: int) -> dict:
    env = os.environ.copy()
    env["PYTHONHASHSEED"] = "0"
    env["MAX_PAGES"] = str(max_pages)
    env["CRAWL_ITEM_LIMIT"] = str(item_limit)
    env["TEST_MODE"] = "true"
    env["DRY_RUN_S3"] = "true"

    cmd = [
        PYTHON,
        "run_crawlers.py",
        "--test",
        "--sequential",
        "--site",
        site_id,
        "--item-limit",
        str(item_limit),
    ]
    print(f"\n=== Benchmarking {site_id} (max_pages={max_pages}, item_limit={item_limit}) ===")
    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            env=env,
            cwd=REPO_ROOT,
            timeout=timeout,
            capture_output=True,
            text=True,
        )
        elapsed = time.monotonic() - t0
        status = "completed" if proc.returncode == 0 else "failed"
        tail = (proc.stdout or "").splitlines()[-25:]
        err_tail = (proc.stderr or "").splitlines()[-10:]
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - t0
        status = "timeout"
        tail = []
        err_tail = []
    print(f"[{site_id}] {status} in {elapsed:.2f}s")
    for line in tail:
        print(f"  | {line}")
    return {
        "site": site_id,
        "status": status,
        "elapsed_s": round(elapsed, 2),
        "max_pages": max_pages,
        "item_limit": item_limit,
        "stdout_tail": tail,
        "stderr_tail": err_tail,
    }


def main() -> None:
    args = parse_args()
    sites = args.site or DEFAULT_SITES
    results = []
    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for site in sites:
        r = run_one(site, args.item_limit, args.max_pages, args.timeout)
        results.append(r)
    summary = {
        "started_at": started_at,
        "item_limit": args.item_limit,
        "max_pages": args.max_pages,
        "results": results,
        "totals": {
            "elapsed_s": round(sum(r["elapsed_s"] for r in results), 2),
        },
    }
    Path(args.output).write_text(json.dumps(summary, indent=2))
    print("\n=== SUMMARY ===")
    for r in results:
        print(f"  {r['site']:<22} {r['status']:<10} {r['elapsed_s']:>8.2f}s")
    print(f"  {'TOTAL':<22} {'':<10} {summary['totals']['elapsed_s']:>8.2f}s")
    print(f"\nResults written to {args.output}")


if __name__ == "__main__":
    main()
