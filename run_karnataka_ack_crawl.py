#!/usr/bin/env python3
"""
Targeted Karnataka RERA deep crawl — processes all acknowledgement numbers
from keys.txt using the existing crawler internals. No changes to any existing file.

Usage:
    python run_karnataka_ack_crawl.py                          # run all 613 keys
    python run_karnataka_ack_crawl.py --test                   # dry-run (no S3/DB writes)
    python run_karnataka_ack_crawl.py --resume-from <ack_no>   # skip keys before this one
    python run_karnataka_ack_crawl.py --keys-file /other/keys.txt
"""
import sys
import os

# Must be set before any import — makes generate_project_key() deterministic.
if os.environ.get("PYTHONHASHSEED") != "0":
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = "0"
    os.execvpe(sys.executable, [sys.executable, *sys.argv], env)

import argparse
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from core.config import settings
from core.db import insert_crawl_run, update_crawl_run
from core.logger import CrawlerLogger
from core.project_normalizer import get_machine_context
from sites import karnataka_rera
from sites_config import select_sites

KEYS_FILE = Path(__file__).parent / "keys.txt"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Targeted Karnataka RERA deep crawl by acknowledgement number"
    )
    parser.add_argument(
        "--test", action="store_true",
        help="Dry run — skip all S3 uploads and DB writes",
    )
    parser.add_argument(
        "--keys-file", default=str(KEYS_FILE),
        help=f"Path to the acknowledgement-number keys file (default: {KEYS_FILE})",
    )
    parser.add_argument(
        "--resume-from", default=None, metavar="ACK_NO",
        help="Start from this acknowledgement number (inclusive); skip all earlier keys",
    )
    args = parser.parse_args()

    # ── Test-mode wiring ──────────────────────────────────────────────────────
    if args.test:
        os.environ["TEST_MODE"] = "true"
        os.environ["DRY_RUN_S3"] = "true"
        settings.TEST_MODE = True
        settings.DRY_RUN_S3 = True

    # ── Load keys ─────────────────────────────────────────────────────────────
    keys_path = Path(args.keys_file)
    if not keys_path.exists():
        print(f"[ERROR] Keys file not found: {keys_path}")
        sys.exit(1)

    keys = [k.strip() for k in keys_path.read_text().splitlines() if k.strip()]
    if not keys:
        print(f"[ERROR] No keys found in {keys_path}")
        sys.exit(1)

    if args.resume_from:
        if args.resume_from in keys:
            start_idx = keys.index(args.resume_from)
            keys = keys[start_idx:]
            print(f"Resuming from key #{start_idx + 1}: {args.resume_from} ({len(keys)} remaining)")
        else:
            print(f"[WARN] resume key {args.resume_from!r} not found in keys file — running all {len(keys)} keys")

    # ── Resolve Karnataka site config ─────────────────────────────────────────
    sites, unknown, _ = select_sites(["karnataka_rera"])
    if unknown or not sites:
        print("[ERROR] karnataka_rera not found in sites_config.py")
        sys.exit(1)
    config = sites[0]

    machine_name, machine_ip = get_machine_context()
    site_id = config["id"]
    state   = config.get("state", "karnataka")
    total   = len(keys)

    overall = dict(projects_new=0, projects_updated=0, projects_skipped=0,
                   documents_uploaded=0, error_count=0)

    print(f"\n{'='*60}")
    print(f"  Karnataka RERA — Targeted Ack-Number Deep Crawl")
    print(f"  Keys file : {keys_path}")
    print(f"  Keys total: {total}")
    if args.test:
        print(f"  Mode      : TEST (no S3 / no DB writes)")
    print(f"{'='*60}\n")

    t_start = time.monotonic()
    try:
        for i, ack_no in enumerate(keys, 1):
            print(f"[{i}/{total}] {ack_no}", end="  ", flush=True)

            run_id = insert_crawl_run(site_id, "weekly_deep")
            logger = CrawlerLogger(site_id, run_id)

            # Synthetic listing row — only acknowledgement_no is known up front;
            # the rest is populated by _fetch_detail + _parse_detail from the portal.
            listing_row = {
                "acknowledgement_no":       ack_no,
                "project_registration_no":  None,
                "project_name":             None,
                "promoter_name":            None,
                "promoter_registration_no": None,
                "project_city":             "",
                "project_location_raw":     {},
                "data": {
                    "search_district":      "",
                    "listing_fallback":     True,
                    "listing_status":       "",
                    "listing_project_type": "",
                    "listing_approved_on":  "",
                    "targeted_ack_crawl":   True,
                },
            }

            try:
                # district_idx=0 only affects data.district fallback label;
                # the real district is parsed from the detail page HTML.
                deltas = karnataka_rera._process_candidate(
                    0, 0, listing_row, config, run_id, site_id,
                    "weekly_deep", machine_name, machine_ip, state, logger,
                )
            except Exception as exc:
                print(f"ERROR: {exc}")
                deltas = {"error_count": 1}

            counts = {
                "projects_found":     1,
                "projects_new":       deltas.get("projects_new", 0),
                "projects_updated":   deltas.get("projects_updated", 0),
                "projects_skipped":   deltas.get("projects_skipped", 0),
                "documents_uploaded": deltas.get("documents_uploaded", 0),
                "error_count":        deltas.get("error_count", 0),
            }
            update_crawl_run(run_id, "completed", counts)
            logger.close()

            for k in overall:
                overall[k] += deltas.get(k, 0)

            tag  = ("NEW"     if deltas.get("projects_new") else
                    "UPDATED" if deltas.get("projects_updated") else
                    "SKIPPED" if deltas.get("projects_skipped") else
                    "ERROR")
            docs = deltas.get("documents_uploaded", 0)
            print(f"→ {tag}  docs={docs}")

    finally:
        # Always tear down the Selenium driver, even if interrupted.
        karnataka_rera._quit_driver()

    elapsed = time.monotonic() - t_start
    print(f"\n{'='*60}")
    print(f"  DONE — {total} keys processed in {elapsed:.0f}s")
    print(
        f"  new={overall['projects_new']}  "
        f"updated={overall['projects_updated']}  "
        f"skipped={overall['projects_skipped']}  "
        f"docs={overall['documents_uploaded']}  "
        f"errors={overall['error_count']}"
    )
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
