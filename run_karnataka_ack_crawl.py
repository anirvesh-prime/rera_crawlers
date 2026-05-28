#!/usr/bin/env python3
"""
Targeted Karnataka RERA deep crawl — for each DB key in keys.txt, looks up
the project_registration_no from rera_projects, then runs the same flow as
--target-reg-no: _search_by_reg_no (portal reg-no lookup) → _process_candidate
(full detail fetch + upsert + S3 docs). No changes to any existing file.

Usage:
    python run_karnataka_ack_crawl.py                          # run all keys
    python run_karnataka_ack_crawl.py --test                   # dry-run (no S3/DB writes)
    python run_karnataka_ack_crawl.py --resume-from <db_key>   # skip keys before this one
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
from core.db import get_connection, insert_crawl_run, update_crawl_run
from core.logger import CrawlerLogger
from core.project_normalizer import get_machine_context
from sites import karnataka_rera
from sites_config import select_sites

KEYS_FILE = Path(__file__).parent / "keys.txt"


def _resolve_reg_nos(keys: list[str]) -> list[dict]:
    """
    Batch-fetch project_registration_no (and project_name for logging) from
    rera_projects for every DB key. Returns rows in input order; keys with no
    matching DB row get {"key": k, "_not_found": True}.
    """
    if not keys:
        return []
    conn = get_connection()
    placeholders = ", ".join(["%s"] * len(keys))
    rows = conn.execute(
        f"SELECT key, project_registration_no, project_name "
        f"FROM rera_projects WHERE key IN ({placeholders})",
        keys,
    ).fetchall()
    row_by_key = {r["key"]: r for r in rows}
    resolved = []
    for k in keys:
        row = row_by_key.get(k)
        if row:
            resolved.append(dict(row))
        else:
            resolved.append({"key": k, "_not_found": True})
    return resolved


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Targeted Karnataka RERA deep crawl by DB project key"
    )
    parser.add_argument(
        "--test", action="store_true",
        help="Dry run — skip all S3 uploads and DB writes",
    )
    parser.add_argument(
        "--keys-file", default=str(KEYS_FILE),
        help=f"Path to the DB-keys file (default: {KEYS_FILE})",
    )
    parser.add_argument(
        "--resume-from", default=None, metavar="DB_KEY",
        help="Start from this DB key (inclusive); skip all earlier keys",
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
            print(f"[WARN] resume key {args.resume_from!r} not in keys file — running all {len(keys)} keys")

    # ── Resolve DB keys → registration numbers ───────────────────────────────
    print(f"Resolving {len(keys)} DB keys against rera_projects …", flush=True)
    projects = _resolve_reg_nos(keys)
    not_found = [p["key"] for p in projects if p.get("_not_found")]
    if not_found:
        print(f"[WARN] {len(not_found)} keys not found in DB and will be skipped:")
        for k in not_found[:10]:
            print(f"       {k}")
        if len(not_found) > 10:
            print(f"       … and {len(not_found) - 10} more")
    projects = [p for p in projects if not p.get("_not_found")]

    # ── Resolve Karnataka site config ─────────────────────────────────────────
    sites, unknown, _ = select_sites(["karnataka_rera"])
    if unknown or not sites:
        print("[ERROR] karnataka_rera not found in sites_config.py")
        sys.exit(1)
    config = sites[0]

    machine_name, machine_ip = get_machine_context()
    site_id = config["id"]
    state   = config.get("state", "karnataka")
    total   = len(projects)

    overall = dict(projects_new=0, projects_updated=0, projects_skipped=0,
                   documents_uploaded=0, error_count=0)

    print(f"\n{'='*60}")
    print(f"  Karnataka RERA — Targeted DB-Key Deep Crawl")
    print(f"  Keys file  : {keys_path}")
    print(f"  Resolved   : {total}  (skipped {len(not_found)} not found in DB)")
    if args.test:
        print(f"  Mode       : TEST (no S3 / no DB writes)")
    print(f"{'='*60}\n")

    t_start = time.monotonic()
    try:
        for i, proj in enumerate(projects, 1):
            reg_no = proj.get("project_registration_no") or ""
            db_key = proj["key"]

            print(f"[{i}/{total}] key={db_key}  reg_no={reg_no}", end="  ", flush=True)

            if not reg_no:
                print("→ SKIP (no project_registration_no in DB)")
                overall["error_count"] += 1
                continue

            run_id = insert_crawl_run(site_id, "weekly_deep")
            logger = CrawlerLogger(site_id, run_id)

            try:
                # Exact same flow as --target-reg-no:
                # Step 1 — search the portal by registration number to get the
                #           listing row (which includes the ack_no + district).
                listing_row = karnataka_rera._search_by_reg_no(reg_no, logger)
                if listing_row is None:
                    print(f"→ NOT FOUND on portal")
                    overall["error_count"] += 1
                    update_crawl_run(run_id, "completed",
                                     {"projects_found": 0, "error_count": 1})
                    logger.close()
                    continue

                # Step 2 — full detail fetch, parse, upsert, S3 docs.
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
        # Always tear down the Selenium driver, even on Ctrl+C.
        karnataka_rera._quit_driver()

    elapsed = time.monotonic() - t_start
    print(f"\n{'='*60}")
    print(f"  DONE — {total} projects in {elapsed:.0f}s")
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
