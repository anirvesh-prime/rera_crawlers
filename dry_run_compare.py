#!/usr/bin/env python3
"""
dry_run_compare.py

Runs every RERA site crawler in dry-run mode — NO DB writes, NO S3 uploads.
For each site it:
  1. Patches all core.db / core.checkpoint / core.s3 symbols in the crawler
     module's namespace so no real connections are attempted.
  2. Applies CRAWL_ITEM_LIMIT (default 1) so only a few projects are scraped.
  3. Captures the dicts that would have been passed to upsert_project().
  4. Writes the first captured project to  dry_run_outputs/<state>.json
  5. Compares it field-by-field against state_projects_sample/<state>.json
  6. Writes a combined comparison report to dry_run_comparison.json

Usage:
    python dry_run_compare.py                              # all sites, 1 project each
    python dry_run_compare.py kerala_rera                  # one site by id
    python dry_run_compare.py maharashtra_rera --limit 3   # capture 3 projects
    python dry_run_compare.py maharashtra_rera --limit 3 --start-page 4770
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

# ── PYTHONHASHSEED must be fixed before anything imports core ─────────────────
if os.environ.get("PYTHONHASHSEED") != "0":
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = "0"
    os.execvpe(sys.executable, [sys.executable, *sys.argv], env)

from sites_config import SITES  # noqa: E402  (after hash-seed guard)

SAMPLE_DIR  = Path("state_projects_sample")
OUTPUT_DIR  = Path("dry_run_outputs")
REPORT_PATH = Path("dry_run_comparison.json")
FAKE_RUN_ID = 9999

# Fields that are always crawler-infra specific — excluded from comparison diffs
_INFRA_FIELDS = frozenset({
    "key", "url", "domain", "state", "config_id",
    "crawl_machine_ip", "machine_name", "retrieved_on",
    "last_crawled_date", "last_updated", "is_updated",
    "is_duplicate", "iw_processed", "iw_part_processed",
    "checked_updates", "checked_updates_date", "rera_housing_found",
    "is_live", "old_updates", "updated_fields", "data",
})


# ── Patch helpers ─────────────────────────────────────────────────────────────

def _make_patches(module, captured: list[dict], start_page: int = 0) -> list:
    """Return a list of patch objects targeting every DB/checkpoint/s3 symbol
    that exists in the given crawler module's namespace."""

    def _upsert(data: dict) -> str:
        captured.append({k: v for k, v in data.items()})
        return "new"

    # Non-zero start_page is injected via the checkpoint so crawlers begin
    # at that listing page rather than always fetching the oldest records.
    checkpoint_val = {"last_page": start_page - 1} if start_page > 0 else {}

    mocks: dict[str, Any] = {
        # DB writes → captured or no-op
        "upsert_project":    _upsert,
        "insert_crawl_error": MagicMock(),
        "upsert_document":   MagicMock(return_value="uploaded"),
        # DB reads → pretend nothing is in DB (triggers sentinel pass + no skip)
        "get_project_by_key": MagicMock(return_value=None),
        "get_document":       MagicMock(return_value=None),
        # Checkpoints — start_page controls where in the listing we begin
        "load_checkpoint":  MagicMock(return_value=checkpoint_val),
        "save_checkpoint":  MagicMock(),
        "reset_checkpoint": MagicMock(),
        # S3 helpers → fake values (DRY_RUN_S3=True handles upload_document too)
        "get_s3_url": MagicMock(return_value="https://s3.example.com/dry-run"),
    }

    patches = []
    for attr, mock in mocks.items():
        if hasattr(module, attr):
            patches.append(patch.object(module, attr, mock))

    # DbLogHandler does a late "from core.db import bulk_insert_logs" — patch source
    patches.append(patch("core.db.bulk_insert_logs", MagicMock()))

    return patches


# ── Comparison ────────────────────────────────────────────────────────────────

def _is_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, (list, dict)) and len(v) == 0:
        return True
    if isinstance(v, str) and v.strip() in ("", "None", "null", "NA"):
        return True
    return False


def _type_label(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, list):
        return "list"
    if isinstance(v, dict):
        return "dict"
    return "str"


def compare(output: dict, sample: dict) -> dict:
    all_keys = set(output.keys()) | set(sample.keys())
    business_keys = all_keys - _INFRA_FIELDS

    populated_in_sample  = {k for k in business_keys if not _is_empty(sample.get(k))}
    populated_in_output  = {k for k in business_keys if not _is_empty(output.get(k))}

    in_both      = sorted(populated_in_sample & populated_in_output)
    only_sample  = sorted(populated_in_sample - populated_in_output)   # regressions
    only_output  = sorted(populated_in_output - populated_in_sample)   # extras

    type_diffs = {}
    for k in in_both:
        st = _type_label(sample.get(k))
        ot = _type_label(output.get(k))
        if st != ot:
            type_diffs[k] = {"sample_type": st, "output_type": ot}

    n_sample = len(populated_in_sample)
    n_match  = len(in_both)
    pct      = round(100 * n_match / n_sample, 1) if n_sample else 100.0

    return {
        "coverage": f"{n_match}/{n_sample} ({pct}%)",
        "fields_matched": in_both,
        "missing_from_output": only_sample,   # in sample but not produced
        "extra_in_output":     only_output,   # produced but not in sample
        "type_mismatches":     type_diffs,
    }


# ── Per-site runner ───────────────────────────────────────────────────────────

def run_site(site_cfg: dict, limit: int = 1, start_page: int = 0) -> dict:
    from core.config import settings

    site_id   = site_cfg["id"]
    state_key = site_cfg["state"].strip().lower().replace(" ", "_")
    sample_path = SAMPLE_DIR / f"{state_key}.json"
    effective_start_page = start_page or int(site_cfg.get("dry_run_compare_start_page", 0) or 0)

    print(f"\n{'='*60}")
    print(f"  {site_id}  (state={state_key}, enabled={site_cfg['enabled']})")
    print(f"{'='*60}")

    module_path = site_cfg["crawler_module"]
    try:
        module = importlib.import_module(module_path)
    except Exception as e:
        print(f"  [IMPORT ERROR] {e}")
        return {"site_id": site_id, "error": f"import: {e}"}

    captured: list[dict] = []
    patches  = _make_patches(module, captured, start_page=effective_start_page)

    settings.CRAWL_ITEM_LIMIT = limit
    settings.DRY_RUN_S3       = True

    for p in patches:
        p.start()
    try:
        module.run(site_cfg, FAKE_RUN_ID, "weekly_deep")
    except Exception as e:
        print(f"  [RUN ERROR] {e}")
    finally:
        for p in patches:
            try:
                p.stop()
            except Exception:
                pass

    if not captured:
        print("  [WARN] No projects captured — site may have errored or returned 0 items.")
        return {"site_id": site_id, "state": state_key, "captured": 0, "note": "no projects captured"}

    # Merge all upsert_project calls for the same project key into one dict.
    # Many crawlers do a second upsert to add uploaded_documents/document_urls
    # after S3 upload — merging gives a complete view of what ends up in the DB.
    # We only overwrite existing keys when the newer value is non-null/non-empty
    # so that a sparse "documents only" second call never clears full project data.
    from collections import OrderedDict
    by_key: "OrderedDict[str, dict]" = OrderedDict()
    for cap in captured:
        k = cap.get("key") or cap.get("project_registration_no") or "__unknown__"
        if k not in by_key:
            by_key[k] = dict(cap)
        else:
            for field, val in cap.items():
                if val not in (None, "", [], {}):
                    by_key[k][field] = val
                elif field not in by_key[k]:
                    by_key[k][field] = val

    merged_projects = list(by_key.values())

    for i, proj in enumerate(merged_projects):
        reg = proj.get("project_registration_no", "?")
        name = proj.get("project_name", "?")
        print(f"  Captured [{i+1}/{len(merged_projects)}]: {reg} / {name}")

    project = merged_projects[0]

    # ── Save output JSON ──────────────────────────────────────────────────────
    OUTPUT_DIR.mkdir(exist_ok=True)
    out_path = OUTPUT_DIR / f"{state_key}.json"
    out_path.write_text(json.dumps(project, indent=2, default=str, ensure_ascii=False))
    print(f"  Saved  → {out_path}")

    # ── Compare with sample ───────────────────────────────────────────────────
    result: dict = {
        "site_id":    site_id,
        "state":      state_key,
        "output_file": str(out_path),
        "captured_reg_no":   project.get("project_registration_no"),
        "captured_name":     project.get("project_name"),
    }

    if not sample_path.exists():
        print(f"  [NO SAMPLE] {sample_path} not found — skipping comparison.")
        result["note"] = "no sample file found"
        return result

    sample = json.loads(sample_path.read_text())
    result["sample_file"]    = str(sample_path)
    result["sample_reg_no"]  = sample.get("project_registration_no")
    result["sample_name"]    = sample.get("project_name")

    cmp = compare(project, sample)
    result["comparison"] = cmp

    pct_str   = cmp["coverage"]
    missing   = cmp["missing_from_output"]
    type_diff = cmp["type_mismatches"]
    print(f"  Coverage: {pct_str}")
    if missing:
        print(f"  Missing from output ({len(missing)}): {', '.join(missing)}")
    if type_diff:
        print(f"  Type mismatches: {type_diff}")

    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Dry-run RERA crawlers without DB/S3 writes.")
    parser.add_argument("sites", nargs="*", help="Site IDs to run (default: all)")
    parser.add_argument(
        "--limit", type=int, default=1, metavar="N",
        help="Max projects to capture per site (default: 1)",
    )
    parser.add_argument(
        "--start-page", type=int, default=0, metavar="P",
        help="Listing page to start from, 0-indexed (default: 0 = oldest first). "
             "Use a high value to reach recent projects.",
    )
    args = parser.parse_args()

    site_filter  = set(args.sites)
    sites_to_run = [s for s in SITES if not site_filter or s["id"] in site_filter]

    if not sites_to_run:
        print(f"[ERROR] No matching sites for: {site_filter}")
        sys.exit(1)

    print(f"Running {len(sites_to_run)} site(s) in dry-run mode …")
    if args.limit != 1 or args.start_page != 0:
        print(f"  limit={args.limit}  start-page={args.start_page}")
    results = []
    for site_cfg in sites_to_run:
        r = run_site(site_cfg, limit=args.limit, start_page=args.start_page)
        results.append(r)

    REPORT_PATH.write_text(
        json.dumps(results, indent=2, default=str, ensure_ascii=False)
    )
    print(f"\n{'='*60}")
    print(f"  Report saved → {REPORT_PATH}")
    print(f"  Outputs  dir → {OUTPUT_DIR}/")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
