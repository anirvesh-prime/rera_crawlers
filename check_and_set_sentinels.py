#!/usr/bin/env python3
"""
check_and_set_sentinels.py

For every enabled site that has a state_projects_sample/<state>.json:
  1. Reads the sample's `url` and `project_registration_no`.
  2. Patches the crawler's listing phase to target ONLY that specific project
     (no DB writes, no S3 uploads).
  3. Lets the crawler fetch the real detail page for the sample project.
  4. Compares the captured output field-by-field against the sample.
  5. If coverage ≥ --min-coverage (default 60%), writes the reg_no into
     sentinel_registration_no in sites_config.py.

Usage:
    python check_and_set_sentinels.py                # all enabled sites
    python check_and_set_sentinels.py kerala_rera    # single site
    python check_and_set_sentinels.py --check-only   # report only, no writes
    python check_and_set_sentinels.py --min-coverage 70
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

# ── PYTHONHASHSEED must be fixed before any imports that touch core ───────────
if os.environ.get("PYTHONHASHSEED") != "0":
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = "0"
    os.execvpe(sys.executable, [sys.executable, *sys.argv], env)

from sites_config import SITES  # noqa: E402

SAMPLE_DIR   = Path("state_projects_sample")
SITES_CONFIG = Path("sites_config.py")
FAKE_RUN_ID  = 9998

# Fields that are infra/run-specific — excluded from field-coverage comparison
_INFRA_FIELDS = frozenset({
    "key", "url", "domain", "state", "config_id",
    "crawl_machine_ip", "machine_name", "retrieved_on",
    "last_crawled_date", "last_updated", "is_updated",
    "is_duplicate", "iw_processed", "iw_part_processed",
    "checked_updates", "checked_updates_date", "rera_housing_found",
    "is_live", "old_updates", "updated_fields", "data",
    "s3_link", "document_urls", "doc_ocr_url",
})


# ─────────────────────────────────────────────────────────────────────────────
# Comparison helpers
# ─────────────────────────────────────────────────────────────────────────────

def _is_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, (list, dict)) and not v:
        return True
    if isinstance(v, str) and v.strip() in ("", "None", "null", "NA"):
        return True
    return False


def _compare(output: dict, sample: dict) -> dict:
    all_keys      = (set(output) | set(sample)) - _INFRA_FIELDS
    pop_sample    = {k for k in all_keys if not _is_empty(sample.get(k))}
    pop_output    = {k for k in all_keys if not _is_empty(output.get(k))}
    matched       = sorted(pop_sample & pop_output)
    missing       = sorted(pop_sample - pop_output)
    pct           = round(100 * len(matched) / len(pop_sample), 1) if pop_sample else 100.0
    return {
        "coverage": pct,
        "coverage_str": f"{len(matched)}/{len(pop_sample)} ({pct}%)",
        "matched": matched,
        "missing": missing,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Patch helpers shared across all crawlers  (DB / S3 / checkpoint → no-op)
# ─────────────────────────────────────────────────────────────────────────────

def _make_db_s3_patches(module, captured: list[dict]) -> list:
    """Patches that prevent any real DB/S3 writes and capture upserted dicts."""

    def _upsert(data: dict) -> str:
        captured.append({k: v for k, v in data.items()})
        return "new"

    mocks: dict[str, Any] = {
        "upsert_project":     _upsert,
        "insert_crawl_error": MagicMock(),
        "upsert_document":    MagicMock(return_value="uploaded"),
        "get_project_by_key": MagicMock(return_value=None),
        "get_document":       MagicMock(return_value=None),
        "load_checkpoint":    MagicMock(return_value={}),
        "save_checkpoint":    MagicMock(),
        "reset_checkpoint":   MagicMock(),
        "get_s3_url":         MagicMock(return_value="https://s3.example.com/dry-run"),
        "upload_document":    MagicMock(return_value="https://s3.example.com/dry-run"),
        "_sentinel_check":    MagicMock(return_value=True),
    }

    patches = []
    for attr, mock in mocks.items():
        if hasattr(module, attr):
            patches.append(patch.object(module, attr, mock))
    patches.append(patch("core.db.bulk_insert_logs", MagicMock()))
    return patches


# ─────────────────────────────────────────────────────────────────────────────
# Per-site listing-injection patch builders
# ─────────────────────────────────────────────────────────────────────────────

def _listing_patches_kerala(module, sample_url: str, reg_no: str, sample: dict) -> list:
    """Inject just the sample card so only that project's detail page is fetched."""
    return [
        patch.object(module, "_get_explore_page",   return_value=MagicMock()),
        patch.object(module, "_get_total_pages",     return_value=1),
        patch.object(module, "_parse_explore_cards", return_value=[
            {"cert_no_from_card": reg_no, "detail_url": sample_url}
        ]),
    ]


def _listing_patches_rajasthan(module, sample_url: str, reg_no: str, sample: dict) -> list:
    """Patch the Playwright listing scraper to return just the sample project row,
    and bypass the Angular search navigation by going directly to the sample URL."""
    row = {
        "reg_no":         reg_no,
        "project_name":   sample.get("project_name", ""),
        "promoter_name":  sample.get("promoter_name", ""),
        "project_type":   sample.get("project_type", ""),
        "district":       (sample.get("project_location_raw") or {}).get("district", ""),
        "application_no": sample.get("acknowledgement_no", ""),
        "approved_on":    sample.get("approved_on_date", ""),
        "status":         sample.get("status_of_the_project", ""),
    }

    def _fake_navigate(page, rn, logger):
        page.goto(sample_url, timeout=60_000)
        page.wait_for_load_state("networkidle", timeout=30_000)
        return sample_url

    return [
        patch.object(module, "_scrape_project_list_playwright", return_value=[row]),
        patch.object(module, "_navigate_to_project_detail", side_effect=_fake_navigate),
    ]


def _listing_patches_odisha(module, sample_url: str, reg_no: str, sample: dict) -> list:
    """Return only the sample card from the Playwright listing, then navigate
    directly to the sample URL instead of clicking in the browser."""
    def _fake_open_detail(page, reg, logger):
        page.goto(sample_url, wait_until="networkidle", timeout=40000)
        return True

    return [
        patch.object(module, "_parse_page_cards", return_value=[
            {"project_registration_no": reg_no}
        ]),
        patch.object(module, "_open_detail_page", side_effect=_fake_open_detail),
    ]


def _listing_patches_pondicherry(module, sample_url: str, reg_no: str, sample: dict) -> list:
    """Return only the sample card from the listing parser."""
    return [
        patch.object(module, "_parse_listing_cards", return_value=[{
            "project_registration_no": reg_no,
            "project_name":  sample.get("project_name", ""),
            "promoter_name": sample.get("promoter_name", ""),
            "promoter_type": "",
            "project_type":  sample.get("project_type", ""),
            "listing_status": sample.get("status_of_the_project", ""),
            "revoke_reason": "",
            "detail_url":    sample_url,
        }]),
    ]


def _listing_patches_bihar(module, sample_url: str, reg_no: str, sample: dict) -> list:
    """Inject the sample URL as the single detail URL and build a matching listing row."""
    row = {
        "project_name":            sample.get("project_name", ""),
        "project_registration_no": reg_no,
        "promoter_name":           sample.get("promoter_name", ""),
        "project_location_raw":    sample.get("project_location_raw") or {},
        "submitted_date":          sample.get("submitted_date", ""),
    }
    return [
        patch.object(module, "_collect_detail_urls", return_value=[sample_url]),
        patch.object(module, "_parse_page_rows",     return_value=[row]),
        # Patch safe_get so listing fetch doesn't abort on HTML mismatch
        patch.object(module, "safe_get",             return_value=MagicMock(
            text="<html><body></body></html>", status_code=200)),
    ]


def _listing_patches_punjab(module, sample_url: str, reg_no: str, sample: dict) -> list:
    """Build the Punjab listing row from the sample URL query params and sample data."""
    qs          = parse_qs(urlparse(sample_url).query)
    project_id  = (qs.get("inProject_ID")  or qs.get("inproject_id")  or [""])[0]
    promoter_id = (qs.get("inPromoter_ID") or qs.get("inpromoter_id") or [""])[0]
    promo_type  = (qs.get("inPromoterType") or ["1"])[0]
    data_field  = sample.get("data") or {}
    row = {
        "district":                (sample.get("project_location_raw") or {}).get("district", ""),
        "project_name":            sample.get("project_name", ""),
        "promoter_name":           sample.get("promoter_name", ""),
        "project_registration_no": reg_no,
        "valid_upto":              "",
        "project_id":              project_id or str(data_field.get("project_id", "")),
        "promoter_id":             promoter_id or str(data_field.get("promo_id", "")),
        "promoter_type":           promo_type  or str(data_field.get("promo_type", "1")),
    }
    return [
        patch.object(module, "_search_projects", return_value=[row]),
    ]


def _listing_patches_maharashtra(module, sample_url: str, reg_no: str, sample: dict) -> list:
    """Inject a card with cert_id from the sample URL so only that project is fetched."""
    # URL format: https://maharerait.maharashtra.gov.in/public/project/view/{cert_id}
    m = re.search(r"/view/(\d+)", sample_url)
    cert_id = m.group(1) if m else ""
    loc = sample.get("project_location_raw") or {}
    card = {
        "project_registration_no": reg_no,
        "project_name":            sample.get("project_name", ""),
        "promoter_name":           sample.get("promoter_name", ""),
        "project_location_raw":    loc,
        "last_modified":           sample.get("last_modified", ""),
        "certificate_available":   bool(cert_id),
        "certificate_id":          cert_id,
        "view_details_url":        sample_url,
        "data":                    None,
    }
    return [
        patch.object(module, "_parse_cards",    return_value=[card]),
        patch.object(module, "_get_total_pages", return_value=1),
    ]


def _listing_patches_gujarat(module, sample_url: str, reg_no: str, sample: dict) -> list:
    """Gujarat iterates project IDs. We use sentinel_project_id from config as the start.
    The run() already handles this via dry_run_compare_start_page + CRAWL_ITEM_LIMIT=1.
    No extra listing patch needed — just return empty list."""
    return []   # Gujarat's run() handles this natively via config fields


# Map site_id → listing injector function
_LISTING_INJECTORS: dict[str, Any] = {
    "kerala_rera":       _listing_patches_kerala,
    "rajasthan_rera":    _listing_patches_rajasthan,
    "odisha_rera":       _listing_patches_odisha,
    "pondicherry_rera":  _listing_patches_pondicherry,
    "bihar_rera":        _listing_patches_bihar,
    "punjab_rera":       _listing_patches_punjab,
    "maharashtra_rera":  _listing_patches_maharashtra,
    "gujarat_rera":      _listing_patches_gujarat,
}


# ─────────────────────────────────────────────────────────────────────────────
# sites_config.py patcher
# ─────────────────────────────────────────────────────────────────────────────

def _update_sentinel(site_id: str, new_sentinel: str) -> bool:
    content    = SITES_CONFIG.read_text(encoding="utf-8")
    id_match   = re.search(rf'"id":\s*"{re.escape(site_id)}"', content)
    if not id_match:
        return False
    tail       = content[id_match.start():]
    sent_match = re.search(r'"sentinel_registration_no":\s*"[^"]*"', tail)
    if not sent_match:
        return False
    start      = id_match.start() + sent_match.start()
    end        = id_match.start() + sent_match.end()
    SITES_CONFIG.write_text(
        content[:start] + f'"sentinel_registration_no": "{new_sentinel}"' + content[end:],
        encoding="utf-8",
    )
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Per-site runner
# ─────────────────────────────────────────────────────────────────────────────

def process_site(site_cfg: dict, min_coverage: float, check_only: bool) -> dict:
    from core.config import settings

    site_id   = site_cfg["id"]
    state_key = site_cfg["state"].strip().lower().replace(" ", "_")

    print(f"\n{'─'*64}")
    print(f"  {site_id}  (enabled={site_cfg['enabled']})")
    print(f"{'─'*64}")

    # 1. Load sample ──────────────────────────────────────────────────────────
    sample_path = SAMPLE_DIR / f"{state_key}.json"
    if not sample_path.exists():
        print("  SKIP — no sample file found.")
        return {"site_id": site_id, "result": "skip", "reason": "no sample file"}

    sample     = json.loads(sample_path.read_text())
    sample_url = sample.get("url", "")
    reg_no     = sample.get("project_registration_no", "")

    if not reg_no:
        print("  SKIP — sample has no project_registration_no.")
        return {"site_id": site_id, "result": "skip", "reason": "no reg_no in sample"}
    if not sample_url:
        print("  SKIP — sample has no url field.")
        return {"site_id": site_id, "result": "skip", "reason": "no url in sample"}

    current_sentinel = site_cfg.get("sentinel_registration_no", "")
    print(f"  Sample reg_no    : {reg_no}")
    print(f"  Sample url       : {sample_url[:80]}{'…' if len(sample_url) > 80 else ''}")
    print(f"  Current sentinel : {current_sentinel or '(empty)'}")

    # 2. Import crawler module ────────────────────────────────────────────────
    module_path = site_cfg["crawler_module"]
    try:
        module = importlib.import_module(module_path)
    except Exception as exc:
        print(f"  IMPORT ERROR: {exc}")
        return {"site_id": site_id, "result": "error", "reason": f"import: {exc}"}

    # 3. Build patches ────────────────────────────────────────────────────────
    captured: list[dict] = []
    db_patches = _make_db_s3_patches(module, captured)

    injector = _LISTING_INJECTORS.get(site_id)
    listing_patches: list = []
    if injector:
        listing_patches = injector(module, sample_url, reg_no, sample)
    else:
        print(f"  WARN — no listing injector defined for {site_id}; "
              "crawl will start from page 0 with limit=1.")

    settings.CRAWL_ITEM_LIMIT = 1
    settings.DRY_RUN_S3       = True

    # 4. Run crawler with all patches ─────────────────────────────────────────
    all_patches = db_patches + listing_patches
    for p in all_patches:
        p.start()
    print("  Running targeted dry-run …")
    try:
        module.run(site_cfg, FAKE_RUN_ID, "weekly_deep")
    except Exception as exc:
        print(f"  RUN ERROR: {exc}")
    finally:
        for p in all_patches:
            try:
                p.stop()
            except Exception:
                pass

    # 5. Evaluate captured output ─────────────────────────────────────────────
    if not captured:
        print("  FAIL — no projects captured; site may have errored.")
        return {"site_id": site_id, "result": "fail", "reason": "nothing captured"}

    # Merge multi-call upserts for the same project key
    from collections import OrderedDict
    by_key: OrderedDict = OrderedDict()
    for cap in captured:
        k = cap.get("key") or cap.get("project_registration_no") or "__unknown__"
        if k not in by_key:
            by_key[k] = dict(cap)
        else:
            for f, v in cap.items():
                if v not in (None, "", [], {}):
                    by_key[k][f] = v
    output = list(by_key.values())[0]

    captured_reg  = (output.get("project_registration_no") or "").strip()
    cmp           = _compare(output, sample)
    coverage_pct  = cmp["coverage"]
    coverage_str  = cmp["coverage_str"]

    print(f"  Captured reg_no  : {captured_reg}")
    print(f"  Field coverage   : {coverage_str}")
    if cmp["missing"]:
        print(f"  Missing fields   : {', '.join(cmp['missing'][:10])}"
              + ("…" if len(cmp["missing"]) > 10 else ""))

    reg_match = captured_reg.lower() == reg_no.lower()
    print(f"  Reg-no match     : {'✓ YES' if reg_match else '✗ NO (captured different project)'}")

    if not reg_match:
        print(f"  SKIP — captured a different project ({captured_reg!r}), "
              "not setting sentinel.")
        return {"site_id": site_id, "result": "skip",
                "reason": f"wrong project captured: {captured_reg!r}"}

    if coverage_pct < min_coverage:
        print(f"  SKIP — coverage {coverage_pct}% < min {min_coverage}%; "
              "too many fields missing.")
        return {"site_id": site_id, "result": "skip",
                "reason": f"coverage {coverage_pct}% below {min_coverage}%"}

    # 6. Check if already correct ─────────────────────────────────────────────
    if current_sentinel == reg_no:
        print(f"  sentinel_registration_no already correct — no write needed.")
        return {"site_id": site_id, "result": "already_set", "sentinel": reg_no}

    # 7. Write to sites_config.py ─────────────────────────────────────────────
    if check_only:
        print(f"  CHECK-ONLY — would set sentinel → {reg_no!r}")
        return {"site_id": site_id, "result": "would_set", "proposed": reg_no,
                "coverage": coverage_str}

    ok = _update_sentinel(site_id, reg_no)
    if ok:
        print(f"  ✓ sites_config.py updated → sentinel_registration_no = {reg_no!r}")
        return {"site_id": site_id, "result": "updated", "sentinel": reg_no,
                "coverage": coverage_str}
    else:
        print(f"  ERROR — could not patch sites_config.py for {site_id}.")
        return {"site_id": site_id, "result": "patch_failed"}


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sites", nargs="*",
                        help="Site IDs to process (default: all enabled with a sample).")
    parser.add_argument("--check-only", action="store_true",
                        help="Report results but do NOT write to sites_config.py.")
    parser.add_argument("--min-coverage", type=float, default=60.0, metavar="PCT",
                        help="Minimum field-coverage %% required to set sentinel (default: 60).")
    parser.add_argument("--all", action="store_true",
                        help="Include disabled sites as well.")
    args = parser.parse_args()

    site_filter  = set(args.sites)
    sites_to_run = [
        s for s in SITES
        if (not site_filter or s["id"] in site_filter)
        and (args.all or s.get("enabled", False))
    ]

    if not sites_to_run:
        print(f"[ERROR] No matching sites: {site_filter or '(all enabled)'}")
        sys.exit(1)

    if args.check_only:
        print("Mode: CHECK-ONLY (sites_config.py will NOT be modified)\n")

    results = []
    for cfg in sites_to_run:
        r = process_site(cfg, min_coverage=args.min_coverage,
                         check_only=args.check_only)
        results.append(r)

    # Summary ─────────────────────────────────────────────────────────────────
    TAG = {
        "updated":      "✓ UPDATED   ",
        "already_set":  "= UNCHANGED ",
        "would_set":    "? WOULD SET ",
        "skip":         "- SKIPPED   ",
        "fail":         "✗ FAILED    ",
        "error":        "✗ ERROR     ",
        "patch_failed": "✗ PATCH ERR ",
    }
    print(f"\n{'='*64}")
    print("  SUMMARY")
    print(f"{'='*64}")
    for r in results:
        tag  = TAG.get(r.get("result", ""), "  UNKNOWN   ")
        info = (r.get("sentinel") or r.get("proposed") or r.get("reason", ""))
        print(f"  {tag}  {r['site_id']:<30}  {info}")
    print(f"{'='*64}\n")


if __name__ == "__main__":
    main()
