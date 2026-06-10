"""Minimal live run of the REAL odisha crawler (no listing injector).

Walks the live listing page 1, processes exactly ONE project (CRAWL_ITEM_LIMIT=1),
captures the dict that would be upserted, and prints the audit-relevant fields.
DB / S3 / document downloads / sentinel are stubbed so nothing is written and the
run stays fast. This exercises the production listing-card + detail-overview
extraction paths (which the dry_run injector bypasses).
"""
from __future__ import annotations

import json
from unittest.mock import patch

import sites.odisha_rera as od
from core.config import settings
from core.crawler_base import SeleniumPageAdapter
from sites_config import SITES

CFG = next(c for c in SITES if c["id"] == "odisha_rera")

captured: list[dict] = []


def _wait_for_url_shim(self, pattern, timeout=15000):
    """Shim for the missing SeleniumPageAdapter.wait_for_url (poll current_url)."""
    import time as _t
    deadline = _t.monotonic() + timeout / 1000.0
    while _t.monotonic() < deadline:
        if "project-details" in (self.url or ""):
            return
        _t.sleep(0.25)


SeleniumPageAdapter.wait_for_url = _wait_for_url_shim


def main() -> None:
    settings.CRAWL_ITEM_LIMIT = 1
    settings.MAX_PAGES = 1
    settings.DRY_RUN_S3 = True

    patches = [
        patch.object(od, "upsert_project", lambda d: (captured.append(d), "new")[1]),
        patch.object(od, "upsert_document", lambda **k: None),
        patch.object(od, "insert_crawl_error", lambda *a, **k: None),
        patch.object(od, "get_project_by_key", lambda k: None),
        patch.object(od, "update_crawl_run_progress", lambda *a, **k: None),
        patch.object(od, "load_checkpoint", lambda *a, **k: {}),
        patch.object(od, "save_checkpoint", lambda *a, **k: None),
        patch.object(od, "reset_checkpoint", lambda *a, **k: None),
        patch.object(od, "upload_document", lambda *a, **k: "dry/key.pdf"),
        patch.object(od, "get_s3_url", lambda k: "http://example/" + str(k)),
        # Skip the sentinel network check + skip per-project document downloads.
        patch.object(od, "_sentinel_check", lambda *a, **k: True),
        patch.object(od, "_handle_document", lambda *a, **k: None),
    ]
    for p in patches:
        p.start()
    try:
        od.run(CFG, 999999, "weekly_deep")
    finally:
        for p in patches:
            try:
                p.stop()
            except Exception:
                pass

    if not captured:
        print("NO PROJECT CAPTURED")
        return

    rec = captured[0]
    fields = [
        "project_registration_no", "project_name",
        "project_location_raw", "project_city", "project_pin_code",
        "land_area", "construction_area",
        "total_floor_area_under_residential",
        "total_floor_area_under_commercial_or_other_uses",
        "estimated_commencement_date", "actual_commencement_date",
        "estimated_finish_date", "actual_finish_date",
        "submitted_date", "approved_on_date", "last_modified",
        "number_of_residential_units", "number_of_commercial_units",
    ]
    print("===== CAPTURED PROJECT (audit fields) =====")
    for f in fields:
        print(f"{f}: {rec.get(f)!r}")

    bd = rec.get("building_details")
    print("\nbuilding_details type:", type(bd).__name__,
          "len:", len(bd) if isinstance(bd, list) else "n/a")
    if isinstance(bd, list) and bd:
        print("building_details[0]:", json.dumps(bd[0], default=str))

    pt = (rec.get("proposed_timeline") or
          (rec.get("data") or {}).get("proposed_timeline"))
    print("\nproposed_timeline:", json.dumps(pt, default=str)[:600])

    print("\n----- data.raw_card -----")
    print(json.dumps((rec.get("data") or {}).get("raw_card"), default=str))
    print("\n----- all top-level keys -----")
    print(sorted(rec.keys()))


if __name__ == "__main__":
    main()
