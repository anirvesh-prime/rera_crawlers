"""Scratch: run Rajasthan crawler for the sample project, capture upsert output."""
import json
from unittest.mock import patch, MagicMock

from core.config import settings
import sites.rajasthan_rera as rj
from sites_config import SITES

cfg = next(c for c in SITES if c["id"] == "rajasthan_rera")

stub = {
    "enc_id": "AHTh/hIBTdU=",
    "reg_no": "RAJ/P/2024/3058",
    "project_name": "VENTURA",
    "promoter_name": "J S BUILDCOM",
    "project_type": "group-housing",
    "district": "Jaipur",
    "application_no": "",
    "approved_on": "",
    "status": "",
}

captured = []

def _upsert(data):
    captured.append(dict(data))
    return "new"

mocks = {
    "upsert_project": _upsert,
    "insert_crawl_error": MagicMock(),
    "upsert_document": MagicMock(return_value="uploaded"),
    "get_project_by_key": MagicMock(return_value=None),
    "get_document": MagicMock(return_value=None),
    "load_checkpoint": MagicMock(return_value={}),
    "save_checkpoint": MagicMock(),
    "reset_checkpoint": MagicMock(),
    "get_s3_url": MagicMock(return_value="https://s3.example.com/dry-run"),
}

patches = [patch.object(rj, a, m) for a, m in mocks.items() if hasattr(rj, a)]
patches.append(patch("core.db.bulk_insert_logs", MagicMock()))
patches.append(patch.object(rj, "_scrape_project_list", return_value=[stub]))
# Skip sentinel coverage gate so detail scrape always runs
patches.append(patch.object(rj, "_sentinel_check", return_value=True))

settings.CRAWL_ITEM_LIMIT = 1
settings.DRY_RUN_S3 = True

for p in patches:
    p.start()
try:
    rj.run(cfg, 99999, "weekly_deep")
finally:
    for p in patches:
        try:
            p.stop()
        except Exception:
            pass

# Merge captured upserts (crawler does 2 upserts: main + documents)
merged = {}
for cap in captured:
    k = cap.get("key") or "?"
    merged.setdefault(k, {})
    for f, v in cap.items():
        if v not in (None, "", [], {}) or f not in merged[k]:
            merged[k][f] = v

print("\n\n========== CRAWLER CAPTURED OUTPUT ==========")
for k, proj in merged.items():
    fields = [
        "project_registration_no", "construction_area", "land_area",
        "submitted_date", "approved_on_date", "estimated_finish_date",
        "estimated_commencement_date", "actual_commencement_date",
        "actual_finish_date", "promoter_contact_details", "members_details",
        "professional_information", "proposed_timeline", "project_location_raw",
    ]
    for f in fields:
        print(f"  {f}: {proj.get(f)!r}")
    print("  data:", json.dumps(proj.get("data"), default=str)[:600])
    print("  ALL KEYS:", sorted(proj.keys()))
