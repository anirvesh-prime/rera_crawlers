"""
Regenerate state_projects_sample/maharashtra.json using the live site.

Scrapes the sentinel project (PP1190002502346 / cert_id=61699) exactly as
run() would, then merges with the listing-level stub fields and writes the
result as the new baseline.
"""
import json, os, sys
sys.path.insert(0, ".")

from core.logger import CrawlerLogger
from sites.maharashtra_rera import _scrape_mh_detail_page

CERT_ID  = "61699"
REG_NO   = "PP1190002502346"
PROJ_URL = f"https://maharerait.maharashtra.gov.in/public/project/view/{CERT_ID}"
SAMPLE_PATH = os.path.join("state_projects_sample", "maharashtra.json")

# Load existing baseline to preserve fields that come from the listing page
# (key, url, state, domain, last_modified, certificate_available, config_id, etc.)
with open(SAMPLE_PATH) as f:
    old_baseline = json.load(f)

logger = CrawlerLogger(site_id="mh_regen", run_id=0)
logger.info(f"Scraping sentinel project {REG_NO} (cert_id={CERT_ID})")

fresh = _scrape_mh_detail_page(CERT_ID, logger)

if not fresh:
    print("ERROR: scrape returned empty — aborting, baseline unchanged")
    sys.exit(1)

# Strip the internal auth token before saving
fresh.pop("_auth_token", None)

# Fields that come from the listing page (not the detail scrape) — keep from old baseline
LISTING_FIELDS = {
    "key", "url", "state", "domain", "crawl_machine_ip", "machine_name",
    "config_id", "is_updated", "is_duplicate", "iw_processed",
    "checked_updates", "rera_housing_found", "is_live",
    "last_modified", "certificate_available",
    "project_registration_no",   # already in detail but ensure correct
}

new_baseline = {}

# Start with listing-level fields from old baseline
for field in LISTING_FIELDS:
    if field in old_baseline:
        new_baseline[field] = old_baseline[field]

# Overlay everything from fresh scrape
new_baseline.update(fresh)

# Ensure registration no is correct
new_baseline["project_registration_no"] = REG_NO
new_baseline["url"] = PROJ_URL

print("\n=== Fields in NEW baseline ===")
for k, v in new_baseline.items():
    if isinstance(v, (dict, list)):
        print(f"  {k}: {json.dumps(v)[:120]}")
    else:
        print(f"  {k}: {v!r}")

print("\n=== Fields REMOVED vs old baseline ===")
old_keys = set(old_baseline.keys())
new_keys = set(new_baseline.keys())
removed = old_keys - new_keys
added   = new_keys - old_keys
for k in sorted(removed):
    print(f"  REMOVED: {k} (was: {json.dumps(old_baseline[k])[:80]})")
for k in sorted(added):
    print(f"  ADDED:   {k}")

with open(SAMPLE_PATH, "w") as f:
    json.dump(new_baseline, f, indent=2, default=str)
    f.write("\n")

print(f"\n[OK] Written to {SAMPLE_PATH}")
