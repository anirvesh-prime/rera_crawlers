"""
Karnataka RERA Crawler — rera.karnataka.gov.in
Type: static (httpx + BeautifulSoup)

How the portal works (observed from live HTML):
- The listing page (viewAllProjects) renders project data as JavaScript arrays.
  Each project appears as:
      var localObj = { appNo : 'ACK/KA/RERA/.../...' };
      applicationArray.push(localObj);
  A district MUST be selected — blank search returns zero results.
  The crawler POSTs each district name and extracts ack_nos via regex.

- Detail fetch (TWO-STEP, as of 2025):
  Step 1: POST /projectViewDetails with appNo=<ack_no>
          → Returns search-results table containing the internal numeric DB ID
            (<a id="<numeric_id>" onclick="return showFileApplicationPreview(this);">)
            and the APPROVED ON date.
  Step 2: POST /projectDetails with action=<numeric_id>
          → Returns full project detail page (HTML ~200–450 KB).
  The portal no longer accepts the ack_no directly as action parameter (returns 400).

- The detail page uses a Bootstrap grid layout (col-md-3 div pairs) for most fields.
  Project Name / Ack No / Reg No appear in <span class="user_name"> elements.

- Registration certificate: GET /certificate?CER_NO=<registration_no>
- Document downloads: GET /download_jc?DOC_ID=<encoded_id>
  (skip links where DOC_ID query param is blank)

- Canonical URL stored per project: https://rera.karnataka.gov.in/projectViewDetails
  (no per-project URL path exists on the public portal)
"""
from __future__ import annotations

import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import download_response, generate_project_key, random_delay, safe_get, safe_post
from core.db import get_project_by_key, upsert_project, insert_crawl_error, upsert_document
from core.document_policy import select_document_for_download
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import (
    build_document_filename,
    build_document_urls,
    document_identity_url,
    document_result_entry,
    get_machine_context,
    merge_data_sections,
    normalize_project_payload,
    parse_datetime,
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings

BASE_URL    = "https://rera.karnataka.gov.in"
LISTING_URL = f"{BASE_URL}/viewAllProjects"
DETAIL_URL  = f"{BASE_URL}/projectDetails"
CERT_URL    = f"{BASE_URL}/certificate"
PROJECT_URL = f"{BASE_URL}/projectViewDetails"   # canonical; no per-project URL path
DOMAIN      = "rera.karnataka.gov.in"
STATE_CODE  = "KA"

# All 31 Karnataka districts as they appear in the portal's <select> options.
# A district must be selected — blank search returns zero results.
DISTRICTS: list[str] = [
    "Bagalkot", "Ballari", "Belagavi",
    "Bengaluru  Rural",   # note: two spaces — matches portal option value exactly
    "Bengaluru Urban", "Bidar", "Chamarajanagar", "Chikkaballapura",
    "Chikkamagaluru", "Chitradurga", "Dakshina Kannada", "Davangere",
    "Dharwad", "Gadag", "Hassan", "Haveri", "Kalaburagi", "Kodagu",
    "Kolar", "Koppal", "Mandya", "Mysore", "Raichur", "Ramanagara",
    "Shivamogga", "Tumakuru", "Udupi", "Uttara Kannada", "Vijayanagara",
    "Vijayapura", "Yadgir",
]

# Regex to extract acknowledgement numbers from the embedded JavaScript arrays.
# Matches: appNo : 'ACK/KA/RERA/...' or appNo : "ACK/KA/RERA/..."
_ACK_RE = re.compile(r"""appNo\s*:\s*['"]([^'"]+)['"]""")
_PROMO_RE = re.compile(r"""applicationNameList2\s*\.push\('([^']*)'\)""")
_PROJECT_NAME_RE = re.compile(r"""applicationNameList3\s*\.push\('([^']*)'\)""")
_PROMOTER_NAME_RE = re.compile(r"""applicationNameList4\s*\.push\('([^']*)'\)""")

# Map lowercased Karnataka portal labels (from detail HTML) → schema field names.
# Covers both the old fragment-style labels and the new Bootstrap-grid labels.
_LABEL_MAP: dict[str, str] = {
    # Project identity
    "project name":                                                 "project_name",
    "project type":                                                 "project_type",
    "type of project":                                              "project_type",
    "registration no":                                              "project_registration_no",
    "application no":                                               "acknowledgement_no",
    "acknowledgement no":                                           "acknowledgement_no",
    "status":                                                       "status_of_the_project",
    "project status":                                               "status_of_the_project",
    # Promoter
    "promoter / company / firm name":                               "promoter_name",
    "promoter name":                                                "promoter_name",
    "company name":                                                 "promoter_name",
    # GST / PAN / registration
    "gst no":                                                       "_gst_no",
    "gstin":                                                        "_gst_no",
    "pan no":                                                       "_pan_no",
    "pan":                                                          "_pan_no",
    "trade licence / registration no":                              "_trade_reg_no",
    "registration number":                                          "_trade_reg_no",
    "objective":                                                    "_objective",
    "main objectives":                                              "_objective",
    # Location
    "district":                                                     "_district",
    "taluk":                                                        "_taluk",
    "village":                                                      "_village",
    "pin code":                                                     "_pin_code",
    "survey / resurvey number":                                     "_survey_no",
    "latitude":                                                     "_latitude",
    "longitude":                                                    "_longitude",
    # Website
    "website":                                                      "_website",
    "promoter website":                                             "_website",
    # Project address (full address string for raw_address)
    "project address":                                              "_project_address",
    # Dates
    "date of commencement":                                         "actual_commencement_date",
    "project start date":                                           "actual_commencement_date",
    "estimated date of commencement":                               "estimated_commencement_date",
    "proposed date of commencement":                                "estimated_commencement_date",
    "estimated commencement date":                                  "estimated_commencement_date",
    "proposed start date":                                          "estimated_commencement_date",
    "proposed date of completion":                                  "estimated_finish_date",
    "proposed completion date":                                     "estimated_finish_date",
    "project end date":                                             "estimated_finish_date",
    "completion date":                                              "actual_finish_date",
    "date of approval":                                             "approved_on_date",
    # Costs — new verbose labels + legacy short labels
    "cost of land":                                                 "_cost_of_land",
    "cost of land (inr) (c1)( as certified by ca in form 1 )":     "_cost_of_land",
    "estimated construction cost":                                  "_est_construction_cost",
    "cost of layout development (inr) (c2)( as certified by ca in form 1 )": "_est_construction_cost",
    "total construction cost":                                      "_est_construction_cost",
    "total project cost":                                           "_total_project_cost",
    "total project cost (inr) (c1+c2)":                            "_total_project_cost",
    # Land
    "land area":                                                    "land_area",
    "extent":                                                       "land_area",
    "extent of land":                                               "land_area",
    "total land area":                                              "land_area",
    # Construction area
    "construction area":                                            "construction_area",
    "total construction area":                                      "construction_area",
    "built up area":                                                "construction_area",
    # Units / plots
    "number of plots":                                              "number_of_residential_units",
    "total number of plots":                                        "number_of_residential_units",
    "total no. of plots":                                           "number_of_residential_units",
    "no. of plots":                                                 "number_of_residential_units",
    "number of residential units":                                  "number_of_residential_units",
    "total no. of units":                                           "number_of_residential_units",
    "number of commercial units":                                   "number_of_commercial_units",
    "no. of commercial units":                                      "number_of_commercial_units",
    # Bank
    "bank name":                                                    "_bank_name",
    "account no":                                                   "_account_no",
    "account no.(70% account)":                                     "_account_no",
    "account name":                                                 "_account_name",
    "ifsc":                                                         "_ifsc",
    "ifsc code":                                                    "_ifsc",
    "branch":                                                       "_branch",
    # Progress
    "total completion percentage":                                  "_total_completion_pct",
    "extent of development carried till date":                      "_total_completion_pct",
    # Description
    "project description":                                          "project_description",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _clean(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _safe_float(val: str | None) -> float | None:
    if not val:
        return None
    try:
        return float(re.sub(r"[^\d.]", "", val))
    except (ValueError, TypeError):
        return None


# ── Listing ───────────────────────────────────────────────────────────────────

def _post_listing(district: str, start_page: int, logger: CrawlerLogger) -> str | None:
    """POST the search form for one district + page offset. Returns raw HTML text."""
    payload = {
        "districtId":    district,
        "talukId":       "",
        "projectName":   "",
        "promoterName":  "",
        "applicationNo": "",
        "registrationNo": "",
        "START_PAGE":    str(start_page),
    }
    resp = safe_post(LISTING_URL, data=payload, retries=3, logger=logger, timeout=45.0)
    return resp.text if resp else None


def _extract_ack_nos(html: str) -> list[str]:
    """
    Extract acknowledgement numbers from the JavaScript arrays embedded in the
    listing page HTML. Each project appears as:
        var localObj = { appNo : 'ACK/KA/RERA/.../...' };
    Returns a deduplicated list preserving document order.
    """
    seen: set[str] = set()
    result: list[str] = []
    for m in _ACK_RE.finditer(html):
        ack = m.group(1).strip()
        if ack and ack not in seen:
            seen.add(ack)
            result.append(ack)
    return result


def _extract_listing_rows(html: str, district: str) -> list[dict]:
    """Recover per-project listing data from the JS arrays embedded in the page."""
    acks = _extract_ack_nos(html)
    promoter_regs = [m.group(1).strip() for m in _PROMO_RE.finditer(html)]
    project_names = [m.group(1).strip() for m in _PROJECT_NAME_RE.finditer(html)]
    promoter_names = [m.group(1).strip() for m in _PROMOTER_NAME_RE.finditer(html)]

    rows: list[dict] = []
    for idx, ack_no in enumerate(acks):
        rows.append({
            "acknowledgement_no": ack_no,
            "project_registration_no": ack_no,
            "project_name": project_names[idx] if idx < len(project_names) else None,
            "promoter_name": promoter_names[idx] if idx < len(promoter_names) else None,
            "promoter_registration_no": promoter_regs[idx] if idx < len(promoter_regs) else None,
            "project_city": district.upper(),
            "project_location_raw": {"district": district},
            "data": {
                "search_district": district,
                "promoter_registration_no": promoter_regs[idx] if idx < len(promoter_regs) else None,
                "listing_fallback": True,
            },
        })
    return rows


# ── Detail page fetching ─────────────────────────────────────────────────────

def _fetch_detail(ack_no: str, logger: CrawlerLogger) -> tuple[str | None, dict]:
    """
    Two-step detail fetch (new portal behaviour as of 2025):
      1. POST /projectViewDetails with appNo=<ack_no>
         → parse search-results table → extract numeric DB id + approved_on date.
      2. POST /projectDetails with action=<numeric_id>
         → full project detail page (~200–450 KB HTML).
    Returns (html | None, meta_dict).
    meta_dict keys: approved_on_date, status_of_the_project, project_type_listing
    """
    # Step 1: search for project to get its numeric DB id
    search_resp = safe_post(
        PROJECT_URL,
        data={"appNo": ack_no, "regNo": "", "project": "", "firm": "",
              "district": "0", "subdistrict": "0"},
        retries=3, logger=logger, timeout=60.0,
    )
    if not search_resp:
        logger.warning(f"Search POST failed for {ack_no!r}", step="detail")
        return None, {}

    search_soup = BeautifulSoup(search_resp.text, "lxml")
    tbl = search_soup.find("table")
    if not tbl:
        logger.warning(f"No table in search results for {ack_no!r}", step="detail")
        return None, {}

    rows = tbl.find_all("tr")
    numeric_id: str | None = None
    meta: dict = {}

    if rows:
        hdr_cells = rows[0].find_all(["th", "td"])
        headers = [_clean(c.get_text()).lower() for c in hdr_cells]
        approved_idx   = next((i for i, h in enumerate(headers) if "approved on" in h), -1)
        status_idx     = next((i for i, h in enumerate(headers) if h.strip() == "status"), -1)
        proj_type_idx  = next((i for i, h in enumerate(headers) if "project type" in h), -1)

        for row in rows[1:]:
            a = row.find("a", onclick=lambda s: s and "showFileApplicationPreview" in s)
            if not a:
                continue
            numeric_id = a.get("id", "")
            cells = row.find_all("td")
            if approved_idx >= 0 and approved_idx < len(cells):
                raw_date = _clean(cells[approved_idx].get_text())
                parsed = parse_datetime(raw_date)
                meta["approved_on_date"] = (
                    parsed.strftime("%Y-%m-%d %H:%M:%S+00:00") if parsed else None
                )
            if status_idx >= 0 and status_idx < len(cells):
                meta["status_of_the_project"] = _clean(cells[status_idx].get_text())
            if proj_type_idx >= 0 and proj_type_idx < len(cells):
                meta["project_type_listing"] = _clean(cells[proj_type_idx].get_text())
            break

    if not numeric_id:
        logger.warning(f"Numeric DB id not found for {ack_no!r}", step="detail")
        return None, meta

    # Step 2: fetch full detail page
    detail_resp = safe_post(
        DETAIL_URL, data={"action": numeric_id},
        retries=3, logger=logger, timeout=60.0,
    )
    if not detail_resp:
        logger.warning(f"Detail POST failed for numeric_id={numeric_id!r}", step="detail")
        return None, meta

    return detail_resp.text, meta


# ── Detail page parsing ───────────────────────────────────────────────────────

def _extract_kv_pairs(soup: BeautifulSoup) -> dict[str, str]:
    """
    Extract label→value pairs from tr/td rows (still used for address fields
    like 'present address', 'permanent address', 'official address').
    """
    result: dict[str, str] = {}
    for row in soup.find_all("tr"):
        cells = row.find_all("td", recursive=False)
        if len(cells) >= 2:
            raw_key = _clean(cells[0].get_text()).rstrip(":")
            raw_val = _clean(cells[1].get_text())
            key = raw_key.lower().strip()
            if key and raw_val and len(key) < 120 and key not in result:
                result[key] = raw_val
    return result


def _extract_grid_kv(soup: BeautifulSoup) -> dict[str, str]:
    """
    Extract label→value pairs from Bootstrap grid div pairs.
    Handles col-md-3/col-md-3, col-md-3/col-md-9, and col-md-6/col-md-6 layouts.
    The new portal renders fields as adjacent div pairs where the first div
    contains a <p class="text-right"> label and the second contains the value.
    Iterates ALL direct child divs (not just col-md-3) to support all column widths.
    """
    result: dict[str, str] = {}
    for row_div in soup.find_all("div", class_="row"):
        cols = row_div.find_all("div", recursive=False)
        for i in range(0, len(cols) - 1, 2):
            label_div = cols[i]
            value_div = cols[i + 1]
            label_p = label_div.find("p")
            value_tag = value_div.find(["p", "pre"])
            if not label_p or not value_tag:
                continue
            # Strip the decorative colon-span from the label
            for span in label_p.find_all("span", class_="space_LR"):
                span.decompose()
            label = _clean(label_p.get_text()).rstrip(":").lower().strip()
            value = _clean(value_tag.get_text())
            if label and value and len(label) < 200 and label not in result:
                result[label] = value
    return result


def _extract_header_fields(soup: BeautifulSoup) -> dict[str, str]:
    """
    Extract Project Name / Acknowledgement Number / Registration Number from
    <span class="user_name"> elements in the detail page header.
    """
    result: dict[str, str] = {}
    for span in soup.find_all("span", class_="user_name"):
        text = _clean(span.get_text())
        m = re.match(r"^([^:]+)\s*:\s*(.+)$", text)
        if not m:
            continue
        label = m.group(1).strip().lower()
        value = m.group(2).strip()
        if "project name" in label:
            result["project_name"] = value
        elif "acknowledgement" in label:
            result["acknowledgement_no"] = value
        elif "registration" in label:
            result["project_registration_no"] = value
    return result


def _parse_section_table(soup: BeautifulSoup, heading_keywords: list[str]) -> list[dict]:
    """
    Find the first <table> whose nearest preceding heading text contains any
    of the given keywords. Returns list of header-keyed row dicts.
    Searches h1 (new portal) as well as h2/h3/h4/b/strong/th (legacy).
    """
    for el in soup.find_all(["h1", "h2", "h3", "h4", "b", "strong", "th"]):
        if not any(kw in _clean(el.get_text()).lower() for kw in heading_keywords):
            continue
        tbl = el.find_next("table")
        if not tbl:
            continue
        rows = tbl.find_all("tr")
        if len(rows) < 2:
            continue
        headers = [_clean(c.get_text()) for c in rows[0].find_all(["th", "td"])]
        out = []
        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells:
                continue
            rd: dict = {}
            for i, cell in enumerate(cells):
                col = headers[i] if i < len(headers) else f"col_{i}"
                rd[col] = _clean(cell.get_text())
                links = [a["href"] for a in cell.find_all("a", href=True)
                         if "javascript" not in a["href"].lower()]
                if links:
                    rd[f"{col}__links"] = links
            if any(isinstance(v, str) and v for v in rd.values()):
                out.append(rd)
        return out
    return []


def _parse_detail(html: str, ack_no: str, search_district: str,
                  start_page: int, meta: dict | None = None) -> dict:
    """
    Parse the full detail page returned by POST /projectDetails.
    Uses Bootstrap grid (col-md-3) KV pairs for most fields plus
    tr/td KV pairs for address fields.
    meta may contain approved_on_date and status_of_the_project from listing.
    Returns a dict of normalized schema fields ready for merging.
    """
    if meta is None:
        meta = {}
    soup = BeautifulSoup(html, "lxml")

    # Merge both KV extraction strategies: grid (primary) + tr/td (for addresses)
    grid_kv = _extract_grid_kv(soup)
    td_kv   = _extract_kv_pairs(soup)
    # grid_kv takes priority; td_kv fills in what grid misses
    kv: dict[str, str] = {**td_kv, **grid_kv}

    out: dict = {}

    # ── 0. Header fields (project name / ack_no / reg_no) ───────────────────
    hdr = _extract_header_fields(soup)
    out.update(hdr)

    # ── 1. Map label→schema field ────────────────────────────────────────────
    for raw_key, val in kv.items():
        field = _LABEL_MAP.get(raw_key)
        if field and val and not out.get(field):
            out[field] = val

    def _pop_mapped(field: str, *fallback_labels: str) -> str:
        value = out.pop(field, None)
        if value:
            return str(value)
        for label in fallback_labels:
            fallback = kv.get(label, "")
            if fallback:
                return fallback
        return ""

    # ── 2. Apply listing metadata (approved_on, status) ─────────────────────
    if meta.get("approved_on_date") and not out.get("approved_on_date"):
        out["approved_on_date"] = meta["approved_on_date"]
    if meta.get("status_of_the_project") and not out.get("status_of_the_project"):
        out["status_of_the_project"] = meta["status_of_the_project"]
    if meta.get("project_type_listing") and not out.get("project_type"):
        out["project_type"] = meta["project_type_listing"]

    # ── 3. Parse date fields ─────────────────────────────────────────────────
    for f in ("actual_commencement_date", "estimated_commencement_date",
              "estimated_finish_date", "actual_finish_date", "approved_on_date"):
        raw = out.get(f)
        if raw:
            parsed = parse_datetime(raw)
            out[f] = parsed.strftime("%Y-%m-%d %H:%M:%S+00:00") if parsed else None
    # When promoter has applied for completion, set actual_finish_date from
    # estimated_finish_date (portal shows the same project end date for both).
    # Detect via the "Promoter has Applied for Completion" h1 heading on the page.
    if not out.get("actual_finish_date") and out.get("estimated_finish_date"):
        completion_applied = any(
            "applied for completion" in _clean(h1.get_text()).lower()
            for h1 in soup.find_all("h1")
        )
        if completion_applied:
            out["actual_finish_date"] = out["estimated_finish_date"]

    # ── 4. Land area — find "X Acres, Y Gunta/ Z Sq Mtr(s)" pattern in table cells
    land_area_m2: float | None = None
    for td in soup.find_all("td"):
        td_text = _clean(td.get_text())
        m = re.search(r"\d+\s*(?:Acres?|Gunta)[^/]*/\s*(\d+(?:\.\d+)?)\s*Sq\s*Mtrs?",
                      td_text, re.I)
        if m:
            land_area_m2 = float(m.group(1))
            break
    if land_area_m2 is None and out.get("land_area"):
        land_area_m2 = _safe_float(str(out.get("land_area")))
    if land_area_m2 is not None:
        out["land_area"] = land_area_m2
        out["land_area_details"] = {
            "land_area": str(round(land_area_m2)),
            "land_area_unit": None,
            "construction_area": None,
            "construction_area_unit": None,
        }

    # ── 5. Project location ──────────────────────────────────────────────────
    district = _pop_mapped("_district", "district")
    # Prefer the full project address string; fall back to village name
    project_address = _pop_mapped("_project_address", "project address")
    loc: dict = {k: v for k, v in {
        "district":               district,
        "taluk":                  _pop_mapped("_taluk", "taluk"),
        "pin_code":               _pop_mapped("_pin_code", "pin code"),
        "latitude":               _pop_mapped("_latitude", "latitude"),
        "longitude":              _pop_mapped("_longitude", "longitude"),
        "survey_resurvey_number": _pop_mapped("_survey_no", "survey / resurvey number"),
        "raw_address":            project_address or _pop_mapped("_village", "village"),
    }.items() if v}
    for coord_key, store_key in (("latitude", "processed_latitude"),
                                 ("longitude", "processed_longitude")):
        if loc.get(coord_key):
            fv = _safe_float(loc[coord_key])
            if fv is not None:
                loc[store_key] = fv
    if loc:
        out["project_location_raw"] = loc
    if district:
        out["project_city"] = district.upper()
    if loc.get("pin_code"):
        out["project_pin_code"] = loc["pin_code"]

    # ── 6. Promoter address ──────────────────────────────────────────────────
    prom_addr: dict = {}
    # Primary: Bootstrap grid "promoter address" field
    for key in ("promoter address",):
        if kv.get(key):
            prom_addr["raw_address"] = kv[key]
            break
    if not prom_addr.get("raw_address"):
        for key in grid_kv:
            if "promoter" in key and "address" in key:
                prom_addr["raw_address"] = grid_kv[key]
                break
    # Location sub-fields (may come from promoter section of the grid)
    for sub, labels in [
        ("state",    ["promoter state", "promoter's state"]),
        ("taluk",    ["promoter taluk", "promoter's taluk", "taluk"]),
        ("district", ["promoter district", "promoter's district", "district"]),
        ("pin_code", ["promoter pin code", "pin code"]),
    ]:
        for label in labels:
            val = kv.get(label, "")
            if val:
                prom_addr[sub] = val
                break
    # Karnataka is always the state for this portal
    if not prom_addr.get("state"):
        prom_addr["state"] = "Karnataka"
    if prom_addr:
        out["promoter_address_raw"] = prom_addr

    # ── 7. Promoter contact (website) ────────────────────────────────────────
    website = _pop_mapped("_website", "website", "promoter website")
    if website:
        out["promoter_contact_details"] = {"website": website}

    # ── 8. Promoters details (GST, PAN, trade reg, objective) ────────────────
    pd_dict: dict = {
        "gst_no":          _pop_mapped("_gst_no", "gst no", "gstin"),
        "pan_no":          _pop_mapped("_pan_no", "pan no", "pan"),
        "registration_no": _pop_mapped("_trade_reg_no", "trade licence / registration no",
                                       "registration number"),
        "objective":       _pop_mapped("_objective", "objective"),
    }
    pd_dict = {k: v for k, v in pd_dict.items() if v}
    if pd_dict:
        out["promoters_details"] = pd_dict

    # ── 9. Bank details ──────────────────────────────────────────────────────
    # Bank section uses "state" which can conflict; grab it from grid before popping
    bank_state = grid_kv.get("state", "")
    bank_district = grid_kv.get("district", district)
    bank_pin = grid_kv.get("pin code", "")
    bank: dict = {
        "bank_name":    _pop_mapped("_bank_name", "bank name"),
        "account_no":   _pop_mapped("_account_no", "account no", "account no.(70% account)"),
        "account_name": _pop_mapped("_account_name", "account name"),
        "IFSC":         _pop_mapped("_ifsc", "ifsc", "ifsc code"),
        "branch":       _pop_mapped("_branch", "branch"),
    }
    if bank_state:
        bank["state"] = bank_state
    if bank_district:
        bank["district"] = bank_district
    if bank_pin:
        bank["pin_code"] = bank_pin
    bank = {k: v for k, v in bank.items() if v}
    if bank:
        out["bank_details"] = bank

    # ── 10. Project cost ─────────────────────────────────────────────────────
    cost: dict = {
        "cost_of_land": _pop_mapped(
            "_cost_of_land", "cost of land",
            "cost of land (inr) (c1)( as certified by ca in form 1 )"),
        "estimated_construction_cost": _pop_mapped(
            "_est_construction_cost", "estimated construction cost",
            "cost of layout development (inr) (c2)( as certified by ca in form 1 )",
            "total construction cost"),
        "total_project_cost": _pop_mapped(
            "_total_project_cost", "total project cost",
            "total project cost (inr) (c1+c2)"),
    }
    cost = {k: v for k, v in cost.items() if v}
    if cost:
        out["project_cost_detail"] = cost

    # ── 11. Building / plot details ──────────────────────────────────────────
    # New page: "Development Details ( Plot Dimension wise break up )" h1
    # Columns: Sl No. | Plot Type | Number of Sites | Total Area (in Sq.Mtr)
    brows = _parse_section_table(
        soup,
        ["plot dimension", "plot detail", "plot type", "unit detail",
         "building detail", "development detail"])
    if brows:
        bd = []
        skip_keys = {"s.no", "sl.no", "sl no.", "#", "no.", "total"}
        for r in brows:
            # Use header-keyed values; fall back to positional
            flat_type = (r.get("Plot Type (Site Dimension in Mtr)")
                         or r.get("Type of Inventory") or r.get("Flat Type")
                         or list(r.values())[1] if len(r) > 1 else "")
            total_area = (r.get("Total Area (in Sq.Mtr)")
                          or r.get("Carpet Area (Sq Mtr)") or r.get("Total Area")
                          or list(r.values())[3] if len(r) > 3 else "")
            flat_type = _clean(str(flat_type))
            total_area = _clean(str(total_area))
            if flat_type and flat_type.lower() not in skip_keys:
                bd.append({"flat_type": flat_type, "total_area": total_area})
        if bd:
            out["building_details"] = bd

    # ── 11b. Proposed timeline — parse "Project Schedule" table ──────────────
    sched_rows = _parse_section_table(soup, ["project schedule"])
    if sched_rows:
        timeline = []
        for r in sched_rows:
            title = _clean(r.get("Project Work") or r.get("col_1") or "")
            status = _clean(r.get("Is Applicable ?") or r.get("col_2") or "")
            end_date_raw = _clean(r.get("Estimated To Date") or r.get("col_4") or "")
            if not title:
                continue
            parsed_end = parse_datetime(end_date_raw) if end_date_raw else None
            timeline.append({
                "title": title,
                "status": status or None,
                "proposed_end_date": (
                    parsed_end.strftime("%Y-%m-%d %H:%M:%S+00:00") if parsed_end else None
                ),
            })
        if timeline:
            out["proposed_timeline"] = timeline

    # ── 12. Professional information ─────────────────────────────────────────
    # New page uses separate h1 sections per role; collect them all
    profs: list[dict] = []
    role_keywords = [
        ("project chartered accountant", "Accountant"),
        ("project engineer", "Engineers"),
        ("project architect", "Architect"),
        ("project contractor", "Contractor"),
    ]
    for kw, role_label in role_keywords:
        prows = _parse_section_table(soup, [kw])
        for r in prows:
            vals = list(r.values())
            # Columns: Sl No. | Name | Address | Year | Licence No.
            name = _clean(vals[1]) if len(vals) > 1 else ""
            if not name:
                name = _clean(vals[0]) if vals else ""
            addr = _clean(vals[2]) if len(vals) > 2 else ""
            yr   = _clean(vals[3]) if len(vals) > 3 else ""
            lic  = _clean(vals[4]) if len(vals) > 4 else ""
            if name and name.lower() not in ("sl no.", "s.no", "#"):
                e = {k: v for k, v in {
                    "name": name, "role": role_label, "address": addr,
                    "effective_date": yr, "key_real_estate_projects": lic,
                }.items() if v}
                profs.append(e)
    # Fall back to legacy search
    if not profs:
        prows = _parse_section_table(soup, ["professional", "engineer", "architect"])
        for r in prows:
            vals = list(r.values())
            e = {
                "name":                     _clean(vals[0]) if vals else "",
                "role":                     _clean(vals[1]) if len(vals) > 1 else "",
                "address":                  _clean(vals[2]) if len(vals) > 2 else "",
                "effective_date":           _clean(vals[3]) if len(vals) > 3 else "",
                "key_real_estate_projects": _clean(vals[4]) if len(vals) > 4 else "",
            }
            e = {k: v for k, v in e.items() if v}
            if e.get("name"):
                profs.append(e)
    if profs:
        out["professional_information"] = profs

    # ── 13. Co-promoter / land-owner details ─────────────────────────────────
    crows = _parse_section_table(soup, ["co-promoter", "co promoter", "land owner"])
    if crows:
        colist = []
        for r in crows:
            # New columns: Sl No., Land Owner Name, Land Owner Share, Survey Number,
            #              Present Address, Communication Address
            name         = (r.get("Land Owner Name") or r.get("Name", ""))
            survey_no    = (r.get("Survey Number") or r.get("Survey No", ""))
            land_share   = (r.get("Land Owner Share") or r.get("Land Share", ""))
            present_addr = r.get("Present Address", "")
            comm_addr    = r.get("Communication Address") or r.get("Comm Address", "")
            name = _clean(name)
            if name and name.lower() not in ("s.no", "sl.no", "#", "sl no."):
                e = {k: v for k, v in {
                    "name": name,
                    "survey_no": _clean(survey_no),
                    "land_share": _clean(land_share),
                    "present_address": _clean(present_addr),
                    "comm_address": _clean(comm_addr),
                }.items() if v}
                colist.append(e)
        if colist:
            out["co_promoter_details"] = colist

    # ── 14. Authorised signatory ─────────────────────────────────────────────
    # New page: name from grid KV; addresses from tr/td KV
    sg_name = grid_kv.get("name of authorized signatory", "")
    if sg_name:
        sg = {
            "name":              sg_name,
            "pan_no":            grid_kv.get("pan", ""),
            "present_address":   td_kv.get("present address", ""),
            "official_address":  td_kv.get("official address", ""),
            "permanent_address": td_kv.get("permanent address", ""),
        }
        sg["raw_address"] = (sg.get("present_address") or sg.get("official_address") or "")
        sg = {k: v for k, v in sg.items() if v}
        if sg.get("name"):
            out["authorised_signatory_details"] = sg

    # ── 15. Construction progress (total completion %) ───────────────────────
    total_pct = _pop_mapped("_total_completion_pct", "total completion percentage",
                            "extent of development carried till date")
    if not total_pct:
        m2 = re.search(r"extent\s+of\s+development\s+carried\s+till\s+date\s*[:\s]+([^\n<]+)",
                       html, re.I)
        if m2:
            total_pct = m2.group(1).strip()
    if total_pct:
        if "%" not in total_pct:
            total_pct = f"{total_pct} %"
        out["construction_progress"] = [
            {"title": "total_completion_percentage", "progress_percentage": total_pct}
        ]

    # ── 16. Project images — img tags with alt containing "photo" ───────────
    images: list[str] = []
    seen_img_urls: set[str] = set()
    for img in soup.find_all("img"):
        alt = (img.get("alt") or "").lower()
        if "photo" not in alt:
            continue
        src = img.get("src") or ""
        if not src:
            continue
        full_url = src if src.startswith("http") else f"{BASE_URL}{src}"
        if "download_jc" in full_url.lower() and full_url not in seen_img_urls:
            seen_img_urls.add(full_url)
            images.append(full_url)
    if images:
        out["project_images"] = images

    # ── 17. Raw data snapshot ────────────────────────────────────────────────
    out["data"] = {
        "district":                    search_district,
        "START_PAGE":                  str(start_page),
        "project_district":            district.upper() if district else "",
        "total_completion_percentage": total_pct or "",
        "status":                      meta.get("status_of_the_project", ""),
    }

    return out



# ── Document extraction ───────────────────────────────────────────────────────

# Matches "*(Annexure - 60)" or "( Annexure - 60 )" suffixes on portal labels.
_ANNEXURE_RE = re.compile(r"\s*\*?\(?\s*Annexure\s*[-–]\s*\d+\s*\)?", re.I)
# Matches date strings like "06-03-2023" or "28/11/2003".
_DATE_CELL_RE = re.compile(r"^\d{1,2}[-/]\d{2}[-/]\d{2,4}$")
# Matches file extensions (.pdf, .xlsx, .jpeg, etc.).
_FILE_EXT_RE = re.compile(r"\.\w{2,5}$")


def _doc_label_from_row(parent_td, row_cells: list, section_heading: str,
                        link_text: str) -> str:
    """
    Determine the human-readable document type for a download link found inside
    a table cell.

    Strategy (handles three distinct table layouts on the Karnataka portal):

    1. Empty link text  →  the link has no visible label (e.g. licence-number
       columns in the Professional table, or document columns in Land Survey).
       Use the nearest preceding section heading (e.g. "Project Chartered
       Accountant", "Land Survey Details") — always more meaningful than a
       random data cell value.

    2. Non-empty link text  →  walk LEFT through the current row's cells to
       find the nearest cell whose text looks like a document-category label
       (not a serial number, not a date, not another filename).  This correctly
       handles:
         • Financial docs:  [Label | 2022.pdf | 2021.pdf | 2020.pdf]
           — year 2 & 3 get the same Label as year 1.
         • Other Docs:      [Doc Name | Date | file.pdf]
           — walks past the date to reach the doc name in col-0.
         • 4-col project docs: [Label1 | file1 | Label2 | file2]
           — each file's immediate left neighbour IS its own label.
       If no label is found (e.g. numbered "Other Documents" rows where col-0
       is a serial number), falls back to link_text (the filename itself).
    """
    if not link_text:
        # Empty link tag — use section heading as the category label.
        return section_heading or "Document"

    if parent_td not in row_cells:
        return link_text

    col_idx = row_cells.index(parent_td)
    # Walk left through preceding cells to find a proper label.
    for i in range(col_idx - 1, -1, -1):
        candidate = _clean(row_cells[i].get_text())
        if not candidate:
            continue
        # Serial number → we've reached the leftmost data; no label exists.
        if candidate.isdigit():
            break
        # Date cell (e.g. "06-03-2023") → skip and keep walking.
        if _DATE_CELL_RE.match(candidate):
            continue
        # Another filename → keep walking left to find the true label.
        if _FILE_EXT_RE.search(candidate):
            continue
        # Single-word codes without spaces (survey nos., case IDs like "820/3") → skip.
        if "/" in candidate and " " not in candidate:
            continue
        # Looks like a real document-category label — strip the annexure suffix.
        label = _ANNEXURE_RE.sub("", candidate).strip().rstrip("*").strip()
        return label if label else link_text

    # Nothing useful found to the left — use the filename as the type.
    return link_text


def _extract_documents(html: str, reg_no: str) -> list[dict]:
    """
    Extract all document links from the detail HTML.
    - Scans <a href> for /download_jc?DOC_ID= patterns; skips entries with empty DOC_ID.
    - Skips placeholder links whose filename is "Not Applicable.pdf" — the portal
      renders these for document categories that don't apply to a project (the server
      returns a tiny blank 5 KB PDF with no content).  Downloading these wastes S3
      storage and clutters the document list with meaningless files.
    - Resolves each document's human-readable type via _doc_label_from_row.
    - Adds the auto-generated RERA registration certificate entry.
    Returns list of {link, type} dicts.
    """
    # Matches filenames that explicitly indicate "not applicable" placeholders.
    # Anchored so "Some Not Applicable Doc.pdf" is not accidentally skipped.
    _NOT_APPLICABLE_RE = re.compile(r'^not\s*applicable(\.pdf)?$', re.I)

    soup = BeautifulSoup(html, "lxml")
    docs: list[dict] = []
    seen_links: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "download_jc" not in href.lower():
            continue
        # Skip entries with blank DOC_ID
        if "DOC_ID=" in href and (
            href.endswith("DOC_ID=") or "DOC_ID=&" in href
        ):
            continue
        full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
        if full_url in seen_links:
            continue
        seen_links.add(full_url)

        link_text = _clean(a.get_text())

        # Skip placeholder "Not Applicable.pdf" links — the promoter uploads these
        # when a required document category doesn't apply to their project.
        # The server returns a real but blank PDF; downloading it provides no value.
        if _NOT_APPLICABLE_RE.match(link_text):
            continue

        parent_td = a.find_parent("td")

        if parent_td is None:
            # Standalone link (e.g. inside a Bootstrap grid div, not a table).
            # The link text itself is the filename and serves as a reasonable label.
            doc_type = link_text or "Document"
        else:
            row = parent_td.find_parent("tr")
            row_cells = row.find_all("td") if row else []

            # Nearest section heading above this link's table.
            section_heading = ""
            table = row.find_parent("table") if row else None
            if table:
                for hdr in table.find_all_previous(["h1", "h2", "h3", "h4"]):
                    section_heading = _clean(hdr.get_text())
                    break

            doc_type = _doc_label_from_row(
                parent_td, row_cells, section_heading, link_text
            )

        docs.append({"link": full_url, "type": doc_type})

    # Auto-add registration certificate for approved projects
    if reg_no:
        cert_link = f"{CERT_URL}?CER_NO={reg_no}"
        if cert_link not in seen_links:
            docs.append({"link": cert_link, "type": "Rera Registration Certificate 1"})

    return docs


# ── Document download constants ───────────────────────────────────────────────

_DOC_CONNECT_TIMEOUT = 10.0   # seconds to establish TCP connection
_DOC_READ_TIMEOUT    = 20.0   # seconds between data chunks
_DOC_TOTAL_TIMEOUT   = 60.0   # hard cap: total download time in seconds
_DOC_MAX_BYTES       = 50 * 1024 * 1024  # 50 MB safety limit
_MAX_DOC_WORKERS     = 5      # parallel document download threads

# psycopg connections are not thread-safe — serialise all DB writes from doc threads.
_DB_LOCK = threading.Lock()


def _handle_document(
    project_key: str,
    doc: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
    client: httpx.Client,
) -> dict | None:
    """Download a document, upload to S3, and record it in rera_project_documents."""
    url = doc.get("link")
    doc_type = doc.get("type", "document")
    if not url:
        return None
    filename = build_document_filename(doc)
    try:
        doc_timeout = httpx.Timeout(
            connect=_DOC_CONNECT_TIMEOUT,
            read=_DOC_READ_TIMEOUT,
            write=_DOC_READ_TIMEOUT,
            pool=10.0,
        )
        resp = download_response(
            url,
            client=client,
            timeout=doc_timeout,
            total_timeout=_DOC_TOTAL_TIMEOUT,
            max_bytes=_DOC_MAX_BYTES,
        )
        if not resp or len(resp.content) < 100:
            logger.warning("Document download empty or failed", url=url, step="documents")
            return None
        data = resp.content
        if len(data) > _DOC_MAX_BYTES:
            logger.warning(
                f"Document too large ({len(data)/1024/1024:.1f} MB), skipping",
                url=url, step="documents",
            )
            return None
        md5 = compute_md5(data)
        s3_key = upload_document(project_key, filename, data, dry_run=settings.DRY_RUN_S3)
        if s3_key is None:
            return None
        s3_url = get_s3_url(s3_key)
        with _DB_LOCK:
            upsert_document(
                project_key=project_key,
                document_type=doc_type,
                original_url=url,
                s3_key=s3_key,
                s3_bucket=settings.S3_BUCKET_NAME,
                file_name=filename,
                md5_checksum=md5,
                file_size_bytes=len(data),
            )
        logger.log_document(doc_type, url, "uploaded", s3_key=s3_key, file_size_bytes=len(data))
        return {**doc, "s3_link": s3_url}
    except Exception as exc:
        logger.warning(f"Document handling error: {exc}", url=url, step="documents")
        with _DB_LOCK:
            insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(exc),
                               url=url, project_key=project_key)
        return None


def _process_documents(
    project_key: str,
    documents: list[dict],
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
    state: str = "karnataka",
) -> tuple[list[dict], int]:
    # ── Phase 1: filter to policy-allowed documents (sequential, CPU-only) ─────
    counters: dict[str, int] = {}
    selected_pairs: list[tuple[dict, dict]] = []  # (original_doc, selected_doc)
    skipped_entries: list[dict] = []
    for doc in documents:
        sel = select_document_for_download(state, doc, counters)
        if sel:
            selected_pairs.append((doc, sel))
        else:
            skipped_entries.append({"link": doc.get("link"), "type": doc.get("type", "document")})
    if not selected_pairs:
        return skipped_entries, 0

    # ── Phase 2: parallel downloads ────────────────────────────────────────────
    doc_timeout = httpx.Timeout(
        connect=_DOC_CONNECT_TIMEOUT,
        read=_DOC_READ_TIMEOUT,
        write=_DOC_READ_TIMEOUT,
        pool=10.0,
    )
    dl_results: list[dict | None] = [None] * len(selected_pairs)
    with httpx.Client(timeout=doc_timeout, follow_redirects=True) as client:
        with ThreadPoolExecutor(max_workers=_MAX_DOC_WORKERS) as executor:
            futures = {
                executor.submit(_handle_document, project_key, sel, run_id, site_id, logger, client): i
                for i, (_orig, sel) in enumerate(selected_pairs)
            }
            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    dl_results[idx] = fut.result()
                except Exception as exc:
                    logger.warning(f"Doc thread error: {exc}", step="documents")

    # ── Phase 3: reassemble in original order ─────────────────────────────────
    upload_count = 0
    enriched: list[dict] = []
    for (orig, _sel), result in zip(selected_pairs, dl_results):
        if result:
            enriched.append(result)
            upload_count += 1
        else:
            enriched.append({"link": orig.get("link"), "type": orig.get("type", "document")})
    enriched.extend(skipped_entries)
    return enriched, upload_count



# ── Sentinel ──────────────────────────────────────────────────────────────────

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Karnataka RERA.
    Loads state_projects_sample/karnataka.json as the baseline, fetches the
    sentinel project detail via _fetch_detail + _parse_detail, and verifies
    ≥ 80% field coverage.

    Karnataka has no per-project URLs — all detail fetches go through a generic
    POST endpoint.  The sentinel therefore uses the acknowledgement_no stored in
    the baseline JSON to drive _fetch_detail (which expects an ack_no, not a
    reg_no), then cross-checks the returned reg_no against
    sentinel_registration_no from config.
    """
    import json as _json
    import os as _os
    from core.sentinel_utils import check_field_coverage

    sentinel_reg = config.get("sentinel_registration_no", "")
    if not sentinel_reg:
        logger.warning("No sentinel_registration_no configured — skipping", step="sentinel")
        return True

    sample_path = _os.path.join(
        _os.path.dirname(_os.path.dirname(__file__)),
        "state_projects_sample", "karnataka.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check",
                       path=sample_path, step="sentinel")
        return True

    # Karnataka has no per-project URL; use the acknowledgement_no from the
    # baseline to look up the project via the two-step POST flow.
    ack_no = baseline.get("acknowledgement_no", "")
    if not ack_no:
        logger.warning("Sentinel: no acknowledgement_no in baseline — skipping", step="sentinel")
        return True

    logger.info(f"Sentinel: fetching detail for {sentinel_reg} (ack={ack_no})", step="sentinel")
    try:
        html, meta = _fetch_detail(ack_no, logger)
        if not html:
            logger.error("Sentinel: detail fetch returned no HTML", step="sentinel")
            return False
        fresh = _parse_detail(html, ack_no, DISTRICTS[0], start_page=0, meta=meta) or {}
    except Exception as exc:
        logger.error(f"Sentinel: fetch/parse error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", step="sentinel")
        return False

    # Verify the fetched project is actually the sentinel project
    scraped_reg = fresh.get("project_registration_no", "")
    if scraped_reg and scraped_reg.upper() != sentinel_reg.upper():
        logger.error(
            f"Sentinel: reg_no mismatch — expected {sentinel_reg!r}, got {scraped_reg!r}",
            step="sentinel",
        )
        insert_crawl_error(
            run_id, config.get("id", "karnataka_rera"),
            "SENTINEL_FAILED", f"reg_no mismatch: {scraped_reg!r}",
        )
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "karnataka_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


# ── Main entry point ──────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    """
    Main crawl loop for Karnataka RERA.

    Flow:
    1. Sentinel check (one test district POST).
    2. For each of 31 districts → paginate listing POSTs to collect ack_nos.
    3. For each ack_no → POST detail, parse, merge, normalise, upsert.
    4. Documents: download + S3 upload on new projects or weekly_deep mode.
    5. Checkpoint saved after each district.
    """
    logger = CrawlerLogger(config["id"], run_id)
    counters = dict(
        projects_found=0, projects_new=0, projects_updated=0,
        projects_skipped=0, documents_uploaded=0, error_count=0,
    )
    machine_name, machine_ip = get_machine_context()
    item_limit  = settings.CRAWL_ITEM_LIMIT or 0   # 0 = unlimited
    max_pages   = settings.MAX_PAGES or 0           # 0 = unlimited
    delay_range = config.get("rate_limit_delay", (2, 5))
    state       = config.get("state", "karnataka")
    districts   = DISTRICTS
    items_processed = 0
    stop_all = False
    t_run = time.monotonic()

    # ── Sentinel health check ────────────────────────────────────────────────
    t0 = time.monotonic()
    sentinel_ok = _sentinel_check(config, run_id, logger)
    if not sentinel_ok:
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counters["error_count"] += 1
        return counters
    logger.warning(f"Step timing [sentinel]: {time.monotonic()-t0:.2f}s", step="timing")

    checkpoint = load_checkpoint(config["id"], mode) or {}
    start_district_idx = int(checkpoint.get("last_page", 0))

    t0 = time.monotonic()
    first_district_logged = False
    for district_idx, district in enumerate(districts):
        if stop_all:
            break
        if district_idx < start_district_idx:
            continue

        logger.info(
            f"District {district_idx + 1}/{len(districts)}: {district!r}",
            step="listing",
        )

        start_page  = 0
        page_number = 0

        while True:
            if stop_all:
                break

            html = _post_listing(district, start_page, logger)
            if html is None:
                logger.error(
                    f"Listing POST failed for district={district!r} start={start_page}",
                    step="listing",
                )
                insert_crawl_error(
                    run_id, config["id"], "HTTP_ERROR",
                    f"listing POST failed: district={district} start={start_page}",
                    url=LISTING_URL,
                )
                counters["error_count"] += 1
                break

            listing_rows = _extract_listing_rows(html, district)
            ack_nos = [row["acknowledgement_no"] for row in listing_rows]
            logger.info(
                f"  start={start_page}: {len(ack_nos)} ack_nos",
                district=district, step="listing",
            )

            if not ack_nos:
                break

            counters["projects_found"] += len(ack_nos)
            if not first_district_logged:
                logger.warning(f"Step timing [search]: {time.monotonic()-t0:.2f}s  rows={len(ack_nos)}", step="timing")
                first_district_logged = True

            for listing_row in listing_rows:
                ack_no = listing_row["acknowledgement_no"]
                if stop_all:
                    break
                if item_limit and items_processed >= item_limit:
                    logger.info(f"Item limit {item_limit} reached — stopping", step="listing")
                    stop_all = True
                    break

                project_key = generate_project_key(ack_no)

                # ── daily_light: skip projects already in the DB ──────────────
                if mode == "daily_light" and get_project_by_key(project_key):
                    counters["projects_skipped"] += 1
                    continue

                logger.set_project(
                    key=project_key,
                    reg_no=ack_no,
                    url=PROJECT_URL,
                    page=page_number,
                )
                try:

                    # ── Fetch and parse detail page (two-step) ──────────────────
                    detail_html, fetch_meta = _fetch_detail(ack_no, logger)
                    if detail_html:
                        detail = _parse_detail(
                            detail_html, ack_no, district, start_page, meta=fetch_meta
                        )
                        reg_no = detail.get("project_registration_no", "")
                        if reg_no:
                            project_key = generate_project_key(reg_no)
                            logger.set_project(
                                key=project_key,
                                reg_no=reg_no,
                                url=PROJECT_URL,
                                page=page_number,
                            )
                        uploaded_docs = _extract_documents(detail_html, reg_no)
                    else:
                        logger.warning(
                            f"Detail fetch failed for {ack_no!r}; using listing fallback",
                            step="detail",
                        )
                        detail = dict(listing_row)
                        reg_no = detail.get("project_registration_no", ack_no)
                        uploaded_docs = []

                    # ── Build merged record ─────────────────────────────────────
                    merged: dict = {
                        **detail,
                        "acknowledgement_no": ack_no,
                        "url":    PROJECT_URL,
                        "domain": DOMAIN,
                        "state":  state,
                        "data":   merge_data_sections(detail.get("data"), {}),
                        "is_live": True,
                    }
                    if uploaded_docs:
                        merged["uploaded_documents"] = uploaded_docs
                    merged = {k: v for k, v in merged.items() if v is not None}

                    # ── Normalize + upsert ──────────────────────────────────────
                    try:
                        normalized = normalize_project_payload(
                            merged, config,
                            machine_name=machine_name,
                            machine_ip=machine_ip,
                        )
                        record  = ProjectRecord(**normalized)
                        db_dict = record.to_db_dict()
                        status  = upsert_project(db_dict)
                        items_processed += 1

                        if status == "new":
                            counters["projects_new"] += 1
                            logger.info(f"New: {ack_no}", step="upsert")
                        elif status == "updated":
                            counters["projects_updated"] += 1
                            logger.info(f"Updated: {ack_no}", step="upsert")
                        else:
                            counters["projects_skipped"] += 1

                        # ── Document upload (new or weekly_deep) ────────────────
                        if uploaded_docs:
                            enriched, doc_count = _process_documents(
                                project_key, uploaded_docs, run_id, config["id"], logger, state,
                            )
                            counters["documents_uploaded"] += doc_count
                            if doc_count:
                                upsert_project({
                                    "key": project_key,
                                    "uploaded_documents": enriched,
                                    "document_urls": build_document_urls(enriched),
                                })

                    except ValidationError as exc:
                        counters["error_count"] += 1
                        logger.error(
                            f"Validation error for {ack_no}: {exc}",
                            step="validate",
                        )
                        insert_crawl_error(
                            run_id, config["id"], "VALIDATION_FAILED", str(exc),
                            project_key=project_key, url=PROJECT_URL,
                        )
                    except Exception as exc:
                        counters["error_count"] += 1
                        logger.error(
                            f"Unexpected error for {ack_no}: {exc}",
                            step="upsert",
                        )
                        insert_crawl_error(
                            run_id, config["id"], "CRAWLER_EXCEPTION", str(exc),
                            project_key=project_key, url=PROJECT_URL,
                        )
                finally:
                    logger.clear_project()

                # Rate-limit: pause every 10 projects, not after every single one
                if items_processed > 0 and items_processed % 10 == 0:
                    random_delay(*delay_range)

            # ── Pagination ──────────────────────────────────────────────────
            page_number += 1
            if max_pages and page_number >= max_pages:
                logger.info(f"Reached max_pages={max_pages} for district={district!r}")
                break
            # The portal uses start_page as an offset (total rows seen so far)
            next_start = start_page + len(ack_nos)
            if next_start == start_page or not ack_nos:
                break  # no progress / empty page
            start_page = next_start
            random_delay(*delay_range)

        # Save checkpoint after each district
        save_checkpoint(config["id"], mode, district_idx + 1, None, run_id)
        logger.info(
            f"District complete: {district!r} — counters so far: {counters}",
            step="district_done",
        )

    reset_checkpoint(config["id"], mode)
    logger.info(f"Karnataka RERA crawl complete: {counters}", step="done")
    logger.warning(f"Step timing [total_run]: {time.monotonic()-t_run:.2f}s", step="timing")
    return counters
