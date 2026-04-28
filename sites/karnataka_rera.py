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

- Detail popup: jQuery $.ajax POST to /projectDetails with body action=<ack_no>.
  The server returns an HTML fragment inserted into a Bootstrap modal.
  The crawler replicates this POST directly with httpx.

- Registration certificate: GET /certificate?CER_NO=<registration_no>
- Document downloads: GET /download_jc?DOC_ID=<encoded_id>
  (skip links where DOC_ID query param is blank)

- Canonical URL stored per project: https://rera.karnataka.gov.in/projectViewDetails
  (no per-project URL path exists on the public portal)
"""
from __future__ import annotations

import re

from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import generate_project_key, random_delay, safe_get, safe_post
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
_LABEL_MAP: dict[str, str] = {
    "project name":                        "project_name",
    "type of project":                     "project_type",
    "registration no":                     "project_registration_no",
    "application no":                      "acknowledgement_no",
    "acknowledgement no":                  "acknowledgement_no",
    "status":                              "status_of_the_project",
    "project status":                      "status_of_the_project",
    "promoter / company / firm name":      "promoter_name",
    "promoter name":                       "promoter_name",
    "company name":                        "promoter_name",
    "gst no":                              "_gst_no",
    "pan no":                              "_pan_no",
    "trade licence / registration no":     "_trade_reg_no",
    "objective":                           "_objective",
    "district":                            "_district",
    "taluk":                               "_taluk",
    "village":                             "_village",
    "pin code":                            "_pin_code",
    "survey / resurvey number":            "_survey_no",
    "latitude":                            "_latitude",
    "longitude":                           "_longitude",
    "website":                             "_website",
    "date of commencement":                "actual_commencement_date",
    "proposed date of completion":         "estimated_finish_date",
    "completion date":                     "actual_finish_date",
    "date of approval":                    "approved_on_date",
    "cost of land":                        "_cost_of_land",
    "estimated construction cost":         "_est_construction_cost",
    "total project cost":                  "_total_project_cost",
    "land area":                           "land_area",
    "bank name":                           "_bank_name",
    "account no":                          "_account_no",
    "account name":                        "_account_name",
    "ifsc":                                "_ifsc",
    "branch":                              "_branch",
    "total completion percentage":         "_total_completion_pct",
    "project description":                 "project_description",
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


# ── Detail page parsing ───────────────────────────────────────────────────────

def _post_detail(ack_no: str, logger: CrawlerLogger) -> str | None:
    """POST to /projectDetails with action=<ack_no>. Returns raw HTML fragment."""
    resp = safe_post(DETAIL_URL, data={"action": ack_no}, retries=3,
                     logger=logger, timeout=60.0)
    return resp.text if resp else None


def _extract_kv_pairs(soup: BeautifulSoup) -> dict[str, str]:
    """
    Extract label→value pairs from the detail HTML fragment.
    Handles two common JSP table patterns:
      1. <tr><td>Label</td><td>Value</td></tr>
      2. <tr><td>Label :</td><td>Value</td></tr>
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


def _parse_section_table(soup: BeautifulSoup, heading_keywords: list[str]) -> list[dict]:
    """
    Find the first <table> whose nearest preceding heading text contains any
    of the given keywords. Returns list of header-keyed row dicts.
    """
    for el in soup.find_all(["h2", "h3", "h4", "b", "strong", "th"]):
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
                  start_page: int) -> dict:
    """
    Parse the HTML fragment returned by POST /projectDetails.
    Returns a dict of normalized schema fields ready for merging.
    """
    soup = BeautifulSoup(html, "lxml")
    kv   = _extract_kv_pairs(soup)
    out: dict = {}

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

    # ── 2. Parse date fields ─────────────────────────────────────────────────
    for f in ("actual_commencement_date", "estimated_finish_date",
              "actual_finish_date", "approved_on_date"):
        raw = out.get(f)
        if raw:
            parsed = parse_datetime(raw)
            out[f] = parsed.strftime("%Y-%m-%d %H:%M:%S+00:00") if parsed else None

    # ── 3. Land area as float ────────────────────────────────────────────────
    if out.get("land_area"):
        out["land_area"] = _safe_float(str(out["land_area"]))

    # ── 4. Project location ──────────────────────────────────────────────────
    district = _pop_mapped("_district", "district")
    loc: dict = {k: v for k, v in {
        "district":              district,
        "taluk":                 _pop_mapped("_taluk", "taluk"),
        "pin_code":              _pop_mapped("_pin_code", "pin code"),
        "latitude":              _pop_mapped("_latitude", "latitude"),
        "longitude":             _pop_mapped("_longitude", "longitude"),
        "survey_resurvey_number": _pop_mapped("_survey_no", "survey / resurvey number"),
        "raw_address":           _pop_mapped("_village", "village"),
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

    # ── 5. Promoter address ──────────────────────────────────────────────────
    prom_addr: dict = {}
    for key in kv:
        if "promoter" in key and "address" in key:
            prom_addr["raw_address"] = kv[key]
            break
    for sub, label in [("state", "state"), ("taluk", "taluk"),
                       ("district", "district"), ("pin_code", "pin code")]:
        val = kv.get(f"promoter {label}") or kv.get(f"promoter's {label}")
        if val:
            prom_addr[sub] = val
    if prom_addr:
        out["promoter_address_raw"] = prom_addr

    # ── 6. Promoter contact (website) ────────────────────────────────────────
    website = _pop_mapped("_website", "website")
    if website:
        out["promoter_contact_details"] = {"website": website}

    # ── 7. Promoters details (GST, PAN, trade reg, objective) ────────────────
    pd: dict = {
        "gst_no":          _pop_mapped("_gst_no", "gst no"),
        "pan_no":          _pop_mapped("_pan_no", "pan no"),
        "registration_no": _pop_mapped("_trade_reg_no", "trade licence / registration no"),
        "objective":       _pop_mapped("_objective", "objective"),
    }
    pd = {k: v for k, v in pd.items() if v}
    if pd:
        out["promoters_details"] = pd

    # ── 8. Bank details ──────────────────────────────────────────────────────
    bank: dict = {
        "bank_name":    _pop_mapped("_bank_name", "bank name"),
        "account_no":   _pop_mapped("_account_no", "account no"),
        "account_name": _pop_mapped("_account_name", "account name"),
        "IFSC":         _pop_mapped("_ifsc", "ifsc"),
        "branch":       _pop_mapped("_branch", "branch"),
    }
    bank = {k: v for k, v in bank.items() if v}
    if bank:
        out["bank_details"] = bank

    # ── 9. Project cost ──────────────────────────────────────────────────────
    cost: dict = {
        "cost_of_land":                _pop_mapped("_cost_of_land", "cost of land"),
        "estimated_construction_cost": _pop_mapped("_est_construction_cost", "estimated construction cost"),
        "total_project_cost":          _pop_mapped("_total_project_cost", "total project cost"),
    }
    cost = {k: v for k, v in cost.items() if v}
    if cost:
        out["project_cost_detail"] = cost

    # ── 10. Building / plot details ──────────────────────────────────────────
    brows = _parse_section_table(
        soup, ["plot detail", "plot type", "unit detail", "building detail"])
    if brows:
        bd = []
        for r in brows:
            vals = list(r.values())
            ft   = _clean(vals[0]) if vals else ""
            area = _clean(vals[1]) if len(vals) > 1 else ""
            if ft and ft.lower() not in ("s.no", "sl.no", "#", "no."):
                bd.append({"flat_type": ft, "total_area": area})
        if bd:
            out["building_details"] = bd

    # ── 11. Professional information ─────────────────────────────────────────
    prows = _parse_section_table(soup, ["professional", "engineer", "architect"])
    if prows:
        profs = []
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

    # ── 12. Co-promoter / land-owner details ─────────────────────────────────
    crows = _parse_section_table(soup, ["co-promoter", "co promoter", "land owner"])
    if crows:
        colist = []
        for r in crows:
            vals = list(r.values())
            e = {
                "name":            _clean(vals[0]) if vals else "",
                "survey_no":       _clean(vals[1]) if len(vals) > 1 else "",
                "land_share":      _clean(vals[2]) if len(vals) > 2 else "",
                "present_address": _clean(vals[3]) if len(vals) > 3 else "",
                "comm_address":    _clean(vals[4]) if len(vals) > 4 else "",
            }
            e = {k: v for k, v in e.items() if v}
            if e.get("name"):
                colist.append(e)
        if colist:
            out["co_promoter_details"] = colist

    # ── 13. Authorised signatory ─────────────────────────────────────────────
    srows = _parse_section_table(soup, ["authorised signatory", "authorized signatory"])
    if srows:
        vals = list(srows[0].values())
        sg = {
            "name":             _clean(vals[0]) if vals else "",
            "pan_no":           _clean(vals[1]) if len(vals) > 1 else "",
            "present_address":  _clean(vals[2]) if len(vals) > 2 else "",
            "official_address": _clean(vals[3]) if len(vals) > 3 else "",
            "permanent_address": _clean(vals[4]) if len(vals) > 4 else "",
        }
        sg["raw_address"] = sg.get("present_address") or sg.get("official_address", "")
        sg = {k: v for k, v in sg.items() if v}
        if sg.get("name"):
            out["authorised_signatory_details"] = sg

    # ── 14. Construction progress (total completion %) ───────────────────────
    total_pct = _pop_mapped("_total_completion_pct", "total completion percentage")
    if not total_pct:
        m = re.search(r"total\s+completion\s+percentage\s*[:\s]+(\d+\s*%?)", html, re.I)
        if m:
            total_pct = m.group(1).strip()
    if total_pct:
        if "%" not in total_pct:
            total_pct = f"{total_pct} %"
        out["construction_progress"] = [
            {"title": "total_completion_percentage", "progress_percentage": total_pct}
        ]

    # ── 15. Raw data snapshot ────────────────────────────────────────────────
    out["data"] = {
        "district":                  search_district,
        "START_PAGE":                str(start_page),
        "project_district":          district.upper() if district else "",
        "total_completion_percentage": total_pct or "",
    }

    return out



# ── Document extraction ───────────────────────────────────────────────────────

def _extract_documents(html: str, reg_no: str) -> list[dict]:
    """
    Extract all document links from the detail HTML.
    - Scans <a href> for /download_jc?DOC_ID= patterns; skips entries with empty DOC_ID.
    - Adds the auto-generated RERA registration certificate entry.
    Returns list of {link, type} dicts.
    """
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
        # Infer document type from link text or nearest sibling cell
        doc_type = _clean(a.get_text())
        if not doc_type:
            parent = a.find_parent("td")
            if parent:
                prev_td = parent.find_previous_sibling("td")
                doc_type = _clean(prev_td.get_text()) if prev_td else "Document"
        if not doc_type:
            doc_type = "Document"
        docs.append({"link": full_url, "type": doc_type})

    # Auto-add registration certificate for approved projects
    if reg_no:
        cert_link = f"{CERT_URL}?CER_NO={reg_no}"
        if cert_link not in seen_links:
            docs.append({"link": cert_link, "type": "Rera Registration Certificate 1"})

    return docs


def _handle_document(
    project_key: str,
    doc: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
) -> dict | None:
    """Download a document, upload to S3, and record in project_documents table."""
    url = doc.get("link")
    doc_type = doc.get("type", "document")
    if not url:
        return None
    filename = build_document_filename(doc)
    try:
        resp = safe_get(url, logger=logger, timeout=60.0)
        if not resp or len(resp.content) < 100:
            logger.warning("Document download empty or failed", url=url, step="documents")
            return None
        data = resp.content
        md5 = compute_md5(data)
        s3_key = upload_document(project_key, filename, data, dry_run=settings.DRY_RUN_S3)
        if s3_key is None:
            return None
        s3_url = get_s3_url(s3_key)
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
        return {**doc, "s3_link": s3_url}
    except Exception as exc:
        logger.error(f"Document processing failed: {exc}", url=url, step="documents")
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(exc),
                           url=url, project_key=project_key)
        return None


def _process_documents(
    project_key: str,
    documents: list[dict],
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
) -> tuple[list[dict], int]:
    enriched: list[dict] = []
    upload_count = 0
    for doc in documents:
        result = _handle_document(project_key, doc, run_id, site_id, logger)
        if result:
            enriched.append(result)
            upload_count += 1
        else:
            enriched.append(doc)
    return enriched, upload_count



# ── Sentinel ──────────────────────────────────────────────────────────────────

def _sentinel_check(logger: CrawlerLogger) -> bool:
    """Verify the Karnataka RERA portal is reachable and returns project rows."""
    test_district = DISTRICTS[0]

    html = _post_listing(test_district, 0, logger)
    if html is None:
        logger.error("Sentinel: listing POST failed — portal unreachable", step="sentinel")
        return False

    ack_nos = _extract_ack_nos(html)
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text().lower()

    if not ack_nos:
        logger.warning(
            "Sentinel: no ack_nos found for test district; portal may be blocking",
            district=test_district,
            step="sentinel",
        )
        if "error" in text or "not found" in text or len(soup.get_text()) < 200:
            logger.error("Sentinel: portal returned error page", step="sentinel")
            return False

    logger.info(
        f"Sentinel passed: {len(ack_nos)} project(s) found for district={test_district!r}",
        step="sentinel",
    )
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
    districts   = DISTRICTS
    items_processed = 0
    stop_all = False

    if not _sentinel_check(logger):
        return counters

    checkpoint = load_checkpoint(config["id"], mode) or {}
    start_district_idx = checkpoint.get("district_idx", 0)

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

            for listing_row in listing_rows:
                ack_no = listing_row["acknowledgement_no"]
                if stop_all:
                    break
                if item_limit and items_processed >= item_limit:
                    logger.info(f"Item limit {item_limit} reached — stopping", step="listing")
                    stop_all = True
                    break

                project_key = generate_project_key(ack_no)
                logger.set_project(
                    key=project_key,
                    reg_no=ack_no,
                    url=PROJECT_URL,
                    page=page_number,
                )
                try:

                    # ── Fetch and parse detail page ─────────────────────────────
                    detail_html = _post_detail(ack_no, logger)
                    if detail_html:
                        detail = _parse_detail(detail_html, ack_no, district, start_page)
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
                        "state":  config.get("state", "karnataka"),
                        "data":   merge_data_sections(detail.get("data"), {}),
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
                        if uploaded_docs and (mode == "weekly_deep" or status == "new"):
                            enriched, doc_count = _process_documents(
                                project_key, uploaded_docs, run_id, config["id"], logger,
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
        save_checkpoint(config["id"], mode, {"district_idx": district_idx + 1})
        logger.info(
            f"District complete: {district!r} — counters so far: {counters}",
            step="district_done",
        )

    reset_checkpoint(config["id"], mode)
    logger.info(f"Karnataka RERA crawl complete: {counters}", step="done")
    return counters
