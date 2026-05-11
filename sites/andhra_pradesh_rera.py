"""
Andhra Pradesh RERA Crawler — rera.ap.gov.in
Type: Static (ASP.NET WebForms, server-rendered HTML + jQuery DataTables)

Strategy:
- Listing page: GET ApprovedProjects.aspx → parse ASP.NET GridView table
  (jQuery DataTables decorates the server-rendered table client-side — all rows
   are present in the initial HTML response).
  Columns: serial#, APRERA Registration ID, Project Name, Place, Project Type,
           Status, Date of Approval, Expected Date of Completion.
  Each row has an onclick="openProject(enc)" attribute whose `enc` value is used
  to build the detail URL: https://rera.ap.gov.in/RERA/Views/Project.aspx?{enc}

- Detail page: GET Project.aspx?{enc} → parse server-rendered HTML.
  Extracts: promoter, location, financial, building, professional, member,
            document fields — using robust label/value and table parsing.

- Documents: PDF links found in the uploaded-documents section are downloaded,
  checksummed, and uploaded to S3 (per SPEC §7 policy).
"""
from __future__ import annotations

import re
import time
from urllib.parse import urljoin
from typing import Any

import httpx
from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import generate_project_key, random_delay, safe_get, safe_post
from core.db import (
    get_project_by_key,
    upsert_project,
    insert_crawl_error,
)
from core.document_policy import select_document_for_download
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import (
    build_document_filename,
    build_document_urls,
    document_result_entry,
    get_machine_context,
    merge_data_sections,
    normalize_project_payload,
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL      = "https://rera.ap.gov.in"
LISTING_URL   = "https://rera.ap.gov.in/RERA/Views/Reports/ApprovedProjects.aspx"
DETAIL_BASE   = "https://rera.ap.gov.in/RERA/Views/Project.aspx"
DOMAIN        = "rera.ap.gov.in"
STATE         = "andhra pradesh"
STATE_CODE    = "AP"
PROJECT_STATE = "Andhra Pradesh"

_GRID_ID = "ContentPlaceHolder1_gvApprovedProject"

_LISTING_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": BASE_URL,
}

# Column indices in the listing GridView (0-based, skipping serial#)
_COL_REG_ID   = 1
_COL_NAME     = 2
_COL_PLACE    = 3
_COL_TYPE     = 4
_COL_STATUS   = 5
_COL_APPROVED = 6
_COL_FINISH   = 7


# ── String helpers ─────────────────────────────────────────────────────────────

def _clean(text: Any) -> str | None:
    if text is None:
        return None
    s = re.sub(r"\s+", " ", str(text)).strip()
    return s or None


def _cell(cells: list, idx: int) -> str | None:
    try:
        return _clean(cells[idx].get_text(separator=" ", strip=True))
    except IndexError:
        return None


def _parse_lat_lon(raw: str | None) -> tuple[float | None, float | None]:
    """Extract numeric lat/lon from strings like '17.825991N' / '83.354983E'."""
    if not raw:
        return None, None
    m = re.search(r"([\d.]+)\s*[NnSs]", raw)
    lat = float(m.group(1)) if m else None
    m = re.search(r"([\d.]+)\s*[EeWw]", raw)
    lon = float(m.group(1)) if m else None
    return lat, lon


def _parse_float_val(text: Any) -> float | None:
    if text is None:
        return None
    m = re.search(r"[\d.]+", str(text).replace(",", ""))
    try:
        return float(m.group()) if m else None
    except (ValueError, TypeError):
        return None


def _parse_int_val(text: Any) -> int | None:
    if text is None:
        return None
    m = re.search(r"\d+", str(text).replace(",", ""))
    try:
        return int(m.group()) if m else None
    except (ValueError, TypeError):
        return None


# ── Label/value extraction ─────────────────────────────────────────────────────

def _extract_label_values(soup: BeautifulSoup) -> dict[str, str]:
    """
    Scan all `<td>` or `<label>` + sibling/next-td pairs for label:value.
    AP RERA detail pages use several HTML patterns; this covers the common ones.
    """
    result: dict[str, str] = {}

    # Pattern 1: <td class="...label...">Label</td><td>Value</td>
    for tr in soup.find_all("tr"):
        cells = tr.find_all("td")
        i = 0
        while i < len(cells) - 1:
            label_text = _clean(cells[i].get_text(separator=" "))
            value_text = _clean(cells[i + 1].get_text(separator=" "))
            if label_text and value_text and len(label_text) < 120:
                result[label_text] = value_text
            i += 2

    # Pattern 2: <label>Key</label> <span/strong>Value</span>
    for label in soup.find_all("label"):
        key = _clean(label.get_text())
        if not key:
            continue
        sib = label.find_next_sibling(["span", "strong", "div", "td"])
        if sib:
            val = _clean(sib.get_text(separator=" "))
            if val:
                result[key] = val

    return result


# ── Listing page ──────────────────────────────────────────────────────────────

def _fetch_listing(logger: CrawlerLogger) -> BeautifulSoup | None:
    """Fetch the ApprovedProjects listing page and return parsed soup."""
    resp = safe_get(LISTING_URL, headers=_LISTING_HEADERS, retries=3,
                    timeout=45, logger=logger)
    if not resp:
        logger.error("Failed to fetch AP RERA listing page", url=LISTING_URL)
        return None
    return BeautifulSoup(resp.text, "lxml")


def _parse_listing_rows(soup: BeautifulSoup) -> list[dict]:
    """
    Parse the GridView table and return a list of listing-level project dicts.

    Each row contains: registration_no, project_name, place, project_type,
    status, approved_date, finish_date, and the enc token for the detail URL.
    """
    table = soup.find("table", id=_GRID_ID)
    if not table:
        # DataTables may render using a wrapper; try any table that contains
        # an onclick with openProject
        table = soup.find(
            "table",
            attrs={"id": re.compile(r"gvApprovedProject", re.I)},
        )
    if not table:
        return []

    rows: list[dict] = []
    seen_reg: set[str] = set()

    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 7:
            continue  # header or pager row

        reg_id = _cell(cells, _COL_REG_ID)
        if not reg_id or not re.match(r"P\d{11}", reg_id):
            # Skip pager rows (contain page numbers, not registration IDs)
            continue
        if reg_id in seen_reg:
            continue
        seen_reg.add(reg_id)

        # Extract enc from onclick of any element in the row
        enc: str | None = None
        onclick_src = tr.get("onclick", "") or ""
        if not onclick_src:
            for tag in tr.find_all(onclick=True):
                onclick_src = tag.get("onclick", "")
                if "openProject" in onclick_src:
                    break
        m = re.search(r"openProject\s*\(\s*['\"]([^'\"]+)['\"]", onclick_src)
        if m:
            enc = m.group(1)

        # Also try links inside the row (some portals use <a onclick="...">)
        if not enc:
            for a in tr.find_all("a", onclick=True):
                m = re.search(r"openProject\s*\(\s*['\"]([^'\"]+)['\"]",
                               a.get("onclick", ""))
                if m:
                    enc = m.group(1)
                    break

        detail_url: str | None = None
        if enc:
            # The listing JS does:  window.open("../Project.aspx?" + enc, ...)
            # enc always starts with "?enc=...", so the resulting URL is
            # "Project.aspx??enc=..." (double-??) which is what the server
            # requires.  Do NOT strip the leading "?" — the single-"?" URL
            # silently redirects to the home page.
            detail_url = f"{DETAIL_BASE}?{enc}"

        rows.append({
            "project_registration_no": reg_id,
            "project_name":            _cell(cells, _COL_NAME),
            "place":                   _cell(cells, _COL_PLACE),
            "project_type":            _cell(cells, _COL_TYPE),
            "status_of_the_project":   _cell(cells, _COL_STATUS),
            "approved_on_date":        _cell(cells, _COL_APPROVED),
            "estimated_finish_date":   _cell(cells, _COL_FINISH),
            "enc":                     enc,
            "detail_url":              detail_url,
        })

    return rows


# ── Detail page fetching ───────────────────────────────────────────────────────

def _fetch_detail(
    detail_url: str,
    logger: CrawlerLogger,
    client: httpx.Client | None = None,
) -> tuple[BeautifulSoup, dict] | tuple[None, dict]:
    """Fetch a project detail page and return (parsed soup, unused-cookie-stub).

    Pass *client* (a persistent httpx.Client) so that the ASP.NET session cookie
    set during this GET is automatically included in subsequent POST requests that
    use the same client — enabling document downloads without re-authenticating.
    """
    resp = safe_get(detail_url, headers=_LISTING_HEADERS, retries=3,
                    timeout=60, logger=logger, client=client)
    if not resp:
        return None, {}
    return BeautifulSoup(resp.text, "lxml"), {}


# ── Detail page field extraction ───────────────────────────────────────────────

# Mapping from AP RERA label text (lowercased, stripped) → schema field name.
# Multiple variants are listed because the portal label wording may differ
# between project types or portal revisions.
_LABEL_MAP: dict[str, str] = {
    # Project metadata
    "project name":                "project_name",
    "name of the project":         "project_name",
    "registration id":             "project_registration_no",
    "aprera registration id":      "project_registration_no",
    "registration number":         "project_registration_no",
    "project type":                "project_type",
    "type of project":             "project_type",
    "status":                      "status_of_the_project",
    "project status":              "status_of_the_project",
    # Promoter
    "promoter name":               "promoter_name",
    "name of the promoter":        "promoter_name",
    "name of promoter":            "promoter_name",
    # Dates
    "date of approval":            "approved_on_date",
    "approval date":               "approved_on_date",
    "date of commencement":        "actual_commencement_date",
    "commencement date":           "actual_commencement_date",
    "start date":                  "actual_commencement_date",
    "expected date of completion": "estimated_finish_date",
    "date of completion":          "actual_finish_date",
    "completion date":             "actual_finish_date",
    "revised completion date":     "estimated_finish_date",
    # Location
    "district":                    "_district",
    "taluk":                       "_taluk",
    "village":                     "_village",
    "locality":                    "_locality",
    "exact location":              "_exact_location",
    "pin code":                    "project_pin_code",
    "pincode":                     "project_pin_code",
    "latitude":                    "_latitude",
    "longitude":                   "_longitude",
    # Promoter contact
    "email":                       "_promoter_email",
    "email id":                    "_promoter_email",
    "mobile no":                   "_promoter_phone",
    "contact number":              "_promoter_phone",
    "phone":                       "_promoter_phone",
    # Land & construction
    "land area":                   "_land_area_raw",
    "total land area":             "_land_area_raw",
    "construction area":           "_construction_area_raw",
    "total built up area":         "_construction_area_raw",
    "built up area":               "_construction_area_raw",
    "unbuilt area":                "_unbuilt_area",
    "open area":                   "_unbuilt_area",
    # Costs
    "cost of land":                "_cost_of_land",
    "estimated construction cost": "_estimated_construction_cost",
    "total project cost":          "_total_project_cost",
    # Units
    "no. of residential units":    "number_of_residential_units",
    "residential units":           "number_of_residential_units",
    "no. of commercial units":     "number_of_commercial_units",
    "commercial units":            "number_of_commercial_units",
    # Promoter firm details
    "type of firm":                "_type_of_firm",
    "pan no":                      "_pan_no",
    "pan number":                  "_pan_no",
    "firm registration no":        "_firm_reg_no",
    "registration no":             "_firm_reg_no",
}

# ── ID-based field map (ASP.NET label id → internal field name) ───────────────
# The AP RERA detail page renders all values inside <label id="lbl..."> tags.
# _LABEL_MAP covers plain label/td pairs; _ID_MAP covers these ID-keyed labels.
_ID_MAP: dict[str, str] = {
    # Project metadata
    "lblprojectname":    "project_name",
    "lblPromoterName":   "promoter_name",
    "lblRegId":          "project_registration_no",
    "lblAppNo":          "acknowledgement_no",
    # Promoter firm details
    "lbltypepromoter":   "_type_of_firm",
    "lblotherregno":     "_firm_reg_no",
    "lblotherpan":       "_pan_no",
    "lblotheremail":     "_promoter_email",
    "lblothermobile":    "_promoter_phone",
    # Project type / status
    "lblprjtype":        "project_type",
    "lblprjstatus":      "status_of_the_project",
    # Dates  (DD/MM/YYYY format on page — normalizer parses them)
    "lblstartDate":      "actual_commencement_date",
    "lblendDate":        "_end_date_raw",      # mapped to both finish date fields below
    # Project location
    "lblprjdist":        "_district",
    "lblPrjMandal":      "_taluk",
    "lblPrjVillage":     "_village",
    "lblPrjPincode":     "project_pin_code",
    "lblprjlat":         "_latitude",
    "lblprjlong":        "_longitude",
    "lblprjadd1":        "_addr1",
    "lblprjadd2":        "_addr2",
    "lblulbname":        "_locality",
    # Land & construction areas (numeric, unit always Sq.m for AP)
    "lblareaofland":     "_land_area_raw",
    "lbltotbuilduparea": "_construction_area_raw",
    "lblopnarea":        "_unbuilt_area",
    # Project costs (INR)
    "lblestconcost":     "_estimated_construction_cost",
    "lbllndcost":        "_cost_of_land",
    "lbltotprjcost":     "_total_project_cost",
    # Authorised Signatory
    "lblASName":         "_as_name",
    "lblASMobile":       "_as_mobile",
    "lblASEmail":        "_as_email",
}

# Amenity progress label IDs → display name (matches sample status_update structure)
_AMENITY_ID_MAP: list[tuple[str, str]] = [
    ("lblroad",      "Road System"),
    ("lblwater",     "Water Supply"),
    ("lblsewage",    "Sewage and Drainage System"),
    ("lblelectric",  "Electricity Supply Transformer And Sub Station"),
    ("lblsolidwate", "Solid Waste Management And Disposal"),
    ("lblfire",      "Fire Fighting Facility"),
    ("lbldrinkwat",  "Drinking Water Facility"),
    ("lblemergency", "Eemrgency Evacuation Service"),
    ("lbluserenew",  "Use of Renewable Energy"),
]


# ── Normalisation helpers ─────────────────────────────────────────────────────

def _norm_label(text: str) -> str:
    """Lowercase + collapse spaces + strip trailing colon for _LABEL_MAP lookup."""
    return re.sub(r"\s+", " ", text.lower().strip()).strip(":")


def _extract_unit(text: Any) -> str | None:
    """Extract unit string: '575.26 Sq.m' → 'Sq.m'."""
    if not text:
        return None
    m = re.match(r"[\d.,]+\s*(.+)", str(text).strip())
    return _clean(m.group(1)) if m else None


def _lbl(soup: BeautifulSoup, lbl_id: str) -> str | None:
    """Return cleaned text of <label id=lbl_id>, or None if absent/NA."""
    tag = soup.find("label", id=lbl_id)
    if tag:
        val = _clean(tag.get_text(separator=" "))
        if val and val.upper() not in ("NA", "N/A", "-"):
            return val
    return None


def _extract_by_id(soup: BeautifulSoup) -> dict[str, str]:
    """
    Primary field extractor for AP RERA detail pages.
    Reads every <label id="lbl..."> value element and maps it via _ID_MAP.
    This is the main extraction path; _extract_label_values is a fallback.
    """
    result: dict[str, str] = {}
    for lbl_id, field in _ID_MAP.items():
        val = _lbl(soup, lbl_id)
        if val:
            result[field] = val
    return result


def _parse_authorised_signatory(soup: BeautifulSoup) -> dict | None:
    """Extract Authorised Signatory block using its dedicated label IDs."""
    name  = _lbl(soup, "lblASName")
    phone = _lbl(soup, "lblASMobile")
    email = _lbl(soup, "lblASEmail")
    entry = {k: v for k, v in {"name": name, "email": email, "phone": phone}.items() if v}
    return entry or None


def _parse_amenity_status(soup: BeautifulSoup) -> list[dict] | None:
    """
    Extract amenity completion percentages from their label IDs.
    Returns a list of {name, percent_completed} dicts, or None if not found.
    """
    amenities: list[dict] = []
    for lbl_id, name in _AMENITY_ID_MAP:
        val = _lbl(soup, lbl_id)
        if val is not None:
            amenities.append({"name": name, "percent_completed": val})
    return amenities or None


# ── Sub-table parsers ─────────────────────────────────────────────────────────

def _parse_ap_building_table(table) -> list[dict]:
    """
    Parse a single AP RERA building table which uses rowspan columns.

    The AP portal merges the Block Name and Built-up Area cells across all
    rows (rowspan = total rows), and the Floor Number cell across the flats
    on each floor (rowspan = flats per floor).  BeautifulSoup only sees the
    TD in the row it was written, so cell counts vary:

      12 cells → first row of table (has block + buildup + floor + flat-level cols)
      10 cells → first flat on a new floor (has floor + flat-level cols)
       9 cells → subsequent flats on the same floor (flat-level cols only)

    Flat-level column order (0-indexed within each variant):
      variant-12:  [block, buildup, floor, flat_no, flat_type, carpet,
                    ow, balcony, open_terrace, common, parking, total]
      variant-10:  [floor, flat_no, flat_type, carpet,
                    ow, balcony, open_terrace, common, parking, total]
      variant-9:   [flat_no, flat_type, carpet,
                    ow, balcony, open_terrace, common, parking, total]
    """
    results: list[dict] = []
    current_block: str | None = None

    for tr in table.find_all("tr")[1:]:        # skip header row
        cells = [_clean(td.get_text()) for td in tr.find_all("td")]
        n = len(cells)
        if n < 9:
            continue

        if n >= 12:
            current_block   = cells[0]
            flat_no, flat_type = cells[3], cells[4]
            carpet, ow, balcony, ot, total = cells[5], cells[6], cells[7], cells[8], cells[11]
        elif n >= 10:
            flat_no, flat_type = cells[1], cells[2]
            carpet, ow, balcony, ot, total = cells[3], cells[4], cells[5], cells[6], cells[9]
        else:   # 9 cells
            flat_no, flat_type = cells[0], cells[1]
            carpet, ow, balcony, ot, total = cells[2], cells[3], cells[4], cells[5], cells[8]

        if not flat_no:
            continue

        row: dict = {}
        if current_block:  row["block_name"]   = current_block
        if flat_no:        row["flat_name"]     = flat_no
        if flat_type:      row["flat_type"]     = flat_type
        if carpet:         row["carpet_area"]   = carpet
        if balcony:        row["balcony_area"]  = balcony
        if ot:             row["open_area"]     = ot
        if total:          row["total_area"]    = total
        row["no_of_units"] = "1"
        results.append(row)

    return results


def _parse_building_details(soup: BeautifulSoup) -> list[dict] | None:
    """
    Parse the flat/unit GridView tables from the AP RERA detail page.

    Returns the residential building units list (commercial units, if any,
    are returned separately via _parse_commercial_units).
    """
    for table in soup.find_all("table"):
        headers = [_clean(th.get_text()) for th in table.find_all("th")]
        if not headers:
            first_tr = table.find("tr")
            if first_tr:
                headers = [_clean(td.get_text()) for td in first_tr.find_all("td")]
        if not headers:
            continue

        hset = {h.lower() for h in headers if h}
        # AP residential table always has "Flat Number" and "Type of Flat"
        if "flat number" not in hset and "flat name" not in hset:
            continue

        rows = _parse_ap_building_table(table)
        if rows:
            return rows

    return None


def _parse_commercial_units(soup: BeautifulSoup) -> list[dict] | None:
    """Parse commercial unit rows from the AP RERA commercial building table."""
    for table in soup.find_all("table"):
        headers = [_clean(th.get_text()) for th in table.find_all("th")]
        if not headers:
            first_tr = table.find("tr")
            if first_tr:
                headers = [_clean(td.get_text()) for td in first_tr.find_all("td")]
        if not headers:
            continue

        hset = {h.lower() for h in headers if h}
        # AP commercial table uses "Unit Number" (not "Flat Number")
        if "unit number" not in hset:
            continue

        rows = _parse_ap_building_table(table)
        if rows:
            return rows

    return None


def _parse_documents(soup: BeautifulSoup, base_url: str) -> list[dict]:
    """
    Parse the uploaded-documents section from the AP RERA detail page.

    Strategy 1: locate the table whose header contains 'Document Type' and
    'Uploaded Document', then extract each row's document type name and the
    numeric document ID embedded in the anchor's onclick handler:
      onclick="return DownloadFile(N);"
    That ID is later POSTed as hdndocid to download the actual file.
    Rows where the anchor text is 'NA' (no file uploaded) are skipped.

    Strategy 2: fallback scan for numbered/APRERA-prefixed rows in any table.
    """
    docs: list[dict] = []
    seen: set[str] = set()

    # ── Strategy 1: targeted Document Type table ─────────────────────────────
    for table in soup.find_all("table"):
        headers = [_clean(th.get_text()) for th in table.find_all("th")]
        if not headers:
            first_tr = table.find("tr")
            if first_tr:
                headers = [_clean(td.get_text()) for td in first_tr.find_all("td")]
        if not headers:
            continue

        hset_lower = {h.lower() for h in headers if h}
        if "document type" not in hset_lower:
            continue

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if not cells:
                continue
            doc_type = _clean(cells[0].get_text())
            if not doc_type or doc_type.lower() == "document type":
                continue

            key = doc_type.lower()
            if key in seen:
                continue
            seen.add(key)

            doc: dict = {"type": doc_type}
            # Extract DownloadFile(N) doc_id from anchor onclick; skip NA rows.
            for a in tr.find_all("a"):
                onclick = a.get("onclick", "") or ""
                m_dl = re.search(r"DownloadFile\s*\(\s*(\d+)\s*\)", onclick)
                if not m_dl:
                    continue
                # 'NA' anchor text means no file was uploaded for this row
                if _clean(a.get_text()) == "NA":
                    break
                doc["doc_id"] = int(m_dl.group(1))
                # Also capture a direct PDF href if one exists (rare but possible)
                href = (a.get("href", "") or "").strip()
                if href and not href.startswith("javascript:") and href != "#":
                    if href.startswith("/"):
                        href = BASE_URL + href
                    elif not href.startswith("http"):
                        href = urljoin(base_url, href)
                    if href.lower().endswith(".pdf") or "pdf" in href.lower():
                        doc["link"] = href
                break
            docs.append(doc)

        if docs:
            return docs

    # ── Strategy 2: numbered / APRERA-prefixed rows (fallback) ───────────────
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            for cell in tr.find_all("td"):
                text = _clean(cell.get_text())
                if text and len(text) > 10 and (text[0].isdigit() or text.startswith("APRERA")):
                    key = text.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    doc = {"type": text}
                    link_tag = tr.find("a")
                    if link_tag:
                        onclick = link_tag.get("onclick", "") or ""
                        m_dl = re.search(r"DownloadFile\s*\(\s*(\d+)\s*\)", onclick)
                        if m_dl and _clean(link_tag.get_text()) != "NA":
                            doc["doc_id"] = int(m_dl.group(1))
                        href = (link_tag.get("href", "") or "").strip()
                        if href and not href.startswith("javascript:") and href != "#":
                            if href.startswith("/"):
                                href = BASE_URL + href
                            elif not href.startswith("http"):
                                href = urljoin(base_url, href)
                            if href.lower().endswith(".pdf") or "pdf" in href.lower():
                                doc["link"] = href
                    docs.append(doc)
                    break

    return docs


def _extract_asp_form_fields(soup: BeautifulSoup, detail_url: str) -> dict[str, str]:
    """
    Extract all ASP.NET hidden form fields and the resolved form action URL.

    Returns a flat dict of {field_name: value}.  A special key ``_form_action``
    holds the absolute POST URL.  This dict is passed to _handle_document so it
    can POST with hdndocid=N to trigger a server-side document download.
    """
    form = soup.find("form")
    if not form:
        return {}
    fields: dict[str, str] = {}
    for inp in form.find_all("input"):
        name = inp.get("name", "")
        val  = inp.get("value", "") or ""
        if name:
            fields[name] = val
    action = (form.get("action", "") or "").strip()
    if action.startswith("./"):
        action = "https://rera.ap.gov.in/RERA/Views/" + action[2:]
    elif action.startswith("/"):
        action = BASE_URL + action
    elif not action.startswith("http"):
        action = urljoin(detail_url, action)
    fields["_form_action"] = action
    return fields


def _parse_professionals(soup: BeautifulSoup) -> list[dict] | None:
    """Parse professional information (architect, engineer, CA) from detail page."""
    _role_norm = {
        "architect": "Project Architects",
        "structural": "Structural Engineers",
        "engineer": "Structural Engineers",
        "chartered": "Chartered Accountant",
        "accountant": "Chartered Accountant",
    }
    professionals: list[dict] = []

    for table in soup.find_all("table"):
        headers = [_clean(th.get_text(separator=" ")) for th in table.find_all("th")]
        if not headers:
            first_tr = table.find("tr")
            if first_tr:
                headers = [_clean(td.get_text(separator=" ")) for td in first_tr.find_all("td")]
        if not headers:
            continue

        hl = [h.lower() if h else "" for h in headers]

        # Skip building/unit tables (contain carpet area, balcony area, etc.)
        joined_hl = " ".join(h for h in hl if h)
        if any(k in joined_hl for k in ["carpet area", "balcony area", "built-up area", "built up area"]):
            continue
        # Skip partner/member tables (have 'position' or 'partner' in any column header,
        # e.g. "Position", "Name of the Partner", "Partner Type")
        if any("position" in h or "partner" in h for h in hl):
            continue

        has_name = any("name" in h for h in hl)
        _ROLE_KEYS = ["role", "designation", "type", "profession", "category", "qualification"]
        has_role = any(any(k in h for k in _ROLE_KEYS) for h in hl)
        has_addr = any("address" in h for h in hl)
        if not (has_name and (has_role or has_addr)):
            continue

        idx_name = next((i for i, h in enumerate(hl) if "name" in h), None)
        idx_role = next((i for i, h in enumerate(hl) if any(k in h for k in _ROLE_KEYS)), None)
        idx_addr = next((i for i, h in enumerate(hl) if "address" in h), None)
        if idx_name is None:
            continue

        # AP RERA professional tables embed the role in the name column header
        # (e.g. "Architect Name", "Engineer Name", "Chartered Accountant Name")
        # rather than having a separate role column.  Derive a fallback role from
        # the column header so that role is always populated when no explicit
        # role column exists.
        header_derived_role: str | None = None
        if idx_role is None and idx_name is not None:
            name_col_header = hl[idx_name] if idx_name < len(hl) else ""
            for kw, norm in _role_norm.items():
                if kw in name_col_header:
                    header_derived_role = norm
                    break

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if len(cells) < 2:
                continue
            name = _clean(cells[idx_name].get_text()) if idx_name < len(cells) else None
            role = _clean(cells[idx_role].get_text()) if idx_role is not None and idx_role < len(cells) else header_derived_role
            addr = _clean(cells[idx_addr].get_text()) if idx_addr is not None and idx_addr < len(cells) else None
            if not name:
                continue
            if role and idx_role is not None:
                # Normalise role text from an explicit role cell
                for kw, norm in _role_norm.items():
                    if kw in role.lower():
                        role = norm
                        break
            entry: dict = {}
            if name:
                entry["name"] = name
            if role:
                entry["role"] = role
            if addr:
                entry["address"] = addr
            if entry:
                professionals.append(entry)

    # Post-filter: drop entries whose role is an organisational title
    # (e.g. "Partner", "Director") rather than a recognised professional role.
    _KNOWN_PROFESSIONAL_ROLES = set(_role_norm.values())
    professionals = [
        p for p in professionals
        if "role" not in p or p["role"] in _KNOWN_PROFESSIONAL_ROLES
    ]

    # Deduplicate by (name, address) to remove repeated entries from
    # multiple table occurrences (e.g. structural engineer listed twice).
    seen_prof: set[tuple] = set()
    deduped: list[dict] = []
    for p in professionals:
        key = (p.get("name", ""), p.get("address", ""))
        if key not in seen_prof:
            seen_prof.add(key)
            deduped.append(p)
    professionals = deduped

    # Sort by (role, name) to produce a deterministic order regardless of the
    # sequence in which the portal renders the professional tables.
    professionals.sort(key=lambda p: (p.get("role", ""), p.get("name", "")))

    return professionals or None


def _parse_members(soup: BeautifulSoup) -> list[dict] | None:
    """Parse partner/member details from the project detail page."""
    members: list[dict] = []

    for table in soup.find_all("table"):
        headers = [_clean(th.get_text(separator=" ")) for th in table.find_all("th")]
        if not headers:
            first_tr = table.find("tr")
            if first_tr:
                headers = [_clean(td.get_text(separator=" ")) for td in first_tr.find_all("td")]
        if not headers:
            continue

        hl = [h.lower() if h else "" for h in headers]
        has_name = any("name" in h for h in hl)
        # Require an explicit position/partner/designation column to identify
        # member tables, avoiding accidental capture of professional tables
        # (which have phone/address but no position column).
        has_member = any(any(k in h for k in ["position", "designation", "partner"]) for h in hl)
        if not (has_name and has_member):
            continue

        idx_name  = next((i for i, h in enumerate(hl) if "name" in h), None)
        idx_email = next((i for i, h in enumerate(hl) if "email" in h), None)
        idx_phone = next((i for i, h in enumerate(hl) if any(k in h for k in ["phone", "mobile"])), None)
        idx_pos   = next((i for i, h in enumerate(hl) if any(k in h for k in ["position", "designation", "partner"])), None)
        if idx_name is None:
            continue

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if len(cells) < 2:
                continue
            name  = _clean(cells[idx_name].get_text())  if idx_name is not None and idx_name < len(cells) else None
            email = _clean(cells[idx_email].get_text()) if idx_email is not None and idx_email < len(cells) else None
            phone = _clean(cells[idx_phone].get_text()) if idx_phone is not None and idx_phone < len(cells) else None
            pos   = _clean(cells[idx_pos].get_text())   if idx_pos is not None and idx_pos < len(cells) else None
            if not name:
                continue
            entry: dict = {}
            if name:
                entry["name"] = name
            if email:
                entry["email"] = email
            if phone:
                entry["phone"] = phone
            if pos:
                entry["position"] = pos
            if entry:
                members.append(entry)

    return members or None


# ── Detail page orchestration ──────────────────────────────────────────────────

def _scrape_detail_page(soup: BeautifulSoup, detail_url: str) -> dict:
    """
    Extract all structured fields from a project detail page.

    Primary extraction uses _extract_by_id() which reads <label id="lbl...">
    elements — the main data carrier on AP RERA detail pages.
    _extract_label_values() is merged in as a secondary fallback for any
    field not already populated by the ID-based pass.

    Returns a raw dict with schema-aligned keys populated where found.
    """
    raw: dict[str, Any] = {}

    # ── Primary: ID-based extraction ─────────────────────────────────────────
    id_vals = _extract_by_id(soup)
    raw.update(id_vals)

    # ── Secondary: label/value fallback (doesn't overwrite ID results) ───────
    lv = _extract_label_values(soup)
    for label_text, value in lv.items():
        field = _LABEL_MAP.get(_norm_label(label_text))
        if field and field not in raw:
            raw[field] = value

    # ── End-date: both actual and estimated finish dates come from lblendDate ─
    end_date_raw = raw.pop("_end_date_raw", None)
    if end_date_raw:
        raw.setdefault("actual_finish_date",    end_date_raw)
        raw.setdefault("estimated_finish_date", end_date_raw)

    # ── Location ──────────────────────────────────────────────────────────────
    district  = raw.pop("_district", None)
    taluk     = raw.pop("_taluk", None)
    village   = raw.pop("_village", None)
    locality  = raw.pop("_locality", None)
    exact_loc = raw.pop("_exact_location", None)
    addr1     = raw.pop("_addr1", None)
    addr2     = raw.pop("_addr2", None)
    lat_raw   = raw.pop("_latitude", None)
    lon_raw   = raw.pop("_longitude", None)
    lat, _    = _parse_lat_lon(lat_raw)
    _, lon    = _parse_lat_lon(lon_raw)
    pin_code  = raw.get("project_pin_code")

    # Build exact_location from addr1 + addr2 when not already captured via label
    if not exact_loc:
        primary = " ".join(p for p in [addr1, addr2] if p)
        exact_loc = primary or None

    # Build raw_address: addr1+addr2 are a single primary address component, then
    # comma-separated from locality / village / district / pin_code.
    primary_addr = " ".join(p for p in [addr1, addr2] if p) or None
    addr_parts = [p for p in [primary_addr, locality, village, district, pin_code] if p]
    raw_address = ", ".join(addr_parts) if addr_parts else None

    loc_dict: dict = {}
    for k, v in {
        "taluk": taluk, "village": village, "district": district,
        "latitude": lat_raw, "locality": locality, "pin_code": pin_code,
        "longitude": lon_raw, "raw_address": raw_address,
        "exact_location": exact_loc,
        "processed_latitude": lat, "processed_longitude": lon,
    }.items():
        if v is not None:
            loc_dict[k] = v
    if loc_dict:
        raw["project_location_raw"] = loc_dict

    # ── Promoter address ──────────────────────────────────────────────────────
    promoter_location_bits = {"village": village, "district": district, "locality": locality}
    addr_dict: dict = {}
    if any(v for v in promoter_location_bits.values()):
        addr_dict["state"] = PROJECT_STATE
    for k, v in promoter_location_bits.items():
        if v is not None:
            addr_dict[k] = v
    if addr_dict:
        raw["promoter_address_raw"] = addr_dict

    # ── Promoter contact ──────────────────────────────────────────────────────
    email = raw.pop("_promoter_email", None)
    phone = raw.pop("_promoter_phone", None)
    if email or phone:
        raw["promoter_contact_details"] = {k: v for k, v in {"email": email, "phone": phone}.items() if v}

    # ── Promoters details (firm) ───────────────────────────────────────────────
    pan_no    = raw.pop("_pan_no", None)
    firm_type = raw.pop("_type_of_firm", None)
    firm_reg  = raw.pop("_firm_reg_no", None)
    pd: dict = {}
    for k, v in {"pan_no": pan_no, "type_of_firm": firm_type, "registration_no": firm_reg}.items():
        if v is not None:
            pd[k] = v
    if pd:
        raw["promoters_details"] = pd

    # ── Land & construction ───────────────────────────────────────────────────
    # AP RERA always uses Sq.m; the page shows bare numbers (no unit suffix).
    _AP_AREA_UNIT = "Sq.m"
    land_raw  = raw.pop("_land_area_raw", None)
    const_raw = raw.pop("_construction_area_raw", None)
    unbuilt   = raw.pop("_unbuilt_area", None)
    land_val  = _parse_float_val(land_raw)
    const_val = _parse_float_val(const_raw)
    if land_val is not None:
        raw["land_area"] = land_val
    if const_val is not None:
        raw["construction_area"] = const_val
        raw["total_floor_area_under_residential"] = const_val
    if land_val is not None or const_val is not None:
        raw["land_area_details"] = {
            k: v for k, v in {
                "land_area":               f"{land_val:.2f}"  if land_val  is not None else None,
                "land_area_unit":          _AP_AREA_UNIT      if land_val  is not None else None,
                "construction_area":       f"{const_val:.2f}" if const_val is not None else None,
                "construction_area_unit":  _AP_AREA_UNIT      if const_val is not None else None,
            }.items() if v is not None
        }

    # ── Project cost ──────────────────────────────────────────────────────────
    cost_land  = raw.pop("_cost_of_land", None)
    cost_const = raw.pop("_estimated_construction_cost", None)
    cost_total = raw.pop("_total_project_cost", None)
    cost_dict: dict = {}
    for k, v in {
        "cost_of_land": cost_land,
        "estimated_construction_cost": cost_const,
        "total_project_cost": cost_total,
    }.items():
        if v is not None:
            cost_dict[k] = v
    if cost_dict:
        raw["project_cost_detail"] = cost_dict

    # ── Data blob (ancillary fields) ──────────────────────────────────────────
    data_blob: dict = {}
    for k, v in {
        "unbuilt_area":          unbuilt,
        "end_date":              end_date_raw,
        "land_area_unit":        _AP_AREA_UNIT if land_val is not None else None,
        "construction_area_unit": _AP_AREA_UNIT if const_val is not None else None,
        "project_district":      district,
        "govt_type":             "state",
    }.items():
        if v is not None:
            data_blob[k] = v
    if data_blob:
        raw["data"] = data_blob

    # ── Authorised Signatory ──────────────────────────────────────────────────
    as_name  = raw.pop("_as_name", None)
    as_phone = raw.pop("_as_mobile", None)
    as_email = raw.pop("_as_email", None)
    as_dict  = {k: v for k, v in {"name": as_name, "email": as_email, "phone": as_phone}.items() if v}
    if as_dict:
        raw["authorised_signatory_details"] = as_dict

    # ── Building details ──────────────────────────────────────────────────────
    bd = _parse_building_details(soup)
    if bd:
        raw["building_details"] = bd
        raw["number_of_residential_units"] = len(bd)

    comm_bd = _parse_commercial_units(soup)
    # Prefer the explicit page label value (captured by label extraction into
    # raw["number_of_commercial_units"] via _LABEL_MAP) over counting rows from
    # the commercial-unit table parser, which can false-match other tables.
    if "number_of_commercial_units" not in raw:
        raw["number_of_commercial_units"] = len(comm_bd) if comm_bd else 0

    # ── Professionals / Members ───────────────────────────────────────────────
    pi = _parse_professionals(soup)
    if pi:
        raw["professional_information"] = pi

    md = _parse_members(soup)
    if md:
        raw["members_details"] = md

    # ── Status update (amenity progress + building snapshot) ─────────────────
    amenities = _parse_amenity_status(soup)
    if amenities or bd:
        status_entry: dict = {}
        if amenities:
            status_entry["amenity_detail"] = amenities
        if bd:
            status_entry["building_details"] = bd
        raw["status_update"] = [status_entry]

    # ── Documents ─────────────────────────────────────────────────────────────
    docs = _parse_documents(soup, detail_url)
    if docs and any(raw.get(f) for f in (
        "project_registration_no", "promoter_name", "project_location_raw",
        "building_details", "land_area", "construction_area",
        "project_cost_detail", "number_of_residential_units",
    )):
        raw["uploaded_documents"] = docs

    raw["url"] = detail_url
    raw["project_state"] = PROJECT_STATE
    return raw




# ── Document download ─────────────────────────────────────────────────────────

def _handle_document(
    project_key: str,
    doc: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
    form_fields: dict[str, str] | None = None,
    client: httpx.Client | None = None,
) -> dict | None:
    """
    Download a document, upload to S3, return a document result entry or None.

    Primary path (AP RERA): POST to the detail page with hdndocid=doc["doc_id"]
    plus all ASP.NET hidden form fields (ViewState, EventValidation, …) captured
    when the detail page was first fetched.  The server responds with the raw
    file bytes (Content-Disposition: attachment).

    *client* MUST be the same httpx.Client that was used to fetch the detail page
    so that the ASP.NET_SessionId cookie is automatically included in the POST.

    Fallback: plain GET to doc["link"] for any doc that carries a direct URL
    (rare on AP RERA but kept for robustness).
    """
    doc_id      = doc.get("doc_id")
    direct_url  = doc.get("link") or doc.get("source_url") or doc.get("url")

    resp = None
    source_url: str = ""

    if doc_id is not None and form_fields:
        form_action = form_fields.get("_form_action", "")
        if not form_action:
            logger.warning("No form action — cannot POST for doc_id", doc_id=doc_id)
        else:
            # Exclude only our own internal metadata keys (prefixed with a single
            # underscore + lowercase letter).  ASP.NET hidden fields like
            # __VIEWSTATE and __EVENTVALIDATION also start with '_' but must
            # be included — they are identified by the double-underscore prefix.
            _SKIP = frozenset({"_form_action", "_session_cookies", "_detail_url"})
            post_data = {k: v for k, v in form_fields.items() if k not in _SKIP}
            post_data["hdndocid"]        = str(doc_id)
            post_data["btndownload"]     = ""
            post_data["__EVENTTARGET"]   = ""
            post_data["__EVENTARGUMENT"] = ""
            post_headers = {**_LISTING_HEADERS,
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Referer": form_action}
            try:
                # Use the shared project_client so the ASP.NET_SessionId cookie
                # accumulated during the detail-page GET is automatically sent.
                resp = safe_post(form_action, data=post_data, headers=post_headers,
                                 retries=2, timeout=60, logger=logger, client=client)
                if resp is not None:
                    ct = resp.headers.get("content-type", "")
                    if "html" in ct.lower():
                        logger.warning("Document POST returned HTML — skipping",
                                       doc_id=doc_id, content_type=ct)
                        resp = None
                    else:
                        source_url = form_action
            except Exception as exc:
                logger.warning("Document POST error", doc_id=doc_id, error=str(exc))
                resp = None

    if resp is None and direct_url:
        # Fallback: direct GET (e.g. rare projects with a plain PDF href)
        try:
            resp = safe_get(direct_url, headers=_LISTING_HEADERS,
                            retries=2, timeout=60, logger=logger, client=client)
            source_url = direct_url
        except Exception as exc:
            logger.warning("Document GET error", url=direct_url, error=str(exc))
            return None

    if not resp or not resp.content:
        logger.warning("Document download empty", doc_id=doc_id, url=source_url)
        return None

    try:
        content  = resp.content
        filename = build_document_filename(doc)
        s3_key   = upload_document(project_key, filename, content,
                                   dry_run=getattr(settings, "DRY_RUN_S3", False))
        if s3_key is None:
            return None
        s3_url = get_s3_url(s3_key)
        label  = doc.get("type") or doc.get("label") or "document"
        logger.info("Document uploaded", label=label, s3_key=s3_key, step="documents")
        logger.log_document(label, source_url, "uploaded",
                            s3_key=s3_key, file_size_bytes=len(content))
        return document_result_entry({**doc, "source_url": source_url}, s3_url, filename)
    except Exception as exc:
        logger.warning("Document handling error", doc_id=doc_id,
                       url=source_url, error=str(exc))
        return None


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Andhra Pradesh RERA.
    Loads state_projects_sample/andhra_pradesh.json as the baseline, re-scrapes
    the sentinel project's detail page, and verifies ≥ 80% field coverage.
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
        "state_projects_sample", "andhra_pradesh.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check",
                       path=sample_path, step="sentinel")
        return True

    detail_url = baseline.get("url", "")
    if not detail_url:
        logger.warning("Sentinel: no detail URL in sample — skipping", step="sentinel")
        return True

    logger.info(f"Sentinel: scraping {sentinel_reg}", url=detail_url, step="sentinel")
    try:
        soup, _ = _fetch_detail(detail_url, logger)
        if not soup:
            logger.error("Sentinel: failed to fetch detail page", url=detail_url, step="sentinel")
            return False
        fresh = _scrape_detail_page(soup, detail_url) or {}
    except Exception as exc:
        logger.error(f"Sentinel: scrape error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", url=detail_url, step="sentinel")
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "andhra_pradesh_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


# ── Main entry point ──────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    """
    Main entry point for the Andhra Pradesh RERA crawler.

    Steps:
    1. Fetch listing page → parse all project rows from the GridView table.
    2. For each project, fetch the detail page → extract fields.
    3. Normalize + validate → upsert to DB.
    4. Download and upload selected documents to S3.
    5. Save checkpoint periodically and on completion.
    """
    site_id   = config["id"]
    config_id = config.get("config_id")
    logger    = CrawlerLogger(site_id, run_id)
    counts    = dict(projects_found=0, projects_new=0, projects_updated=0,
                     projects_skipped=0, documents_uploaded=0, error_count=0)

    machine_name, machine_ip = get_machine_context()
    t_run = time.monotonic()

    # ── Sentinel health check ────────────────────────────────────────────────
    t0 = time.monotonic()
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counts["error_count"] += 1
        return counts
    logger.warning(f"Step timing [sentinel]: {time.monotonic()-t0:.2f}s", step="timing")

    # ── Checkpoint ────────────────────────────────────────────────────────────
    checkpoint  = load_checkpoint(site_id, mode) or {}
    done_keys: set[str] = set(checkpoint.get("done_keys", []))
    item_limit  = getattr(settings, "CRAWL_ITEM_LIMIT", 0) or 0

    # ── Listing ───────────────────────────────────────────────────────────────
    t0 = time.monotonic()
    logger.info("Fetching AP RERA listing page", url=LISTING_URL)
    soup = _fetch_listing(logger)
    if not soup:
        insert_crawl_error(run_id, site_id, "LISTING_FAILED",
                           "Could not fetch listing page", url=LISTING_URL)
        return counts

    rows = _parse_listing_rows(soup)
    logger.info("Listing parsed", row_count=len(rows))
    counts["projects_found"] = len(rows)
    logger.warning(f"Step timing [search]: {time.monotonic()-t0:.2f}s  rows={len(rows)}", step="timing")

    if not rows:
        insert_crawl_error(run_id, site_id, "NO_PROJECTS",
                           "No project rows parsed from listing", url=LISTING_URL)
        return counts

    delay_min, delay_max = config.get("rate_limit_delay", (1, 3))

    # ── Per-project loop ──────────────────────────────────────────────────────
    for i, row in enumerate(rows):
        if item_limit and i >= item_limit:
            logger.info("Item limit reached", limit=item_limit)
            break

        reg_no      = row.get("project_registration_no") or ""
        detail_url  = row.get("detail_url")
        project_key = generate_project_key(reg_no) if reg_no else ""

        if not reg_no or not project_key:
            counts["error_count"] += 1
            continue

        # Skip if already processed this run (non-full modes)
        if project_key in done_keys and mode != "full":
            counts["projects_skipped"] += 1
            continue

        # daily_light: skip if already in DB
        if mode == "daily_light" and get_project_by_key(project_key):
            counts["projects_skipped"] += 1
            done_keys.add(project_key)
            continue

        if not detail_url:
            logger.warning("No detail URL for project", reg_no=reg_no)
            counts["error_count"] += 1
            continue
        logger.set_project(key=project_key, reg_no=reg_no, url=detail_url, page=i)
        # One persistent client per project: preserves ASP.NET_SessionId across
        # the detail-page GET and all subsequent document POSTs.
        project_client = httpx.Client(timeout=60, follow_redirects=True)
        try:
            try:
                random_delay(delay_min, delay_max)
                logger.info("Fetching detail page")
                detail_soup, _cookies = _fetch_detail(
                    detail_url, logger, client=project_client
                )
                if not detail_soup:
                    logger.warning("Detail page fetch failed")
                    insert_crawl_error(run_id, site_id, "DETAIL_FAILED",
                                       "Detail page fetch failed",
                                       project_key=project_key, url=detail_url)
                    counts["error_count"] += 1
                    continue

                # Capture ASP.NET form fields; the shared client carries cookies.
                form_fields = _extract_asp_form_fields(detail_soup, detail_url)

                # Extract detail fields
                detail_data = _scrape_detail_page(detail_soup, detail_url)

                # Fill in listing-level data where detail didn't provide it
                detail_data.setdefault("project_registration_no", reg_no)
                detail_data.setdefault("project_name",          row.get("project_name"))
                detail_data.setdefault("project_type",          row.get("project_type"))
                detail_data.setdefault("status_of_the_project", row.get("status_of_the_project"))
                detail_data.setdefault("approved_on_date",      row.get("approved_on_date"))
                detail_data.setdefault("estimated_finish_date", row.get("estimated_finish_date"))

                # Core metadata
                detail_data["state"]            = config["state"]
                detail_data["project_state"]    = PROJECT_STATE
                detail_data["domain"]           = DOMAIN
                detail_data["config_id"]        = config_id
                detail_data["crawl_machine_ip"] = machine_ip
                detail_data["machine_name"]     = machine_name
                detail_data["key"]              = project_key
                detail_data["is_live"]          = True

                # Merge raw data blob with listing info
                existing_data = detail_data.pop("data", None) or {}
                detail_data["data"] = merge_data_sections(
                    existing_data,
                    {"listing_row": {k: v for k, v in row.items() if k != "enc"}},
                )

                # Normalize + validate
                try:
                    normalized = normalize_project_payload(
                        detail_data, config,
                        machine_name=machine_name, machine_ip=machine_ip,
                    )
                    record  = ProjectRecord(**normalized)
                    db_dict = record.to_db_dict()
                except (ValidationError, ValueError) as ve:
                    logger.warning("Validation failed — raw fallback", error=str(ve))
                    insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(ve),
                                       project_key=project_key, url=detail_url)
                    counts["error_count"] += 1
                    try:
                        detail_data["data"] = merge_data_sections(
                            detail_data.get("data"), {"validation_fallback": True}
                        )
                        db_dict = normalize_project_payload(
                            detail_data, config,
                            machine_name=machine_name, machine_ip=machine_ip,
                        )
                    except Exception:
                        continue

                # Upsert to DB
                logger.info("Upserting to DB")
                action = upsert_project(db_dict)
                if action == "new":
                    counts["projects_new"] += 1
                elif action == "updated":
                    counts["projects_updated"] += 1
                else:
                    counts["projects_skipped"] += 1
                logger.info(f"DB result: {action}", step="db_upsert")

                # Documents
                all_docs = detail_data.get("uploaded_documents") or []
                doc_name_counts: dict[str, int] = {}
                uploaded_results: list[dict] = []

                for doc in all_docs:
                    selected = select_document_for_download(
                        config["state"], doc, doc_name_counts, domain=DOMAIN
                    )
                    if selected:
                        result = _handle_document(project_key, selected, run_id, site_id, logger,
                                                  form_fields=form_fields,
                                                  client=project_client)
                        if result:
                            uploaded_results.append(result)
                            counts["documents_uploaded"] += 1

                if uploaded_results:
                    upsert_project({
                        "key":                    db_dict["key"],
                        "url":                    db_dict.get("url"),
                        "state":                  db_dict["state"],
                        "domain":                 db_dict.get("domain"),
                        "project_registration_no": db_dict["project_registration_no"],
                        "uploaded_documents":     uploaded_results,
                        "document_urls":          build_document_urls(uploaded_results),
                    })

                done_keys.add(project_key)

                if (i + 1) % 50 == 0:
                    save_checkpoint(site_id, mode, i + 1, project_key, run_id)
                    logger.info("Checkpoint saved", done=i + 1, total=len(rows))

            except Exception as exc:
                logger.error("Project processing error", error=str(exc))
                insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                                   project_key=project_key, url=detail_url)
                counts["error_count"] += 1
        finally:
            project_client.close()
            logger.clear_project()

    # ── Final checkpoint ──────────────────────────────────────────────────────
    save_checkpoint(site_id, mode, len(rows), None, run_id)
    reset_checkpoint(site_id, mode)
    logger.info("AP RERA crawl complete", **counts)
    logger.warning(f"Step timing [total_run]: {time.monotonic()-t_run:.2f}s", step="timing")
    return counts
