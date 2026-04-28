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
from urllib.parse import urljoin
from typing import Any

from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import generate_project_key, random_delay, safe_get
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
            # enc may already start with "?" (e.g. "?enc=..."), so strip leading
            # "?" before joining to avoid the double-"??" bug.
            detail_url = f"{DETAIL_BASE}?{enc.lstrip('?')}"

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

def _fetch_detail(detail_url: str, logger: CrawlerLogger) -> BeautifulSoup | None:
    """Fetch a project detail page and return parsed soup."""
    resp = safe_get(detail_url, headers=_LISTING_HEADERS, retries=3, timeout=30, logger=logger)
    if not resp:
        return None
    return BeautifulSoup(resp.text, "lxml")


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


# ── Sub-table parsers ─────────────────────────────────────────────────────────

def _parse_building_details(soup: BeautifulSoup) -> list[dict] | None:
    """Parse the flat/unit GridView table; returns list of unit dicts or None."""
    building_keywords = {"block", "flat", "carpet", "balcony"}
    candidates: list[dict] = []

    for table in soup.find_all("table"):
        headers = [_clean(th.get_text()) for th in table.find_all("th")]
        if not headers:
            first_tr = table.find("tr")
            if first_tr:
                headers = [_clean(td.get_text()) for td in first_tr.find_all("td")]
        if not headers:
            continue

        hset = {h.lower() for h in headers if h}
        if not (building_keywords & hset):
            continue

        idx_map: dict[str, int] = {}
        for i, h in enumerate(headers):
            if not h:
                continue
            hl = h.lower()
            if "block" in hl:
                idx_map["block_name"] = i
            elif "flat name" in hl or "unit name" in hl:
                idx_map["flat_name"] = i
            elif "flat type" in hl or "unit type" in hl:
                idx_map["flat_type"] = i
            elif "carpet" in hl:
                idx_map["carpet_area"] = i
            elif "balcony" in hl:
                idx_map["balcony_area"] = i
            elif "open" in hl and "area" in hl:
                idx_map["open_area"] = i
            elif "total" in hl and "area" in hl:
                idx_map["total_area"] = i
            elif "no" in hl and "unit" in hl:
                idx_map["no_of_units"] = i

        if len(idx_map) < 3:
            continue

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if len(cells) < 3:
                continue
            row: dict = {}
            for field, idx in idx_map.items():
                if idx < len(cells):
                    val = _clean(cells[idx].get_text())
                    if val:
                        row[field] = val
            if row:
                candidates.append(row)

    return candidates or None


def _parse_documents(soup: BeautifulSoup, base_url: str) -> list[dict]:
    """Parse uploaded-documents section; returns list of {type, link?} dicts."""
    docs: list[dict] = []
    seen: set[str] = set()
    generic_doc_markers = (
        "/documents/notice/",
        "/views/downloads.aspx",
        "appealtobuyer.pdf",
        "legal_aprera_corporate_presentation.pdf",
    )
    generic_label_markers = (
        "cause list",
        "proceeding sheets",
        "special holiday",
        "regulations",
        "forms download",
        "presentation",
        "appeal to buyer",
        "quarterly updates",
        "office order",
    )

    for a in soup.find_all("a", href=True):
        href = str(a["href"]).strip()
        if not href or href == "#":
            continue
        if href.startswith("/"):
            href = BASE_URL + href
        elif not href.startswith("http"):
            href = urljoin(base_url, href)

        label = _clean(a.get_text(separator=" ")) or "Document"
        href_l = href.lower()
        label_l = label.lower()
        if any(marker in href_l for marker in generic_doc_markers):
            continue
        if any(marker in label_l for marker in generic_label_markers):
            continue

        # Try to get a better label from sibling cell
        tr = a.find_parent("tr")
        if tr:
            for cell in tr.find_all("td"):
                cell_text = _clean(cell.get_text())
                if cell_text and cell_text != label and len(cell_text) > 5:
                    label = cell_text
                    break

        doc: dict = {"type": label}
        if href.lower().endswith(".pdf") or "pdf" in href.lower() or "download" in href.lower():
            doc["link"] = href

        key = label.lower()
        if key not in seen:
            seen.add(key)
            docs.append(doc)

    # Strategy 2: numbered doc-type rows with no hyperlink
    if not docs:
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
                        link_tag = tr.find("a", href=True)
                        if link_tag:
                            href = str(link_tag["href"]).strip()
                            if href and href != "#":
                                if href.startswith("/"):
                                    href = BASE_URL + href
                                elif not href.startswith("http"):
                                    href = urljoin(base_url, href)
                                doc["link"] = href
                        docs.append(doc)
                        break
    return docs


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
        has_name = any("name" in h for h in hl)
        has_role = any(any(k in h for k in ["role", "designation", "type", "profession"]) for h in hl)
        has_addr = any("address" in h for h in hl)
        if not (has_name and (has_role or has_addr)):
            continue

        idx_name = next((i for i, h in enumerate(hl) if "name" in h), None)
        idx_role = next((i for i, h in enumerate(hl) if any(k in h for k in ["role", "designation", "type", "profession"])), None)
        idx_addr = next((i for i, h in enumerate(hl) if "address" in h), None)
        if idx_name is None:
            continue

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if len(cells) < 2:
                continue
            name = _clean(cells[idx_name].get_text()) if idx_name < len(cells) else None
            role = _clean(cells[idx_role].get_text()) if idx_role is not None and idx_role < len(cells) else None
            addr = _clean(cells[idx_addr].get_text()) if idx_addr is not None and idx_addr < len(cells) else None
            if not name:
                continue
            if role:
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
        has_member = any(any(k in h for k in ["email", "phone", "mobile", "position", "designation", "partner"]) for h in hl)
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
    Returns a raw dict with schema-aligned keys populated where found.
    """
    lv = _extract_label_values(soup)

    raw: dict[str, Any] = {}
    for label_text, value in lv.items():
        field = _LABEL_MAP.get(_norm_label(label_text))
        if field and field not in raw:
            raw[field] = value

    # ── Location ──────────────────────────────────────────────────────────────
    district  = raw.pop("_district", None)
    taluk     = raw.pop("_taluk", None)
    village   = raw.pop("_village", None)
    locality  = raw.pop("_locality", None)
    exact_loc = raw.pop("_exact_location", None)
    lat_raw   = raw.pop("_latitude", None)
    lon_raw   = raw.pop("_longitude", None)
    lat, lon  = _parse_lat_lon(lat_raw)
    pin_code  = raw.get("project_pin_code")

    addr_parts = [p for p in [exact_loc, locality, village, district, pin_code] if p]
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
    promoter_location_bits = {
        "village": village,
        "district": district,
        "locality": locality,
    }
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
                "land_area": str(land_val) if land_val is not None else None,
                "land_area_unit": _extract_unit(land_raw),
                "construction_area": str(const_val) if const_val is not None else None,
                "construction_area_unit": _extract_unit(const_raw),
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
        "unbuilt_area": unbuilt,
        "land_area_unit": _extract_unit(land_raw),
        "construction_area_unit": _extract_unit(const_raw),
        "project_district": district,
        "govt_type": "state",
    }.items():
        if v is not None:
            data_blob[k] = v
    if data_blob:
        raw["data"] = data_blob

    # ── Sub-tables ────────────────────────────────────────────────────────────
    bd = _parse_building_details(soup)
    if bd:
        raw["building_details"] = bd

    pi = _parse_professionals(soup)
    if pi:
        raw["professional_information"] = pi

    md = _parse_members(soup)
    if md:
        raw["members_details"] = md

    docs = _parse_documents(soup, detail_url)
    detail_has_project_fields = any(
        raw.get(field)
        for field in (
            "project_registration_no",
            "promoter_name",
            "project_location_raw",
            "building_details",
            "professional_information",
            "members_details",
            "land_area",
            "construction_area",
            "project_cost_detail",
            "number_of_residential_units",
            "number_of_commercial_units",
        )
    )
    if docs and detail_has_project_fields:
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
) -> dict | None:
    """Download a document, upload to S3, return a document result entry or None."""
    url = doc.get("link") or doc.get("source_url") or doc.get("url")
    if not url:
        return None
    try:
        resp = safe_get(url, headers=_LISTING_HEADERS, retries=2, timeout=60, logger=logger)
        if not resp or not resp.content:
            logger.warning("Document download empty", url=url)
            return None
        data     = resp.content
        filename = build_document_filename(doc)
        s3_key   = upload_document(project_key, filename, data,
                                   dry_run=getattr(settings, "DRY_RUN_S3", False))
        if s3_key is None:
            return None
        s3_url = get_s3_url(s3_key)
        return document_result_entry(doc, s3_url, filename)
    except Exception as exc:
        logger.warning("Document handling error", url=url, error=str(exc))
        return None


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

    # ── Checkpoint ────────────────────────────────────────────────────────────
    checkpoint  = load_checkpoint(site_id, mode) or {}
    done_keys: set[str] = set(checkpoint.get("done_keys", []))
    item_limit  = getattr(settings, "CRAWL_ITEM_LIMIT", 0) or 0

    # ── Listing ───────────────────────────────────────────────────────────────
    logger.info("Fetching AP RERA listing page", url=LISTING_URL)
    soup = _fetch_listing(logger)
    if not soup:
        insert_crawl_error(run_id, site_id, "LISTING_FAILED",
                           "Could not fetch listing page", url=LISTING_URL)
        return counts

    rows = _parse_listing_rows(soup)
    logger.info("Listing parsed", row_count=len(rows))
    counts["projects_found"] = len(rows)

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
        try:
            try:
                random_delay(delay_min, delay_max)
                logger.info("Fetching detail page")
                detail_soup = _fetch_detail(detail_url, logger)
                if not detail_soup:
                    logger.warning("Detail page fetch failed")
                    insert_crawl_error(run_id, site_id, "DETAIL_FAILED",
                                       "Detail page fetch failed",
                                       project_key=project_key, url=detail_url)
                    counts["error_count"] += 1
                    continue

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
                logger.info(f"DB result: {action}")

                # Documents
                all_docs = detail_data.get("uploaded_documents") or []
                doc_name_counts: dict[str, int] = {}
                uploaded_results: list[dict] = []

                for doc in all_docs:
                    selected = select_document_for_download(
                        config["state"], doc, doc_name_counts, domain=DOMAIN
                    )
                    if selected:
                        result = _handle_document(project_key, selected, run_id, site_id, logger)
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
            logger.clear_project()

    # ── Final checkpoint ──────────────────────────────────────────────────────
    save_checkpoint(site_id, mode, len(rows), None, run_id)
    reset_checkpoint(site_id, mode)
    logger.info("AP RERA crawl complete", **counts)
    return counts
