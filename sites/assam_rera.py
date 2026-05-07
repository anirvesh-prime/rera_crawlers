"""
Assam RERA Crawler — rera.assam.gov.in
Type: static (server-rendered HTML, CodeIgniter PHP)

Single listing URL:
  /admincontrol/registered_projects/1  →  all registered projects

Strategy:
- Listing page loads ALL projects in a single HTML response (DataTables, client-side).
- Parse #compliant_hearing DataTable rows → collect stubs (reg_no, name, location, detail link).
- For each stub: fetch /view_project/searchprojectDetail/{id}
    → extract project_id (acknowledgement_no), submitted_date, status, approval_date, cert_url,
      Form-A link.
- Fetch /view_project/project_preview_open/{id} (Form-A) → extract bank, land, cost, units,
    facilities, uploaded documents.
- De-duplicate by project_registration_no.
"""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from core.checkpoint import load_checkpoint, save_checkpoint
from core.crawler_base import generate_project_key, random_delay, safe_get
from core.db import get_project_by_key, upsert_project, insert_crawl_error, upsert_document
from core.document_policy import select_document_for_download
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import (
    build_document_filename,
    build_document_urls,
    document_result_entry,
    get_machine_context,
    normalize_project_payload,
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings
from pydantic import ValidationError

BASE_URL   = "https://rera.assam.gov.in"
STATE_CODE = "AS"
STATE      = "assam"
DOMAIN     = "rera.assam.gov.in"
CONFIG_ID  = 11804

LISTING_URL = f"{BASE_URL}/admincontrol/registered_projects/1"


# ── Date helpers ──────────────────────────────────────────────────────────────

_MONTH_ABBR: dict[str, str] = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def _normalize_date_str(val: Any) -> str | None:
    """Normalize Assam RERA date strings to canonical ISO timestamptz format."""
    if not val:
        return None
    v = str(val).strip()
    if not v or v in ("-", "--", "---", "NA", "N/A", "null", "None"):
        return None
    # dd-Mon-yyyy  (e.g. 27-Dec-2024)
    m = re.match(r"^(\d{1,2})-([A-Za-z]{3})-(\d{4})$", v)
    if m:
        mon = _MONTH_ABBR.get(m.group(2).lower())
        if mon:
            return f"{m.group(3)}-{mon}-{int(m.group(1)):02d} 00:00:00+00:00"
    # dd-mm-yyyy [HH:MM:SS [AM/PM]]
    m = re.match(
        r"^(\d{2})-(\d{2})-(\d{4})(?:\s+(\d{2}):(\d{2}):(\d{2})(?:\s*(AM|PM))?)?", v, re.I
    )
    if m:
        day, mon, yr = m.group(1), m.group(2), m.group(3)
        hr, mi, sec = m.group(4) or "00", m.group(5) or "00", m.group(6) or "00"
        ampm = (m.group(7) or "").upper()
        if ampm == "PM" and int(hr) < 12:
            hr = str(int(hr) + 12).zfill(2)
        elif ampm == "AM" and int(hr) == 12:
            hr = "00"
        return f"{yr}-{mon}-{day} {hr}:{mi}:{sec}+00:00"
    # dd/mm/yyyy
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", v)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)} 00:00:00+00:00"
    # yyyy-mm-dd
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", v)
    if m:
        return f"{v} 00:00:00+00:00"
    return None


def _float_val(text: Any) -> float | None:
    if not text:
        return None
    m = re.search(r"([\d,]+\.?\d*)", str(text).replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _clean_phone_val(val: str) -> str:
    """Strip HTML noise (&nbsp, trailing annotations) from phone numbers."""
    if not val:
        return ""
    # Remove &nbsp and everything that follows (browser-rendered noise)
    val = re.sub(r"&nbsp.*|[^\S\n]*\(Number\s+Shared.*", "", val, flags=re.I)
    # Remove any remaining non-digit, non-+ characters that aren't part of a number
    return val.strip()


# ── Listing page parsing ──────────────────────────────────────────────────────

def _fetch_listing(logger: CrawlerLogger) -> list[dict]:
    """Fetch Assam RERA listing page and return all project stubs."""
    resp = safe_get(LISTING_URL, logger=logger, timeout=90.0)
    if not resp:
        logger.warning("Listing fetch failed", url=LISTING_URL)
        return []
    soup = BeautifulSoup(resp.text, "lxml")
    return _parse_listing_rows(soup)


def _parse_listing_rows(soup: BeautifulSoup) -> list[dict]:
    """
    Parse the #compliant_hearing DataTable from the Assam RERA listing page.

    Visible column mapping (0-indexed, after BeautifulSoup strips HTML comments):
      0  Serial No.
      1  Registration Certificate Number  → project_registration_no (has <a> to searchprojectDetail)
      2  Project Name                     → project_name
      3  Promoter                         → promoter_name
      4  Project Location                 → project_location_raw.raw_address
      5  Project District                 → project_city / project_location_raw.district
      6  View Webpage                     → (skipped)
      7  View Certificate                 → cert_url (may be '---')
      8  View Occupancy                   → (skipped)
      9  View Cumulative QPR              → (skipped)
      10 View Quarterly Progress          → (skipped)
      11 Approved Date                    → approved_on_date
      12 Expiry Date                      → estimated_finish_date
    """
    table = (
        soup.find("table", id="compliant_hearing")
        or soup.find("table", id="example")
    )
    if not table:
        tables = soup.find_all("table")
        table = tables[0] if tables else None
    if not table:
        return []

    results: list[dict] = []
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 11:
            continue

        def cell_text(idx: int) -> str:
            return cells[idx].get_text(separator=" ", strip=True) if idx < len(cells) else ""

        def cell_link(idx: int, pattern: str | None = None) -> str | None:
            if idx >= len(cells):
                return None
            cell = cells[idx]
            # Check <a href> tags first
            for a in cell.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("javascript"):
                    continue
                if pattern and pattern not in href:
                    continue
                return urljoin(BASE_URL, href)
            # Assam RERA uses <form action> (POST) for certificate links
            for form in cell.find_all("form", action=True):
                action = form["action"].strip()
                if pattern and pattern not in action:
                    continue
                return urljoin(BASE_URL, action)
            return None

        # Col 1: reg number + detail link
        detail_url = cell_link(1, "searchprojectDetail")
        reg_no_raw = cell_text(1)
        # Strip any stray numeric suffixes or spaces
        reg_no = reg_no_raw.strip()
        if not reg_no or not re.search(r"[A-Z0-9]", reg_no):
            continue

        # Internal ID from detail URL
        internal_id: str | None = None
        if detail_url:
            m = re.search(r"/searchprojectDetail/(\d+)", detail_url)
            if m:
                internal_id = m.group(1)

        # Col 7: certificate link
        cert_url = cell_link(7, "view_certificate")

        # Col 11: Approved Date  Col 12: Expiry Date
        approved_date = _normalize_date_str(cell_text(11))
        expiry_date   = _normalize_date_str(cell_text(12))

        results.append({
            "project_registration_no": reg_no,
            "project_name":            cell_text(2),
            "promoter_name":           cell_text(3),
            "project_location_raw_address": cell_text(4),
            "project_city":            cell_text(5).upper(),
            "detail_url":              detail_url or (
                f"{BASE_URL}/view_project/searchprojectDetail/{internal_id}"
                if internal_id else None
            ),
            "internal_id":             internal_id,
            "cert_url":                cert_url,
            "approved_on_date":        approved_date,
            "estimated_finish_date":   expiry_date,
        })

    return results


# ── searchprojectDetail page parser ──────────────────────────────────────────

def _parse_detail_page(html: str, detail_url: str) -> dict:
    """
    Parse /view_project/searchprojectDetail/{id}.

    Extracts from three sections:
    1. Project Location Details → project_name, raw_address, district, tehsil, promoter_name
    2. Project Detail           → acknowledgement_no, submitted_date, status, form_a_url
    3. Project Approval Status  → approved_on_date, cert_url (if not found in listing)
    """
    soup = BeautifulSoup(html, "lxml")
    out: dict[str, Any] = {"url": detail_url}

    tables = soup.find_all("table")

    # ── 1. Project Location Details ──────────────────────────────────────────
    for table in tables:
        header_text = table.get_text(" ", strip=True).lower()
        rows = table.find_all("tr")
        if not rows:
            continue
        first_row_headers = [c.get_text(strip=True).lower() for c in rows[0].find_all(["th", "td"])]
        if "project name" in first_row_headers and "promoter name" in first_row_headers:
            if len(rows) > 1:
                vals = [c.get_text(separator=" ", strip=True) for c in rows[1].find_all("td")]
                header_keys = [c.get_text(strip=True).lower() for c in rows[0].find_all(["th", "td"])]
                for i, key in enumerate(header_keys):
                    if i >= len(vals):
                        break
                    v = vals[i]
                    if "project name" == key:
                        out["project_name"] = v
                    elif "project location" == key:
                        out["project_location_address"] = v
                    elif "project district" == key:
                        out["project_district"] = v
                    elif "project tehsil" == key or "tehsil" in key:
                        out["project_tehsil"] = v
                    elif "promoter name" == key:
                        out["promoter_name_detail"] = v
            break

    # ── 2. Project Detail ────────────────────────────────────────────────────
    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue
        header_keys = [c.get_text(strip=True).lower() for c in rows[0].find_all(["th", "td"])]
        if "project registration number" not in header_keys:
            continue
        if len(rows) > 1:
            vals = [c.get_text(separator=" ", strip=True) for c in rows[1].find_all("td")]
            # Also look for Form-A link inside cells
            for cell in rows[1].find_all("td"):
                for a in cell.find_all("a", href=True):
                    href = a["href"]
                    if "project_preview_open" in href:
                        out["form_a_url"] = urljoin(BASE_URL, href)
                        break
            for i, key in enumerate(header_keys):
                if i >= len(vals):
                    break
                v = vals[i]
                if "project id" == key:
                    out["acknowledgement_no"] = v
                elif "online submission date" in key:
                    out["submitted_date"] = _normalize_date_str(v)
                elif "receiving date" in key:
                    out["receiving_date"] = _normalize_date_str(v)
                elif "current status" in key:
                    out["current_status"] = v
        break

    # ── 3. Project Approval Status ───────────────────────────────────────────
    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue
        header_text = rows[0].get_text(" ", strip=True).lower()
        if "approval" not in header_text and "certificate" not in header_text:
            continue
        if len(rows) > 1:
            data_row = rows[1]
            cells = data_row.find_all("td")
            # Find date cell — matches dd-Mon-yyyy or dd-mm-yyyy pattern
            for cell in cells:
                txt = cell.get_text(strip=True)
                d = _normalize_date_str(txt)
                if d and "approved_on_date" not in out:
                    out["approved_on_date"] = d
            # Find certificate link — site uses <form action> (POST), not <a href>
            for cell in cells:
                for a in cell.find_all("a", href=True):
                    href = a["href"]
                    if "view_certificate" in href:
                        out["cert_url"] = urljoin(BASE_URL, href)
                        break
                if "cert_url" not in out:
                    for form in cell.find_all("form", action=True):
                        action = form["action"]
                        if "view_certificate" in action:
                            out["cert_url"] = urljoin(BASE_URL, action)
                            break
        break

    return {k: v for k, v in out.items() if v not in (None, "", {}, [])}


# ── Form-A (project_preview_open) page parser ─────────────────────────────────

def _extract_kv_from_tables(soup: BeautifulSoup) -> dict[str, str]:
    """Extract key-value pairs from 2- and 3-column table rows."""
    result: dict[str, str] = {}
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        n = len(cells)
        if n == 2:
            label_cell, value_cell = cells[0], cells[1]
        elif n == 3:
            first = cells[0].get_text(strip=True)
            if re.match(r"^\d+\.?$", first) or not first:
                label_cell, value_cell = cells[1], cells[2]
            elif not cells[1].get_text(strip=True):
                label_cell, value_cell = cells[0], cells[2]
            else:
                continue
        else:
            continue
        label = label_cell.get_text(separator=" ", strip=True)
        label = re.sub(r"\(Annex[^)]*\)", "", label, flags=re.I).strip()
        label = re.sub(r"\s+", " ", label).strip().rstrip(":")
        value = value_cell.get_text(separator=" ", strip=True)
        if not label or not value or label.isdigit() or len(label) > 200:
            continue
        if label not in result or (not result[label] and value):
            result[label] = value
    return result


def _extract_units_table(soup: BeautifulSoup) -> list[dict]:
    """Parse apartment/plot units table from Form-A."""
    _ALIASES = {
        "plot/ apartment type":                             "flat_type",
        "apartment type":                                   "flat_type",
        "plot type":                                        "flat_type",
        "type of bhk":                                      "flat_type",
        "size of the plot/carpet area of the apartments":  "carpet_area",
        "carpet area":                                      "carpet_area",
        # Assam Form-A: "BHK Wise Carpet area(In Square Meter)"
        "bhk wise carpet area":                            "carpet_area",
        "total number of plots/apartments in the project": "no_of_units",
        "number of units":                                  "no_of_units",
        # Assam Form-A: "Number of apartments"
        "number of apartments":                            "no_of_units",
        "number of plots":                                  "no_of_units",
    }
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header = rows[0].get_text(" ", strip=True).lower()
        if ("apartment" not in header and "plot" not in header) or "type" not in header:
            continue
        raw_headers = [c.get_text(" ", strip=True) for c in rows[0].find_all(["th", "td"])]
        units: list[dict] = []
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            first_non_serial = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            if not first_non_serial or first_non_serial.lower() == "total":
                continue
            mapped: dict[str, str] = {}
            for i, cell in enumerate(cells):
                raw_key = raw_headers[i] if i < len(raw_headers) else f"col_{i}"
                raw_key_lower = raw_key.lower()
                # Exact match first; then try prefix/substring match so that
                # headers like "BHK Wise Carpet area(In Square Meter)" resolve
                # to the "bhk wise carpet area" alias entry.
                std_key = _ALIASES.get(raw_key_lower) or next(
                    (v for k, v in _ALIASES.items() if raw_key_lower.startswith(k)),
                    None,
                )
                if std_key and std_key not in mapped:
                    mapped[std_key] = cell.get_text(" ", strip=True)
            if mapped:
                units.append(mapped)
        if units:
            return units
    return []


def _extract_facilities_table(soup: BeautifulSoup) -> list[dict]:
    """Parse facilities table from Form-A."""
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header = rows[0].get_text(" ", strip=True).lower()
        if "facility" not in header and "service" not in header:
            continue
        # Skip the external-agency approvals table (has "external" or
        # "connecting service" in its header — different from the internal
        # facilities plan table which has "estimated cost").
        if "external" in header or "connecting service" in header:
            continue
        header_cells = [c.get_text(" ", strip=True) for c in rows[0].find_all(["th", "td"])]
        facilities: list[dict] = []
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 2 or not cells[0].get_text(strip=True):
                continue
            entry: dict[str, str] = {}
            for i, cell in enumerate(cells):
                col_hdr = header_cells[i].lower() if i < len(header_cells) else ""
                val = cell.get_text(" ", strip=True)
                if "name of the facility" in col_hdr or ("facility" in col_hdr and "facility" not in entry):
                    entry["facility"] = val
                elif "estimated cost" in col_hdr:
                    entry["description"] = f"{header_cells[i]}: {val}"
                elif "remark" in col_hdr or "status" in col_hdr or "yet to" in col_hdr:
                    entry["status"] = val
            if entry:
                facilities.append(entry)
        if facilities:
            return facilities
    return []


def _extract_documents(soup: BeautifulSoup) -> list[dict]:
    """Extract uploaded documents from Form-A or detail page document tables."""
    docs: list[dict] = []
    seen_urls: set[str] = set()
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header = rows[0].get_text(" ", strip=True).lower()
        if "document" not in header:
            continue
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            doc_type  = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            dated_on  = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            link_url: str | None = None
            for cell in cells:
                for a in cell.find_all("a", href=True):
                    href = a["href"].strip()
                    if href.startswith("javascript"):
                        continue
                    link_url = urljoin(BASE_URL, href)
                    break
                if link_url:
                    break
            if not doc_type or not link_url or link_url in seen_urls:
                continue
            seen_urls.add(link_url)
            docs.append({
                "type": doc_type.upper(),
                "link": link_url,
                "dated_on": _normalize_date_str(dated_on),
                "updated": True,
            })
    return docs


def _parse_form_a(html: str, form_a_url: str) -> dict:
    """
    Parse /view_project/project_preview_open/{id} (Form-A Parts 1-8).

    Uses KV table extraction (same CodeIgniter layout as Haryana).
    """
    soup = BeautifulSoup(html, "lxml")
    kv   = _extract_kv_from_tables(soup)
    kv_lower = {k.lower(): v for k, v in kv.items()}
    out: dict[str, Any] = {}

    # ── Promoter / firm details ───────────────────────────────────────────────
    # "1. Name of the firm" is the Assam-specific label; fall back to generic ones.
    # "applicant name" is deliberately excluded — it false-positives on the
    # "Signature of the applicant Name (In Block Letters):" footer row.
    firm_key = next(
        (k for k in kv if any(t in k.lower() for t in (
            "name of the firm",
            "name and registered address", "name of the promoter",
            "name of promoter", "promoter name",
        ))), None
    )
    firm_name = kv.get(firm_key, "").strip() if firm_key else ""

    # PAN key: must contain "pan" as a whole word and must NOT be the
    # NOC/Building-Permit row which contains "Panchayat" (pan) and "NOC" (no).
    pan_key = next(
        (k for k in kv if re.search(r"\bpan\b", k, re.I)
         and "permit" not in k.lower()
         and "panchayat" not in k.lower()
         and any(t in k.lower() for t in ("of the firm", "card", "no.", "number", "of firm"))),
        None,
    )
    reg_key = next((k for k in kv if any(t in k.lower() for t in (
        "registration no", "registration number", "reg no",
    ))), None)
    if firm_name:
        out["promoters_details"] = {
            "name":            firm_name,
            "pan_no":          kv.get(pan_key, "") if pan_key else "",
            "registration_no": kv.get(reg_key, "") if reg_key else "",
        }

    # Promoter address — Assam uses "Address of the firm for correspondence"
    addr_key = next(
        (k for k in kv if any(t in k.lower() for t in (
            "address of the firm for correspondence",
            "address of the firm",
            "registered address",
            "correspondence address",
        ))), None
    )
    if addr_key:
        correspondence_addr = kv[addr_key]
        out["promoter_address_raw"] = {
            "raw_address":           correspondence_addr,
            "correspondence_address": correspondence_addr,
        }

    # Contact details — clean phone values of HTML noise
    contact: dict[str, str] = {}
    for label_pat, field in (
        (("phone(mobile)", "mobile no", "mobile number"), "phone"),
        (("phone(landline)", "landline", "telephone"), "telephone_no"),
        (("email id", "email"), "email"),
        (("website",), "website"),
    ):
        for pat in label_pat:
            v = kv_lower.get(pat)
            if v:
                contact[field] = _clean_phone_val(v) if field == "phone" else v
                break
    if contact:
        out["promoter_contact_details"] = contact

    # ── Co-promoters / partners ───────────────────────────────────────────────
    co_promoters = _parse_persons_section(soup.get_text(separator="\n", strip=True))
    if co_promoters.get("directors"):
        out["co_promoter_details"] = co_promoters["directors"]
    if co_promoters.get("authorized_rep"):
        out["authorised_signatory_details"] = co_promoters["authorized_rep"]
    if co_promoters.get("contact_person"):
        out["members_details"] = [co_promoters["contact_person"]]

    # ── Land area ────────────────────────────────────────────────────────────
    land_key = next(
        (k for k in kv if "land area" in k.lower() and "construction" not in k.lower()), None
    )
    if land_key:
        out["land_area"] = _float_val(kv[land_key])

    land_unit_key = next((k for k in kv if "land area unit" in k.lower()), None)
    land_unit = kv.get(land_unit_key, "(Sqr/mtrs)") if land_unit_key else "(Sqr/mtrs)"

    if out.get("land_area") is not None:
        out["land_area_details"] = {
            "land_area": str(out["land_area"]),
            "land_area_unit": land_unit,
        }

    # Mouza / village for land_detail
    mouza_key = next((k for k in kv if "mouza" in k.lower()), None)
    # "5. Total Land area, if the land area of the present project is a part thereof"
    # does NOT contain the substring "total area" verbatim — broaden the search.
    total_area_key = next(
        (k for k in kv if "total" in k.lower() and "land area" in k.lower()), None
    ) or next((k for k in kv if "total area" in k.lower()), None)
    land_detail: dict[str, str] = {}
    if mouza_key:
        land_detail["mouza"] = kv[mouza_key]
    if total_area_key:
        land_detail["total_area"] = str(_float_val(kv[total_area_key]) or kv[total_area_key])
    if land_detail:
        out["land_detail"] = land_detail

    # ── Project location extra fields (village, pin, post office) ─────────────
    # These come from the project-site section of Form-A Part 1. Stash them
    # here so _build_payload can merge them into project_location_raw.
    proj_village = kv.get("Town/ Village") or kv.get("Town/Village")
    proj_pin     = kv.get("PIN")
    proj_po      = kv.get("P.O.")
    loc_extra: dict[str, str] = {}
    if proj_village: loc_extra["village"]      = proj_village
    if proj_pin:     loc_extra["pin_code"]     = proj_pin
    if proj_po:      loc_extra["post_office"]  = proj_po
    if loc_extra:
        out["_project_location_extra"] = loc_extra

    # ── Construction area — compute from units table ───────────────────────────
    # The kv dict contains "LAND AREA TO BE USED FOR CONSTRUCTION OF APARTMENTS"
    # which falsely matches "construction" + "area" but is a land-use area, not
    # the built-up / carpet area.  Derive the real figure from the units table.
    const_unit = "In Square Meter"
    units_for_area = _extract_units_table(soup)
    if units_for_area:
        total_carpet: float = 0.0
        total_count: int = 0
        for u in units_for_area:
            count_str = re.sub(r"[^\d.]", "", u.get("no_of_units", "") or "0") or "0"
            area_str  = re.sub(r"[^\d.]", "", u.get("carpet_area",  "") or "0") or "0"
            cnt = int(float(count_str))
            area = float(area_str)
            total_count  += cnt
            total_carpet += cnt * area
        if total_count > 0:
            out["number_of_residential_units"] = total_count
        if total_carpet > 0:
            out["construction_area"] = round(total_carpet, 2)
            if out.get("land_area_details"):
                out["land_area_details"]["construction_area"] = round(total_carpet, 2)
                out["land_area_details"]["construction_area_unit"] = const_unit
    else:
        # Fallback: look for an explicit "construction area" kv entry but
        # exclude the land-use table rows (all uppercase, area-allocation rows).
        const_key = next(
            (k for k in kv if "construction area" in k.lower()
             and k != k.upper()),  # skip ALL-CAPS land-use labels
            None,
        )
        if const_key:
            out["construction_area"] = _float_val(kv[const_key])
            if out.get("land_area_details"):
                out["land_area_details"]["construction_area"] = out["construction_area"]
                out["land_area_details"]["construction_area_unit"] = const_unit

    # ── Project cost ──────────────────────────────────────────────────────────
    cost_detail: dict[str, Any] = {}
    total_cost_key = next((k for k in kv if "estimated cost of the project" in k.lower() or
                           ("total" in k.lower() and "project cost" in k.lower())), None)
    if total_cost_key:
        cost_detail["total_project_cost"] = kv[total_cost_key]
        cost_detail["estimated_project_cost"] = kv[total_cost_key]

    land_cost_key = next((k for k in kv if "cost of the land" in k.lower() or
                          "cost of land" in k.lower()), None)
    if land_cost_key:
        cost_detail["cost_of_land"] = kv[land_cost_key]

    constr_cost_key = next((k for k in kv if "cost of construction" in k.lower() or
                            "construction cost" in k.lower()), None)
    if constr_cost_key:
        cost_detail["estimated_construction_cost"] = kv[constr_cost_key]

    if cost_detail:
        out["project_cost_detail"] = cost_detail

    # number_of_residential_units and construction_area are now computed from
    # the units table above; fall back to a kv lookup only if the table is empty.
    if "number_of_residential_units" not in out:
        units_key = next(
            (k for k in kv if "number of" in k.lower() and
             any(t in k.lower() for t in ("flat", "unit", "apartment", "plot"))), None
        )
        if units_key:
            try:
                out["number_of_residential_units"] = int(
                    float(re.sub(r"[^\d.]", "", kv[units_key]))
                )
            except (ValueError, TypeError):
                pass

    # ── Bank details ──────────────────────────────────────────────────────────
    # Assam Form-A stores bank info as "Bank and Branch address" (full string),
    # "Bank Account number", "IFSC code", "MICR code", "Branch code".
    # Split "Bank and Branch address" → bank_name (first segment) + branch (2nd).
    bank: dict[str, str] = {}
    bank_and_branch_raw = next(
        (v for k, v in kv_lower.items() if "bank and branch" in k), None
    )
    if bank_and_branch_raw:
        parts = [p.strip() for p in bank_and_branch_raw.split(",")]
        bank["bank_name"] = parts[0]
        if len(parts) > 1:
            # Join all segments after the bank name to preserve the full branch address
            # e.g. "STATE BANK OF INDIA, 1ST FLOOR, DD TOWER, CHRISTIAN BASTI, GS ROAD..."
            bank["branch"] = ", ".join(parts[1:])
    # Remaining fields via label matching
    extra_bank_map = {
        "bank account":   "account_no",
        "account number": "account_no",
        "account no":     "account_no",
        "ifsc":           "IFSC",
        "branch code":    "branch_code",
        "micr":           "micr_code",
    }
    for raw_label, schema_key in extra_bank_map.items():
        for k, v in kv_lower.items():
            if raw_label in k and schema_key not in bank:
                bank[schema_key] = v
                break
    # Fallback for plain "bank name" (no "and branch")
    if "bank_name" not in bank:
        bank_name_raw = next(
            (v for k, v in kv_lower.items()
             if k.strip() == "bank name" or ("bank name" in k and "branch" not in k)),
            None,
        )
        if bank_name_raw:
            bank["bank_name"] = bank_name_raw
    if bank:
        out["bank_details"] = bank

    # ── Dates ─────────────────────────────────────────────────────────────────
    # Commencement date ("Likely date of starting the construction work")
    start_key = next(
        (k for k in kv if "start date" in k.lower()
         or "commencement date" in k.lower()
         or "likely date of starting" in k.lower()), None
    )
    if start_key:
        out["estimated_commencement_date"] = _normalize_date_str(kv[start_key])

    # Completion / finish date ("Likely date of completing the project")
    finish_key = next(
        (k for k in kv if "likely date of completing" in k.lower()
         or "date of completing" in k.lower()), None
    )
    if finish_key:
        out["estimated_finish_date"] = _normalize_date_str(kv[finish_key])

    # Submission date with time — parse from Form-A header text
    sub_m = re.search(
        r"Submission\s+Date\s*:\s*(\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2}\s*(?:AM|PM)?)",
        html, re.I,
    )
    if sub_m:
        out["submitted_date"] = _normalize_date_str(sub_m.group(1).strip())

    # ── Status of project from Form-A ─────────────────────────────────────────
    status_key = next(
        (k for k in kv if "status" in k.lower() and "project" in k.lower()), None
    )
    if status_key:
        out["status_of_the_project"] = kv[status_key].upper()

    # ── Building details (units table) ────────────────────────────────────────
    if units_for_area:
        out["building_details"] = units_for_area

    # ── Facilities ───────────────────────────────────────────────────────────
    facilities = _extract_facilities_table(soup)
    if facilities:
        out["provided_faciltiy"] = facilities

    # ── Uploaded documents ────────────────────────────────────────────────────
    docs = _extract_documents(soup)
    if docs:
        out["uploaded_documents"] = docs

    # ── Data blob ────────────────────────────────────────────────────────────
    # Capture raw construction area string and unit for downstream use.
    # Do NOT include is_processed — that is a DB-level sentinel, not a crawl field.
    const_area_1_key = next(
        (k for k in kv if "construction area" in k.lower() and k != k.upper()), None
    )
    data_blob: dict[str, Any] = {"govt_type": "state"}
    if const_area_1_key:
        data_blob["construction_area_1"] = kv[const_area_1_key]
    elif units_for_area:
        # Assam Form-A has no standalone "construction area" KV entry.
        # Build the canonical space-joined string from the units table:
        # e.g. "1439.10 18 599.34 6" (carpet_area no_of_units per row).
        parts: list[str] = []
        for u in units_for_area:
            ca = u.get("carpet_area", "")
            nu = u.get("no_of_units", "")
            if ca and nu:
                parts.extend([ca, nu])
        if parts:
            data_blob["construction_area_1"] = " ".join(parts)

    const_unit_key2 = next((k for k in kv if "construction area unit" in k.lower()), None)
    if const_unit_key2:
        data_blob["construction_unit"] = kv[const_unit_key2]
    elif units_for_area:
        # Fall back to the unit parsed from the units table header
        # (e.g. "BHK Wise Carpet area(In Square Meter)" → "In Square Meter").
        data_blob["construction_unit"] = const_unit

    out["data"] = data_blob

    return {k: v for k, v in out.items() if v not in (None, "", {}, [])}


# ── Persons section parser ────────────────────────────────────────────────────

def _parse_person_block(block: str) -> dict:
    """
    Extract name, phone, email, address from a newline-separated person block.
    The Assam Form-A renders each field on its own line, e.g.:
      Name :\nHANUMAN MAL KALANI\nOffice Address :\n1ST FLOOR...
    """
    def _find(pattern: str) -> str:
        m = re.search(pattern, block, re.I | re.S)
        return m.group(1).strip() if m else ""

    # Name comes on the line(s) after "Name :"
    name  = _find(r"Name\s*:?\s*\n+\s*([^\n]+)")
    # Address on line after "Office Address :" or "Present Address :"
    addr  = _find(r"(?:Office|Present|Residential)\s+Address\s*:?\s*\n+\s*([^\n]+)")
    # Phone: digits on the line right after "Phone (Mobile)"
    phone = _find(r"Phone\s*\(Mobile\)\s*\n+\s*(\d[\d\s]*)")
    phone = re.sub(r"\s+", "", phone)  # collapse spaces
    # Email
    email = _find(r"Email\s*(?:ID)?\s*\n+\s*([\w@.\-]+@[\w.\-]+)")

    entry: dict = {}
    if name:  entry["name"]            = name
    if email: entry["email"]           = email
    if phone: entry["phone"]           = phone
    if addr:  entry["present_address"] = addr
    return entry


def _parse_persons_section(text: str) -> dict:
    """
    Parse partners/directors, authorised representative, and site contact person
    from the Form-A plain text (extracted via soup.get_text).

    Assam Form-A layout (no leading "N. " before role labels):
      Partner 1\\nName :\\nFULL NAME\\nOffice Address :\\n...
      6. Person/partner who is authorized...:\\nName :\\n...
      Contact person at the site office:\\nName\\nFULL NAME\\n...
    """
    directors: list[dict] = []
    authorized_rep: dict | None = None
    contact_person: dict | None = None

    # ── Partners ──────────────────────────────────────────────────────────────
    # Split on "Partner N" labels; the text between consecutive Partner labels
    # (or up to "I hereby declare") is one partner block.
    parts = re.split(r"\bPartner\s+(\d+)\b", text, flags=re.I)
    # parts = [pre, num1, block1, num2, block2, ...]
    for i in range(1, len(parts), 2):
        if i + 1 >= len(parts):
            break
        role_num = parts[i].strip()
        block    = parts[i + 1]
        # Trim at next major boundary
        block = re.split(r"I\s+hereby\s+declare|6\.\s+Person", block, flags=re.I)[0]
        entry = _parse_person_block(block)
        if entry:
            entry["role"] = f"Partner {role_num}"
            directors.append(entry)

    # ── Authorised representative (section 6) ────────────────────────────────
    auth_m = re.search(
        r"Person/partner\s+who\s+is\s+authorized[^:]*:(.*?)(?:I\s+hereby\s+declare|$)",
        text, re.S | re.I,
    )
    if auth_m:
        entry = _parse_person_block(auth_m.group(1))
        if entry:
            authorized_rep = entry
            # The sample includes the authorized person in co_promoter_details
            # (without a role tag), so append here too.
            directors.append(entry)

    # ── Contact person at site office ────────────────────────────────────────
    contact_m = re.search(
        r"Contact\s+person\s+at\s+the\s+site\s+office[^:]*:(.*?)"
        r"(?:I\s+hereby\s+declare|FORM-A|$)",
        text, re.S | re.I,
    )
    if contact_m:
        block = contact_m.group(1)
        # Name comes directly on the next non-empty line (no colon separator)
        name_m  = re.search(r"Name\s*\n+\s*([^\n]+)", block, re.I)
        phone_m = re.search(r"Phone\s*\(Mobile\)\s*\n+\s*(\d[\d\s]*)", block, re.I)
        email_m = re.search(r"Email\s*\n+\s*([\w@.\-]+@[\w.\-]+)", block, re.I)
        desig_m = re.search(r"Designation\s*\n+\s*([^\n]+)", block, re.I)

        cp: dict = {}
        if name_m:  cp["name"]     = name_m.group(1).strip()
        if phone_m: cp["phone"]    = re.sub(r"\s+", "", phone_m.group(1))
        if email_m: cp["email"]    = email_m.group(1).strip()
        if desig_m: cp["position"] = desig_m.group(1).strip()
        if cp:
            contact_person = cp

    return {
        "directors":      directors or None,
        "authorized_rep": authorized_rep,
        "contact_person": contact_person,
    }


# ── Document download + S3 upload ─────────────────────────────────────────────

def _handle_document(
    project_key: str,
    doc: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
) -> dict | None:
    """
    Download a single document and upload to S3. Returns result entry or None.

    Shared flow:
    1. Download bytes from source URL.
    2. Compute MD5.
    3. Upload to S3.
    4. Upsert the document row in rera_project_documents.
    5. Build and return a result entry with both the original URL (link) and s3_link.
    """
    url = doc.get("url") or doc.get("link")
    if not url:
        return None
    filename = build_document_filename(doc)
    doc_type = doc.get("type") or doc.get("label") or "document"
    try:
        resp = safe_get(url, logger=logger, timeout=60.0)
        if not resp or len(resp.content) < 100:
            logger.warning("Document download empty or failed", url=url)
            return None
        data = resp.content
        md5  = compute_md5(data)

        s3_key = upload_document(project_key, filename, data, dry_run=settings.DRY_RUN_S3)
        if s3_key is None:
            return None
        if not settings.DRY_RUN_S3:
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

        s3_url = get_s3_url(s3_key)
        result = document_result_entry(doc, s3_url=s3_url, md5=md5)
        # document_result_entry looks for doc["url"] or doc["source_url"], but Assam
        # documents store the source URL under "link" — preserve it explicitly.
        if result is not None and not result.get("link"):
            result["link"] = url
        logger.info("Document uploaded", doc_type=doc_type, s3_key=s3_key, step="documents")
        logger.log_document(doc_type, url, "uploaded", s3_key=s3_key, file_size_bytes=len(data))
        return result
    except Exception as exc:
        logger.warning("Document handling error", url=url, error=str(exc))
        return None



# ── Sentinel check ────────────────────────────────────────────────────────────

def _sentinel_check(config: dict, logger: "CrawlerLogger", run_id: int) -> bool:
    """
    Data-quality sentinel for Assam RERA.
    Full-flow check: fetches the sentinel project's searchprojectDetail page AND its
    project_preview_open (Form-A) page, merges all three sources via _build_payload()
    (same as run()), and verifies ≥ 80% field coverage against the full baseline.
    """
    import json as _json
    import os as _os
    from core.sentinel_utils import check_field_coverage

    sentinel_reg_no = config.get("sentinel_registration_no")
    if not sentinel_reg_no:
        logger.warning("No sentinel_registration_no configured — skipping sentinel check")
        return True

    sample_path = _os.path.join(
        _os.path.dirname(_os.path.dirname(__file__)),
        "state_projects_sample", "assam.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check",
                       path=sample_path, step="sentinel")
        return True

    sentinel_url = baseline.get("url", "")
    if not sentinel_url or "searchprojectDetail" not in sentinel_url:
        logger.warning("Sentinel: no valid detail URL in sample — skipping", step="sentinel")
        return True

    logger.info(f"Sentinel: fetching detail for {sentinel_reg_no}",
                url=sentinel_url, step="sentinel")
    try:
        resp = safe_get(sentinel_url, logger=logger, timeout=60.0)
        if not resp:
            logger.error("Sentinel fetch failed", url=sentinel_url, step="sentinel")
            insert_crawl_error(
                run_id, config["id"], "SENTINEL_FAILED",
                "HTTP error fetching sentinel detail page", url=sentinel_url,
            )
            return False

        detail = _parse_detail_page(resp.text, sentinel_url) or {}

        # Build a minimal stub (listing-level fields sourced from baseline)
        stub = {
            "project_registration_no": sentinel_reg_no,
            "project_name":            baseline.get("project_name") or detail.get("project_name", ""),
            "promoter_name":           baseline.get("promoter_name", ""),
            "project_location_raw_address": (
                (baseline.get("project_location_raw") or {}).get("raw_address", "")
            ),
            "project_city":            baseline.get("project_city", ""),
            "approved_on_date":        baseline.get("approved_on_date", ""),
            "estimated_finish_date":   baseline.get("estimated_finish_date", ""),
            "detail_url":              sentinel_url,
            "internal_id":             sentinel_url.rstrip("/").split("/")[-1],
        }

        # ── Fetch Form-A (project_preview_open) for full field coverage ───────
        form_a_url = detail.get("form_a_url", "")
        internal_id = stub["internal_id"]
        if not form_a_url and internal_id:
            form_a_url = f"{BASE_URL}/view_project/project_preview_open/{internal_id}"

        form_a: dict = {}
        if form_a_url:
            logger.info("Sentinel: fetching Form-A", url=form_a_url, step="sentinel")
            try:
                resp_form = safe_get(form_a_url, logger=logger, timeout=60.0)
                if resp_form:
                    form_a = _parse_form_a(resp_form.text, form_a_url) or {}
                    logger.info(f"Sentinel: Form-A parsed ({len(form_a)} fields)", step="sentinel")
                else:
                    logger.warning("Sentinel: Form-A fetch failed — proceeding without it",
                                   url=form_a_url, step="sentinel")
            except Exception as exc:
                logger.warning(f"Sentinel: Form-A error — {exc}", step="sentinel")

        # ── Merge stub + detail + form_a (same as run()) ──────────────────────
        fresh = _build_payload(stub, detail, form_a, config.get("config_id", 0))

    except Exception as exc:
        logger.error("Sentinel check raised an exception", error=str(exc))
        insert_crawl_error(
            run_id, config["id"], "SENTINEL_FAILED",
            f"Exception during sentinel check: {exc}", url=sentinel_url,
        )
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", url=sentinel_url, step="sentinel")
        insert_crawl_error(
            run_id, config["id"], "SENTINEL_FAILED",
            "Sentinel page yielded no extractable fields (possible layout change)",
            url=sentinel_url,
        )
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "assam_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg_no}",
        )
        return False

    logger.info("Sentinel check passed", reg_no=sentinel_reg_no, step="sentinel")
    return True


# ── Merge listing stub + detail + Form-A ──────────────────────────────────────

def _build_payload(
    stub: dict,
    detail: dict,
    form_a: dict,
    config_id: int,
) -> dict:
    """Merge the three data sources into a single normalized payload dict."""
    reg_no = stub["project_registration_no"]
    internal_id = stub.get("internal_id")

    # Primary URL is the searchprojectDetail page
    primary_url = (
        detail.get("url")
        or stub.get("detail_url")
        or f"{BASE_URL}/view_project/searchprojectDetail/{internal_id}"
    )

    # Build project_location_raw, merging detail-page + listing + Form-A extras.
    # Drop "taluk" (not in schema); add village/pin_code/post_office from Form-A.
    loc_extra = form_a.pop("_project_location_extra", {})  # stashed in _parse_form_a
    project_location_raw: dict[str, Any] = {
        "raw_address": (
            detail.get("project_location_address")
            or stub.get("project_location_raw_address")
        ),
        "district": (
            detail.get("project_district")
            or stub.get("project_city")
        ),
    }
    project_location_raw.update(loc_extra)  # adds village, pin_code, post_office

    payload: dict[str, Any] = {
        "project_registration_no": reg_no,
        "project_name":   (
            detail.get("project_name")
            or stub.get("project_name")
            or form_a.get("project_name")
        ),
        "promoter_name":  (
            detail.get("promoter_name_detail")
            or stub.get("promoter_name")
        ),
        "acknowledgement_no": detail.get("acknowledgement_no"),
        "project_city":   stub.get("project_city"),
        "project_location_raw": project_location_raw,
        "state":     STATE,
        "domain":    DOMAIN,
        "config_id": config_id,
        "url":       primary_url,
        # Dates — prefer Form-A's submitted_date (has full timestamp incl. time)
        "submitted_date": (
            form_a.get("submitted_date")
            or detail.get("submitted_date")
        ),
        "approved_on_date":       detail.get("approved_on_date") or stub.get("approved_on_date"),
        # Form-A "likely date of completing" overrides the listing expiry date
        "estimated_finish_date":  (
            form_a.get("estimated_finish_date")
            or stub.get("estimated_finish_date")
        ),
        "estimated_commencement_date": form_a.get("estimated_commencement_date"),
    }

    # Status: prefer Form-A, fall back to detail page current_status
    raw_status = form_a.get("status_of_the_project") or detail.get("current_status", "")
    if raw_status:
        # Map "APPROVED AND CERTIFICATE UPLOADED" → "NEW" (as seen in sample)
        if "approved" in raw_status.lower():
            payload["status_of_the_project"] = "NEW"
        else:
            payload["status_of_the_project"] = raw_status.upper()

    # Enrich from Form-A
    for field in (
        "land_area", "construction_area", "number_of_residential_units",
        "land_area_details", "land_detail",
        "project_cost_detail", "building_details", "provided_faciltiy",
        "bank_details", "promoters_details", "promoter_address_raw",
        "promoter_contact_details", "co_promoter_details",
        "authorised_signatory_details", "members_details",
        "data",
    ):
        val = form_a.get(field)
        if val is not None:
            payload[field] = val

    # Documents: cert from detail/listing + uploaded docs from Form-A
    cert_url = detail.get("cert_url") or stub.get("cert_url")
    listing_docs: list[dict] = []
    if cert_url:
        listing_docs.append({
            "type": "RERA REGISTRATION CERTIFICATE 1",
            "link": cert_url,
            "updated": True,
        })

    form_a_docs: list[dict] = form_a.get("uploaded_documents") or []
    existing_links = {d.get("link") for d in form_a_docs}
    for doc in listing_docs:
        if doc.get("link") not in existing_links:
            form_a_docs.insert(0, doc)
    if form_a_docs:
        payload["uploaded_documents"] = form_a_docs

    # Project pin code — derive from project_location_raw
    loc_raw = payload.get("project_location_raw") or {}
    pin = loc_raw.get("pin_code")
    if pin:
        payload["project_pin_code"] = pin

    return {k: v for k, v in payload.items() if v not in (None, "", {}, [])}


# ── Main entry point ───────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    """
    Main entry point for the Assam RERA crawler.

    1. Fetch /admincontrol/registered_projects/1 → parse all project stubs.
    2. For each stub: fetch searchprojectDetail → acknowledgement_no, dates, cert_url, form_a_url.
    3. Fetch project_preview_open (Form-A)       → full metadata + documents.
    4. Merge → normalize → validate → upsert.
    5. Download + upload selected documents.
    """
    site_id   = config["id"]
    config_id = config.get("config_id", CONFIG_ID)
    logger    = CrawlerLogger(site_id, run_id)
    counts    = dict(
        projects_found=0, projects_new=0, projects_updated=0,
        projects_skipped=0, documents_uploaded=0, error_count=0,
    )
    item_limit = settings.CRAWL_ITEM_LIMIT or 0

    # ── Step 0: Sentinel check (spec Section 10) ─────────────────────────────
    logger.info("Starting Assam RERA crawl", mode=mode, listing_url=LISTING_URL)
    if not _sentinel_check(config, logger, run_id):
        logger.error("Sentinel check failed — aborting Assam RERA crawl")
        return counts

    # ── Step 1: Fetch listing ────────────────────────────────────────────────
    all_stubs = _fetch_listing(logger)
    logger.info("Listing parsed", total=len(all_stubs))
    counts["projects_found"] = len(all_stubs)

    if not all_stubs:
        logger.error("No projects found — aborting")
        return counts

    # ── Step 2: Checkpoint ───────────────────────────────────────────────────
    checkpoint     = load_checkpoint(site_id, mode) or {}
    last_proj_key  = checkpoint.get("last_project_key")
    resume_pending = bool(last_proj_key and mode != "full")
    machine_name, machine_ip = get_machine_context()

    # ── Step 3: Process each stub ────────────────────────────────────────────
    items_processed = 0
    project_key = None

    for i, stub in enumerate(all_stubs):
        if item_limit and items_processed >= item_limit:
            logger.info(f"CRAWL_ITEM_LIMIT={item_limit} reached")
            break

        reg_no      = stub["project_registration_no"]
        project_key = generate_project_key(reg_no)
        detail_url  = stub.get("detail_url")
        internal_id = stub.get("internal_id")

        if resume_pending:
            if project_key == last_proj_key:
                resume_pending = False
            counts["projects_skipped"] += 1
            continue

        # ── daily_light: skip projects already in the DB ───────────────────
        if mode == "daily_light" and get_project_by_key(project_key):
            counts["projects_skipped"] += 1
            continue

        logger.set_project(key=project_key, reg_no=reg_no, url=detail_url or LISTING_URL, page=i)
        try:
            if not detail_url:
                logger.warning("No detail URL for project")
                counts["error_count"] += 1
                continue

            try:
                # ── Fetch searchprojectDetail ──────────────────────────────
                random_delay(*config.get("rate_limit_delay", (1, 3)))
                resp_detail = safe_get(detail_url, logger=logger, timeout=60.0)
                if not resp_detail:
                    logger.warning("Detail fetch failed", url=detail_url)
                    insert_crawl_error(run_id, site_id, "detail_fetch_failed", detail_url,
                                       project_key=project_key, url=detail_url)
                    counts["error_count"] += 1
                    continue

                detail = _parse_detail_page(resp_detail.text, detail_url)

                # ── Fetch Form-A (project_preview_open) ───────────────────
                form_a_url = detail.get("form_a_url") or (
                    f"{BASE_URL}/view_project/project_preview_open/{internal_id}"
                    if internal_id else None
                )
                form_a: dict = {}
                if form_a_url:
                    random_delay(*config.get("rate_limit_delay", (1, 3)))
                    resp_form = safe_get(form_a_url, logger=logger, timeout=60.0)
                    if resp_form:
                        form_a = _parse_form_a(resp_form.text, form_a_url)
                    else:
                        logger.warning("Form-A fetch failed", url=form_a_url)

                # ── Merge + normalize ──────────────────────────────────────
                raw_payload = _build_payload(stub, detail, form_a, config_id)
                raw_payload["is_live"] = True
                payload = normalize_project_payload(
                    raw_payload, config,
                    machine_name=machine_name,
                    machine_ip=machine_ip,
                )

                # ── Validate + persist ─────────────────────────────────────
                try:
                    record = ProjectRecord(**payload)
                    db_dict = record.to_db_dict()
                except ValidationError as ve:
                    logger.warning("Validation error", errors=ve.errors()[:3])
                    db_dict = payload

                status = upsert_project(db_dict)
                if status == "new":
                    counts["projects_new"] += 1
                elif status == "updated":
                    counts["projects_updated"] += 1
                else:
                    counts["projects_skipped"] += 1
                items_processed += 1

                # ── Documents ──────────────────────────────────────────────
                all_docs: list[dict] = raw_payload.get("uploaded_documents") or []
                doc_name_counts: dict[str, int] = {}
                persisted_docs: list[dict] = []

                for doc in all_docs:
                    selected = select_document_for_download(
                        config["state"], doc, doc_name_counts, domain=DOMAIN,
                    )
                    if not selected:
                        persisted_docs.append({
                            "link": doc.get("url") or doc.get("link"),
                            "type": doc.get("label") or doc.get("type") or "document",
                        })
                        continue
                    result = _handle_document(project_key, selected, run_id, site_id, logger)
                    if result:
                        counts["documents_uploaded"] += 1
                        persisted_docs.append(result)
                    else:
                        persisted_docs.append({
                            "link": selected.get("url") or selected.get("link"),
                            "type": selected.get("label") or selected.get("type") or "document",
                        })

                if persisted_docs:
                    upsert_project({
                        "key":                    db_dict["key"],
                        "url":                    db_dict["url"],
                        "state":                  db_dict["state"],
                        "domain":                 db_dict["domain"],
                        "project_registration_no": db_dict["project_registration_no"],
                        "uploaded_documents":     persisted_docs,
                        "document_urls":          build_document_urls(persisted_docs),
                    })

                # ── Checkpoint every 50 projects ───────────────────────────
                if (i + 1) % 50 == 0:
                    save_checkpoint(site_id, mode, i, project_key, run_id)
                    logger.info("Checkpoint saved", done=i + 1, total=len(all_stubs))

            except Exception as exc:
                logger.error("Unexpected error processing project", error=str(exc))
                insert_crawl_error(run_id, site_id, "unexpected_error", str(exc),
                                   project_key=project_key)
                counts["error_count"] += 1
                continue
        finally:
            logger.clear_project()

    # ── Final checkpoint ──────────────────────────────────────────────────────
    save_checkpoint(site_id, mode, len(all_stubs), project_key, run_id)
    logger.info("Crawl complete", **counts)
    return counts
