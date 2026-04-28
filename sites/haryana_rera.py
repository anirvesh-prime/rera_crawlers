"""
Haryana RERA Crawler — haryanarera.gov.in
Type: static (server-rendered HTML, CodeIgniter PHP)

Two authorities, one combined config (config_id=11813):
  /admincontrol/registered_projects/2  →  HRERA Gurugram  (GRG, authority_id=2)
  /admincontrol/registered_projects/1  →  HRERA Panchkula (PKL, authority_id=1)

Strategy:
- Each listing URL loads ALL projects in a single HTML response (DataTables, client-side).
- Parse #compliant_hearing DataTable rows → collect stubs (reg_no, name, location, detail link).
- For each stub: fetch /view_project/project_preview_open/{id} → extract Form REP-I fields.
- Documents: registration certificate + QPR from listing columns; uploaded docs from detail page.
- De-duplicate across both listing pages by project_registration_no.
"""
from __future__ import annotations

import base64
import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import generate_project_key, random_delay, safe_get
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
    normalize_project_payload,
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings
from pydantic import ValidationError

BASE_URL   = "https://haryanarera.gov.in"
STATE_CODE = "HR"
DOMAIN     = "haryanarera.gov.in"
CONFIG_ID  = 11813

# Both authority listing pages — Gurugram (authority 2) and Panchkula (authority 1).
LISTING_URLS: list[str] = [
    f"{BASE_URL}/admincontrol/registered_projects/2",  # HRERA Gurugram
    f"{BASE_URL}/admincontrol/registered_projects/1",  # HRERA Panchkula
]


# ── Date helpers ─────────────────────────────────────────────────────────────

def _normalize_date_str(val: Any) -> str | None:
    """Normalize Haryana RERA date strings to canonical ISO timestamptz format."""
    if not val:
        return None
    v = str(val).strip()
    if not v or v in ("-", "--", "NA", "N/A", "null", "None"):
        return None
    # dd-mm-yyyy [HH:MM:SS [AM/PM]]
    m = re.match(r"^(\d{2})-(\d{2})-(\d{4})(?:\s+(\d{2}):(\d{2}):(\d{2})(?:\s*(AM|PM))?)?", v, re.I)
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


def _lakhs_to_rupees(lakhs_str: str) -> int | None:
    """Convert '440 Lakhs' → 44000000 (int rupees)."""
    m = re.search(r"([\d,\.]+)", str(lakhs_str))
    if not m:
        return None
    try:
        return int(float(m.group(1).replace(",", "")) * 100_000)
    except (ValueError, TypeError):
        return None


def _float_val(text: str) -> float | None:
    m = re.search(r"([\d,]+\.?\d*)", str(text).replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


# ── QPR URL construction ──────────────────────────────────────────────────────

def _build_qpr_url(project_registration_no: str) -> str | None:
    """
    Construct the QPR URL from the project registration number.
    RERA-PKL-456-2019 → auth=1, num=456, year=2019
    RERA-GRG-741-2020 → auth=2, num=741, year=2020
    """
    m = re.match(r"RERA-(PKL|GRG)-(\d+)-(\d{4})", project_registration_no, re.I)
    if not m:
        return None
    authority = "1" if m.group(1).upper() == "PKL" else "2"
    num, year = m.group(2), m.group(3)
    b64 = lambda s: base64.b64encode(s.encode()).decode()
    return (
        f"{BASE_URL}/assistancecontrol/print_quarterly_schedules"
        f"/{b64(authority)}/{b64(num)}/{b64(year)}"
    )



# ── Listing page parsing ──────────────────────────────────────────────────────

def _fetch_listing(url: str, logger: CrawlerLogger) -> list[dict]:
    """Fetch a Haryana RERA listing page and parse all project rows."""
    resp = safe_get(url, logger=logger, timeout=60.0)
    if not resp:
        logger.warning("Listing fetch failed", url=url)
        return []
    soup = BeautifulSoup(resp.text, "lxml")
    return _parse_listing_rows(soup, url)


def _parse_listing_rows(soup: BeautifulSoup, listing_url: str) -> list[dict]:
    """
    Parse the #compliant_hearing DataTable from a Haryana RERA listing page.

    Column mapping (0-indexed):
      0  Serial No.
      1  Registration Certificate Number  → acknowledgement_no
      2  Project ID (inside <span>)       → project_registration_no
      3  Project Name                     → project_name
      4  Builder                          → promoter_name
      5  Project Location                 → project_location_raw.raw_address
      6  Project District                 → project_city
      7  Registered With                  → authority_type
      8  Details of Project (Form A-H)   → detail_url (href extracted)
      9  Registration Up-to              → estimated_finish_date
      10 View Certificate                → cert_url (direct <a> if present)
      11 View Quarterly Progress         → qpr_url (direct <a> if present)
    """
    table = soup.find("table", id="compliant_hearing") or soup.find("table", id="example")
    if not table:
        tables = soup.find_all("table")
        table = tables[0] if tables else None
    if not table:
        return []

    rows = table.find_all("tr")
    results: list[dict] = []

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 9:
            continue

        def cell_text(idx: int) -> str:
            if idx >= len(cells):
                return ""
            return cells[idx].get_text(separator=" ", strip=True)

        def cell_link(idx: int, pattern: str | None = None) -> str | None:
            if idx >= len(cells):
                return None
            for a in cells[idx].find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("javascript"):
                    continue
                if pattern and pattern not in href:
                    continue
                return urljoin(BASE_URL, href)
            return None

        # Column 2: Project ID — may be wrapped in <span>
        span = cells[2].find("span")
        project_id_raw = span.get_text(strip=True) if span else cell_text(2)
        project_id = project_id_raw.strip()

        if not project_id or not re.match(r"RERA-", project_id, re.I):
            continue

        # Column 8: detail page link
        detail_href = cell_link(8, "project_preview_open") or cell_link(8)

        # Internal project ID from detail URL
        internal_id: str | None = None
        if detail_href:
            m = re.search(r"/project_preview_open/(\d+)", detail_href)
            if m:
                internal_id = m.group(1)

        # Certificate link (column 10)
        cert_url = cell_link(10, "view_certificate")

        # QPR link (column 11)
        qpr_url = cell_link(11, "quarterly") or cell_link(11, "print_quarterly")

        # Registration Up-to → estimated_finish_date
        estimated_finish = _normalize_date_str(cell_text(9))

        fallback_detail = (
            f"{BASE_URL}/view_project/project_preview_open/{internal_id}"
            if internal_id else None
        )

        results.append({
            "project_registration_no": project_id,
            "acknowledgement_no": cell_text(1),
            "project_name": cell_text(3),
            "promoter_name": cell_text(4),
            "project_location_raw_address": cell_text(5),
            "project_city": cell_text(6).upper(),
            "authority_type": cell_text(7),
            "detail_url": detail_href or fallback_detail,
            "internal_id": internal_id,
            "estimated_finish_date": estimated_finish,
            "cert_url": cert_url,
            "qpr_url": qpr_url,
            "_listing_url": listing_url,
        })

    return results


# ── Detail page parsing helpers ───────────────────────────────────────────────

def _extract_kv_from_tables(soup: BeautifulSoup) -> dict[str, str]:
    """
    Extract key-value pairs from table rows throughout the page.
    Supports 2-column (label, value) AND 3-column (serial, label, value) rows.
    Cleans up annotation text like '(Annex a copy in Folder A)'.
    First non-empty value wins for duplicate keys.
    """
    result: dict[str, str] = {}
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        n = len(cells)
        if n == 2:
            label_cell, value_cell = cells[0], cells[1]
        elif n == 3:
            # 3-col: first cell is serial number, skip if it's just a number
            first = cells[0].get_text(strip=True)
            if re.match(r"^\d+\.?$", first):
                label_cell, value_cell = cells[1], cells[2]
            else:
                continue
        else:
            continue
        label = label_cell.get_text(separator=" ", strip=True)
        label = re.sub(r"\(Annex[^)]*\)", "", label, flags=re.I).strip()
        label = re.sub(r"\s+", " ", label).strip().rstrip(":")
        value = value_cell.get_text(separator=" ", strip=True)
        # Skip pure number rows (serial numbers) and very long labels (header text)
        if not label or not value or label.isdigit() or len(label) > 200:
            continue
        if label not in result or (not result[label] and value):
            result[label] = value
    return result


def _extract_units_table(soup: BeautifulSoup) -> list[dict]:
    """
    Parse the apartment/plot units table from Form REP-I Part C.
    Looks for a table whose header row contains 'apartment' or 'plot' + 'type'.
    Returns list of {flat_type, carpet_area, no_of_units, total_units_booked}.
    """
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header = rows[0].get_text(" ", strip=True).lower()
        if ("apartment" not in header and "plot" not in header) or "type" not in header:
            continue
        headers = [c.get_text(" ", strip=True) for c in rows[0].find_all(["th", "td"])]
        units: list[dict] = []
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            row_dict = {
                headers[i] if i < len(headers) else f"col_{i}": c.get_text(" ", strip=True)
                for i, c in enumerate(cells)
            }
            # Skip serial-number-only rows and summary/total rows
            first_non_serial = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            if not first_non_serial or first_non_serial.lower() == "total":
                continue
            units.append(row_dict)
        if units:
            return units
    return []


def _extract_facilities_table(soup: BeautifulSoup) -> list[dict]:
    """
    Parse the facilities/services table from Form REP-I Part C.
    Looks for tables with 'facility' or 'service' in the header.
    """
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header = rows[0].get_text(" ", strip=True).lower()
        if "facility" not in header and "service" not in header:
            continue
        headers = [c.get_text(" ", strip=True) for c in rows[0].find_all(["th", "td"])]
        facilities: list[dict] = []
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            row_dict = {
                headers[i] if i < len(headers) else f"col_{i}": c.get_text(" ", strip=True)
                for i, c in enumerate(cells)
            }
            first_val = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            if not cells[0].get_text(strip=True):
                continue
            facilities.append(row_dict)
        if facilities:
            return facilities
    return []


def _extract_documents(soup: BeautifulSoup, detail_url: str) -> list[dict]:
    """
    Extract uploaded documents from the detail page.
    Finds tables with 'Document' in the header and captures View Document links.
    Covers:
      - 'List of Uploaded Documents' table
      - 'Documents Uploaded After Registration' table
    """
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
            if len(cells) < 3:
                continue

            doc_type = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            upload_date = cells[2].get_text(strip=True) if len(cells) > 2 else ""

            # Find first non-JS anchor in the row (any cell)
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

            if not doc_type or not link_url:
                continue
            if link_url in seen_urls:
                continue
            seen_urls.add(link_url)

            docs.append({
                "type": doc_type,
                "link": link_url,
                "dated_on": _normalize_date_str(upload_date),
                "updated": True,
            })

    return docs


# ── Detail page main parser ───────────────────────────────────────────────────

def _parse_detail_page(html: str, detail_url: str) -> dict:
    """
    Parse a Haryana RERA project_preview_open page (Form REP-I Parts A–H).

    Extraction strategy:
    1. Page text (separator="\\n") for regex-based field extraction.
    2. _extract_kv_from_tables() for 2-column label→value table rows.
    3. Dedicated helpers for units, facilities, and document tables.
    4. Persons section parser for directors / authorized signatory.
    """
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator="\n", strip=True)
    kv   = _extract_kv_from_tables(soup)

    out: dict[str, Any] = {"url": detail_url}

    # ── Header: submission date, applicant type, project status ──────────────
    m = re.search(r"Submission Date\s*:?\s*(\d{2}-\d{2}-\d{4}(?:\s+\d{2}:\d{2}:\d{2}\s*(?:AM|PM)?)?)", text, re.I)
    if m:
        out["submitted_date"] = _normalize_date_str(m.group(1).strip())

    # Haryana labels project lifecycle status as "Project Type:" in the header
    m = re.search(r"Project Type\s*:\s*(ONGOING|COMPLETED|LAPSED|REVOKED|NEW)\b", text, re.I)
    if m:
        out["status_of_the_project"] = m.group(1).strip().upper()

    # ── Part A: Company name and registered address ───────────────────────────
    # The company name is the value in the KV table for the matching label.
    # Label may appear as "Name and registered address of the Promoter" (case-insensitive)
    company_name_key = next(
        (k for k in kv if "name and registered address" in k.lower()), None
    )
    company_name = kv.get(company_name_key, "").strip() if company_name_key else ""
    # Fallback: look for "Promoter Name" label
    if not company_name:
        for candidate in ("Promoter Name", "Name of the Promoter", "Applicant Name", "Name of Applicant"):
            if kv.get(candidate):
                company_name = kv[candidate].strip()
                break

    # Registered address: the row labelled "(Annex a copy in Folder A)" that
    # immediately follows the company name contains the address value.
    company_address = kv.get("", "").strip()  # placeholder; refined below

    # Scan raw rows for address pattern: follows "(Annex a copy in Folder A)" label
    _CIN_RE = re.compile(r"^[A-Z]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}$")
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) in (2, 3):
            lbl = cells[-2].get_text(separator=" ", strip=True)
            val = cells[-1].get_text(separator=" ", strip=True)
            if "(Annex" in lbl and val and not val.startswith("XXXX") and len(val) > 10:
                # Skip CIN numbers (format: L/U + 5 digits + 2 letters + 4 digits + 3 letters + 6 digits)
                if _CIN_RE.match(val.strip()):
                    continue
                if not company_address and val:
                    company_address = val
                    break

    # Regex fallback for company name if KV extraction failed
    if not company_name:
        m_name = re.search(
            r"(?:Name and [Rr]egistered [Aa]ddress|Promoter Name|Name of [Pp]romoter)[^\n]*\n\s*([A-Z][A-Z &.,\-PRIVATE LIMITED\d]+)",
            text, re.I
        )
        if m_name:
            company_name = m_name.group(1).strip()

    if company_name:
        out["promoters_details"] = {
            "name": company_name,
            "pan_no": kv.get("Pan No", kv.get("Pan No.", "")),
            "cin_no": kv.get("CIN No", kv.get("CIN No.", "")),
        }
    if company_address:
        out["promoter_address_raw"] = {"raw_address": company_address}

    # Contact details
    # Build a lowercase-key lookup for flexible matching
    kv_lower = {k.lower(): v for k, v in kv.items()}
    contact: dict[str, str] = {}
    landline = (kv.get("Phone(Landline)") or kv.get("Phone (landline)")
                or kv.get("Phone (Landline)") or kv_lower.get("phone(landline)"))
    mobile   = (kv.get("Phone(Mobile)") or kv.get("Phone (Mobile)")
                or kv.get("Phone(mobile)") or kv_lower.get("phone(mobile)")
                or kv_lower.get("mobile no") or kv_lower.get("mobile number"))
    email    = (kv.get("Email ID") or kv.get("Email Id") or kv.get("Email")
                or kv_lower.get("email id") or kv_lower.get("email"))
    website  = kv.get("Website") or kv_lower.get("website")
    if landline: contact["telephone_no"] = landline
    if mobile:   contact["phone"] = mobile
    if email:    contact["email"] = email
    if website:  contact["website"] = website
    if contact:
        out["promoter_contact_details"] = contact

    # ── Part A: Persons (directors + authorized signatory + contact person) ───
    persons_data = _parse_persons_section(text)
    if persons_data.get("directors"):
        out["co_promoter_details"] = persons_data["directors"]
    if persons_data.get("authorized_rep"):
        out["authorised_signatory_details"] = persons_data["authorized_rep"]
    if persons_data.get("contact_person"):
        out["members_details"] = persons_data["contact_person"]

    # Fallback: derive promoter_contact_details from authorized rep if not found above
    if not out.get("promoter_contact_details") and persons_data.get("authorized_rep"):
        auth = persons_data["authorized_rep"]
        fb_contact = {k: v for k, v in {"phone": auth.get("phone"), "email": auth.get("email")}.items() if v}
        if fb_contact:
            out["promoter_contact_details"] = fb_contact

    # ── Part B: Land area ─────────────────────────────────────────────────────
    land_area_raw = kv.get("1. Land area of the project") or kv.get("Land area of the project")
    if land_area_raw:
        out["land_area"] = _float_val(land_area_raw)
    else:
        m = re.search(r"Land area of the project\s+([\d,\.]+)\s*\(", text, re.I)
        if m:
            out["land_area"] = _float_val(m.group(1))
    if out.get("land_area") is not None:
        out["land_area_details"] = {
            "land_area": str(out["land_area"]),
            "land_area_unit": "Sqr/mtrs",
        }

    # License number (various label formats)
    lic_key = next((k for k in kv if any(
        term in k.lower() for term in ("license number", "licence number", "license no", "licence no",
                                        "rera license", "rera licence", "permit no", "dtcp")
    )), None)
    if lic_key:
        out["_license_no"] = kv[lic_key]
        out["alternative_rera_ids"] = [kv[lic_key]]
    else:
        # Regex fallback for license number in text
        m_lic = re.search(r"(?:License|Licence)\s*(?:No\.?|Number)\s*:?\s*([\w/\-]+)", text, re.I)
        if m_lic:
            lic_val = m_lic.group(1).strip()
            out["_license_no"] = lic_val
            out["alternative_rera_ids"] = [lic_val]

    # ── Part C: Project cost ──────────────────────────────────────────────────
    cost_key = next((k for k in kv if "Estimated cost of the project" in k), None)
    cost_detail: dict[str, Any] = {}
    if cost_key:
        total_cost_str = kv[cost_key]
        cost_detail["total_project_cost"] = total_cost_str
        total_rupees = _lakhs_to_rupees(total_cost_str)
        if total_rupees:
            cost_detail["total_project_cost_in_inr"] = total_rupees

    land_cost_key = next((k for k in kv if "Cost of the land" in k), None)
    if land_cost_key:
        cost_detail["cost_of_land"] = _lakhs_to_rupees(kv[land_cost_key])

    construction_cost_key = next((k for k in kv if "cost of construction" in k.lower()), None)
    if construction_cost_key:
        cost_detail["construction_cost"] = _lakhs_to_rupees(kv[construction_cost_key])

    if cost_detail:
        if "construction_cost" in cost_detail:
            cost_detail["estimated_construction_cost"] = cost_detail.pop("construction_cost")
        out["project_cost_detail"] = cost_detail

    # ── Part C: Units table → building_details ────────────────────────────────
    units = _extract_units_table(soup)
    if units:
        out["building_details"] = units

    # ── Part C: Facilities table → provided_faciltiy ──────────────────────────
    facilities = _extract_facilities_table(soup)
    if facilities:
        out["provided_faciltiy"] = facilities

    # ── Part C-X: Financial info (flats constructed/booked) ───────────────────
    m = re.search(r"No\.\s+of\s+Flats[^0-9]*constructed\s+([\d,]+)", text, re.I)
    if m:
        try:
            out["_flats_constructed"] = int(m.group(1).replace(",", ""))
        except ValueError:
            pass

    m = re.search(r"No\.\s+of\s+Flats[^0-9]*booked\s+([\d,]+)", text, re.I)
    if m:
        try:
            total_units = int(m.group(1).replace(",", ""))
            out["number_of_residential_units"] = total_units
        except ValueError:
            pass

    # ── Part D: Bank account ──────────────────────────────────────────────────
    bank: dict[str, str] = {}
    bank_keys = {
        "Bank and Branch address": "bank_name",
        "Bank Account number": "account_no",
        "IFSC code": "IFSC",
        "MICR code": "micr_code",
        "Branch code": "branch_code",
    }
    for raw_key, schema_key in bank_keys.items():
        val = kv.get(raw_key) or kv.get(raw_key.lower())
        if val:
            bank[schema_key] = val
    if bank:
        out["bank_details"] = bank

    # ── Documents section ─────────────────────────────────────────────────────
    docs = _extract_documents(soup, detail_url)
    if docs:
        out["uploaded_documents"] = docs

    # ── Raw safety net ────────────────────────────────────────────────────────
    out["data"] = {
        "govt_type": "state",
        "is_processed": False,
        "all_kv_labels": list(kv.keys()),
        "source_url": detail_url,
    }

    return {k: v for k, v in out.items() if v not in (None, "", {}, [])}


# ── Persons section parser ────────────────────────────────────────────────────

def _parse_persons_section(text: str) -> dict:
    """
    Parse Form REP-I Part A numbered person entries (directors, authorized rep,
    contact person at site office).

    Pattern in text:
        N. <role title>:
        Name : <name>
        Residential Address : <address>
        Phone (Mobile) <mobile>
        Email ID <email>
        PAN No. <pan>
    """
    directors: list[dict] = []
    authorized_rep: dict | None = None
    contact_person: dict | None = None

    # Split on numbered person headers
    person_blocks = re.split(
        r"\n\s*(\d+)\.\s+(Managing Director|Director \d+|Authorised\s+rep[re]+sentative[^\n]*|"
        r"Contact person[^\n]*)\s*:\s*\n",
        text,
        flags=re.I,
    )

    # person_blocks: [pre_text, num1, role1, content1, num2, role2, content2, ...]
    for i in range(1, len(person_blocks), 3):
        if i + 2 >= len(person_blocks):
            break
        num     = person_blocks[i].strip()
        role    = person_blocks[i + 1].strip()
        content = person_blocks[i + 2]

        def _extract(pattern: str, txt: str = content) -> str:
            m = re.search(pattern, txt, re.I)
            return m.group(1).strip() if m else ""

        name    = _extract(r"Name\s*:?\s*(.+?)(?:\n|$)")
        address = _extract(r"Residential Address\s*:?\s*(.+?)(?:\n|$)")
        mobile  = _extract(r"Phone\s*\(Mobile\)\s*([\d\s]+?)(?:\(|&|\n|$)")
        email   = _extract(r"Email ID\s*([\w@\.\-]+@[\w\.\-]+)")
        pan     = _extract(r"PAN No\.?\s*([A-Z]{5}\d{4}[A-Z])")

        entry: dict = {}
        if name:     entry["name"]    = name
        if role:     entry["role"]    = role
        if address:  entry["address"] = address
        if mobile:   entry["phone"]   = mobile.strip()
        if email:    entry["email"]   = email
        if pan:      entry["pan_no"]  = pan

        if not entry:
            continue

        role_lower = role.lower()
        if "contact person" in role_lower:
            contact_person = {**entry, "position": "Contact person at site office"}
        elif "authoris" in role_lower or "authoriz" in role_lower:
            authorized_rep = entry
        else:
            directors.append(entry)

    return {
        "directors":      directors or None,
        "authorized_rep": authorized_rep,
        "contact_person": contact_person,
    }


# ── Document download + S3 upload ────────────────────────────────────────────

def _handle_document(
    project_key: str,
    doc: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
) -> dict | None:
    """Download a single document and upload to S3. Returns result entry or None."""
    url = doc.get("url") or doc.get("link")
    if not url:
        return None
    label = doc.get("type") or doc.get("label") or "document"
    filename = build_document_filename(doc)
    try:
        resp = safe_get(url, logger=logger, timeout=60.0)
        if not resp or len(resp.content) < 100:
            logger.warning("Document download empty or failed", url=url, label=label)
            return None
        data   = resp.content
        md5    = compute_md5(data)
        s3_key = upload_document(project_key, filename, data, dry_run=settings.DRY_RUN_S3)
        if s3_key is None:
            return None
        s3_url = get_s3_url(s3_key)
        return document_result_entry(doc, s3_url=s3_url, md5=md5)
    except Exception as exc:
        logger.warning("Document handling error", url=url, error=str(exc))
        return None


# ── Normalization helper ──────────────────────────────────────────────────────

def _merge_stub_and_detail(stub: dict, detail: dict, config_id: int) -> dict:
    """
    Merge listing stub fields with detail page fields into a normalized payload.

    Field priority:
    - detail page fields take precedence for enriched values (promoter_name, city, etc.)
    - listing stub provides the registration_no, acknowledgement_no, and listing-level dates
    """
    project_registration_no = stub["project_registration_no"]

    # Base payload from stub
    payload: dict[str, Any] = {
        "project_registration_no": project_registration_no,
        "acknowledgement_no": stub.get("acknowledgement_no") or None,
        "project_name": stub.get("project_name") or detail.get("project_name"),
        "promoter_name": stub.get("promoter_name") or None,
        "project_city": stub.get("project_city") or None,
        "project_location_raw": {
            "raw_address": stub.get("project_location_raw_address"),
            "district": stub.get("project_city"),
        },
        "state": "haryana",
        "domain": DOMAIN,
        "config_id": config_id,
        "url": detail.get("url") or stub.get("detail_url"),
    }

    # Dates from listing
    payload["estimated_finish_date"] = stub.get("estimated_finish_date")

    # Enrich from detail page (non-None values overwrite stub)
    detail_fields = [
        "status_of_the_project", "submitted_date",
        "land_area", "construction_area", "number_of_residential_units",
        "land_area_details",  # land_area_details was extracted but missing from this list
        "project_cost_detail", "building_details", "provided_faciltiy",
        "bank_details", "promoters_details", "promoter_address_raw",
        "promoter_contact_details", "co_promoter_details",
        "authorised_signatory_details", "members_details",
        "alternative_rera_ids",  # set from _license_no but was missing from transfer list
        "uploaded_documents", "data", "_license_no",
        "_flats_constructed", "_promoter_pan",
    ]
    for field in detail_fields:
        val = detail.get(field)
        if val is not None:
            payload[field] = val

    # QPR and certificate documents — prepend to uploaded_documents if available
    cert_url = stub.get("cert_url")
    qpr_url  = stub.get("qpr_url") or _build_qpr_url(project_registration_no)

    listing_docs: list[dict] = []
    if cert_url:
        listing_docs.append({"type": "Rera Registration Certificate 1", "link": cert_url})
    if qpr_url:
        listing_docs.append({"type": "Quarterly Progress Report", "link": qpr_url})

    if listing_docs:
        existing_docs = payload.get("uploaded_documents") or []
        existing_links = {d.get("link") for d in existing_docs}
        for doc in listing_docs:
            if doc.get("link") not in existing_links:
                existing_docs.insert(0, doc)
        payload["uploaded_documents"] = existing_docs

    # Authority metadata inside data blob
    data_blob: dict = payload.get("data") or {}
    data_blob["authority_type"] = stub.get("authority_type")
    data_blob["listing_url"]    = stub.get("_listing_url")
    payload["data"] = data_blob

    return payload


# ── Main entry point ──────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    """
    Main entry point for the Haryana RERA crawler.

    Iterates both listing URLs (Gurugram /2 and Panchkula /1), de-duplicates
    by project_registration_no, fetches detail pages, normalizes, persists
    to the DB, and uploads selected documents to S3.

    Args:
        config:  Site configuration dict (from sites_config.py).
        run_id:  Integer run identifier for checkpointing.
        mode:    "full" | "incremental" | "retry_errors"

    Returns:
        Summary counts dict.
    """
    site_id   = config["id"]
    config_id = config.get("config_id", CONFIG_ID)
    logger    = CrawlerLogger(site_id, run_id)
    counts    = dict(
        projects_found=0, projects_new=0, projects_updated=0,
        projects_skipped=0, documents_uploaded=0, error_count=0,
    )
    item_limit = settings.CRAWL_ITEM_LIMIT or 0

    # ── Step 1: Collect stubs from both listing pages ─────────────────────────
    listing_urls = LISTING_URLS
    logger.info("Starting Haryana RERA crawl", listing_count=len(listing_urls), mode=mode)

    all_stubs: list[dict] = []
    seen_reg_nos: set[str] = set()

    for listing_url in listing_urls:
        logger.info("Fetching listing page", url=listing_url)
        stubs = _fetch_listing(listing_url, logger)
        logger.info("Listing parsed", url=listing_url, row_count=len(stubs))
        for stub in stubs:
            reg_no = stub.get("project_registration_no", "")
            if reg_no and reg_no not in seen_reg_nos:
                seen_reg_nos.add(reg_no)
                all_stubs.append(stub)

    counts["projects_found"] = len(all_stubs)
    logger.info("Total unique projects found", count=len(all_stubs))

    if not all_stubs:
        logger.error("No projects found — aborting")
        return counts

    # ── Step 2: Load checkpoint ───────────────────────────────────────────────
    checkpoint = load_checkpoint(site_id, mode) or {}
    last_project_key = checkpoint.get("last_project_key")
    resume_pending = bool(last_project_key and mode != "full")
    machine_name, machine_ip = get_machine_context()

    # ── Step 3: Process each project ─────────────────────────────────────────
    items_processed = 0
    for i, stub in enumerate(all_stubs):
        if item_limit and items_processed >= item_limit:
            logger.info(f"CRAWL_ITEM_LIMIT={item_limit} reached", step="listing")
            break

        reg_no      = stub["project_registration_no"]
        project_key = generate_project_key(reg_no)
        detail_url  = stub.get("detail_url")

        if resume_pending:
            if project_key == last_project_key:
                resume_pending = False
            counts["projects_skipped"] += 1
            continue

        logger.set_project(key=project_key, reg_no=reg_no, url=detail_url or LISTING_URLS[0], page=i)
        try:
            if not detail_url:
                logger.warning("No detail URL for project")
                counts["error_count"] += 1
                continue

            try:
                # ── Fetch detail page ─────────────────────────────────────────────
                random_delay(*config.get("rate_limit_delay", (1, 3)))
                resp = safe_get(detail_url, logger=logger, timeout=60.0)
                if not resp:
                    logger.warning("Detail page fetch failed", url=detail_url)
                    insert_crawl_error(
                        run_id, site_id, "detail_fetch_failed", detail_url,
                        project_key=project_key, url=detail_url,
                    )
                    counts["error_count"] += 1
                    continue

                # ── Parse detail page ─────────────────────────────────────────────
                detail = _parse_detail_page(resp.text, detail_url)

                # ── Merge stub + detail → normalized payload ──────────────────────
                raw_payload = _merge_stub_and_detail(stub, detail, config_id)
                payload = normalize_project_payload(
                    raw_payload,
                    config,
                    machine_name=machine_name,
                    machine_ip=machine_ip,
                )

                # ── Validate + persist ────────────────────────────────────────────
                try:
                    record = ProjectRecord(**payload)
                    db_dict = record.to_db_dict()
                except ValidationError as ve:
                    logger.warning(
                        "Validation error",
                        errors=ve.errors()[:3],  # truncate for readability
                    )
                    db_dict = payload

                status = upsert_project(db_dict)
                if status == "new":
                    counts["projects_new"] += 1
                elif status == "updated":
                    counts["projects_updated"] += 1
                else:
                    counts["projects_skipped"] += 1
                items_processed += 1

                # ── Documents ────────────────────────────────────────────────────
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
                        "key": db_dict["key"],
                        "url": db_dict["url"],
                        "state": db_dict["state"],
                        "domain": db_dict["domain"],
                        "project_registration_no": db_dict["project_registration_no"],
                        "uploaded_documents": persisted_docs,
                        "document_urls": build_document_urls(persisted_docs),
                    })

                # ── Checkpoint ───────────────────────────────────────────────────
                if (i + 1) % 50 == 0:
                    save_checkpoint(site_id, mode, i, project_key, run_id)
                    logger.info("Progress checkpoint saved", done=i + 1, total=len(all_stubs))

            except Exception as exc:
                logger.error("Unexpected error processing project", error=str(exc))
                insert_crawl_error(run_id, site_id, "unexpected_error", str(exc), project_key=project_key)
                counts["error_count"] += 1
                continue
        finally:
            logger.clear_project()

    # ── Final checkpoint ──────────────────────────────────────────────────────
    save_checkpoint(site_id, mode, len(all_stubs), project_key if all_stubs else None, run_id)
    logger.info("Crawl complete", **counts)
    return counts
