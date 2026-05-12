"""
Jharkhand RERA Crawler — jharera.jharkhand.gov.in
Type: static (httpx + BeautifulSoup)

Strategy:
- Listing page is server-rendered MVC; pagination via ?page=N query parameter.
- Each listing row has a "View Profile" link to /Home/ViewProjectProfile/{id}.
- Detail page is a plain GET, parsed with httpx + BeautifulSoup.
- Fields from listing: project_registration_no, project_name, address, detail_url.
- Fields from detail: project_type, actual_commencement_date, estimated_finish_date,
    promoter_name, promoter_contact_details, promoters_details, project_location_raw,
    bank_details, co_promoter_details, professional_information, building_details,
    land_detail, project_cost_detail, uploaded_documents.
"""
from __future__ import annotations

import re
import time

from bs4 import BeautifulSoup, Comment, Tag
from pydantic import ValidationError

from core.checkpoint import reset_checkpoint
from core.config import settings
from core.crawler_base import download_response, generate_project_key, random_delay, safe_get
from core.db import get_project_by_key, upsert_project, upsert_document, insert_crawl_error
from core.document_policy import select_document_for_download
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import (
    build_document_filename,
    build_document_urls,
    existing_uploaded_document_entry,
    get_machine_context,
    merge_data_sections,
    normalize_project_payload,
)
from core.s3 import compute_md5, upload_document, get_s3_url

LISTING_URL = "https://jharera.jharkhand.gov.in/Home/OnlineRegisteredProjectsList"
DETAIL_BASE  = "https://jharera.jharkhand.gov.in/Home/ViewProjectProfile"
DOMAIN       = "jharera.jharkhand.gov.in"
BASE_URL     = "https://jharera.jharkhand.gov.in"

# Document types that appear as table-row labels in Section 1
_DOC_LABELS: set[str] = {
    "map", "permit/sanction letter", "allotment letter", "brochure",
    "agreement for sale", "conveyence deed", "gant chart",
    "details of all past real estate details",
}

# Development-work document labels (Section 5 / provided_facility rows)
_DEV_DOC_LABELS: set[str] = {
    "road", "supply water", "seawage and drainage system",
    "electricity supply and transformer/sub station",
    "solid waste management and disposal", "fire fighting facility",
    "drinking water facility", "emergency evacuation service",
    "use of renewable energy", "rain water harvesting",
}


# ── Text helpers ──────────────────────────────────────────────────────────────

def _clean(text: str) -> str:
    """Collapse whitespace and strip."""
    return re.sub(r"\s+", " ", text or "").strip()


def _safe_float(val: str) -> float | None:
    if not val:
        return None
    try:
        v = float(re.sub(r"[^\d.\-]", "", val))
        return v if v != 0 else None
    except ValueError:
        return None


def _abs_url(href: str) -> str:
    if not href:
        return ""
    return href if href.startswith("http") else BASE_URL + href


# ── KV extraction ─────────────────────────────────────────────────────────────

def _kv_from_table(table: Tag) -> dict[str, str]:
    """Extract label→value pairs from a two-column (or label:value) HTML table."""
    out: dict[str, str] = {}
    for tr in table.find_all("tr"):
        cells = [_clean(c.get_text(separator=" ")) for c in tr.find_all(["td", "th"])]
        if len(cells) == 2 and cells[0]:
            out[cells[0].lower()] = cells[1]
        elif len(cells) >= 3 and cells[1] in (":", "") and cells[0]:
            out[cells[0].lower()] = cells[2]
    return out


def _kv_from_divrows(soup: BeautifulSoup) -> dict[str, str]:
    """Extract label→value pairs from Bootstrap div.row / <label> pattern.
    The Jharkhand portal uses:
      <div class="row">
        <div class="col-md-4"><label>Field Name</label></div>
        <div class="col-md-8"><label>Field Value</label></div>
      </div>
    """
    out: dict[str, str] = {}
    for row_div in soup.find_all("div", class_="row"):
        labels = row_div.find_all("label", recursive=True)
        if len(labels) >= 2:
            key = _clean(labels[0].get_text()).rstrip(":").lower()
            val = _clean(" ".join(l.get_text() for l in labels[1:]))
            if key and val:
                out[key] = val
    return out


def _rows_from_table(table: Tag) -> tuple[list[str], list[list[str]]]:
    """Return (headers, data_rows) for a header+data table."""
    all_rows = table.find_all("tr")
    if not all_rows:
        return [], []
    headers = [_clean(c.get_text()) for c in all_rows[0].find_all(["th", "td"])]
    data: list[list[str]] = []
    for tr in all_rows[1:]:
        cells = [_clean(c.get_text(separator=" ")) for c in tr.find_all(["td", "th"])]
        if any(cells):
            data.append(cells)
    return headers, data


# ── Address parser ────────────────────────────────────────────────────────────

def _parse_address(raw_text: str) -> dict:
    """
    Parse multi-part comma-separated address.
    Pattern (observed): locality, area, town, district, state, pincode .
    """
    text = re.sub(r"\s+", " ", raw_text).strip().rstrip(". ").strip()
    parts = [p.strip() for p in text.split(",") if p.strip()]
    result: dict = {"raw_address": text.lower()}
    if len(parts) >= 2:
        result["state"] = parts[-2].lower()
    if len(parts) >= 3:
        result["city"] = parts[-3].lower()
    if len(parts) >= 1:
        pin = parts[-1].strip(" .")
        if pin.isdigit() and len(pin) == 6:
            result["pincode"] = pin
    return result


# ── Listing page parser ───────────────────────────────────────────────────────

def _parse_listing_rows(soup: BeautifulSoup) -> list[dict]:
    """Extract project rows from listing page table."""
    rows: list[dict] = []
    for table in soup.find_all("table"):
        first_row = table.find("tr")
        if not first_row:
            continue
        header_text = first_row.get_text(separator="|", strip=True)
        if "Reg No" not in header_text and "Registration" not in header_text:
            continue
        for tr in table.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if len(tds) < 6:
                continue
            reg_no = _clean(tds[1].get_text())
            # Skip pager rows or empty rows
            if not reg_no or reg_no.isdigit() or len(reg_no) < 5:
                continue
            profile_a = tds[-1].find("a", href=True)
            detail_url = _abs_url(profile_a["href"]) if profile_a else ""
            rows.append({
                "project_name": _clean(tds[2].get_text(separator=" ")),
                "project_registration_no": reg_no,
                "address": _clean(tds[3].get_text(separator=" ")),
                "detail_url": detail_url,
            })
        break  # found the right table
    return rows


def _has_next_page(soup: BeautifulSoup, current_page: int) -> bool:
    """Return True if there is a page beyond current_page."""
    # Pager text: «Page N of M.Showing items...»»»
    pager_text = soup.get_text(separator=" ")
    match = re.search(r"Page\s+(\d+)\s+of\s+(\d+)", pager_text, re.IGNORECASE)
    if match:
        total = int(match.group(2))
        return current_page < total
    # Fallback: look for a "»»»" link or any link beyond current page number
    for a in soup.find_all("a"):
        txt = a.get_text(strip=True)
        if txt in ("»»»", "»", "Next") or (txt.isdigit() and int(txt) > current_page):
            return True
    return False


# ── Detail page parser ────────────────────────────────────────────────────────

def _parse_detail_page(html: str, detail_url: str) -> dict:  # noqa: C901
    """Extract all available fields from a ViewProjectProfile detail page."""
    soup = BeautifulSoup(html, "lxml")

    # Collect all tables; we'll index them for section-specific parsing
    tables = soup.find_all("table")

    # ── Build a global flat KV dict from all table rows + div rows ───────
    global_kv: dict[str, str] = {}
    for tbl in tables:
        global_kv.update(_kv_from_table(tbl))
    # The portal now renders core fields in Bootstrap div.row / <label> pairs
    global_kv.update(_kv_from_divrows(soup))

    def _f(*keys: str) -> str:
        for k in keys:
            v = global_kv.get(k.lower(), "")
            if v:
                return v
        return ""

    # ── Registration number ───────────────────────────────────────────────
    reg_no_text = ""
    for tag in soup.find_all(["h4", "h3", "h2", "strong", "b", "th", "td"]):
        t = _clean(tag.get_text())
        m = re.search(r"(JHARERA/[A-Z0-9/]+)", t, re.IGNORECASE)
        if m:
            reg_no_text = m.group(1).strip()
            break

    # ── Project core fields ───────────────────────────────────────────────
    project_name = _f("project name")
    project_type = _f("project type")
    permit_from  = _f("permit valid from")
    permit_to    = _f("permit valid to")
    pan_no       = _f("pan no.", "pan no")
    firm_reg_no  = _f("firm registration no.", "firm registration no")
    # The portal wraps the firm registration field in an HTML comment; also search there
    if not firm_reg_no:
        for _cmt in soup.find_all(string=lambda t: isinstance(t, Comment)):
            _m = re.search(
                r'firm\s+registration\s+no\.?\s*</label[^>]*>.*?<label[^>]*>\s*([^<\s][^<]*?)\s*</label',
                str(_cmt), re.IGNORECASE | re.DOTALL,
            )
            if _m:
                firm_reg_no = _m.group(1).strip()
                break
    email        = _f("email id", "email")

    # ── Address ───────────────────────────────────────────────────────────
    address_raw  = _f("project address")
    loc_raw: dict = _parse_address(address_raw) if address_raw else {}

    # ── Coordinates (portal labels are swapped: "Latitude"=E, "Longitude"=N) ──
    lat_str = _f("project latitude")
    lng_str = _f("project longitude")
    if lat_str or lng_str:
        # strip direction letters
        lat_val = _safe_float(re.sub(r"[NSEW]", "", lat_str, flags=re.IGNORECASE))
        lng_val = _safe_float(re.sub(r"[NSEW]", "", lng_str, flags=re.IGNORECASE))
        # Portal swaps labels: "Project Latitude" = East (longitude), "Project Longitude" = North (latitude)
        if lat_val and lng_val:
            loc_raw["processed_latitude"]  = lng_val   # "Project Longitude" field = North = true latitude
            loc_raw["processed_longitude"] = lat_val   # "Project Latitude"  field = East  = true longitude
        elif lat_val or lng_val:
            # If only one is available, use directional hint
            if lat_val:
                loc_raw["processed_longitude"] = lat_val  # East = true longitude
            if lng_val:
                loc_raw["processed_latitude"] = lng_val   # North = true latitude

    # ── Promoter name (Builder Details label in Section 2) ────────────────
    promoter_name = _f("builder details")

    # ── Promoter contact ──────────────────────────────────────────────────
    contact: dict | None = None
    mobile = _f("mobile", "mobile no", "mobile number")
    if email or mobile:
        contact = {k: v for k, v in {"email": email, "mobile": mobile}.items() if v}


    # ── Documents from Section 1 rows (MAP, Permit, etc.) ────────────────
    docs: list[dict] = []
    _collected_doc_labels: set[str] = set()

    # Strategy A: Old layout — two-column table rows (label | link)
    for tbl in tables:
        for tr in tbl.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            label_cell = cells[0]
            value_cell = cells[1] if len(cells) == 2 else cells[-1]
            label_text = _clean(label_cell.get_text()).lower()
            if label_text in _DOC_LABELS or label_text in _DEV_DOC_LABELS:
                a_tag = value_cell.find("a", href=True)
                href = _abs_url(a_tag["href"]) if a_tag else BASE_URL + "/FirstLevel/ViewDocument"
                display_label = _clean(label_cell.get_text())
                if label_text not in _collected_doc_labels:
                    docs.append({"link": href, "type": display_label})
                    _collected_doc_labels.add(label_text)

    # Strategy B: New layout — Bootstrap div.row with <label> key + <a> link
    # (Section 1 docs: MAP, Permit, Brochure, etc.)
    for row_div in soup.find_all("div", class_="row"):
        labels = row_div.find_all("label", recursive=True)
        if not labels:
            continue
        label_text = _clean(labels[0].get_text()).rstrip(":").lower()
        if label_text in _DOC_LABELS:
            a_tag = row_div.find("a", href=True)
            href = _abs_url(a_tag["href"]) if a_tag else BASE_URL + "/FirstLevel/ViewDocument"
            display_label = _clean(labels[0].get_text()).rstrip(":")
            if label_text not in _collected_doc_labels:
                docs.append({"link": href, "type": display_label})
                _collected_doc_labels.add(label_text)

    # Strategy C: New layout — single merged <td> containing inline dev-doc links
    # Format: "Road : <span><a href="...">View</a></span><br/>Supply Water : ..."
    for tbl in tables:
        for td in tbl.find_all("td"):
            colspan = td.get("colspan")
            if not colspan or int(colspan) < 2:
                continue
            cell_text = td.get_text()
            if not any(lbl in cell_text.lower() for lbl in ("road", "supply water", "seawage")):
                continue
            # Walk direct children: NavigableString = label candidate, <span> = link
            current_label = ""
            for node in td.children:
                if isinstance(node, str):
                    text = _clean(node).rstrip(":").strip()
                    if text:
                        current_label = text
                elif hasattr(node, "name"):
                    if node.name in ("span", "a"):
                        a_tag = node if node.name == "a" else node.find("a", href=True)
                        if a_tag and a_tag.get("href") and current_label:
                            lbl_lower = current_label.lower()
                            if lbl_lower in _DEV_DOC_LABELS and lbl_lower not in _collected_doc_labels:
                                docs.append({"link": _abs_url(a_tag["href"]), "type": current_label})
                                _collected_doc_labels.add(lbl_lower)
                        current_label = ""  # reset after consuming link

    # ── Co-promoters (Partners/Directors table) ───────────────────────────
    # Some Jharkhand detail pages include a sub-header row (e.g. "Designation",
    # "Photo") as a <td> row right after the <th> header. We skip any row whose
    # first cell matches a known column-header word so it isn't treated as data.
    _JHAR_HDR_NAMES = {
        "name", "designation", "role", "email", "photo", "mobile",
        "address", "s.no", "sl.no", "sr.no", "s no",
    }
    co_promoters: list[dict] = []
    for tbl in tables:
        rows = tbl.find_all("tr")
        if not rows:
            continue
        hdr_text = rows[0].get_text(separator="|", strip=True).lower()
        if not ("partner" in hdr_text or "director" in hdr_text or "chairman" in hdr_text):
            continue
        # The Jharkhand table often has a large combined first <tr> containing ALL
        # data (header labels + every data row concatenated into one cell).
        # The actual column headers are in rows[1], and data starts at rows[2].
        # Detect this by counting cells in rows[0] vs rows[1].
        row0_cells = rows[0].find_all(["th", "td"])
        if len(rows) >= 2:
            row1_cells = rows[1].find_all(["th", "td"])
            # If row 0 has more cells than row 1, it's the combined row
            if len(row0_cells) > len(row1_cells) and len(row1_cells) <= 6:
                header_row_idx = 1
            else:
                header_row_idx = 0
        else:
            header_row_idx = 0
        # Use separator=" " so that column headers containing inline <br> tags
        # (e.g. <th>Present<br>Address</th>) are joined with a space rather than
        # concatenated ("PresentAddress"), which would break the column-name lookup.
        header_cells = [_clean(c.get_text(separator=" ")) for c in rows[header_row_idx].find_all(["th", "td"])]
        for tr in rows[header_row_idx + 1:]:
            cells = tr.find_all(["td", "th"])
            if not cells:
                continue
            row_vals = [_clean(c.get_text(separator=" ")) for c in cells]
            if not any(row_vals):
                continue
            # Skip sub-header rows whose first cell looks like a column name
            if row_vals[0].lower().strip() in _JHAR_HDR_NAMES:
                continue
            entry: dict = dict(zip([h.lower() for h in header_cells], row_vals))
            rec: dict = {}
            for col, field in [
                ("name", "name"), ("role", "role"), ("designation", "role"),
                ("email", "email"), ("emaild", "email"),
                ("present address", "present_address"),
                ("address", "present_address"),
            ]:
                v = entry.get(col, "")
                if v:
                    rec[field] = v
            # Extract photo <a> href from photo cell
            for i, cell in enumerate(cells):
                a = cell.find("a", href=True)
                if a and ("ViewDocument" in a["href"] or "Photo" in a["href"]):
                    rec["photo"] = _abs_url(a["href"])
                    break
            if rec and rec.get("name"):
                co_promoters.append(rec)
        break


    # ── Bank details ──────────────────────────────────────────────────────
    bank_details: list[dict] | None = None
    for tbl in tables:
        rows = tbl.find_all("tr")
        if not rows:
            continue
        hdr_text = rows[0].get_text(separator="|", strip=True).lower()
        if "bank" not in hdr_text and "account" not in hdr_text and "ifsc" not in hdr_text:
            continue
        bank_rows: list[dict] = []
        for tr in rows[1:]:
            cells = [_clean(c.get_text(separator=" ")) for c in tr.find_all(["td", "th"])]
            if not any(cells):
                continue
            if len(cells) >= 5:
                # 5-col layout: Account Type | Bank Name | Account No | Account Holder Name | IFSC Code
                rec: dict = {}
                if cells[1]: rec["bank_name"]    = cells[1]  # Bank Name (e.g. CANARA BANK)
                if cells[2]: rec["account_no"]   = cells[2]  # Account Number
                if cells[3]: rec["account_name"] = cells[3]  # Account Holder Name
                if cells[4]: rec["IFSC"]         = cells[4]  # IFSC Code (e.g. CNRB0004902)
                if rec:
                    bank_rows.append(rec)
            elif len(cells) >= 3:
                bank_rows.append({"IFSC": cells[0], "account_no": cells[1], "account_name": cells[2]})
            else:
                bank_rows.append({"account_no": cells[0], "account_name": cells[1]})
        if bank_rows:
            bank_details = bank_rows
        break

    # ── Professional information ───────────────────────────────────────────
    # Jharkhand has SEPARATE tables for contractors, architects, and structural
    # engineers. Collect from ALL matching tables; infer role from header text.
    _PROF_PLACEHOLDER_NAMES = _JHAR_HDR_NAMES | {
        "contractor name", "archiect name", "structural engineer name",
        "email id", "mobile", "pan no.", "pan no",
    }
    prof_all_rows: list[dict] = []
    for tbl in tables:
        rows = tbl.find_all("tr")
        if not rows:
            continue
        hdr_text = rows[0].get_text(separator="|", strip=True).lower()
        if not any(kw in hdr_text for kw in ("contractor", "archiect", "architect", "engineer", "professional")):
            continue
        # Infer role from the table's header text
        if "contractor" in hdr_text:
            inferred_role = "Contractor"
        elif "archiect" in hdr_text or "architect" in hdr_text:
            inferred_role = "Archiect"
        elif "structural engineer" in hdr_text:
            inferred_role = "Structural Engineer"
        else:
            inferred_role = ""
        # Same combined-row detection as co_promoters above
        row0_cells = rows[0].find_all(["th", "td"])
        if len(rows) >= 2:
            row1_cells = rows[1].find_all(["th", "td"])
            header_row_idx = 1 if (len(row0_cells) > len(row1_cells) and len(row1_cells) <= 8) else 0
        else:
            header_row_idx = 0
        header_cells = [_clean(c.get_text(separator=" ")) for c in rows[header_row_idx].find_all(["th", "td"])]
        # Skip description-only tables (single cell or no data rows)
        if len(header_cells) <= 1:
            continue
        for tr in rows[header_row_idx + 1:]:
            cells_raw = tr.find_all(["td", "th"])
            vals = [_clean(c.get_text(separator=" ")) for c in cells_raw]
            if not any(vals):
                continue
            # Skip header-label rows (placeholder or repeated header)
            if vals[0].lower().strip() in _PROF_PLACEHOLDER_NAMES:
                continue
            entry = dict(zip([h.lower() for h in header_cells], vals))
            rec: dict = {}
            for col, field in [
                ("name", "name"), ("contractor name", "name"), ("archiect name", "name"),
                ("structural engineer name", "name"),
                ("role", "role"), ("email", "email"),
                ("email id", "email"), ("email id.", "email"),
                ("mobile", "mobile"), ("mobile no", "mobile"), ("mobile number", "mobile"),
                ("pan no", "pan_no"), ("pan no.", "pan_no"),
                ("address", "address"),
            ]:
                v = entry.get(col, "")
                if v:
                    rec[field] = v
            # Add inferred role if not already present
            if inferred_role and not rec.get("role"):
                rec["role"] = inferred_role
            if rec and rec.get("name"):
                prof_all_rows.append(rec)
    # Deduplicate by (name, email), keeping the LAST occurrence.
    # Outer wrapper tables are scanned first and assign a generic role; individual
    # per-role sub-tables come later in document order and carry the correct role,
    # so "last wins" ensures accurate role assignment.
    _seen_prof: dict[tuple, int] = {}   # key -> index in _deduped_prof
    _deduped_prof: list[dict] = []
    for _row in prof_all_rows:
        _key = (_row.get("name", "").lower(), _row.get("email", "").lower())
        if _key in _seen_prof:
            _deduped_prof[_seen_prof[_key]] = _row   # overwrite with more specific entry
        else:
            _seen_prof[_key] = len(_deduped_prof)
            _deduped_prof.append(_row)
    professionals: list[dict] | None = _deduped_prof or None

    # ── Building / floor / flat details ───────────────────────────────────
    building_details: list[dict] | None = None
    for tbl in tables:
        rows = tbl.find_all("tr")
        if not rows:
            continue
        hdr_text = rows[0].get_text(separator="|", strip=True).lower()
        if "flat" not in hdr_text and "floor" not in hdr_text and "carpet" not in hdr_text:
            continue
        # Determine column indices from header — the portal now uses 6 columns:
        # Sl.No. | flat No. | floor No. | Completion Status | Sold Status | Carpet Area(in m sq.)
        # Older pages used 2 columns: Flat Name | Carpet Area
        hdr_cells = [_clean(c.get_text()) for c in rows[0].find_all(["th", "td"])]
        hdr_lower  = [h.lower() for h in hdr_cells]
        flat_idx   = next((i for i, h in enumerate(hdr_lower) if "flat" in h), 0)
        carpet_idx = next((i for i, h in enumerate(hdr_lower) if "carpet" in h), 1)
        sold_idx   = next((i for i, h in enumerate(hdr_lower) if "sold" in h), None)
        bldg_rows: list[dict] = []
        for tr in rows[1:]:
            vals = [_clean(c.get_text(separator=" ")) for c in tr.find_all(["td", "th"])]
            if not any(vals):
                continue
            rec: dict = {}
            if flat_idx < len(vals) and vals[flat_idx]:
                rec["flat_name"] = vals[flat_idx]
            if carpet_idx < len(vals) and vals[carpet_idx]:
                rec["carpet_area"] = vals[carpet_idx]
            if sold_idx is not None and sold_idx < len(vals) and vals[sold_idx]:
                rec["sold_status"] = vals[sold_idx]
            if rec.get("flat_name"):
                bldg_rows.append(rec)
        if bldg_rows:
            building_details = bldg_rows
        break

    # ── Land records ──────────────────────────────────────────────────────
    land_detail: list[dict] | None = None
    for tbl in tables:
        rows = tbl.find_all("tr")
        if not rows:
            continue
        hdr_text = rows[0].get_text(separator="|", strip=True).lower()
        if "khata" not in hdr_text and "plot" not in hdr_text and "land" not in hdr_text:
            continue
        land_rows: list[dict] = []
        header_cells = [_clean(c.get_text(separator=" ")) for c in rows[0].find_all(["th", "td"])]
        # Skip wrapper/description tables with a single header cell
        if len(header_cells) <= 1:
            continue
        for tr in rows[1:]:
            cells_raw = tr.find_all(["td", "th"])
            vals = [_clean(c.get_text(separator=" ")) for c in cells_raw]
            if not any(vals):
                continue
            entry = dict(zip([h.lower() for h in header_cells], vals))
            rec: dict = {}
            for col, field in [
                ("plot no", "plot_no"), ("plot no.", "plot_no"),
                ("khata no", "khata_no"), ("khata no.", "khata_no"),
                # "Ownership Detail" column holds the title-holder name on this portal
                ("ownership detail", "title_holder_name"),
                ("title holder name", "title_holder_name"),
                ("title holder", "title_holder_name"),
            ]:
                v = entry.get(col, "")
                if v:
                    rec[field] = v
            # Extract sale deed link
            for i, cell in enumerate(cells_raw):
                a = cell.find("a", href=True)
                if a and "ViewDocument" in a["href"]:
                    rec["sale_deed"] = _abs_url(a["href"])
                    break
            if rec:
                land_rows.append(rec)
        # Deduplicate by (plot_no, sale_deed) — nested table scanning can repeat rows
        _seen_land: set = set()
        _deduped_land: list[dict] = []
        for _lr in land_rows:
            _lkey = (_lr.get("plot_no", ""), _lr.get("sale_deed", ""))
            if _lkey not in _seen_land:
                _seen_land.add(_lkey)
                _deduped_land.append(_lr)
        if _deduped_land:
            land_detail = _deduped_land
        break

    # ── Project cost ──────────────────────────────────────────────────────
    estimated_cost = _f("estimated project cost", "project estimated cost", "project cost", "total project cost")
    project_cost: dict | None = None
    if estimated_cost:
        project_cost = {"estimated_project_cost": estimated_cost}

    # ── Promoter entity details ───────────────────────────────────────────
    promoters_det: dict | None = None
    pdet: dict = {}
    if pan_no:
        pdet["pan_no"] = pan_no
    if firm_reg_no:
        pdet["registration_no"] = firm_reg_no
    if pdet:
        promoters_det = pdet

    return {
        "project_name":               project_name or None,
        "project_type":               project_type or None,
        "project_registration_no":    reg_no_text or None,
        "promoter_name":              promoter_name or None,
        "actual_commencement_date":   permit_from or None,
        "estimated_finish_date":      permit_to or None,
        "project_location_raw":       loc_raw or None,
        "promoter_contact_details":   contact,
        "promoters_details":          promoters_det,
        "bank_details":               bank_details,
        "co_promoter_details":        co_promoters or None,
        "professional_information":   professionals,
        "building_details":           building_details,
        "land_detail":                land_detail,
        "project_cost_detail":        project_cost,
        "uploaded_documents":         docs or None,
        "data": {
            "govt_type": "state",
            "is_processed": False,
        },
    }


# ── Sentinel ──────────────────────────────────────────────────────────────────

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Jharkhand RERA.
    Loads state_projects_sample/jharkhand.json as the baseline, re-scrapes the
    sentinel project's detail page, and verifies ≥ 80% field coverage.
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
        "state_projects_sample", "jharkhand.json",
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
        resp = safe_get(detail_url, retries=2, logger=logger)
        if not resp:
            logger.error("Sentinel: failed to fetch detail page", url=detail_url, step="sentinel")
            return False
        fresh = _parse_detail_page(resp.text, detail_url) or {}
    except Exception as exc:
        logger.error(f"Sentinel: scrape error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", url=detail_url, step="sentinel")
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "jharkhand_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


# ── Document processing ───────────────────────────────────────────────────────

def _process_documents(
    project_key: str,
    documents: list[dict],
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
) -> tuple[list[dict], int]:
    """Download, MD5-check, and upload each document to S3."""
    enriched: list[dict] = []
    upload_count = 0
    doc_name_counts: dict[str, int] = {}

    for doc in documents:
        selected = select_document_for_download("jharkhand", doc, doc_name_counts, domain=DOMAIN)
        if not selected:
            enriched.append(doc)
            continue

        url = selected.get("link") or selected.get("url") or ""
        doc_type = selected.get("type", "document")
        if not url or url.endswith("/FirstLevel/ViewDocument"):
            # No real document ID — record as-is
            enriched.append(selected)
            continue

        reused, existing_s3_key = existing_uploaded_document_entry(
            project_key, {**selected, "link": url}
        )
        if reused:
            logger.info(f"Document reused: {doc_type!r}", s3_key=existing_s3_key, step="documents")
            logger.log_document(doc_type, url, "reused", s3_key=existing_s3_key)
            enriched.append(reused)
            continue

        filename = build_document_filename(selected)

        try:
            resp = download_response(url, logger=logger, timeout=60.0)
            if not resp or len(resp.content) < 100:
                enriched.append(selected)
                logger.warning(f"Document download failed or too small: {url}", step="documents")
                continue

            data = resp.content
            md5 = compute_md5(data)
            s3_key = upload_document(project_key, filename, data, dry_run=settings.DRY_RUN_S3)
            if s3_key is None:
                enriched.append(selected)
                continue

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
            enriched.append({**selected, "link": url, "s3_link": s3_url, "updated": True})
            upload_count += 1
            logger.info(f"Document uploaded: {doc_type!r}", s3_key=s3_key, step="documents")
            logger.log_document(doc_type, url, "uploaded", s3_key=s3_key, file_size_bytes=len(data))

        except Exception as exc:
            enriched.append(selected)
            logger.error(f"Document error: {exc}", url=url, step="documents")
            insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(exc),
                               url=url, project_key=project_key)

    return enriched, upload_count


# ── Main entry point ──────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:  # noqa: C901
    logger = CrawlerLogger(config["id"], run_id)
    counters = dict(projects_found=0, projects_new=0, projects_updated=0,
                    projects_skipped=0, documents_uploaded=0, error_count=0)
    machine_name, machine_ip = get_machine_context()
    t_run = time.monotonic()

    # ── Sentinel health check ────────────────────────────────────────────────
    t0 = time.monotonic()
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counters["error_count"] += 1
        return counters
    logger.warning(f"Step timing [sentinel]: {time.monotonic()-t0:.2f}s", step="timing")

    item_limit    = settings.CRAWL_ITEM_LIMIT or 0
    items_processed = 0
    delay_range   = config.get("rate_limit_delay", (2, 4))

    current_page = 1
    max_pages    = settings.MAX_PAGES
    stop_all     = False
    t0 = time.monotonic()
    first_page_logged = False

    while not stop_all:
        page_url = LISTING_URL if current_page == 1 else f"{LISTING_URL}?page={current_page}"
        resp = safe_get(page_url, retries=config.get("max_retries", 3), logger=logger)
        if not resp:
            logger.error(f"Failed to fetch listing page {current_page}", step="listing")
            insert_crawl_error(run_id, config["id"], "HTTP_ERROR",
                               f"listing page {current_page} unreachable", url=page_url)
            counters["error_count"] += 1
            break

        soup = BeautifulSoup(resp.text, "lxml")
        rows = _parse_listing_rows(soup)
        counters["projects_found"] += len(rows)
        logger.info(f"Page {current_page}: {len(rows)} projects", step="listing")
        if not first_page_logged:
            logger.warning(f"Step timing [search]: {time.monotonic()-t0:.2f}s  rows={len(rows)}", step="timing")
            first_page_logged = True

        if not rows:
            logger.warning(f"No rows on page {current_page} — stopping", step="listing")
            break

        for raw in rows:
            if item_limit and items_processed >= item_limit:
                logger.info(f"Item limit {item_limit} reached — stopping", step="listing")
                stop_all = True
                break

            reg_no = raw.get("project_registration_no", "").strip()
            if not reg_no:
                counters["error_count"] += 1
                continue

            key        = generate_project_key(reg_no)
            detail_url = raw.get("detail_url", "") or LISTING_URL

            # ── daily_light: skip projects already in the DB ──────────────────
            if mode == "daily_light" and get_project_by_key(key):
                counters["projects_skipped"] += 1
                continue

            logger.set_project(key=key, reg_no=reg_no, url=detail_url, page=current_page)
            try:
                # ── Fetch & parse detail page ─────────────────────────────────
                detail_extra: dict = {}
                if raw.get("detail_url"):
                    detail_resp = safe_get(raw["detail_url"],
                                           retries=config.get("max_retries", 3), logger=logger)
                    if detail_resp:
                        try:
                            detail_extra = _parse_detail_page(detail_resp.text, raw["detail_url"])
                            logger.info(f"Detail parsed: {reg_no!r}", step="detail")
                        except Exception as exc:
                            logger.error(f"Detail parse error for {reg_no}: {exc}", step="detail")
                            insert_crawl_error(run_id, config["id"], "EXTRACTION_FAILED", str(exc),
                                               project_key=key, url=raw["detail_url"])
                            counters["error_count"] += 1
                    else:
                        logger.warning(f"Detail fetch failed: {reg_no!r}", step="detail")

                # ── Merge listing + detail ────────────────────────────────────
                try:
                    merged: dict = {
                        **detail_extra,
                        # Listing fields always win for core identity
                        "project_name":            raw["project_name"] or detail_extra.get("project_name"),
                        "project_registration_no": reg_no,
                        "project_location_raw": {
                            **{"raw_address": raw.get("address", "").lower()},
                            **(detail_extra.get("project_location_raw") or {}),
                        },
                        "domain": DOMAIN,
                        "url":    detail_url,
                        "state":  config.get("state", "jharkhand"),
                        "is_live": True,
                        "data": merge_data_sections(
                            detail_extra.get("data"),
                            {"listing_address": raw.get("address", "")},
                        ),
                    }
                    merged = {k: v for k, v in merged.items() if v is not None}

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
                        logger.info(f"New project: {reg_no}", step="upsert")
                    elif status == "updated":
                        counters["projects_updated"] += 1
                        logger.info(f"Updated: {reg_no}", step="upsert")
                    else:
                        counters["projects_skipped"] += 1

                    # ── Process documents ─────────────────────────────────────
                    uploaded_docs = detail_extra.get("uploaded_documents") or []
                    if uploaded_docs:
                        enriched_docs, doc_count = _process_documents(
                            key, uploaded_docs, run_id, config["id"], logger,
                        )
                        counters["documents_uploaded"] += doc_count
                        upsert_project({
                            "key": key,
                            "url": db_dict["url"],
                            "state": db_dict["state"],
                            "domain": db_dict["domain"],
                            "project_registration_no": db_dict["project_registration_no"],
                            "uploaded_documents": enriched_docs,
                            "document_urls": build_document_urls(enriched_docs),
                        })

                except ValidationError as exc:
                    counters["error_count"] += 1
                    logger.error(f"Validation error {reg_no}: {exc}", step="validate")
                    insert_crawl_error(run_id, config["id"], "VALIDATION_FAILED", str(exc),
                                       project_key=key, url=detail_url)
                except Exception as exc:
                    counters["error_count"] += 1
                    logger.error(f"Unexpected error {reg_no}: {exc}", step="upsert")
                    insert_crawl_error(run_id, config["id"], "CRAWLER_EXCEPTION", str(exc),
                                       project_key=key, url=detail_url)
            finally:
                logger.clear_project()

            random_delay(*delay_range)

        # ── Advance pagination ────────────────────────────────────────────
        if max_pages and current_page >= max_pages:
            logger.info(f"Reached max_pages={max_pages}, stopping", step="listing")
            break
        if stop_all:
            break
        if not _has_next_page(soup, current_page):
            logger.info("No more pages", step="listing")
            break
        current_page += 1
        random_delay(*delay_range)

    reset_checkpoint(config["id"], mode)
    logger.info(f"Jharkhand RERA complete: {counters}", step="done")
    logger.warning(f"Step timing [total_run]: {time.monotonic()-t_run:.2f}s", step="timing")
    return counters
