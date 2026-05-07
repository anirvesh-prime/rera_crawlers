"""
Tripura RERA Crawler — reraonline.tripura.gov.in
Type: static (httpx + BeautifulSoup — server-rendered Java MVC)

Strategy:
- GET /viewApprovedProjects returns ALL ~214 registered projects in a single HTML
  response.  The server renders a client-side-only pagination widget whose links
  are wrapped in an HTML comment (<!-- ... -->), so BeautifulSoup never sees them
  as active anchors; _has_next_page() returns False after the first page and the
  loop exits cleanly.
- Each listing card (div.row.defalter_result_list): project name, address,
  registration number, promoter, promoter type, property type, land area.
  Cards contain NO explicit detail-page anchor (<a href="/viewProjectDetailPage">)
  so the detail URL is always derived from the registration number.
- Registration number format: PRTR{MM}{YY}{NNNN} (8 digits after PRTR) where
  NNNN is the 4-digit zero-padded sequential project ID.
  detail URL = /viewProjectDetailPage?projectID=int(reg_no[-4:])
  e.g. PRTR03240386 → projectID=386  |  PRTR01200001 → projectID=1
- Detail pages: /viewProjectDetailPage?projectID=N — full project metadata
- Documents: links matching getdocument/download/.pdf patterns on detail page
"""
from __future__ import annotations

import re

from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, reset_checkpoint, save_checkpoint
from core.crawler_base import generate_project_key, random_delay, safe_get
from core.db import get_project_by_key, insert_crawl_error, upsert_document, upsert_project
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
)
from core.s3 import compute_md5, get_s3_url, upload_document
from core.config import settings

LISTING_URL = "https://reraonline.tripura.gov.in/viewApprovedProjects"
BASE_URL    = "https://reraonline.tripura.gov.in"
DOMAIN      = "reraonline.tripura.gov.in"
PAGE_SIZE   = 10  # kept for checkpoint arithmetic; server returns all rows in one page

# Registration numbers: PRTR{MM}{YY}{NNNN} — last 4 chars are the numeric project ID
_REG_RE      = re.compile(r"\bPRTR\d+\b", re.I)
_AREA_UNIT_RE = re.compile(
    r"([\d,]+(?:\.\d+)?)\s*(sq\.?\s*mtr|sq\.?\s*ft|sq\.?\s*m|sqmt|sqmtr|sqft|m2|ft2)?",
    re.I,
)


def _get(url: str, logger: CrawlerLogger, params: dict | None = None):
    """Thin wrapper — SSL verification disabled for legacy government cert."""
    return safe_get(url, verify=False, logger=logger, timeout=60.0, params=params)


# ── Listing page parsing ───────────────────────────────────────────────────────

def _parse_listing_rows(soup: BeautifulSoup) -> list[dict]:
    """
    Extract project rows from the approved-projects listing table.
    Expected columns (order may vary):
      S.No | Registration No. | Project Name | Promoter Name | Project Type | Status | View
    """
    rows = []
    cards = soup.select("div.row.defalter_result_list")
    if cards:
        for card in cards:
            heading = card.find("h1")
            project_name = None
            if heading:
                heading_text = heading.get_text(" ", strip=True)
                project_name = heading_text.split("Project:")[-1].strip()

            paragraphs = card.find_all("p")
            address = None
            reg_no = None
            for p in paragraphs:
                text = p.get_text(" ", strip=True)
                if "Reg No." in text:
                    match = _REG_RE.search(text)
                    if match:
                        reg_no = match.group(0)
                elif address is None and text:
                    address = text.replace("glyphicon-map-marker", "").strip()

            if not reg_no:
                continue

            summary_row = {}
            table = card.find("table")
            if table:
                data_rows = table.find_all("tr")
                if len(data_rows) >= 2:
                    headers = [th.get_text(" ", strip=True).lower() for th in data_rows[0].find_all(["th", "td"])]
                    values = [td.get_text(" ", strip=True) for td in data_rows[1].find_all("td")]
                    summary_row = dict(zip(headers, values))

            land_area = None
            land_area_unit_from_listing: str | None = None
            land_text = summary_row.get("total area of land (sq.mtr.)", "")
            if land_text:
                try:
                    land_area = float(land_text.replace(",", ""))
                    # The listing column header explicitly says "(Sq.Mtr.)" — use it as
                    # the authoritative unit instead of trusting the detail-page text,
                    # which can contain promoter data-entry errors (e.g. "sq ft").
                    land_area_unit_from_listing = "sq Mtr"
                except ValueError:
                    land_area = None

            # Find detail page URL from an explicit link in the card
            detail_url = None
            for a in card.find_all("a", href=True):
                href = a["href"]
                if re.search(r"viewProjectDetailPage|viewProject|projectDetail", href, re.I):
                    if href.startswith("http"):
                        detail_url = href
                    elif href.startswith("/"):
                        detail_url = f"{BASE_URL}{href}"
                    else:
                        detail_url = f"{BASE_URL}/{href}"
                    break
            # Fallback: derive detail URL from registration number.
            # Format: PRTR{MM}{YY}{NNNN} — last 4 chars are the zero-padded project ID.
            # int() strips leading zeros: "0386" → 386, "0001" → 1.
            if not detail_url and reg_no and len(reg_no) >= 4:
                try:
                    project_id = int(reg_no[-4:])
                    detail_url = f"{BASE_URL}/viewProjectDetailPage?projectID={project_id}"
                except ValueError:
                    pass

            row = {
                "project_registration_no": reg_no,
                "project_name": project_name,
                "promoter_name": (
                    summary_row.get("promoter")
                    or summary_row.get("promoter name")
                    or summary_row.get("name of promoter")
                    or summary_row.get("name of the promoter")
                ),
                "project_type": summary_row.get("property type"),
                "detail_url": detail_url,
            }
            if address:
                row["project_location_raw"] = {"raw_address": address}
            if land_area is not None:
                row["land_area"] = land_area
            if land_area_unit_from_listing:
                row["land_area_unit"] = land_area_unit_from_listing
            if summary_row.get("promoter type"):
                row["data"] = {"promoter_type": summary_row.get("promoter type")}
            rows.append(row)
        return rows

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            # Locate the cell that holds a PRTR-format registration number
            reg_no, reg_idx = None, None
            for idx, td in enumerate(tds):
                text = td.get_text(strip=True)
                if _REG_RE.match(text):
                    reg_no, reg_idx = text, idx
                    break
            if not reg_no:
                continue

            def _cell(offset: int) -> str | None:
                i = reg_idx + offset
                return tds[i].get_text(separator=" ", strip=True) or None if 0 <= i < len(tds) else None

            project_name  = _cell(1)
            promoter_name = _cell(2)
            project_type  = _cell(3)
            status        = _cell(4)

            # Prefer an explicit anchor link; fall back to deriving from reg digits
            detail_url = None
            for td in tds:
                a = td.find("a", href=re.compile(r"viewProjectDetailPage", re.I))
                if a:
                    href = a["href"]
                    if href.startswith("http"):
                        detail_url = href
                    elif href.startswith("/"):
                        detail_url = f"{BASE_URL}{href}"
                    else:
                        detail_url = f"{BASE_URL}/{href}"
                    break
            # Fallback: PRTR{MM}{YY}{NNNN} — last 4 chars are the zero-padded project ID.
            if not detail_url and reg_no and len(reg_no) >= 4:
                try:
                    project_id = int(reg_no[-4:])
                    detail_url = f"{BASE_URL}/viewProjectDetailPage?projectID={project_id}"
                except ValueError:
                    pass

            rows.append({
                "project_registration_no": reg_no,
                "project_name":            project_name,
                "promoter_name":           promoter_name,
                "project_type":            project_type,
                "status_of_the_project":   status,
                "detail_url":              detail_url,
            })
    return rows


def _has_next_page(soup: BeautifulSoup, current_start: int) -> bool:
    """Return True if pagination links indicate more pages exist."""
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        href = a["href"]
        if text in ("next", "»", ">") or "next" in text:
            return True
        m = re.search(r"startFrom=(\d+)", href)
        if m and int(m.group(1)) > current_start:
            return True
        m = re.search(r"[?&]page=(\d+)", href)
        if m and int(m.group(1)) > (current_start // PAGE_SIZE) + 1:
            return True
    return False



# ── Detail page parsing ────────────────────────────────────────────────────────

_DETAIL_LABEL_MAP: dict[str, str] = {
    "registration number":          "project_registration_no",
    "registration no":              "project_registration_no",
    "project name":                 "project_name",
    "name of the project":          "project_name",
    "promoter name":                "promoter_name",
    "name of promoter":             "promoter_name",
    "name of the promoter":         "promoter_name",
    "name of promoter firm":        "promoter_name",
    "project type":                 "project_type",
    "type of project":              "project_type",
    "status":                       "status_of_the_project",
    "project status":               "status_of_the_project",
    "status of the project":        "status_of_the_project",
    "date of registration":         "submitted_date",
    "registration date":            "submitted_date",
    "date of submission":           "submitted_date",
    "start date":                   "estimated_commencement_date",
    "project start date":           "estimated_commencement_date",
    "commencement date":            "estimated_commencement_date",
    "end date":                     "estimated_finish_date",
    "project end date":             "estimated_finish_date",
    "completion date":              "estimated_finish_date",
    "proposed completion date":     "estimated_finish_date",
    "land area":                    "_land_area_raw",
    "total land area":              "_land_area_raw",
    "total area of project land":   "_land_area_raw",
    "construction area":            "_construction_area_raw",
    "total construction area":      "_construction_area_raw",
    "carpet area":                  "_construction_area_raw",
    "area of project (all floors together) (sq mtr)": "_construction_area_raw",
    "no. of residential units":     "number_of_residential_units",
    "number of residential units":  "number_of_residential_units",
    "residential units":            "number_of_residential_units",
    "total number of flats":        "number_of_residential_units",
    "no. of commercial units":      "number_of_commercial_units",
    "number of commercial units":   "number_of_commercial_units",
    "commercial units":             "number_of_commercial_units",
    "project description":          "project_description",
    "description":                  "project_description",
    "district":                     "_district",
    "taluk":                        "_taluk",
    "tehsil/sub district":          "_taluk",
    "tehsil/sub  district":         "_taluk",
    "address":                      "_address",
    "project address":              "_address",
    "pin code":                     "project_pin_code",
    "pincode":                      "project_pin_code",
}


def _extract_area(raw: str) -> tuple[float | None, str | None]:
    """Return (numeric_value, unit_string) from a raw area text."""
    m = _AREA_UNIT_RE.search(raw)
    if not m:
        return None, None
    try:
        val = float(m.group(1).replace(",", ""))
    except ValueError:
        val = None
    return val, m.group(2)


def _process_label_value(
    label: str, value: str, out: dict, raw: dict, location_parts: dict,
) -> None:
    """Apply _DETAIL_LABEL_MAP and populate out / raw / location_parts."""
    if not label or not value:
        return
    raw[label] = value
    schema_field = _DETAIL_LABEL_MAP.get(label.lower().strip())
    if schema_field == "_land_area_raw":
        # First-wins: Pattern 1 (<tr>) runs before Pattern 2 (Bootstrap grid).
        # Don't let a later match for the same field overwrite the first value.
        if "land_area" not in out:
            val, unit = _extract_area(value)
            if val is not None:
                out["land_area"] = val
                raw["land_area_unit"] = unit or "sq Mtr"
    elif schema_field == "_construction_area_raw":
        if "construction_area" not in out:
            val, unit = _extract_area(value)
            if val is not None:
                out["construction_area"] = val
                raw["construction_area_unit"] = unit or "Sq Mtr"
    elif schema_field == "_district":
        location_parts["district"] = value
    elif schema_field == "_taluk":
        location_parts["taluk"] = value
    elif schema_field == "_address":
        # First-wins: preserve the first (typically fuller) address encountered.
        if "raw_address" not in location_parts:
            location_parts["raw_address"] = value
            raw["raw_address"] = value
    elif schema_field and schema_field not in out:
        out[schema_field] = value


def _parse_section_table(table) -> list[dict]:
    """Parse an HTML table into a list of row dicts keyed by header text."""
    ths = [th.get_text(strip=True) for th in table.find_all("th")]
    all_trs = table.find_all("tr")
    if not ths and all_trs:
        ths = [td.get_text(strip=True) for td in all_trs[0].find_all(["th", "td"])]
        data_trs = all_trs[1:]
    else:
        data_trs = all_trs
    result = []
    for tr in data_trs:
        tds = tr.find_all("td")
        if not tds:
            continue
        row = {(ths[i] if i < len(ths) else f"col_{i}"): td.get_text(separator=" ", strip=True)
               for i, td in enumerate(tds)
               if td.get_text(separator=" ", strip=True)}
        if row:
            result.append(row)
    return result


def _normalize_member(raw: dict) -> dict:
    out: dict = {}
    for k, v in raw.items():
        kl = k.lower()
        if "name" in kl:
            out.setdefault("name", v)
        elif "email" in kl or "e-mail" in kl:
            out.setdefault("email", v)
        elif any(x in kl for x in ("phone", "mobile", "contact")):
            out.setdefault("phone", v)
        elif any(x in kl for x in ("position", "designation", "role", "type")):
            out.setdefault("position", v)
    return {k: v for k, v in out.items() if v}


def _normalize_professional(raw: dict, role: str | None = None) -> dict:
    out: dict = {}
    if role:
        out["role"] = role
    for k, v in raw.items():
        kl = k.lower()
        if "name" in kl:
            out.setdefault("name", v)
        elif "email" in kl or "e-mail" in kl:
            out.setdefault("email", v)
        elif "address" in kl:
            out.setdefault("address", v)
        elif any(x in kl for x in ("role", "designation")) and "role" not in out:
            out["role"] = v
    return {k: v for k, v in out.items() if v}


def _parse_detail_page(url: str, logger: CrawlerLogger) -> dict:
    """Fetch and parse a Tripura project detail page."""
    resp = _get(url, logger)
    if not resp:
        return {}
    soup = BeautifulSoup(resp.text, "lxml")
    out: dict = {}
    raw: dict = {"source_url": url, "govt_type": "state"}
    location_parts: dict = {}

    # Pattern 1: <tr> rows with <th> label + <td> value pairs
    for tr in soup.find_all("tr"):
        ths = tr.find_all("th")
        tds = tr.find_all("td")
        if ths and tds:
            for th, td in zip(ths, tds):
                _process_label_value(
                    th.get_text(strip=True).rstrip(":").strip(),
                    td.get_text(separator=" ", strip=True),
                    out, raw, location_parts,
                )
        elif len(tds) == 2:
            label = tds[0].get_text(strip=True).rstrip(":").strip()
            value = tds[1].get_text(separator=" ", strip=True)
            if label and value and not re.match(r"^\d+$", label):
                _process_label_value(label, value, out, raw, location_parts)

    # Pattern 2: Bootstrap grid — text-right <p> label + adjacent value column
    # Skip any direct child that is itself a div.row — those are sub-rows and
    # will be visited in their own loop iteration; treating them as label/value
    # columns would pair entire rows against each other and corrupt field values.
    for row_div in soup.find_all("div", class_="row"):
        cols = row_div.find_all("div", recursive=False)
        i = 0
        while i < len(cols):
            col = cols[i]
            if "row" in col.get("class", []):
                i += 1
                continue
            p_label = col.find("p", class_=re.compile(r"text-right|label", re.I))
            if not p_label:
                p_label = col.find(["b", "strong"])
            if p_label and i + 1 < len(cols):
                _process_label_value(
                    p_label.get_text(strip=True).rstrip(":").strip(),
                    cols[i + 1].get_text(separator=" ", strip=True),
                    out, raw, location_parts,
                )
                i += 2
            else:
                i += 1

    if location_parts:
        location_parts["has_same_data"] = True
        out["project_location_raw"] = location_parts

    # ── Section tables (members, professionals) ───────────────────────────────
    members: list[dict] = []
    professionals: list[dict] = []
    doc_links: list[dict] = []
    seen_doc_urls: set[str] = set()
    used_tables: set[int] = set()

    def _is_member_table(ths_lower: list[str]) -> bool:
        has_name = any("name" in h for h in ths_lower)
        has_contact = any(any(x in h for x in ("email", "phone", "mobile")) for h in ths_lower)
        has_pos = any(any(x in h for x in ("position", "designation")) for h in ths_lower)
        return has_name and (has_contact or has_pos)

    def _is_professional_table(ths_lower: list[str]) -> bool:
        has_name = any("name" in h for h in ths_lower)
        has_role = any(any(x in h for x in ("role", "architect", "engineer")) for h in ths_lower)
        has_addr = any("address" in h for h in ths_lower)
        return has_name and (has_role or has_addr)

    # Capture project_description from <h1>Project Description</h1> + next <p>
    if not out.get("project_description"):
        for h1 in soup.find_all("h1"):
            if "description" in h1.get_text(strip=True).lower():
                p_sib = h1.find_next_sibling("p")
                if p_sib:
                    desc = p_sib.get_text(separator=" ", strip=True)
                    if desc:
                        out["project_description"] = desc
                break

    # Walk headings to classify nearby tables by section name
    # h1 is included because Tripura uses h1 for section headings like
    # "Project MemberDetails", "ProjectArchitects", "StructuralEngineers"
    for heading in soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
        htext = heading.get_text(strip=True).lower()
        sibling, table = heading.find_next_sibling(), None
        for _ in range(6):
            if sibling is None:
                break
            if sibling.name == "table":
                table = sibling
                break
            if sibling.name == "div":
                t = sibling.find("table")
                if t:
                    table = t
                    break
            sibling = sibling.find_next_sibling()
        if table is None or id(table) in used_tables:
            continue
        # Only mark a table used when a branch actually processes it, so unrecognised
        # headings (e.g. "Project Description") don't accidentally claim nearby tables.
        # "promoter" sections: extract promoter name + contact details
        if "promoter" in htext and not any(kw in htext for kw in ("member", "director")):
            used_tables.add(id(table))
            for r in _parse_section_table(table):
                # Extract promoter/company name from the table row.
                # Only match columns whose header contains "name" (e.g. "Name of
                # Promoter") and NOT contact/address-like headers.  Avoid matching
                # "Promoter Type" or similar by NOT checking for "promoter" here.
                if not out.get("promoter_name"):
                    for col_key, col_val in r.items():
                        kl = col_key.lower()
                        if (
                            "name" in kl
                            and not any(x in kl for x in ("email", "e-mail", "phone", "mobile", "contact", "address"))
                            and col_val
                        ):
                            out["promoter_name"] = col_val
                            break
                # Extract contact details
                if not out.get("promoter_contact_details"):
                    contact: dict = {}
                    for col_key, col_val in r.items():
                        kl = col_key.lower()
                        if ("email" in kl or "e-mail" in kl) and col_val:
                            contact["email"] = col_val
                        elif any(x in kl for x in ("mobile", "phone", "contact")) and col_val:
                            contact["phone"] = col_val
                    if contact.get("email") or contact.get("phone"):
                        out["promoter_contact_details"] = contact
        # "chairman" / "lead member" sections are organisational headings, not board members
        elif any(kw in htext for kw in ("member", "director", "partner")) and "chairman" not in htext:
            used_tables.add(id(table))
            for r in _parse_section_table(table):
                m = _normalize_member(r)
                if m.get("name"):
                    m["has_same_data"] = True
                    members.append(m)
        elif "architect" in htext:
            used_tables.add(id(table))
            for r in _parse_section_table(table):
                p = _normalize_professional(r, role="Architects")
                if p.get("name"):
                    p["has_same_data"] = True
                    professionals.append(p)
        elif "structural" in htext and "engineer" in htext:
            used_tables.add(id(table))
            for r in _parse_section_table(table):
                p = _normalize_professional(r, role="Structural Engineers")
                if p.get("name"):
                    p["has_same_data"] = True
                    professionals.append(p)
        elif "engineer" in htext:
            used_tables.add(id(table))
            for r in _parse_section_table(table):
                p = _normalize_professional(r, role="Engineers")
                if p.get("name"):
                    p["has_same_data"] = True
                    professionals.append(p)
        elif any(kw in htext for kw in ("professional", "consultant")):
            used_tables.add(id(table))
            for r in _parse_section_table(table):
                p = _normalize_professional(r)
                if p.get("name"):
                    p["has_same_data"] = True
                    professionals.append(p)

    # Fallback: scan remaining tables by header-column signature
    for table in soup.find_all("table"):
        if id(table) in used_tables:
            continue
        ths = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if not ths:
            first_tr = table.find("tr")
            if first_tr:
                ths = [td.get_text(strip=True).lower()
                       for td in first_tr.find_all(["th", "td"])]
        if not ths:
            continue
        if not members and _is_member_table(ths):
            used_tables.add(id(table))
            for r in _parse_section_table(table):
                m = _normalize_member(r)
                if m.get("name"):
                    m["has_same_data"] = True
                    members.append(m)
        elif not professionals and _is_professional_table(ths):
            used_tables.add(id(table))
            for r in _parse_section_table(table):
                p = _normalize_professional(r)
                if p.get("name"):
                    p["has_same_data"] = True
                    professionals.append(p)

    # Document links
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"(getdocument|download|filedownload|viewdoc|\.pdf)", href, re.I):
            # Skip site-wide static documents that appear in the navigation bar
            # (e.g. /resources/staticpage/FAQ.pdf, /resources/staticpage/Form_B.pdf).
            # These are not project-specific documents.
            if "/resources/staticpage/" in href:
                continue
            # Skip broken document links where the DOC_ID parameter is empty.
            if "DOC_ID=" in href and not href.split("DOC_ID=")[-1].strip():
                continue
            if href.startswith("http"):
                full_url = href
            elif href.startswith("/"):
                full_url = f"{BASE_URL}{href}"
            else:
                full_url = f"{BASE_URL}/{href}"
            if full_url not in seen_doc_urls:
                seen_doc_urls.add(full_url)
                doc_links.append({"label": a.get_text(strip=True) or "document", "url": full_url})

    project_images: list[str] = []
    seen_img_urls: set[str] = set()
    for img in soup.find_all("img", src=True):
        src = img["src"].strip()
        if not src:
            continue
        lower = src.lower()
        if any(token in lower for token in (
            "logo", "captcha", "icon", "banner",
            # Social-media sharing buttons that appear in the site navigation bar
            "face-book", "facebook", "linkedin", "twitter",
            "youtube", "instagram", "social",
        )):
            continue
        if src.startswith("http"):
            full_src = src
        elif src.startswith("/"):
            full_src = f"{BASE_URL}{src}"
        else:
            full_src = f"{BASE_URL}/{src}"
        if full_src in seen_img_urls:
            continue
        seen_img_urls.add(full_src)
        project_images.append(full_src)

    if members:
        out["members_details"] = members
        if not out.get("promoter_contact_details"):
            first = members[0]
            contact = {k: first[k] for k in ("email", "phone") if first.get(k)}
            if contact:
                out["promoter_contact_details"] = contact
    if professionals:
        out["professional_information"] = professionals
    if project_images:
        out["project_images"] = project_images
    out["_doc_links"] = doc_links
    out["data"] = raw
    return out



# ── Document handling ─────────────────────────────────────────────────────────

def _handle_document(
    project_key: str, doc: dict, run_id: int, site_id: str, logger: CrawlerLogger,
) -> dict | None:
    url   = doc["url"]
    label = doc["label"]
    fname = build_document_filename(doc)
    try:
        resp = _get(url, logger)
        if not resp or len(resp.content) < 100:
            return None
        md5    = compute_md5(resp.content)
        s3_key = upload_document(project_key, fname, resp.content, dry_run=settings.DRY_RUN_S3)
        if s3_key is None:
            return None
        s3_url = get_s3_url(s3_key)
        upsert_document(
            project_key=project_key,
            document_type=label,
            original_url=document_identity_url(doc) or url,
            s3_key=s3_key,
            s3_bucket=settings.S3_BUCKET_NAME,
            file_name=fname,
            md5_checksum=md5,
            file_size_bytes=len(resp.content),
        )
        logger.info("Document handled", label=label, s3_key=s3_key, step="documents")
        logger.log_document(label, url, "uploaded", s3_key=s3_key, file_size_bytes=len(resp.content))
        return document_result_entry(doc, s3_url, fname)
    except Exception as e:
        logger.error(f"Doc failed for {project_key}: {e}")
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(e),
                           project_key=project_key, url=url)
        return None


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Tripura RERA.
    Loads state_projects_sample/tripura.json as the baseline, re-scrapes the
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
        "state_projects_sample", "tripura.json",
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
        fresh = _parse_detail_page(detail_url, logger) or {}
    except Exception as exc:
        logger.error(f"Sentinel: scrape error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", url=detail_url, step="sentinel")
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "tripura_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


# ── Main run() ────────────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    """
    Args:
        config:  site dict from sites_config.SITES
        run_id:  crawl_runs.id for this run
        mode:    'daily_light' | 'weekly_deep'
    Returns:
        dict with keys: projects_found, projects_new, projects_updated,
                        projects_skipped, documents_uploaded, error_count
    """
    logger   = CrawlerLogger(config["id"], run_id)
    site_id  = config["id"]
    counts   = dict(projects_found=0, projects_new=0, projects_updated=0,
                    projects_skipped=0, documents_uploaded=0, error_count=0)

    checkpoint = load_checkpoint(site_id, mode) or {}
    done_regs: set[str] = set(checkpoint.get("done_regs", []))
    start_page = checkpoint.get("last_page", 0)
    item_limit = settings.CRAWL_ITEM_LIMIT or 0
    max_pages  = settings.MAX_PAGES or 0
    machine_name, machine_ip = get_machine_context()

    # ── Sentinel health check ────────────────────────────────────────────────
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counts["error_count"] += 1
        return counts

    page = start_page
    consecutive_empty = 0
    items_processed = 0

    while True:
        if max_pages and page >= max_pages:
            logger.info(f"Reached MAX_PAGES={max_pages}, stopping pagination")
            break

        start_from = page * PAGE_SIZE
        logger.info(f"Fetching listing page {page + 1} (startFrom={start_from})")

        resp = _get(LISTING_URL, logger, params={"startFrom": start_from})
        if not resp:
            logger.error("Failed to fetch listing page", page=page + 1)
            insert_crawl_error(run_id, site_id, "HTTP_ERROR",
                               f"Listing page {page + 1} failed", url=LISTING_URL)
            counts["error_count"] += 1
            break

        soup = BeautifulSoup(resp.text, "lxml")
        rows = _parse_listing_rows(soup)

        # On the first page, also try page=1 style if startFrom=0 yields nothing
        if not rows and page == 0:
            resp2 = _get(LISTING_URL, logger, params={"page": 1})
            if resp2:
                soup2 = BeautifulSoup(resp2.text, "lxml")
                rows2 = _parse_listing_rows(soup2)
                if rows2:
                    rows, soup = rows2, soup2

        if not rows:
            consecutive_empty += 1
            if consecutive_empty >= 2:
                logger.info("No rows found on two consecutive pages — pagination complete")
                break
            page += 1
            continue

        consecutive_empty = 0
        logger.info(f"Page {page + 1}: found {len(rows)} project rows")
        counts["projects_found"] += len(rows)

        for row in rows:
            if item_limit and items_processed >= item_limit:
                logger.info(f"CRAWL_ITEM_LIMIT={item_limit} reached, stopping")
                reset_checkpoint(site_id, mode)
                return counts

            reg_no = row["project_registration_no"]
            if reg_no in done_regs:
                counts["projects_skipped"] += 1
                continue

            key = generate_project_key(reg_no)
            logger.set_project(key=key, reg_no=reg_no, url=row.get("detail_url", LISTING_URL))

            try:
                if mode == "daily_light" and get_project_by_key(key):
                    counts["projects_skipped"] += 1
                    logger.info("Skipping — already in DB (daily_light)")
                    done_regs.add(reg_no)
                    logger.clear_project()
                    continue

                data: dict = {
                    "key":                     key,
                    "state":                   config["state"],
                    "project_state":           "Tripura",
                    "project_registration_no": reg_no,
                    "project_name":            row.get("project_name"),
                    "promoter_name":           row.get("promoter_name"),
                    "project_type":            row.get("project_type"),
                    "status_of_the_project":   row.get("status_of_the_project"),
                    "domain":                  DOMAIN,
                    "config_id":               config["config_id"],
                    "url":                     row.get("detail_url") or LISTING_URL,
                    "is_live":                 True,
                    "machine_name":            machine_name,
                    "crawl_machine_ip":        machine_ip,
                    "promoters_details": (
                        {"name": row["promoter_name"]} if row.get("promoter_name") else None
                    ),
                }
                if row.get("project_location_raw"):
                    data["project_location_raw"] = row["project_location_raw"]
                if row.get("land_area") is not None:
                    data["land_area"] = row["land_area"]

                doc_links: list[dict] = []
                if row.get("detail_url") and settings.SCRAPE_DETAILS:
                    random_delay(*config.get("rate_limit_delay", (1, 3)))
                    logger.info("Fetching detail page", step="detail_fetch")
                    detail = _parse_detail_page(row["detail_url"], logger)
                    doc_links   = detail.pop("_doc_links", [])
                    detail_data = detail.pop("data", {})

                    for k, v in detail.items():
                        # Never overwrite the canonical registration number
                        if v is not None and k != "project_registration_no":
                            data[k] = v

                    # If promoter_name was only found on the detail page,
                    # backfill promoters_details which was built from the listing row.
                    if data.get("promoter_name") and not data.get("promoters_details"):
                        data["promoters_details"] = {"name": data["promoter_name"]}

                    la = data.get("land_area")
                    ca = data.get("construction_area")
                    if la or ca:
                        # Prefer the listing-level unit (derived from the column header
                        # "Total Area of Land (Sq.Mtr.)") over the detail-page text,
                        # which can contain promoter data-entry errors (e.g. "sq ft").
                        la_unit = (
                            row.get("land_area_unit")
                            or detail_data.get("land_area_unit")
                            or "sq Mtr"
                        )
                        data["land_area_details"] = {
                            k: v for k, v in {
                                "land_area":              str(la) if la else None,
                                "land_area_unit":         la_unit,
                                "construction_area":      str(ca) if ca else None,
                                "construction_area_unit": detail_data.get("construction_area_unit", "Sq Mtr"),
                            }.items() if v
                        }

                    merged_data = merge_data_sections(
                        {"listing_row": {k: v for k, v in row.items() if k != "detail_url"}},
                        detail_data,
                    )
                    # Override land_area_unit in the data blob with the authoritative
                    # listing-column value (always "sq Mtr") when it was captured, so
                    # that detail-page data-entry errors (e.g. "sq ft") don't propagate.
                    if row.get("land_area_unit") and isinstance(merged_data, dict):
                        merged_data["land_area_unit"] = row["land_area_unit"]
                    data["data"] = merged_data
                else:
                    data["data"] = merge_data_sections(
                        row.get("data"),
                        {"listing_row": {k: v for k, v in row.items() if k not in {"detail_url", "data"}}},
                    )

                logger.info("Normalizing and validating", step="normalize")
                try:
                    normalized = normalize_project_payload(
                        data, config, machine_name=machine_name, machine_ip=machine_ip,
                    )
                    record  = ProjectRecord(**normalized)
                    db_dict = record.to_db_dict()
                except (ValidationError, ValueError) as e:
                    logger.warning("Validation failed — using raw fallback", error=str(e))
                    insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(e),
                                       project_key=key, url=data.get("url"), raw_data=data)
                    counts["error_count"] += 1
                    db_dict = normalize_project_payload(
                        {**data, "data": merge_data_sections(
                            data.get("data"), {"validation_fallback": True}
                        )},
                        config, machine_name=machine_name, machine_ip=machine_ip,
                    )

                logger.info("Upserting to DB", step="db_upsert")
                action = upsert_project(db_dict)
                items_processed += 1
                if action == "new":         counts["projects_new"] += 1
                elif action == "updated":   counts["projects_updated"] += 1
                else:                       counts["projects_skipped"] += 1
                logger.info(f"DB result: {action}", step="db_upsert")

                logger.info(f"Downloading {len(doc_links)} documents", step="documents")
                uploaded_documents: list[dict] = []
                doc_name_counts: dict[str, int] = {}
                for doc in doc_links:
                    selected = select_document_for_download(
                        config["state"], doc, doc_name_counts, domain=DOMAIN,
                    )
                    if selected:
                        uploaded = _handle_document(
                            db_dict["key"], selected, run_id, site_id, logger,
                        )
                        if uploaded:
                            uploaded_documents.append(uploaded)
                            counts["documents_uploaded"] += 1
                        else:
                            uploaded_documents.append({
                                "link": doc.get("url"),
                                "type": doc.get("label", "document"),
                            })
                    else:
                        uploaded_documents.append({
                            "link": doc.get("url"),
                            "type": doc.get("label", "document"),
                        })

                if uploaded_documents:
                    upsert_project({
                        "key":                     db_dict["key"],
                        "url":                     db_dict["url"],
                        "state":                   db_dict["state"],
                        "domain":                  db_dict["domain"],
                        "project_registration_no": db_dict["project_registration_no"],
                        "uploaded_documents":      uploaded_documents,
                        "document_urls":           build_document_urls(uploaded_documents),
                    })

                done_regs.add(reg_no)

            except Exception as exc:
                logger.exception("Project processing failed", exc, step="project_loop")
                insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                                   project_key=key, url=row.get("detail_url"))
                counts["error_count"] += 1
            finally:
                logger.clear_project()

        save_checkpoint(site_id, mode, page, None, run_id)

        if not _has_next_page(soup, start_from):
            logger.info("No next-page indicator found — pagination complete")
            break

        page += 1
        random_delay(*config.get("rate_limit_delay", (1, 3)))

    reset_checkpoint(site_id, mode)
    logger.info(f"Tripura RERA complete: {counts}")
    return counts
