"""
Tripura RERA Crawler — reraonline.tripura.gov.in
Type: static (httpx + BeautifulSoup — server-rendered Java MVC)

Strategy:
- A persistent httpx.Client is created per run so the JSESSIONID cookie acquired
  from the homepage warm-up is automatically re-sent with every subsequent request.

Primary listing — POST /search
  Submitting the homepage search form (searchTxt='', startFrom=N) returns 5
  projects per page with DIRECT "Read More" links to viewProjectDetailPage.
  Pagination: startFrom increments by 5; stop when startFrom >= total_records.
  Card selector: div.row.search_result_list > div.col-md-9.no_pad_lft

Supplementary listing — GET /viewApprovedProjects
  Returns ALL ~213 approved projects (including older completed ones not shown by
  /search). Cards here carry NO explicit detail link, so the detail URL is derived:
    PRTR{MM}{YY}{NNNN} → projectID = int(reg_no[-4:])
    e.g. PRTR03240386 → projectID=386  |  PRTR01200001 → projectID=1
  Projects already collected from /search are skipped.

Detail pages — /viewProjectDetailPage?projectID=N
  Full project metadata extracted via Bootstrap grid label/value pairs and
  section tables (Promoter, Members, Architects, Structural Engineers, Contractors).

Documents — download?DOC_ID=N links on the detail page.
  Anchors with empty DOC_ID are silently skipped (not uploaded by promoter).
"""
from __future__ import annotations

import re
import time

import httpx
from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, reset_checkpoint, save_checkpoint
from core.crawler_base import download_response, generate_project_key, get_random_ua, random_delay, safe_get, safe_post
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

SEARCH_URL  = "https://reraonline.tripura.gov.in/search"
LISTING_URL = "https://reraonline.tripura.gov.in/viewApprovedProjects"
HOME_URL    = "https://reraonline.tripura.gov.in/"
BASE_URL    = "https://reraonline.tripura.gov.in"
DOMAIN      = "reraonline.tripura.gov.in"
SEARCH_PAGE_SIZE = 5   # /search returns 5 results per page

# Registration numbers: PRTR{MM}{YY}{NNNN} — last 4 chars are the numeric project ID
_REG_RE      = re.compile(r"\bPRTR\d+\b", re.I)
_AREA_UNIT_RE = re.compile(
    r"([\d,]+(?:\.\d+)?)\s*(sq\.?\s*mtr|sq\.?\s*ft|sq\.?\s*m|sqmt|sqmtr|sqft|m2|ft2)?",
    re.I,
)
# Headings that are NOT the project name (used to skip them when extracting project name from h1)
_SKIP_H1_RE = re.compile(
    r"(last\s*updated|chairman|lead\s*member|applicant|description|"
    r"promoter|member|agent|architect|engineer|contractor|development|"
    r"construction|document|financial|declaration|photograph|associated|"
    r"vendor|rera\s*registration|project\s*type|project\s*status)",
    re.I,
)


def _make_client() -> httpx.Client:
    """Return a session-aware httpx.Client with SSL verification disabled."""
    return httpx.Client(
        verify=False,
        follow_redirects=True,
        timeout=60.0,
        headers={
            "User-Agent": get_random_ua(),
            "Origin":  BASE_URL,
            "Referer": HOME_URL,
        },
    )


def _get(url: str, logger: CrawlerLogger, client: httpx.Client,
         params: dict | None = None):
    """GET wrapper — reuses persistent session client."""
    return safe_get(url, verify=False, logger=logger, timeout=60.0,
                    params=params, client=client)


def _post(url: str, data: dict, logger: CrawlerLogger, client: httpx.Client):
    """POST wrapper — reuses persistent session client."""
    return safe_post(url, data=data, verify=False, logger=logger,
                     timeout=60.0, client=client)


# ── Search listing parsing (primary — POST /search) ───────────────────────────

def _parse_search_rows(soup: BeautifulSoup) -> tuple[list[dict], int]:
    """
    Parse one page of POST /search results.

    Card structure (div.row.search_result_list > div.col-md-9.no_pad_lft):
      <h1><span>Project: </span> {project_name}</h1>
      <p>{address} <span class="glyphicon-map-marker"/></p>
      <p>Reg No. : {PRTR…}</p>
      <table>
        <thead><tr><th>PROMOTER</th><th>PROMOTER TYPE</th>
                   <th>TOTAL AREA</th><th>PROPERTY TYPE</th><th>STATUS</th></tr></thead>
        <tbody><tr><td>…</td>…</tr></tbody>
      </table>
      <p class="pull-right"><a href="viewProjectDetailPage?projectID=N">Read More</a></p>

    Returns (rows, total_records).
    """
    # Extract total record count from "Showing record X to Y out of Z Records"
    total = 0
    m = re.search(r"out of.*?(\d+)\s*Records", soup.get_text())
    if m:
        total = int(m.group(1))

    rows: list[dict] = []
    cards = soup.select("div.row.search_result_list div.col-md-9.no_pad_lft")
    for card in cards:
        # Project name — strip the "Project:" span text
        h1 = card.find("h1")
        project_name = None
        if h1:
            project_name = h1.get_text(" ", strip=True)
            project_name = re.sub(r"^project\s*:\s*", "", project_name, flags=re.I).strip()
            project_name = project_name or None

        # Address and reg no — first two <p> tags
        address, reg_no = None, None
        for p in card.find_all("p"):
            text = p.get_text(" ", strip=True)
            m_reg = _REG_RE.search(text)
            if m_reg:
                reg_no = m_reg.group(0)
            elif address is None and "glyphicon" not in text and text:
                # Clean map-marker glyph text
                address = text.replace("glyphicon-map-marker", "").strip() or None

        if not reg_no:
            continue

        # Table: PROMOTER | PROMOTER TYPE | TOTAL AREA | PROPERTY TYPE | STATUS
        summary: dict[str, str] = {}
        table = card.find("table")
        if table:
            rows_tr = table.find_all("tr")
            if len(rows_tr) >= 2:
                headers = [th.get_text(" ", strip=True).lower()
                           for th in rows_tr[0].find_all(["th", "td"])]
                values  = [td.get_text(" ", strip=True)
                           for td in rows_tr[1].find_all("td")]
                summary = dict(zip(headers, values))

        # Land area — "TOTAL AREA" column e.g. "453.0 (sq Mtr)"
        land_area: float | None = None
        land_area_unit: str | None = None
        total_area_raw = summary.get("total area", "")
        if total_area_raw:
            val, unit = _extract_area(total_area_raw)
            if val is not None:
                land_area = val
                land_area_unit = unit or "sq Mtr"

        # Direct detail URL from "Read More" link
        detail_url: str | None = None
        link = card.find("a", href=re.compile(r"viewProjectDetailPage", re.I))
        if link:
            href = link["href"]
            detail_url = href if href.startswith("http") else f"{BASE_URL}/{href.lstrip('/')}"

        # Fallback: derive from reg number
        if not detail_url and reg_no and len(reg_no) >= 4:
            try:
                detail_url = f"{BASE_URL}/viewProjectDetailPage?projectID={int(reg_no[-4:])}"
            except ValueError:
                pass

        row: dict = {
            "project_registration_no": reg_no,
            "project_name":            project_name,
            "promoter_name":           (
                summary.get("promoter")
                or summary.get("promoter name")
                or summary.get("name of promoter")
            ) or None,
            "project_type":          summary.get("property type") or None,
            "status_of_the_project": summary.get("status") or None,
            "detail_url":            detail_url,
        }
        if address:
            row["project_location_raw"] = {"raw_address": address}
        if land_area is not None:
            row["land_area"] = land_area
        if land_area_unit:
            row["land_area_unit"] = land_area_unit
        if summary.get("promoter type"):
            row["data"] = {"promoter_type": summary["promoter type"]}
        rows.append(row)
    return rows, total


# ── Fallback listing parsing (GET /viewApprovedProjects) ──────────────────────

def _parse_listing_rows(soup: BeautifulSoup) -> list[dict]:
    """
    Extract project rows from GET /viewApprovedProjects (supplementary listing).
    The server returns ALL approved projects in one HTML response.
    Cards (div.row.defalter_result_list) carry NO explicit detail link;
    the detail URL is always derived from the registration number.
    """
    rows: list[dict] = []

    # Layout A: carousel-style cards (div.row.defalter_result_list)
    cards = soup.select("div.row.defalter_result_list")
    if cards:
        for card in cards:
            heading = card.find("h1")
            project_name = None
            if heading:
                heading_text = heading.get_text(" ", strip=True)
                project_name = heading_text.split("Project:")[-1].strip() or None

            address, reg_no = None, None
            for p in card.find_all("p"):
                text = p.get_text(" ", strip=True)
                if "Reg No." in text:
                    m = _REG_RE.search(text)
                    if m:
                        reg_no = m.group(0)
                elif address is None and text:
                    address = text.replace("glyphicon-map-marker", "").strip() or None

            if not reg_no:
                continue

            summary_row: dict[str, str] = {}
            table = card.find("table")
            if table:
                trs = table.find_all("tr")
                if len(trs) >= 2:
                    headers = [th.get_text(" ", strip=True).lower()
                                for th in trs[0].find_all(["th", "td"])]
                    values  = [td.get_text(" ", strip=True)
                                for td in trs[1].find_all("td")]
                    summary_row = dict(zip(headers, values))

            land_area: float | None = None
            land_area_unit: str | None = None
            land_text = summary_row.get("total area of land (sq.mtr.)", "")
            if land_text:
                try:
                    land_area = float(land_text.replace(",", ""))
                    land_area_unit = "sq Mtr"
                except ValueError:
                    pass

            detail_url = _derive_detail_url(reg_no)
            row: dict = {
                "project_registration_no": reg_no,
                "project_name":  project_name,
                "promoter_name": (
                    summary_row.get("promoter")
                    or summary_row.get("promoter name")
                    or summary_row.get("name of promoter")
                    or summary_row.get("name of the promoter")
                ) or None,
                "project_type": summary_row.get("property type") or None,
                "detail_url":   detail_url,
            }
            if address:
                row["project_location_raw"] = {"raw_address": address}
            if land_area is not None:
                row["land_area"] = land_area
            if land_area_unit:
                row["land_area_unit"] = land_area_unit
            if summary_row.get("promoter type"):
                row["data"] = {"promoter_type": summary_row["promoter type"]}
            rows.append(row)
        return rows

    # Layout B: plain HTML table fallback
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            reg_no, reg_idx = None, None
            for idx, td in enumerate(tds):
                text = td.get_text(strip=True)
                if _REG_RE.match(text):
                    reg_no, reg_idx = text, idx
                    break
            if not reg_no:
                continue

            def _cell(offset: int) -> str | None:
                i = reg_idx + offset  # type: ignore[index]
                return tds[i].get_text(separator=" ", strip=True) or None if 0 <= i < len(tds) else None

            rows.append({
                "project_registration_no": reg_no,
                "project_name":          _cell(1),
                "promoter_name":         _cell(2),
                "project_type":          _cell(3),
                "status_of_the_project": _cell(4),
                "detail_url":            _derive_detail_url(reg_no),
            })
    return rows


def _derive_detail_url(reg_no: str) -> str | None:
    """PRTR{MM}{YY}{NNNN} → /viewProjectDetailPage?projectID=int(last_4_digits)."""
    if reg_no and len(reg_no) >= 4:
        try:
            return f"{BASE_URL}/viewProjectDetailPage?projectID={int(reg_no[-4:])}"
        except ValueError:
            pass
    return None



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


def _parse_detail_page(url: str, logger: CrawlerLogger,
                        client: httpx.Client | None = None) -> dict:
    """Fetch and parse a Tripura project detail page."""
    if client is not None:
        resp = _get(url, logger, client)
    else:
        # Sentinel or standalone call — create a temporary client
        with _make_client() as tmp:
            tmp.get(HOME_URL)
            resp = _get(url, logger, tmp)
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

    # Extract project name from h1[3] — the standalone project-name heading.
    # The page structure is always:
    #   h1[0]: "{PROJECT_NAME}Last UpdatedOn{DATE}"
    #   h1[1]: "Last UpdatedOn{DATE}"
    #   h1[2]: "{PERSON}Chairman / Lead Member, {PROJECT_NAME}"  (or "Applicant, …")
    #   h1[3]: "{PROJECT_NAME}"  ← clean standalone name
    #   h1[4]: "Project Description"
    # Using h1[3] is more reliable than splitting h1[0] on "Last UpdatedOn".
    if not out.get("project_name"):
        for h1 in soup.find_all("h1"):
            text = h1.get_text(strip=True)
            if text and not _SKIP_H1_RE.search(text) and len(text) < 120:
                out["project_name"] = text
                break

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
        elif "contractor" in htext:
            used_tables.add(id(table))
            for r in _parse_section_table(table):
                p = _normalize_professional(r, role="Contractors")
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
    client: httpx.Client | None = None,
) -> dict | None:
    url   = doc["url"]
    label = doc["label"]
    fname = build_document_filename(doc)
    try:
        if client is not None:
            resp = download_response(url, logger=logger, timeout=60.0, verify=False, client=client)
        else:
            with _make_client() as tmp:
                resp = download_response(url, logger=logger, timeout=60.0, verify=False, client=tmp)
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


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger,
                     client: httpx.Client | None = None) -> bool:
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
        fresh = _parse_detail_page(detail_url, logger, client=client) or {}
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


# ── Shared project-processing helper ─────────────────────────────────────────

def _process_row(
    row: dict,
    config: dict,
    run_id: int,
    site_id: str,
    client: httpx.Client,
    logger: CrawlerLogger,
    machine_name: str,
    machine_ip: str,
    counts: dict,
    done_regs: set,
    items_processed: int,
    item_limit: int,
) -> int:
    """
    Process a single listing row: fetch detail, normalise, upsert, download docs.
    Returns the updated items_processed count.
    """
    if item_limit and items_processed >= item_limit:
        return items_processed

    reg_no = row["project_registration_no"]
    if reg_no in done_regs:
        counts["projects_skipped"] += 1
        return items_processed

    key = generate_project_key(reg_no)
    logger.set_project(key=key, reg_no=reg_no, url=row.get("detail_url", SEARCH_URL))

    try:
        if config.get("mode") == "daily_light" and get_project_by_key(key):
            counts["projects_skipped"] += 1
            logger.info("Skipping — already in DB (daily_light)")
            done_regs.add(reg_no)
            return items_processed

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
            "url":                     row.get("detail_url") or SEARCH_URL,
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
            detail = _parse_detail_page(row["detail_url"], logger, client)
            doc_links   = detail.pop("_doc_links", [])
            detail_data = detail.pop("data", {})

            for k, v in detail.items():
                if v is not None and k != "project_registration_no":
                    data[k] = v

            if data.get("promoter_name") and not data.get("promoters_details"):
                data["promoters_details"] = {"name": data["promoter_name"]}

            la = data.get("land_area")
            ca = data.get("construction_area")
            if la or ca:
                la_unit = row.get("land_area_unit") or detail_data.get("land_area_unit") or "sq Mtr"
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
                    data.get("data"), {"validation_fallback": True},
                )},
                config, machine_name=machine_name, machine_ip=machine_ip,
            )

        logger.info("Upserting to DB", step="db_upsert")
        action = upsert_project(db_dict)
        items_processed += 1
        if action == "new":       counts["projects_new"] += 1
        elif action == "updated": counts["projects_updated"] += 1
        else:                     counts["projects_skipped"] += 1
        logger.info(f"DB result: {action}", step="db_upsert")

        logger.info(f"Downloading {len(doc_links)} documents", step="documents")
        uploaded_documents: list[dict] = []
        doc_name_counts: dict[str, int] = {}
        for doc in doc_links:
            selected = select_document_for_download(
                config["state"], doc, doc_name_counts, domain=DOMAIN,
            )
            if selected:
                uploaded = _handle_document(db_dict["key"], selected, run_id, site_id, logger, client)
                if uploaded:
                    uploaded_documents.append(uploaded)
                    counts["documents_uploaded"] += 1
                else:
                    uploaded_documents.append({"link": doc.get("url"), "type": doc.get("label", "document")})
            else:
                uploaded_documents.append({"link": doc.get("url"), "type": doc.get("label", "document")})

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

    return items_processed


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
    item_limit    = settings.CRAWL_ITEM_LIMIT or 0
    max_pages     = settings.MAX_PAGES or 0
    items_processed = 0
    machine_name, machine_ip = get_machine_context()
    t_run = time.monotonic()
    # Pass mode through config so _process_row can check daily_light
    config = {**config, "mode": mode}

    with _make_client() as client:
        # ── Warm up session (acquire JSESSIONID cookie) ──────────────────────
        logger.info("Warming up session", url=HOME_URL, step="session")
        try:
            client.get(HOME_URL)
        except Exception as exc:
            logger.warning(f"Session warm-up failed (non-fatal): {exc}", step="session")

        # ── Sentinel health check ────────────────────────────────────────────
        t0 = time.monotonic()
        if not _sentinel_check(config, run_id, logger, client=client):
            logger.error("Sentinel failed — aborting crawl", step="sentinel")
            counts["error_count"] += 1
            return counts
        logger.timing("sentinel", time.monotonic() - t0)

        # ── Phase 1: Primary listing — POST /search ──────────────────────────
        logger.info("Phase 1: POST /search (primary listing)", url=SEARCH_URL, step="listing")
        t0 = time.monotonic()
        first_page_logged = False
        start_from   = 0
        total_records = 1  # will be updated from first response
        search_page   = 0

        while start_from < total_records:
            if max_pages and search_page >= max_pages:
                logger.info(f"Reached MAX_PAGES={max_pages}, stopping search pagination")
                break
            if item_limit and items_processed >= item_limit:
                logger.info(f"CRAWL_ITEM_LIMIT={item_limit} reached, stopping")
                reset_checkpoint(site_id, mode)
                return counts

            logger.info(f"Search page {search_page + 1} (startFrom={start_from})")
            resp = _post(SEARCH_URL, {"searchTxt": "", "startFrom": str(start_from)},
                         logger, client)
            if not resp:
                logger.error("Failed to fetch search page", start_from=start_from)
                insert_crawl_error(run_id, site_id, "HTTP_ERROR",
                                   f"Search startFrom={start_from} failed", url=SEARCH_URL)
                counts["error_count"] += 1
                break

            soup = BeautifulSoup(resp.text, "lxml")
            rows, total_records = _parse_search_rows(soup)
            if not rows:
                logger.info("No rows on search page — stopping", start_from=start_from)
                break

            logger.info(f"Search page {search_page + 1}: {len(rows)} rows (total={total_records})")
            counts["projects_found"] += len(rows)
            if not first_page_logged:
                logger.timing("search", time.monotonic() - t0, rows=len(rows))
                first_page_logged = True

            for row in rows:
                items_processed = _process_row(
                    row, config, run_id, site_id, client, logger,
                    machine_name, machine_ip, counts, done_regs,
                    items_processed, item_limit,
                )
                if item_limit and items_processed >= item_limit:
                    logger.info(f"CRAWL_ITEM_LIMIT={item_limit} reached")
                    reset_checkpoint(site_id, mode)
                    return counts

            save_checkpoint(site_id, mode, search_page, None, run_id)
            start_from  += SEARCH_PAGE_SIZE
            search_page += 1
            random_delay(*config.get("rate_limit_delay", (1, 3)))

        # ── Phase 2: Supplementary — GET /viewApprovedProjects ───────────────
        logger.info("Phase 2: GET /viewApprovedProjects (supplementary)", url=LISTING_URL, step="listing")
        resp = _get(LISTING_URL, logger, client)
        if not resp:
            logger.warning("Failed to fetch supplementary listing — skipping Phase 2",
                           url=LISTING_URL)
        else:
            soup = BeautifulSoup(resp.text, "lxml")
            supp_rows = _parse_listing_rows(soup)
            new_supp  = [r for r in supp_rows if r["project_registration_no"] not in done_regs]
            logger.info(f"Supplementary listing: {len(supp_rows)} total, "
                        f"{len(new_supp)} not seen in search")
            counts["projects_found"] += len(new_supp)

            for row in new_supp:
                items_processed = _process_row(
                    row, config, run_id, site_id, client, logger,
                    machine_name, machine_ip, counts, done_regs,
                    items_processed, item_limit,
                )
                if item_limit and items_processed >= item_limit:
                    logger.info(f"CRAWL_ITEM_LIMIT={item_limit} reached")
                    reset_checkpoint(site_id, mode)
                    return counts
                random_delay(*config.get("rate_limit_delay", (1, 3)))

    reset_checkpoint(site_id, mode)
    logger.info(f"Tripura RERA complete: {counts}")
    logger.timing("total_run", time.monotonic() - t_run)
    return counts
