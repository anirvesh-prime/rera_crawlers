"""
Tamil Nadu RERA Crawler — rera.tn.gov.in
Type: static (httpx + BeautifulSoup)

Strategy:
- Fetch CMS index page to discover year-based listing URLs (2017–present)
- For each year page: parse the HTML table (9 columns) to collect project rows
- Each row yields: reg_no, promoter name, project name/description, expiry date,
  promoter-UUID (public-view1), project-UUID (public-view2), lat/lng, form-C URL
- For each project: fetch public-view1 (promoter details) and
  public-view2 (project details) detail pages
- Documents: form-C QR code PDF + /public/storage/upload/*.pdf links from detail pages
"""
from __future__ import annotations

import re
from datetime import timezone
from typing import Any

from bs4 import BeautifulSoup
from pydantic import ValidationError

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
    merge_data_sections,
    normalize_project_payload,
    parse_datetime,
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings

BASE_URL         = "https://rera.tn.gov.in"
CMS_INDEX_URL    = f"{BASE_URL}/cms/reg_projects_building_tamilnadu.php"
STATE_CODE       = "TN"
DOMAIN           = "rera.tn.gov.in"
# Years present on the portal (oldest to newest; new years auto-discovered from CMS page)
_KNOWN_YEARS     = list(range(2017, 2027))


# ── Date utilities ────────────────────────────────────────────────────────────

def _parse_tn_date(raw: str | None) -> str | None:
    """Parse Tamil Nadu portal date strings (DD.MM.YYYY or DD-MM-YYYY) to UTC ISO."""
    if not raw:
        return None
    text = raw.strip()
    # Replace dots with dashes so parse_datetime handles it
    normalised = text.replace(".", "-")
    # Try DD-MM-YYYY first
    m = re.match(r"^(\d{2})-(\d{2})-(\d{4})$", normalised)
    if m:
        normalised = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    dt = parse_datetime(normalised)
    if dt:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")
    return None


def _extract_number(text: str | None) -> float | None:
    """Extract first numeric value from a string like '9396 Sq.mts'."""
    if not text:
        return None
    m = re.search(r"[\d,]+\.?\d*", text.replace(",", ""))
    if m:
        try:
            return float(m.group(0))
        except ValueError:
            pass
    return None


# ── CMS index → year listing URLs ────────────────────────────────────────────

def _get_year_listing_urls(logger: CrawlerLogger) -> list[str]:
    """
    Fetch the CMS index page and extract all year-specific building listing URLs.
    Falls back to _KNOWN_YEARS if the page is unreachable.
    """
    resp = safe_get(CMS_INDEX_URL, logger=logger, timeout=30.0)
    if not resp:
        logger.warning("CMS index unreachable; falling back to known years", url=CMS_INDEX_URL)
        return [
            f"{BASE_URL}/cms/reg_projects_tamilnadu/Building/{y}.php"
            for y in sorted(_KNOWN_YEARS, reverse=True)
        ]
    urls: list[str] = []
    seen: set[str] = set()
    for href in re.findall(r'https?://rera\.tn\.gov\.in/cms/reg_projects_tamilnadu/Building/\d{4}\.php', resp.text):
        if href not in seen:
            seen.add(href)
            urls.append(href)
    if not urls:
        logger.warning("No year URLs found on CMS index; using known years", url=CMS_INDEX_URL)
        return [
            f"{BASE_URL}/cms/reg_projects_tamilnadu/Building/{y}.php"
            for y in sorted(_KNOWN_YEARS, reverse=True)
        ]
    # Sort newest first so we process recent projects first
    urls.sort(key=lambda u: re.search(r"(\d{4})\.php", u).group(1), reverse=True)
    logger.info(f"Discovered {len(urls)} year listing URLs")
    return urls


# ── Listing table parser ──────────────────────────────────────────────────────

_UUID_RE  = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)
_LAT_RE   = re.compile(r"Latitude[-:\s]*([\d.]+)", re.I)
_LNG_RE   = re.compile(r"Longitude[-:\s]*([\d.]+)", re.I)
_REGNO_RE = re.compile(r"(?:TNRERA/[\w/]+|TN/\d+/Building/[\w/]+/\d{4})", re.I)


def _parse_listing_row(tds) -> dict | None:
    """
    Parse one <tr> of a Tamil Nadu building listing table.

    Column layout (0-indexed):
      0 – S.No
      1 – Registration No + "dated DD-MM-YYYY"
      2 – Promoter name / address
      3 – "Project Name: <name>\\n<description>"
      4 – Approval details (planning/building permission text)
      5 – Expiry / completion date (DD.MM.YYYY or "Completed")
      6 – Links: Promoter Details (view1), Project Details (view2), Lat/Lng span
      7 – Form C QR code link
      8 – (empty / reserved)
    """
    if len(tds) < 7:
        return None

    # Current portal layout is a flat 8-column listing page with direct PDFs.
    if len(tds) == 8:
        td1_text = tds[1].get_text(separator=" ", strip=True)
        reg_match = _REGNO_RE.search(td1_text)
        if not reg_match:
            return None
        reg_no = reg_match.group(0).strip()
        dated_m = re.search(r"dated\s+(\d{2}[/-]\d{2}[/-]\d{4})", td1_text, re.I)
        approved_on = _parse_tn_date(dated_m.group(1).replace("/", "-")) if dated_m else None

        promoter_raw = tds[2].get_text(separator=" ", strip=True)
        promoter_name = promoter_raw.split(",")[0].strip() if promoter_raw else None

        td3_text = tds[3].get_text(separator=" ", strip=True)
        proj_name_m = re.search(r"Project\s+Name\s*:\s*[\"“]?(.+?)[\"”]?\s*-\s*(.+)", td3_text, re.I)
        project_name = proj_name_m.group(1).strip() if proj_name_m else None
        description = proj_name_m.group(2).strip() if proj_name_m else td3_text

        approval_url = None
        approval_a = tds[4].find("a", href=True)
        if approval_a:
            href = approval_a["href"]
            approval_url = href if href.startswith("http") else f"{BASE_URL}{href}"

        td5_text = tds[5].get_text(strip=True)
        status_text = tds[7].get_text(strip=True) or td5_text
        is_completed = "completed" in status_text.lower() or "completed" in td5_text.lower()
        expiry_date = None if is_completed else _parse_tn_date(td5_text.replace("/", "-"))

        td6_html = str(tds[6])
        td6_soup = tds[6]
        promoter_uuid = project_uuid = lat = lng = None
        for a in td6_soup.find_all("a", href=True):
            href = a["href"]
            uuid_match = _UUID_RE.search(href)
            if not uuid_match:
                continue
            if "public-view1" in href:
                promoter_uuid = uuid_match.group(0)
            elif "public-view2" in href:
                project_uuid = uuid_match.group(0)

        lat_m = _LAT_RE.search(td6_html)
        lng_m = _LNG_RE.search(td6_html)
        if lat_m:
            lat = lat_m.group(1)
        if lng_m:
            lng = lng_m.group(1)

        form_c_url = None
        for a in tds[6].find_all("a", href=True):
            href = a["href"]
            if "formcqr" in href.lower():
                form_c_url = href if href.startswith("http") else f"{BASE_URL}{href}"
                break
        if not form_c_url:
            for a in tds[7].find_all("a", href=True):
                href = a["href"]
                if "formcqr" in href.lower():
                    form_c_url = href if href.startswith("http") else f"{BASE_URL}{href}"
                    break

        docs = []
        if approval_url:
            docs.append({"label": "Approval Details", "type": "Approval Details", "url": approval_url})
        if form_c_url:
            docs.insert(0, {"label": "Form C", "type": "Form C", "url": form_c_url})

        row = {
            "project_registration_no": reg_no,
            "approved_on_date": approved_on,
            "promoter_name": promoter_name,
            "promoter_raw_text": promoter_raw,
            "project_name": project_name,
            "project_description": description or None,
            "approval_details": approval_url,
            "estimated_finish_date": expiry_date,
            "is_completed": is_completed,
            "status_of_the_project": "Completed" if is_completed else (status_text or None),
            "promoter_uuid": promoter_uuid,
            "project_uuid": project_uuid,
            "latitude": lat,
            "longitude": lng,
            "form_c_url": form_c_url,
            "promoter_url": (
                f"{BASE_URL}/public-view1/building/pfirm/{promoter_uuid}" if promoter_uuid else None
            ),
            "detail_url": (
                f"{BASE_URL}/public-view2/building/pfirm/{project_uuid}" if project_uuid else None
            ),
            "uploaded_documents": docs or None,
        }
        return {k: v for k, v in row.items() if v not in (None, "", [], {})}

    # td[1]: registration number
    td1_text = tds[1].get_text(separator=" ", strip=True)
    reg_match = _REGNO_RE.search(td1_text)
    if not reg_match:
        return None
    reg_no = reg_match.group(0).strip()

    # Registration date (approved_on_date) from "dated DD-MM-YYYY"
    dated_m = re.search(r"dated\s+(\d{2}-\d{2}-\d{4})", td1_text, re.I)
    approved_on = _parse_tn_date(dated_m.group(1)) if dated_m else None

    # td[2]: promoter name (first meaningful text block)
    promoter_raw = tds[2].get_text(separator="\n", strip=True)
    promoter_lines = [l.strip() for l in promoter_raw.splitlines() if l.strip()]
    promoter_name = promoter_lines[0] if promoter_lines else None

    # td[3]: project name + description
    td3_text = tds[3].get_text(separator="\n", strip=True)
    proj_name_m = re.search(r"Project\s+Name\s*:\s*(.+?)(?:\n|$)", td3_text, re.I)
    project_name = proj_name_m.group(1).strip() if proj_name_m else None
    if project_name:
        description = td3_text[td3_text.index(project_name) + len(project_name):].strip()
    else:
        description = td3_text

    # td[5]: expiry/completion date
    td5_text = tds[5].get_text(strip=True)
    expiry_date = _parse_tn_date(td5_text) if re.search(r"\d", td5_text) else None
    is_completed = "completed" in td5_text.lower()

    # td[6]: extract promoter UUID, project UUID, latitude, longitude
    td6_html = str(tds[6])
    td6_soup = tds[6]
    promoter_uuid = project_uuid = lat = lng = None

    for a in td6_soup.find_all("a", href=_UUID_RE):
        href = a["href"]
        if "public-view1" in href:
            m = _UUID_RE.search(href)
            if m:
                promoter_uuid = m.group(0)
        elif "public-view2" in href:
            m = _UUID_RE.search(href)
            if m:
                project_uuid = m.group(0)

    lat_m = _LAT_RE.search(td6_html)
    lng_m = _LNG_RE.search(td6_html)
    if lat_m:
        lat = lat_m.group(1)
    if lng_m:
        lng = lng_m.group(1)

    # td[7]: form C / QR code URL
    form_c_url = None
    if len(tds) > 7:
        form_c_a = tds[7].find("a", href=re.compile(r"formcqr"))
        if form_c_a:
            href = form_c_a["href"]
            form_c_url = href if href.startswith("http") else f"{BASE_URL}{href}"

    return {
        "project_registration_no": reg_no,
        "approved_on_date":        approved_on,
        "promoter_name":           promoter_name,
        "promoter_raw_text":       promoter_raw,
        "project_name":            project_name,
        "project_description":     description or None,
        "approval_details":        tds[4].get_text(separator=" ", strip=True) or None,
        "estimated_finish_date":   expiry_date,
        "is_completed":            is_completed,
        "promoter_uuid":           promoter_uuid,
        "project_uuid":            project_uuid,
        "latitude":                lat,
        "longitude":               lng,
        "form_c_url":              form_c_url,
        "promoter_url": (
            f"{BASE_URL}/public-view1/building/pfirm/{promoter_uuid}" if promoter_uuid else None
        ),
        "detail_url": (
            f"{BASE_URL}/public-view2/building/pfirm/{project_uuid}" if project_uuid else None
        ),
    }


def _parse_year_listing(url: str, logger: CrawlerLogger) -> list[dict]:
    """Fetch a single year listing page and return parsed row dicts."""
    resp = safe_get(url, logger=logger, timeout=60.0)
    if not resp:
        logger.warning("Year listing page fetch failed", url=url)
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    rows: list[dict] = []
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            parsed = _parse_listing_row(tds)
            if parsed:
                rows.append(parsed)
    logger.info(f"Year listing parsed: {len(rows)} rows", url=url)
    return rows


# ── Detail page label extractor ───────────────────────────────────────────────

def _extract_kv_pairs(soup: BeautifulSoup) -> dict[str, str]:
    """
    General-purpose key-value extractor for Tamil Nadu RERA detail pages.

    Handles three common Bootstrap-panel patterns:
    1. <th> / <td> pairs in a table row
    2. <label>Key :</label> followed by sibling or parent text
    3. <strong>Key:</strong> followed by text node
    """
    out: dict[str, str] = {}

    # Pattern 1: <tr><th>Key</th><td>Value</td></tr>
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) == 2:
            key = cells[0].get_text(separator=" ", strip=True).rstrip(":").strip()
            val = cells[1].get_text(separator=" ", strip=True)
            if key and val and len(key) <= 150:
                out.setdefault(key, val)
        elif len(cells) == 4:
            # Two key-value pairs per row
            for idx in range(0, 4, 2):
                key = cells[idx].get_text(separator=" ", strip=True).rstrip(":").strip()
                val = cells[idx + 1].get_text(separator=" ", strip=True)
                if key and val and len(key) <= 150:
                    out.setdefault(key, val)

    # Pattern 2: <label>Key :</label><span>Value</span> or adjacent text
    for lbl in soup.find_all("label"):
        raw = lbl.get_text(strip=True)
        key = raw.rstrip(":").strip()
        if not key or len(key) > 150:
            continue
        # Value might be in next sibling
        nxt = lbl.find_next_sibling()
        if nxt:
            val = nxt.get_text(strip=True)
            if val and key not in out:
                out[key] = val
            continue
        # Or embedded after colon in same element
        if ":" in raw:
            idx = raw.index(":")
            possible_key = raw[:idx].strip()
            possible_val = raw[idx + 1:].strip()
            if possible_key and possible_val:
                out.setdefault(possible_key, possible_val)

    # Pattern 3: <strong>Key:</strong> value text node
    for strong in soup.find_all("strong"):
        raw = strong.get_text(strip=True)
        key = raw.rstrip(":").strip()
        if not key or len(key) > 150:
            continue
        parent = strong.parent
        if parent:
            full_text = parent.get_text(separator=" ", strip=True)
            after_key = full_text[len(raw):].lstrip(": ").strip()
            if after_key and key not in out:
                out[key] = after_key

    return out


# Promoter detail page label→schema field mapping
_PROMOTER_LABEL_MAP: dict[str, str] = {
    "name of the promoter":           "promoter_name",
    "promoter name":                  "promoter_name",
    "name":                           "promoter_name",
    "type of organisation":           "_promoter_org_type",
    "type of organization":           "_promoter_org_type",
    "organisation type":              "_promoter_org_type",
    "registration number":            "_promoter_reg_no",
    "gstin":                          "_promoter_gst",
    "gst number":                     "_promoter_gst",
    "email":                          "_email",
    "email id":                       "_email",
    "mobile number":                  "_phone",
    "mobile no":                      "_phone",
    "phone":                          "_phone",
    "pan number":                     "_pan",
    "address":                        "_address",
    "permanent address":              "_address",
    "registered address":             "_address",
    "state":                          "_state",
    "district":                       "_district",
    "city":                           "_city",
    "pin code":                       "_pin_code",
    "pincode":                        "_pin_code",
}

# Project detail page label→schema field mapping
_PROJECT_LABEL_MAP: dict[str, str] = {
    "project name":                         "project_name",
    "type of project":                      "project_type",
    "project type":                         "project_type",
    "status of the project":                "status_of_the_project",
    "project status":                       "status_of_the_project",
    "stage of construction":                "status_of_the_project",
    "date of registration":                 "approved_on_date",
    "registration date":                    "approved_on_date",
    "proposed date of commencement":        "estimated_commencement_date",
    "proposed date of completion":          "estimated_finish_date",
    "actual date of commencement":          "actual_commencement_date",
    "actual date of completion":            "actual_finish_date",
    "extended date of completion":          "estimated_finish_date",
    "number of residential units":          "number_of_residential_units",
    "total residential units":              "number_of_residential_units",
    "number of commercial units":           "number_of_commercial_units",
    "total commercial units":              "number_of_commercial_units",
    "land area":                            "land_area",
    "total land area":                      "land_area",
    "construction area":                    "construction_area",
    "carpet area":                          "construction_area",
    "pin code":                             "project_pin_code",
    "pincode":                              "project_pin_code",
    "district":                             "project_city",
    "city":                                 "project_city",
    "taluk":                                "_taluk",
    "village":                              "_village",
    "survey number":                        "_survey_no",
    "survey/resurvey number":               "_survey_no",
    "survey / resurvey number":             "_survey_no",
    "latitude":                             "_latitude",
    "longitude":                            "_longitude",
    "bank name":                            "_bank_name",
    "bank branch":                          "_bank_branch",
    "ifsc code":                            "_ifsc",
    "account number":                       "_account_no",
    "account no":                           "_account_no",
    "cost of land":                         "_cost_of_land",
    "total project cost":                   "_total_project_cost",
    "estimated construction cost":          "_estimated_construction_cost",
}




# ── Promoter detail page (public-view1) ──────────────────────────────────────

def _parse_promoter_page(url: str, logger: CrawlerLogger) -> dict:
    """
    Fetch and parse public-view1 (Promoter Details page).
    Returns a dict with structured fields ready for merging into the project record.
    """
    resp = safe_get(url, logger=logger, timeout=30.0)
    if not resp:
        logger.warning("Promoter detail page fetch failed", url=url)
        return {}
    soup = BeautifulSoup(resp.text, "lxml")
    kv = _extract_kv_pairs(soup)
    out: dict[str, Any] = {}

    # Map labels to internal keys
    mapped: dict[str, str] = {}
    for raw_key, raw_val in kv.items():
        schema_f = _PROMOTER_LABEL_MAP.get(raw_key.lower().strip())
        if schema_f and raw_val:
            mapped.setdefault(schema_f, raw_val)

    # Promoter name (may already be set from listing)
    if mapped.get("promoter_name"):
        out["promoter_name"] = mapped["promoter_name"]

    # Promoter contact details
    contact = {k: mapped[k] for k in ("_email", "_phone") if mapped.get(k)}
    if contact:
        out["promoter_contact_details"] = {
            "email": contact.get("_email"),
            "phone": contact.get("_phone"),
        }

    # Promoter address (structured)
    addr: dict[str, str] = {}
    for raw_k in ("_address", "_state", "_district", "_city", "_pin_code"):
        if mapped.get(raw_k):
            addr[raw_k.lstrip("_")] = mapped[raw_k]
    if addr:
        out["promoter_address_raw"] = addr

    # Members / directors table
    members = _extract_members_table(soup)
    if members:
        out["members_details"] = members

    # Promoters details summary
    promoters_details: dict = {}
    if mapped.get("promoter_name"):
        promoters_details["name"] = mapped["promoter_name"]
    if mapped.get("_promoter_org_type"):
        promoters_details["type_of_firm"] = mapped["_promoter_org_type"]
    if mapped.get("_promoter_reg_no"):
        promoters_details["registration_no"] = mapped["_promoter_reg_no"]
    if mapped.get("_promoter_gst"):
        promoters_details["GSTIN"] = mapped["_promoter_gst"]
    if promoters_details:
        out["promoters_details"] = promoters_details

    reg_cert_link = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/public/storage/upload/" in href.lower() and "registration" in a.get_text(" ", strip=True).lower():
            reg_cert_link = href if href.startswith("http") else f"{BASE_URL}{href}"
            break
    if reg_cert_link:
        out.setdefault("promoters_details", {})
        out["promoters_details"]["registration_certificate"] = reg_cert_link

    # Raw labels stored for data JSONB
    out["_promoter_raw_labels"] = kv
    return out


def _extract_members_table(soup: BeautifulSoup) -> list[dict]:
    """
    Look for a table that lists directors / members / partners on the promoter page.
    Typical headers: Name, Designation, Email, Mobile, Address
    """
    members: list[dict] = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        # Must have at least a "name" column
        if not any("name" in h for h in headers):
            continue
        for tr in rows[1:]:
            tds = tr.find_all("td")
            if not tds:
                continue
            row_dict: dict[str, str] = {}
            for i, td in enumerate(tds):
                if i < len(headers) and headers[i]:
                    row_dict[headers[i]] = td.get_text(strip=True)
            name = row_dict.get("name") or row_dict.get("member name") or row_dict.get("partner name")
            if name and name.strip():
                entry: dict[str, str] = {"name": name.strip()}
                for src, tgt in [
                    ("designation", "position"),
                    ("email", "email"),
                    ("mobile", "phone"),
                    ("mobile no", "phone"),
                    ("address", "raw_address"),
                ]:
                    val = row_dict.get(src, "").strip()
                    if val:
                        entry[tgt] = val
                members.append(entry)
    return members


# ── Project detail page (public-view2) ───────────────────────────────────────

def _parse_project_page(url: str, logger: CrawlerLogger) -> dict:
    """
    Fetch and parse public-view2 (Project Details page).
    Returns structured fields + document links.
    """
    resp = safe_get(url, logger=logger, timeout=30.0)
    if not resp:
        logger.warning("Project detail page fetch failed", url=url)
        return {}
    soup = BeautifulSoup(resp.text, "lxml")
    kv = _extract_kv_pairs(soup)
    out: dict[str, Any] = {}

    # Map labels → schema fields
    for raw_key, raw_val in kv.items():
        schema_f = _PROJECT_LABEL_MAP.get(raw_key.lower().strip())
        if not schema_f or not raw_val:
            continue
        if schema_f.startswith("_"):
            out.setdefault(schema_f, raw_val)
        else:
            out.setdefault(schema_f, raw_val)

    # Normalise date fields
    for date_field in (
        "approved_on_date", "estimated_commencement_date", "estimated_finish_date",
        "actual_commencement_date", "actual_finish_date",
    ):
        if out.get(date_field):
            out[date_field] = _parse_tn_date(out[date_field]) or out[date_field]

    # Coerce numeric fields
    for f in ("land_area", "construction_area"):
        if out.get(f):
            out[f] = _extract_number(str(out[f]))

    for f in ("number_of_residential_units", "number_of_commercial_units"):
        if out.get(f):
            try:
                out[f] = int(str(out[f]).replace(",", "").split(".")[0])
            except (ValueError, TypeError):
                pass

    # Project location JSONB
    loc: dict = {}
    for tgt, src in [
        ("survey_resurvey_number", "_survey_no"),
        ("taluk", "_taluk"),
        ("village", "_village"),
        ("district", "project_city"),
        ("pin_code", "project_pin_code"),
        ("latitude", "_latitude"),
        ("longitude", "_longitude"),
    ]:
        val = out.get(src) or out.get(tgt)
        if val:
            loc[tgt] = str(val)
    if "project_city" in out:
        loc.setdefault("district", out["project_city"])
    if loc:
        out["project_location_raw"] = loc

    # Bank details
    bank: dict = {}
    for tgt, src in [
        ("bank_name", "_bank_name"),
        ("branch", "_bank_branch"),
        ("IFSC", "_ifsc"),
        ("account_no", "_account_no"),
    ]:
        if out.get(src):
            bank[tgt] = out.pop(src)
    if bank:
        out["bank_details"] = bank

    # Project cost detail
    cost: dict = {}
    for tgt, src in [
        ("cost_of_land", "_cost_of_land"),
        ("total_project_cost", "_total_project_cost"),
        ("estimated_construction_cost", "_estimated_construction_cost"),
    ]:
        if out.get(src):
            cost[tgt] = out.pop(src)
    if cost:
        out["project_cost_detail"] = cost

    # Professional information table
    professionals = _extract_professionals_table(soup)
    if professionals:
        out["professional_information"] = professionals

    # Document links (PDF files in the page)
    doc_links = _extract_doc_links(soup)
    if doc_links:
        out["_doc_links"] = doc_links

    out["_project_raw_labels"] = kv
    return out


def _extract_professionals_table(soup: BeautifulSoup) -> list[dict]:
    """Extract professionals table (Architect, Engineer, CA …) from project detail page."""
    professionals: list[dict] = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        # Need at least one of role/type and name
        if not any(h in ("role", "type", "professional type", "type of professional") for h in headers):
            continue
        if not any("name" in h for h in headers):
            continue
        for tr in rows[1:]:
            tds = tr.find_all("td")
            if not tds:
                continue
            row_dict = {headers[i]: tds[i].get_text(strip=True) for i in range(min(len(headers), len(tds)))}
            name = row_dict.get("name") or row_dict.get("professional name", "")
            if not name.strip():
                continue
            entry: dict = {"name": name.strip()}
            role = row_dict.get("role") or row_dict.get("type") or row_dict.get("professional type", "")
            if role:
                entry["role"] = role.strip()
            for col in ("email", "mobile", "mobile no", "phone", "address", "registration no"):
                val = row_dict.get(col, "").strip()
                if val:
                    key = "phone" if col in ("mobile", "mobile no") else col.replace(" ", "_")
                    entry[key] = val
            professionals.append(entry)
    return professionals


def _extract_doc_links(soup: BeautifulSoup) -> list[dict]:
    """
    Extract direct PDF/document links from a detail page.
    Skips JavaScript anchors and navigation links.
    """
    docs: list[dict] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().startswith("javascript"):
            continue
        if not (href.lower().endswith(".pdf") or "/storage/upload" in href.lower()
                or "formcqr" in href.lower() or "/public/" in href.lower()):
            continue
        full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
        if full_url in seen:
            continue
        seen.add(full_url)
        label = a.get_text(strip=True) or a.find_next("img", alt=True)
        if hasattr(label, "get"):
            label = label.get("alt", "document")
        label = str(label).strip() or "document"
        docs.append({"label": label, "url": full_url})
    return docs


# ── Document download + S3 upload ─────────────────────────────────────────────

def _handle_document(
    project_key: str,
    doc: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
) -> dict | None:
    url = doc.get("url")
    if not url:
        return None
    label = doc.get("label", "document")
    filename = build_document_filename(doc)
    try:
        resp = safe_get(url, logger=logger, timeout=60.0)
        if not resp or len(resp.content) < 100:
            logger.warning("Document download failed or too small", url=url, label=label)
            return None
        # Reject HTML soft-404 pages
        ct = resp.headers.get("Content-Type", "").lower()
        if "text/html" in ct and not resp.content[:8].startswith(b"%PDF"):
            logger.warning("Document URL returned HTML (soft-404)", url=url)
            return None
        data = resp.content
        md5 = compute_md5(data)
        s3_key = upload_document(project_key, filename, data, dry_run=settings.DRY_RUN_S3)
        if s3_key is None:
            return None
        s3_url = get_s3_url(s3_key)
        result = document_result_entry(doc, s3_key=s3_key, s3_url=s3_url, md5=md5)
        upsert_document(
            project_key=project_key,
            document_type=label,
            original_url=document_identity_url(doc) or url,
            s3_key=s3_key,
            s3_bucket=settings.S3_BUCKET_NAME,
            file_name=filename,
            md5_checksum=md5,
            file_size_bytes=len(data),
        )
        return result
    except Exception as exc:
        logger.warning("Document handling error", url=url, error=str(exc))
        return None


# ── Project record builder ────────────────────────────────────────────────────

_STATUS_MAP: dict[str, str] = {
    "new booking":        "New Booking",
    "under construction": "Under Construction",
    "completion":         "Completion",
    "completed":          "Completed",
    "lapsed":             "Lapsed",
    "revoked":            "Revoked",
}


def _build_project_record(
    row: dict,
    promoter_data: dict,
    project_data: dict,
    config_id: int,
    run_id: int,
) -> dict:
    """
    Merge listing row + promoter detail + project detail into a single flat record
    ready for normalize_project_payload → ProjectRecord.
    """
    reg_no = row["project_registration_no"]
    project_key = generate_project_key(reg_no)

    # Base record from listing table
    record: dict[str, Any] = {
        "key":                      project_key,
        "project_registration_no":  reg_no,
        "state":                    "tamil_nadu",
        "project_state":            "TAMIL NADU",
        "domain":                   DOMAIN,
        "config_id":                config_id,
        "url":                      row.get("detail_url") or f"{BASE_URL}/registered-building/tn",
        "promoter_name":            row.get("promoter_name"),
        "project_name":             row.get("project_name"),
        "project_description":      row.get("project_description"),
        "approved_on_date":         row.get("approved_on_date"),
        "estimated_finish_date":    row.get("estimated_finish_date"),
        "project_type":             "residential",   # building projects default; detail page may refine
    }

    # Status from listing (completed vs active)
    if row.get("is_completed"):
        record["status_of_the_project"] = "Completed"

    # GPS coordinates → project_location_raw seed
    loc: dict[str, str] = {}
    if row.get("latitude"):
        loc["latitude"] = row["latitude"]
    if row.get("longitude"):
        loc["longitude"] = row["longitude"]
    if loc:
        record["project_location_raw"] = loc

    # Merge promoter detail (deeper fields overwrite listing where applicable)
    for k, v in promoter_data.items():
        if not k.startswith("_") and v not in (None, "", {}, []):
            record.setdefault(k, v)

    # Merge project detail (project detail wins over listing for shared fields)
    for k, v in project_data.items():
        if not k.startswith("_") and v not in (None, "", {}, []):
            record[k] = v   # project detail always wins (more authoritative)

    # Merge project_location_raw: combine lat/lng from listing with fields from detail page
    if isinstance(project_data.get("project_location_raw"), dict):
        merged_loc = dict(loc)  # lat/lng from listing
        merged_loc.update(project_data["project_location_raw"])
        record["project_location_raw"] = merged_loc

    # Normalise status string if set
    raw_status = record.get("status_of_the_project", "")
    if isinstance(raw_status, str):
        record["status_of_the_project"] = _STATUS_MAP.get(
            raw_status.lower().strip(), raw_status
        )

    # Form-C and document links
    doc_links: list[dict] = list(project_data.get("_doc_links") or [])
    if row.get("form_c_url"):
        doc_links.insert(0, {"label": "Form C", "url": row["form_c_url"]})
    if doc_links:
        record["uploaded_documents"] = doc_links

    # Data JSONB
    record["data"] = {
        "govt_type":      "state",
        "is_processed":   False,
        "approval_details": row.get("approval_details"),
        "promoter_url":   row.get("promoter_url"),
        "form_c":         row.get("form_c_url"),
        "all_labels_promoter": promoter_data.get("_promoter_raw_labels"),
        "all_labels_project":  project_data.get("_project_raw_labels"),
    }

    return {k: v for k, v in record.items() if v is not None and v != "" and v != {} and v != []}


# ── Main entry point ──────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    """
    Entry point called by the crawler orchestrator.

    Modes:
        full      – crawl all years from scratch (ignores checkpoint)
        incremental – resume from last checkpoint; stops when recent projects re-seen
        single    – crawl only the current/most-recent year
    """
    site_id   = config.get("id", "tamil_nadu_rera")
    config_id = config.get("config_id", 14374)
    logger    = CrawlerLogger(site_id, run_id)

    counts = {
        "projects_found":    0,
        "projects_new":      0,
        "projects_updated":  0,
        "projects_skipped":  0,
        "documents_uploaded": 0,
        "error_count":       0,
    }
    item_limit = settings.CRAWL_ITEM_LIMIT or 0

    # ── Checkpoint handling ──────────────────────────────────────────────────
    checkpoint = load_checkpoint(site_id, mode) if mode != "full" else {}
    last_project_key: str | None = checkpoint.get("last_project_key")
    last_page = int((checkpoint or {}).get("last_page", 0))

    if mode == "full":
        reset_checkpoint(site_id, mode)

    # ── Discover year listing URLs ───────────────────────────────────────────
    year_urls = _get_year_listing_urls(logger)
    if mode == "single" and year_urls:
        year_urls = year_urls[:1]   # only most-recent year
    logger.info(f"Will crawl {len(year_urls)} year listing(s)", mode=mode)

    machine_name, machine_ip = get_machine_context()
    items_processed = 0

    for year_index, year_url in enumerate(year_urls):
        if year_index < last_page:
            continue
        year_label = re.search(r"(\d{4})\.php", year_url)
        year_label = year_label.group(1) if year_label else year_url

        logger.info(f"Crawling year {year_label}", url=year_url)
        rows = _parse_year_listing(year_url, logger)
        if not rows:
            logger.warning(f"No rows found for year {year_label}", url=year_url)
            continue

        for row in rows:
            if item_limit and items_processed >= item_limit:
                logger.info(f"CRAWL_ITEM_LIMIT={item_limit} reached, stopping")
                return counts

            reg_no = row.get("project_registration_no")
            if not reg_no:
                continue

            counts["projects_found"] += 1
            project_key = generate_project_key(reg_no)
            if last_project_key and mode != "full":
                if project_key == last_project_key:
                    last_project_key = None
                counts["projects_skipped"] += 1
                continue
            logger.set_project(
                key=project_key,
                reg_no=reg_no,
                url=row.get("detail_url") or year_url,
                page=year_label,
            )
            try:
                # ── Detail page fetch ────────────────────────────────────────────
                promoter_data: dict = {}
                project_data: dict  = {}

                if row.get("promoter_url"):
                    random_delay(1.0, 2.5)
                    promoter_data = _parse_promoter_page(row["promoter_url"], logger)

                if row.get("detail_url"):
                    random_delay(1.0, 2.5)
                    project_data = _parse_project_page(row["detail_url"], logger)

                # ── Build merged record ──────────────────────────────────────────
                try:
                    raw_record = _build_project_record(
                        row, promoter_data, project_data, config_id, run_id
                    )
                    payload = normalize_project_payload(
                        raw_record,
                        config,
                        machine_name=machine_name,
                        machine_ip=machine_ip,
                    )
                    record = ProjectRecord(**payload)
                except ValidationError as exc:
                    logger.warning("ProjectRecord validation failed", error=str(exc))
                    insert_crawl_error(
                        run_id=run_id,
                        site_id=site_id,
                        error_type="validation_error",
                        error_message=str(exc),
                        project_key=project_key,
                        url=row.get("detail_url") or year_url,
                    )
                    counts["error_count"] += 1
                    continue
                except Exception as exc:
                    logger.error("Unexpected error building record", error=str(exc))
                    insert_crawl_error(
                        run_id=run_id,
                        site_id=site_id,
                        error_type="build_error",
                        error_message=str(exc),
                        project_key=project_key,
                        url=row.get("detail_url") or year_url,
                    )
                    counts["error_count"] += 1
                    continue

                # ── DB upsert ────────────────────────────────────────────────────
                db_dict = record.to_db_dict()
                status = upsert_project(db_dict)
                items_processed += 1

                if status == "new":
                    counts["projects_new"] += 1
                elif status == "updated":
                    counts["projects_updated"] += 1
                else:
                    counts["projects_skipped"] += 1

                # ── Document handling ─────────────────────────────────────────────
                raw_docs: list[dict] = list(row.get("uploaded_documents") or [])
                if not raw_docs:
                    raw_docs = list(raw_record.get("uploaded_documents") or [])

                doc_name_counts: dict[str, int] = {}
                enriched_docs: list[dict] = []
                for doc in raw_docs:
                    selected = select_document_for_download(
                        config["state"], doc, doc_name_counts, domain=DOMAIN,
                    )
                    if not selected:
                        enriched_docs.append(
                            {
                                "link": doc.get("url") or doc.get("link"),
                                "type": doc.get("label") or doc.get("type") or "document",
                            }
                        )
                        continue
                    result = _handle_document(project_key, selected, run_id, site_id, logger)
                    if result:
                        counts["documents_uploaded"] += 1
                        enriched_docs.append(result)
                    else:
                        enriched_docs.append(
                            {
                                "link": selected.get("url") or selected.get("link"),
                                "type": selected.get("label") or selected.get("type") or "document",
                            }
                        )

                if enriched_docs:
                    upsert_project({
                        "key": project_key,
                        "url": db_dict["url"],
                        "state": db_dict["state"],
                        "domain": db_dict["domain"],
                        "project_registration_no": db_dict["project_registration_no"],
                        "uploaded_documents": enriched_docs,
                        "document_urls": build_document_urls(enriched_docs),
                    })

                # ── Checkpoint save ──────────────────────────────────────────────
                save_checkpoint(site_id, mode, year_index, project_key, run_id)
            finally:
                logger.clear_project()

    logger.info("Tamil Nadu RERA crawl complete", **counts)
    return counts
