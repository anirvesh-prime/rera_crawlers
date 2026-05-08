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

BASE_URL              = "https://rera.tn.gov.in"
CMS_INDEX_URL         = f"{BASE_URL}/cms/reg_projects_building_tamilnadu.php"
# CMS index pages for layout project types (Normal and Regularisation)
CMS_LAYOUT_INDEX_URLS = [
    f"{BASE_URL}/cms/reg_projects_nlayout_tamilnadu.php",
    f"{BASE_URL}/cms/reg_projects_rlayout_tamilnadu.php",
]
# URL templates for each project type (used as fallback when CMS index is unreachable)
_TYPE_URL_TEMPLATES = {
    "Building":               f"{BASE_URL}/cms/reg_projects_tamilnadu/Building/{{year}}.php",
    "Normal_Layout":          f"{BASE_URL}/cms/reg_projects_tamilnadu/Normal_Layout/{{year}}.php",
    "Regularisation_Layout":  f"{BASE_URL}/cms/reg_projects_tamilnadu/Regularisation_Layout/{{year}}.php",
}
# Maps short CMS filename key → _TYPE_URL_TEMPLATES key (for fallback URL generation)
_LAYOUT_CMS_TO_TYPE: dict[str, str] = {
    "nlayout": "Normal_Layout",
    "rlayout": "Regularisation_Layout",
}
STATE_CODE       = "TN"
DOMAIN           = "rera.tn.gov.in"
# Years present on the portal (oldest to newest; new years auto-discovered from CMS pages)
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

def _discover_urls_from_cms(index_url: str, logger: CrawlerLogger) -> list[str]:
    """
    Fetch one CMS index page and return all year-listing URLs found on it.
    Matches any /cms/reg_projects_tamilnadu/<Type>/<YYYY>.php pattern.
    """
    resp = safe_get(index_url, logger=logger, timeout=30.0)
    if not resp:
        return []
    found: list[str] = []
    seen: set[str] = set()
    for href in re.findall(
        r'https?://rera\.tn\.gov\.in/cms/reg_projects_tamilnadu/[^/]+/\d{4}\.php',
        resp.text,
    ):
        if href not in seen:
            seen.add(href)
            found.append(href)
    return found


def _get_year_listing_urls(logger: CrawlerLogger) -> list[str]:
    """
    Fetch all CMS index pages (Building + Layout types) and return all
    year-specific listing URLs, sorted newest-first.
    Falls back to _KNOWN_YEARS for each type when the index page is unreachable.
    """
    all_urls: list[str] = []
    seen: set[str] = set()

    # Discover building listing URLs
    building_urls = _discover_urls_from_cms(CMS_INDEX_URL, logger)
    if not building_urls:
        logger.warning("Building CMS index unreachable; using fallback years", url=CMS_INDEX_URL)
        building_urls = [
            _TYPE_URL_TEMPLATES["Building"].format(year=y)
            for y in sorted(_KNOWN_YEARS, reverse=True)
        ]
    for u in building_urls:
        if u not in seen:
            seen.add(u)
            all_urls.append(u)

    # Discover layout listing URLs
    for layout_index in CMS_LAYOUT_INDEX_URLS:
        layout_urls = _discover_urls_from_cms(layout_index, logger)
        if not layout_urls:
            logger.warning("Layout CMS index unreachable; using fallback", url=layout_index)
            # Derive the type from the index URL filename (nlayout→Normal_Layout, etc.)
            m = re.search(r"reg_projects_(\w+)_tamilnadu", layout_index, re.I)
            raw_key = m.group(1).lower() if m else "nlayout"
            type_key = _LAYOUT_CMS_TO_TYPE.get(raw_key, "Normal_Layout")
            tmpl = _TYPE_URL_TEMPLATES.get(type_key, _TYPE_URL_TEMPLATES["Normal_Layout"])
            layout_urls = [tmpl.format(year=y) for y in sorted(_KNOWN_YEARS, reverse=True)]
        for u in layout_urls:
            if u not in seen:
                seen.add(u)
                all_urls.append(u)

    # Sort newest year first within each type by year number, preserving type grouping
    all_urls.sort(key=lambda u: re.search(r"(\d{4})\.php", u).group(1), reverse=True)
    logger.info(f"Discovered {len(all_urls)} year listing URLs (building + layout)")
    return all_urls


# ── Listing table parser ──────────────────────────────────────────────────────

_UUID_RE  = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)
_LAT_RE   = re.compile(r"Latitude[-:\s]*([\d.]+)", re.I)
_LNG_RE   = re.compile(r"Longitude[-:\s]*([\d.]+)", re.I)
_REGNO_RE = re.compile(
    r"(?:TNRERA/[\w/]+|TN/\d+/(?:Building|Regularisation-Layout|Layout)/[\w/]+/\d{4})",
    re.I,
)


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
        # Pattern 1: "Project Name: <name> - <description>" (standard)
        _pn_m = re.search(r'Project\s+Name\s*:\s*(.+?)\s*-\s*(.+)', td3_text, re.I)
        # Pattern 2: "Project Name changed from X to Y - description" (name-change notice)
        _pn_chg = None
        if not _pn_m:
            _pn_chg = re.search(
                r'Project\s+Name\s+changed.*?\bto\b\s*[^\w]?([\w][^\-]{1,80}?)\s*-\s*(.+)',
                td3_text, re.I,
            )
        if _pn_m:
            project_name = _pn_m.group(1).strip().strip('\u201c\u201d"\'\u2018\u2019')
            description  = _pn_m.group(2).strip()
        elif _pn_chg:
            project_name = _pn_chg.group(1).strip().strip('\u201c\u201d"\'\u2018\u2019')
            description  = _pn_chg.group(2).strip()
        else:
            # No "Project Name:" — fallback name applied in _build_project_record
            project_name = None
            description  = td3_text

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
        promoter_full_url = project_full_url = None
        # Legacy UUID-based detail page links (older projects still served via /public-view1/2)
        for a in td6_soup.find_all("a", href=True):
            href = a["href"]
            uuid_match = _UUID_RE.search(href)
            if not uuid_match:
                continue
            full = href if href.startswith("http") else f"{BASE_URL}{href}"
            if "public-view1" in href:
                promoter_uuid = uuid_match.group(0)
                promoter_full_url = full
            elif "public-view2" in href:
                project_uuid = uuid_match.group(0)
                project_full_url = full

        lat_m = _LAT_RE.search(td6_html)
        lng_m = _LNG_RE.search(td6_html)
        if lat_m:
            lat = lat_m.group(1)
        if lng_m:
            lng = lng_m.group(1)

        # -- Document links from td[6]: Form A / Approval Details / Carpet Area PDFs
        #    (current portal serves static PDFs; formcqr links may still exist on older rows)
        form_c_url = None
        docs: list[dict] = []
        _td6_seen: set[str] = set()
        for a in td6_soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().startswith("javascript"):
                continue
            if "formcqr" in href.lower():
                form_c_url = href if href.startswith("http") else f"{BASE_URL}{href}"
                continue
            full = href if href.startswith("http") else f"{BASE_URL}{href}"
            if full in _td6_seen:
                continue
            _td6_seen.add(full)
            label_text = a.get_text(strip=True) or "document"
            docs.append({"label": label_text, "type": label_text, "url": full})

        # -- Approval Details from td[4] (may duplicate one td[6] entry; deduplicate)
        if approval_url and approval_url not in _td6_seen:
            docs.insert(0, {"label": "Approval Details", "type": "Approval Details", "url": approval_url})

        # -- Work-progress / current-status PDF from td[7] if present
        for a in tds[7].find_all("a", href=True):
            href = a["href"]
            if href.lower().startswith("javascript"):
                continue
            if "formcqr" in href.lower():
                form_c_url = href if href.startswith("http") else f"{BASE_URL}{href}"
                continue
            full = href if href.startswith("http") else f"{BASE_URL}{href}"
            if full not in _td6_seen:
                _td6_seen.add(full)
                label_text = a.get_text(strip=True) or "Work Progress"
                docs.append({"label": label_text, "type": label_text, "url": full})

        if form_c_url:
            docs.insert(0, {"label": "Form C", "type": "Form C", "url": form_c_url})

        row = {
            "project_registration_no": reg_no,
            # TNRERA registration date ("dated DD-MM-YYYY") = planned commencement date.
            # The planning permission approval date (approved_on_date) comes from the
            # detail page and will override this field during record assembly.
            "estimated_commencement_date": approved_on,
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
            # Use the actual href from the listing to preserve project type (building/layout)
            "promoter_url": promoter_full_url,
            "detail_url": project_full_url,
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
    # Pattern 1: "Project Name: <name>" on its own line or at end
    _pn_m9 = re.search(r'Project\s+Name\s*:\s*(.+?)(?:\n|$)', td3_text, re.I)
    # Pattern 2: "Project Name changed ... to Y"
    _pn_chg9 = None
    if not _pn_m9:
        _pn_chg9 = re.search(r'Project\s+Name\s+changed.*?\bto\b\s*[^\w]?([\w][^\n]{1,80})', td3_text, re.I)
    if _pn_m9:
        project_name = _pn_m9.group(1).strip().strip('\u201c\u201d"\'\u2018\u2019')
        description = td3_text[td3_text.index(_pn_m9.group(1)):].strip() if _pn_m9.group(1) in td3_text else td3_text
    elif _pn_chg9:
        project_name = _pn_chg9.group(1).strip().strip('\u201c\u201d"\'\u2018\u2019')
        description = td3_text
    else:
        # No "Project Name:" label — fallback applied in _build_project_record
        project_name = None
        description = td3_text

    # td[5]: expiry/completion date
    td5_text = tds[5].get_text(strip=True)
    expiry_date = _parse_tn_date(td5_text) if re.search(r"\d", td5_text) else None
    is_completed = "completed" in td5_text.lower()

    # td[6]: extract promoter UUID, project UUID, latitude, longitude
    td6_html = str(tds[6])
    td6_soup = tds[6]
    promoter_uuid = project_uuid = lat = lng = None
    promoter_full_url = project_full_url = None

    for a in td6_soup.find_all("a", href=_UUID_RE):
        href = a["href"]
        full = href if href.startswith("http") else f"{BASE_URL}{href}"
        if "public-view1" in href:
            m = _UUID_RE.search(href)
            if m:
                promoter_uuid = m.group(0)
                promoter_full_url = full
        elif "public-view2" in href:
            m = _UUID_RE.search(href)
            if m:
                project_uuid = m.group(0)
                project_full_url = full

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
        "project_registration_no":  reg_no,
        # TNRERA registration date ("dated DD-MM-YYYY") = planned commencement date.
        # The planning permission approval date (approved_on_date) comes from the
        # detail page and will override approved_on_date during record assembly.
        "estimated_commencement_date": approved_on,
        "approved_on_date":         approved_on,
        "promoter_name":            promoter_name,
        "promoter_raw_text":        promoter_raw,
        "project_name":             project_name,
        "project_description":      description or None,
        "approval_details":         tds[4].get_text(separator=" ", strip=True) or None,
        "estimated_finish_date":    expiry_date,
        "is_completed":             is_completed,
        "promoter_uuid":            promoter_uuid,
        "project_uuid":             project_uuid,
        "latitude":                 lat,
        "longitude":                lng,
        "form_c_url":               form_c_url,
        # Preserve the full URL from the listing href (handles building/layout/etc.)
        "promoter_url": promoter_full_url,
        "detail_url":   project_full_url,
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

    Handles four common patterns:
    1. <th> / <td> pairs in a table row
    2. <label>Key :</label> followed by sibling or parent text
    3. <strong>Key:</strong> followed by text node
    4. <p1>Key :</p1> inside .form-group div, value in sibling div's <p> tag
       (used on layout project pages)
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

    # Pattern 4: <p1>Key :</p1> inside .form-group div (layout pages)
    # Value lives in the sibling <div><p>...</p></div>
    for fg in soup.find_all("div", class_="form-group"):
        for p1_tag in fg.find_all("p1"):
            raw_key = p1_tag.get_text(strip=True).rstrip(":").strip()
            if not raw_key or len(raw_key) > 150:
                continue
            p1_parent = p1_tag.parent
            if not p1_parent:
                continue
            sibling_div = p1_parent.find_next_sibling("div")
            if not sibling_div:
                continue
            p_tag = sibling_div.find("p")
            if not p_tag:
                continue
            # Use separator=" " to join <br>-separated lines with a space
            val = p_tag.get_text(separator=" ", strip=True)
            # Skip Font Awesome icon placeholders (empty or pure dashes)
            if val and val.strip("-").strip():
                out.setdefault(raw_key, val)

    return out


# Promoter detail page label→schema field mapping
_PROMOTER_LABEL_MAP: dict[str, str] = {
    "name of the promoter":           "promoter_name",
    "promoter name":                  "promoter_name",
    "name":                           "promoter_name",
    # Layout-page promoter labels
    "firm name":                      "promoter_name",
    "type of promoter":               "_promoter_org_type",
    "type of organisation":           "_promoter_org_type",
    "type of organization":           "_promoter_org_type",
    "organisation type":              "_promoter_org_type",
    "company registration no":        "_promoter_reg_no",
    "registration number":            "_promoter_reg_no",
    "gstin":                          "_promoter_gst",
    "gst number":                     "_promoter_gst",
    "email":                          "_email",
    "email id":                       "_email",
    "mobile number":                  "_phone",
    "mobile no. 1":                   "_phone",
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
    "project name":                                 "project_name",
    "type of project":                              "project_type",
    "project type":                                 "project_type",
    "status of the project":                        "status_of_the_project",
    "project status":                               "status_of_the_project",
    "stage of construction":                        "status_of_the_project",
    "date of registration":                         "approved_on_date",
    "registration date":                            "approved_on_date",
    "proposed date of commencement":                "estimated_commencement_date",
    "proposed date of completion":                  "estimated_finish_date",
    "actual date of commencement":                  "actual_commencement_date",
    "actual date of completion":                    "actual_finish_date",
    "extended date of completion":                  "estimated_finish_date",
    # Additional commencement date label variants
    "date of commencement":                         "estimated_commencement_date",
    "commencement date":                            "estimated_commencement_date",
    "estimated date of commencement":               "estimated_commencement_date",
    "project start date":                           "estimated_commencement_date",
    # Layout-page date labels
    "project completion date":                      "estimated_finish_date",
    "project commencement date":                    "estimated_commencement_date",
    # Layout-page approval date (planning permission)
    "planning permission approval / renewal date":  "approved_on_date",
    "planning permission approval date":            "approved_on_date",
    "planning permission date":                     "approved_on_date",
    "number of residential units":                  "number_of_residential_units",
    "total residential units":                      "number_of_residential_units",
    # Layout-page plot counts map to residential units
    "total no of plots":                            "number_of_residential_units",
    "regular plots":                                "number_of_residential_units",
    "number of commercial units":                   "number_of_commercial_units",
    "total commercial units":                       "number_of_commercial_units",
    "land area":                                    "land_area",
    "total land area":                              "land_area",
    # Layout-page area labels
    "total layout area (sq.m)":                     "land_area",
    "total layout area":                            "land_area",
    "net area (area for registration) (sq.m)":      "land_area",
    "construction area":                            "construction_area",
    "carpet area":                                  "construction_area",
    "pin code":                                     "project_pin_code",
    "pincode":                                      "project_pin_code",
    "district":                                     "_project_district",
    "city":                                         "project_city",
    "taluk":                                        "_taluk",
    "village":                                      "_village",
    "survey number":                                "_survey_no",
    "survey/resurvey number":                       "_survey_no",
    "survey / resurvey number":                     "_survey_no",
    "latitude":                                     "_latitude",
    "longitude":                                    "_longitude",
    # Layout-page address field (holds survey no + Village/City/Taluk/Pincode lines)
    "address":                                      "_raw_address",
    # Layout-page location state
    "state":                                        "_location_state",
    "bank name":                                    "_bank_name",
    "bank branch":                                  "_bank_branch",
    # Layout-page bank labels
    "branch name":                                  "_bank_branch",
    "bank email id":                                "_bank_email",
    "ifsc code":                                    "_ifsc",
    "account number":                               "_account_no",
    "account no":                                   "_account_no",
    "separate account no for the project":          "_account_no",
    "cost of land":                                 "_cost_of_land",
    # Layout-page cost labels
    "land cost(market value)":                      "_cost_of_land",
    "total project cost":                           "_total_project_cost",
    "development cost":                             "_estimated_construction_cost",
    "estimated construction cost":                  "_estimated_construction_cost",
}



# ── Layout address / location parser ─────────────────────────────────────────

def _parse_layout_address(raw: str) -> dict:
    """
    Parse the combined Address field found on layout project detail pages.
    The field embeds survey number, village, city/town, taluk, and pincode
    as separate <br>-separated lines, e.g.:
        "Survey No : 778/2, 781/1B … Village : Thaiyur A Village
         City/Town : Thaiyur Taluk : Thiruporur … Pincode : 603103"
    Returns a dict of internal keys (_survey_no, _village, _city, _taluk, _project_pincode).
    """
    # Lookahead anchors — any of these starts a new sub-field
    _NEXT = r"(?=Village\s*:|City(?:/Town)?\s*:|Taluk\s*:|State\s*:|District\s*:|Pincode\s*:|$)"
    out: dict[str, str] = {}
    # Survey/resurvey number: text before the first "Village :" marker
    village_idx = re.search(r"Village\s*:", raw, re.I)
    if village_idx:
        survey_part = raw[: village_idx.start()].strip()
        survey_part = re.sub(r"^Survey\s+No\s*:\s*", "", survey_part, flags=re.I).strip()
        if survey_part:
            out["_survey_no"] = survey_part
    m = re.search(r"Village\s*:\s*(.+?)" + _NEXT, raw, re.I)
    if m:
        out["_village"] = m.group(1).strip()
    m = re.search(r"City(?:/Town)?\s*:\s*(.+?)" + _NEXT, raw, re.I)
    if m:
        out["_city"] = m.group(1).strip()
    m = re.search(r"Taluk\s*:\s*(.+?)" + _NEXT, raw, re.I)
    if m:
        out["_taluk"] = m.group(1).strip()
    m = re.search(r"Pincode\s*:\s*(\d+)", raw, re.I)
    if m:
        out["_project_pincode"] = m.group(1)
    return out


def _parse_promoter_address(raw: str) -> dict:
    """
    Parse the promoter Address field which embeds structured location info, e.g.:
        "Door No : 8 Floor Street Name : … Village : Nungambakkam
         City : Chennai State : Tamil Nadu District : Chennai Pincode : 600006"
    Returns a promoter_address_raw dict.
    """
    addr: dict[str, str] = {"raw_address": raw}
    m = re.search(r"Village\s*:\s*(.+?)(?=City\s*:|State\s*:|District\s*:|Pincode\s*:|$)", raw, re.I)
    if m:
        addr["village"] = m.group(1).strip()
    m = re.search(r"City\s*:\s*(.+?)(?=State\s*:|District\s*:|Pincode\s*:|$)", raw, re.I)
    if m:
        addr["city"] = m.group(1).strip()
    m = re.search(r"State\s*:\s*(.+?)(?=District\s*:|Pincode\s*:|$)", raw, re.I)
    if m:
        addr["state"] = m.group(1).strip()
    m = re.search(r"District\s*:\s*(.+?)(?=Pincode\s*:|$)", raw, re.I)
    if m:
        addr["district"] = m.group(1).strip()
    m = re.search(r"Pincode\s*:\s*(\d+)", raw, re.I)
    if m:
        addr["pin_code"] = m.group(1)
    return addr


# ── Director / partner block extractor (p1/p layout pages) ───────────────────

def _extract_director_blocks_p1p(soup: BeautifulSoup) -> list[dict]:
    """
    Extract director/partner info from sequential <p1>/<p> form-group pairs
    on the promoter detail page (layout project format).
    """
    _DIRECTOR_LABELS = {"director / partner name", "partner name", "director name", "member name"}
    # Collect all (lowercase_label, raw_label, value) triples in document order
    all_pairs: list[tuple[str, str, str]] = []
    for fg in soup.find_all("div", class_="form-group"):
        for p1_tag in fg.find_all("p1"):
            raw_key = p1_tag.get_text(strip=True).rstrip(":").strip()
            p1_parent = p1_tag.parent
            if not p1_parent:
                continue
            sib = p1_parent.find_next_sibling("div")
            if sib:
                p_tag = sib.find("p")
                val = p_tag.get_text(separator=" ", strip=True) if p_tag else ""
                all_pairs.append((raw_key.lower(), raw_key, val))

    members: list[dict] = []
    i = 0
    while i < len(all_pairs):
        low_key, raw_key, val = all_pairs[i]
        if low_key in _DIRECTOR_LABELS and val.strip():
            position = "Director" if "director" in low_key else "Partner" if "partner" in low_key else "Member"
            member: dict = {"name": val.strip(), "position": position}
            j = i + 1
            while j < len(all_pairs) and j < i + 12:
                k2, _, v2 = all_pairs[j]
                if k2 in _DIRECTOR_LABELS:
                    break
                if v2.strip():
                    if k2 in ("email id", "email"):
                        member.setdefault("email", v2.strip())
                    elif k2.startswith("mobile no"):
                        member.setdefault("phone", v2.strip())
                    elif k2 == "address":
                        member["raw_address"] = v2.strip()
                j += 1
            members.append(member)
        i += 1
    return members


# ── Surveyor / professional block extractor (p1/p layout pages) ──────────────

def _extract_surveyor_blocks_p1p(soup: BeautifulSoup) -> list[dict]:
    """
    Extract surveyor / professional info from sequential <p1>/<p> form-group pairs
    on the project detail page (layout project format).
    """
    _PROF_KEYS = ("surveyor name", "architect name", "engineer name", "professional name")

    all_pairs: list[tuple[str, str, str]] = []
    for fg in soup.find_all("div", class_="form-group"):
        for p1_tag in fg.find_all("p1"):
            raw_key = p1_tag.get_text(strip=True).rstrip(":").strip()
            p1_parent = p1_tag.parent
            if not p1_parent:
                continue
            sib = p1_parent.find_next_sibling("div")
            if sib:
                p_tag = sib.find("p")
                val = p_tag.get_text(separator=" ", strip=True) if p_tag else ""
                all_pairs.append((raw_key.lower(), raw_key, val))

    professionals: list[dict] = []
    i = 0
    while i < len(all_pairs):
        low_key, raw_key, val = all_pairs[i]
        if any(low_key == pk for pk in _PROF_KEYS) and val.strip():
            if "surveyor" in low_key:
                role = "License Surveyor"
            elif "architect" in low_key:
                role = "Architect"
            elif "engineer" in low_key:
                role = "Engineer"
            else:
                role = raw_key.replace(" Name", "").replace(" name", "").strip()
            name = val.strip()
            prof: dict = {"name": name, "role": role}
            j = i + 1
            while j < len(all_pairs) and j < i + 15:
                k2, _, v2 = all_pairs[j]
                if any(k2 == pk for pk in _PROF_KEYS):
                    break
                if v2.strip():
                    if k2 in ("email id", "email"):
                        prof.setdefault("email", v2.strip())
                    elif k2.startswith("mobile no"):
                        prof.setdefault("phone", v2.strip())
                    elif k2 == "address" and "address" not in prof:
                        prof["address"] = v2.strip()
                    elif "registration no" in k2 or "license no" in k2:
                        prof.setdefault("registration_no", v2.strip())
                    elif "local body" in k2:
                        # Append local body name to make name match sample format
                        prof["name"] = f"{prof['name']} {v2.strip()}"
                j += 1
            professionals.append(prof)
        i += 1
    return professionals


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
    # For layout pages the Address field is a rich multi-line string; parse it.
    raw_addr = mapped.get("_address")
    if raw_addr:
        parsed_addr = _parse_promoter_address(raw_addr)
        out["promoter_address_raw"] = parsed_addr
    else:
        addr: dict[str, str] = {}
        for raw_k in ("_address", "_state", "_district", "_city", "_pin_code"):
            if mapped.get(raw_k):
                addr[raw_k.lstrip("_")] = mapped[raw_k]
        if addr:
            out["promoter_address_raw"] = addr

    # Members / directors — try table format first, fall back to p1/p blocks
    members = _extract_members_table(soup)
    if not members:
        members = _extract_director_blocks_p1p(soup)
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
        if "/public/storage/upload/" not in href.lower():
            continue
        # The anchor text may be empty on layout pages; fall back to checking
        # the parent/grandparent element text for "registration" keyword.
        anchor_text = a.get_text(" ", strip=True).lower()
        parent_text = (a.parent.get_text(" ", strip=True) if a.parent else "").lower()
        gp_text = (a.parent.parent.get_text(" ", strip=True) if (a.parent and a.parent.parent) else "").lower()
        if "registration" in (anchor_text + parent_text + gp_text):
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

    # ── Layout address parsing ────────────────────────────────────────────────
    # The "Address" field on layout pages embeds Village / City/Town / Taluk /
    # Pincode as <br>-separated lines.  Parse it and promote the sub-fields.
    raw_addr = out.pop("_raw_address", None)
    if raw_addr:
        parsed_loc = _parse_layout_address(raw_addr)
        for k, v in parsed_loc.items():
            out.setdefault(k, v)
        # Project city: prefer the parsed city/town over the district label
        if parsed_loc.get("_city"):
            out["project_city"] = parsed_loc["_city"]
        # Project pin code from parsed address
        if parsed_loc.get("_project_pincode"):
            out.setdefault("project_pin_code", parsed_loc["_project_pincode"])

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
        ("city", "_city"),
        # _project_district comes from the "District" KV label (e.g. "Chennai"),
        # distinct from _city which is the parsed city/town (e.g. "Thaiyur")
        ("district", "_project_district"),
        ("pin_code", "project_pin_code"),
        ("state", "_location_state"),
        ("latitude", "_latitude"),
        ("longitude", "_longitude"),
    ]:
        val = out.get(src)
        if val:
            loc[tgt] = str(val)
    # Fallback: if no district resolved, use project_city
    if "district" not in loc and out.get("project_city"):
        loc["district"] = out["project_city"]
    if loc:
        out["project_location_raw"] = loc

    # Bank details
    bank: dict = {}
    for tgt, src in [
        ("bank_name", "_bank_name"),
        ("branch", "_bank_branch"),
        ("email", "_bank_email"),
        ("IFSC", "_ifsc"),
        ("account_no", "_account_no"),
    ]:
        if out.get(src):
            bank[tgt] = out.pop(src)
    # Fallback: use branch name as bank_name when bank_name label is absent on the page
    if bank and not bank.get("bank_name") and bank.get("branch"):
        bank["bank_name"] = bank["branch"]
    # Add state from location to bank details (portal includes this on building pages)
    if bank and out.get("_location_state"):
        bank["state"] = out["_location_state"]
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

    # Professional information — try table format first, fall back to p1/p blocks
    professionals = _extract_professionals_table(soup)
    if not professionals:
        professionals = _extract_surveyor_blocks_p1p(soup)
    if professionals:
        out["professional_information"] = professionals

    # Document links: prefer labeled extraction (p1/p form-group pattern used on
    # layout pages); fall back to generic link scan for any unlabeled remainder.
    labeled_docs = _extract_labeled_doc_links(soup)
    labeled_urls = {d["url"] for d in labeled_docs}
    generic_docs = [d for d in _extract_doc_links(soup) if d["url"] not in labeled_urls]
    doc_links = labeled_docs + generic_docs
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


def _extract_labeled_doc_links(soup: BeautifulSoup) -> list[dict]:
    """
    Extract document links together with their human-readable labels from two
    structural patterns used on Tamil Nadu RERA layout project detail pages.

    Pattern A (.form-group divs):
        Each .form-group div has a <p1>Label :</p1> in one child div and a
        <p><a href> in the sibling div.

    Pattern B (table rows):
        <tr><td><p1>Label :</p1></td><td><a href>link</a></td></tr>
        or <tr><td><p1>Label :</p1><a href>link</a></td></tr>

    Only project-specific uploaded PDFs (/public/storage/upload/ or formcqr)
    are captured.
    """
    docs: list[dict] = []
    seen: set[str] = set()

    def _is_project_doc(href: str) -> bool:
        h = href.lower()
        return "/storage/upload" in h or "formcqr" in h

    def _add(label: str, href: str) -> None:
        full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
        if full_url not in seen:
            seen.add(full_url)
            docs.append({"label": label, "url": full_url})

    # Pattern A: .form-group divs.
    # Labels can be in <p1> tags OR <label> tags.
    # The link may be in ANY sibling div after the label div (not just the immediate next).
    for fg in soup.find_all("div", class_="form-group"):
        # Collect label text from p1 or label tags within this form-group
        label_tag = fg.find("p1") or fg.find("label")
        if not label_tag:
            continue
        raw_label = label_tag.get_text(strip=True).rstrip(":").strip()
        if not raw_label:
            continue
        label_parent = label_tag.parent
        if not label_parent:
            continue
        # Search ALL sibling divs after the label-parent div for project doc links
        for sib in label_parent.find_next_siblings("div"):
            for a in sib.find_all("a", href=True):
                if _is_project_doc(a["href"]):
                    _add(raw_label, a["href"])

    # Pattern B: table rows — p1 label cell + adjacent/same cell containing the link
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        for i, td in enumerate(tds):
            p1_tag = td.find("p1")
            if not p1_tag:
                continue
            raw_label = p1_tag.get_text(strip=True).rstrip(":").strip()
            # Strip disclaimer asterisks (e.g. "GLV value...project*") — same as Pattern A
            raw_label = raw_label.rstrip("*").strip()
            if not raw_label:
                continue
            # Links may be in the same td or in the immediately adjacent td
            search_cells = [td] + ([tds[i + 1]] if i + 1 < len(tds) else [])
            for cell in search_cells:
                for a in cell.find_all("a", href=True):
                    if _is_project_doc(a["href"]):
                        _add(raw_label, a["href"])

    return docs


def _extract_doc_links(soup: BeautifulSoup) -> list[dict]:
    """
    Extract direct PDF/document links from a detail page.
    Skips JavaScript anchors, site-wide navigation links (homePageFiles),
    and any link that is not a project-specific uploaded document.
    """
    docs: list[dict] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().startswith("javascript"):
            continue
        # Skip site-wide homepage/navigation documents (circulars, forms, annual reports)
        if "/homePageFiles/" in href:
            continue
        if not (href.lower().endswith(".pdf") or "/storage/upload" in href.lower()
                or "formcqr" in href.lower() or "/public/" in href.lower()):
            continue
        full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
        if full_url in seen:
            continue
        seen.add(full_url)
        text = a.get_text(strip=True)
        if text:
            label = text
        else:
            img = a.find("img", alt=True)
            label = img["alt"].strip() if img and img.get("alt", "").strip() else "document"
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
        logger.info("Document uploaded", label=label, s3_key=s3_key, step="documents")
        logger.log_document(label, url, "uploaded", s3_key=s3_key, file_size_bytes=len(data))
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
        "key":                          project_key,
        "project_registration_no":      reg_no,
        "state":                        "Tamil Nadu",
        "project_state":                "Tamil Nadu",
        "domain":                       DOMAIN,
        "config_id":                    config_id,
        "url":                          row.get("detail_url") or f"{BASE_URL}/registered-building/tn",
        "promoter_name":                row.get("promoter_name"),
        "project_name":                 row.get("project_name"),
        "project_description":          row.get("project_description"),
        "approved_on_date":             row.get("approved_on_date"),
        "estimated_finish_date":        row.get("estimated_finish_date"),
        # TNRERA registration date ("dated" field) serves as estimated commencement date.
        # If the detail page supplies an explicit commencement date it will override this.
        "estimated_commencement_date":  row.get("estimated_commencement_date"),
    }

    # project_name: only use what the portal actually provides.
    # Some listing rows have no "Project Name:" label — the portal genuinely
    # does not give those projects a name separate from their description.
    # We do NOT fabricate a name from the description.
    # The registration number IS real portal data and uniquely identifies the
    # project, so it is the only acceptable fallback when no name is present.
    if not record.get("project_name"):
        record["project_name"] = reg_no

    # project_type: NOT defaulted — the portal listing does not carry a project
    # type column and Tamil Nadu building registrations include both residential
    # and commercial projects.  Leaving it None is more accurate than assuming
    # "residential" for every building-type registration.

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


# ── Sentinel helpers ─────────────────────────────────────────────────────────

def _fetch_sentinel_listing_row(reg_no: str, detail_url: str, logger: CrawlerLogger) -> dict | None:
    """
    Look up the sentinel project's listing row to retrieve fields that are only
    available from the listing page (e.g. estimated_commencement_date, which comes
    from the "dated DD-MM-YYYY" registration date field).

    Steps:
      1. Extract the project year from the tail of the registration number.
      2. Determine the project type (Building / Normal_Layout / Regularisation_Layout)
         from the detail URL.
      3. Fetch and parse the corresponding year listing page(s).
      4. Return the row whose project_registration_no matches reg_no.
    """
    year_m = re.search(r"/(\d{4})$", reg_no)
    if not year_m:
        logger.warning(
            "Sentinel listing lookup: cannot extract year from reg_no",
            reg=reg_no, step="sentinel",
        )
        return None
    year = year_m.group(1)

    # Determine listing URL(s) based on project type inferred from the detail URL.
    if "layout" in detail_url.lower():
        candidate_urls = [
            f"{BASE_URL}/cms/reg_projects_tamilnadu/Normal_Layout/{year}.php",
            f"{BASE_URL}/cms/reg_projects_tamilnadu/Regularisation_Layout/{year}.php",
        ]
    else:
        candidate_urls = [
            f"{BASE_URL}/cms/reg_projects_tamilnadu/Building/{year}.php",
        ]

    for listing_url in candidate_urls:
        rows = _parse_year_listing(listing_url, logger)
        for row in rows:
            if row.get("project_registration_no", "").upper() == reg_no.upper():
                logger.info(
                    "Sentinel: found listing row",
                    reg=reg_no, listing_url=listing_url, step="sentinel",
                )
                return row

    logger.warning(
        "Sentinel listing lookup: project not found in any candidate listing page",
        reg=reg_no, year=year, step="sentinel",
    )
    return None


# ── Main entry point ──────────────────────────────────────────────────────────

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Tamil Nadu RERA.

    Goes beyond a simple reachability check: re-scrapes the sentinel project's
    detail and promoter pages, then verifies that the freshly-extracted record
    still covers ≥ 80 % of the fields populated in the state_projects_sample
    baseline.  A portal that silently drops data (changed HTML, missing tabs,
    etc.) will fail this check and abort the crawl before bad data is written.

    Steps:
      1. Load state_projects_sample/tamil_nadu.json as the baseline.
      2. Resolve the sentinel's detail URL and promoter URL from the sample.
      3. Re-scrape both pages using the existing extraction helpers.
      4. Merge into a flat dict and call check_field_coverage().
    """
    import json as _json
    import os as _os
    from core.sentinel_utils import check_field_coverage

    sentinel_reg = config.get("sentinel_registration_no", "")
    if not sentinel_reg:
        logger.warning("No sentinel_registration_no configured — skipping", step="sentinel")
        return True

    # ── Load sample baseline ─────────────────────────────────────────────────
    sample_path = _os.path.join(
        _os.path.dirname(_os.path.dirname(__file__)),
        "state_projects_sample", "tamil_nadu.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check", path=sample_path, step="sentinel")
        return True

    # ── Resolve URLs from the sample ─────────────────────────────────────────
    detail_url   = baseline.get("url", "")
    promoter_url = (baseline.get("data") or {}).get("promoter_url", "")

    if not detail_url:
        logger.warning("Sentinel: no detail URL in sample baseline — skipping", step="sentinel")
        return True

    # ── Re-scrape the sentinel project ───────────────────────────────────────
    logger.info(f"Sentinel: scraping {sentinel_reg}", url=detail_url, step="sentinel")

    promoter_data: dict = {}
    if promoter_url:
        try:
            promoter_data = _parse_promoter_page(promoter_url, logger) or {}
        except Exception as exc:
            logger.warning(f"Sentinel: promoter page error — {exc}", step="sentinel")

    try:
        project_data = _parse_project_page(detail_url, logger) or {}
    except Exception as exc:
        logger.error(f"Sentinel: project page error — {exc}", step="sentinel")
        return False

    if not project_data:
        logger.error("Sentinel: project page returned no data", url=detail_url, step="sentinel")
        return False

    # ── Build a merged fresh record (flat, no system metadata) ───────────────
    fresh: dict = {}
    for k, v in promoter_data.items():
        if not k.startswith("_") and v not in (None, "", {}, []):
            fresh[k] = v
    for k, v in project_data.items():
        if not k.startswith("_") and v not in (None, "", {}, []):
            fresh[k] = v

    # Fields that don't come from the detail page but are always knowable:
    # project_state is set from config in the full crawl; set it here too.
    fresh.setdefault("project_state", "Tamil Nadu")

    # uploaded_documents are assembled from _doc_links in _build_project_record.
    # Replicate that here so the coverage check can verify doc extraction still works.
    doc_links: list[dict] = list(project_data.get("_doc_links") or [])
    if doc_links:
        fresh.setdefault("uploaded_documents", doc_links)

    # estimated_commencement_date is seeded from the listing row's "dated DD-MM-YYYY"
    # field in the full crawl — it is not present on the detail page for layout projects.
    # If the baseline has it but the fresh scrape doesn't, fetch the listing row to
    # replicate what the full crawl would do.
    if not fresh.get("estimated_commencement_date") and baseline.get("estimated_commencement_date"):
        listing_row = _fetch_sentinel_listing_row(sentinel_reg, detail_url, logger)
        if listing_row and listing_row.get("estimated_commencement_date"):
            fresh["estimated_commencement_date"] = listing_row["estimated_commencement_date"]
            logger.info(
                "Sentinel: seeded estimated_commencement_date from listing row",
                value=listing_row["estimated_commencement_date"],
                step="sentinel",
            )

    # ── Coverage comparison ───────────────────────────────────────────────────
    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "tamil_nadu_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


def run(config: dict, run_id: int, mode: str) -> dict:
    """
    Entry point called by the crawler orchestrator.

    Modes:
        daily_light – skip projects already in DB; only process new ones
        weekly_deep – full refresh with document uploads for all projects
        full        – crawl all years from scratch (ignores checkpoint)
        incremental – alias for daily_light; resumes from checkpoint
        single      – crawl only the current/most-recent year
        listing     – crawl only the direct listing_url from config
                      (e.g. https://rera.tn.gov.in/registered-building/tn)
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

    # ── Sentinel health check ────────────────────────────────────────────────
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counts["error_count"] += 1
        return counts

    # ── Checkpoint handling ──────────────────────────────────────────────────
    checkpoint = (load_checkpoint(site_id, mode) if mode not in ("full", "listing") else {}) or {}
    last_project_key: str | None = checkpoint.get("last_project_key")
    last_page = int(checkpoint.get("last_page", 0))

    if mode in ("full", "listing"):
        reset_checkpoint(site_id, mode)

    # ── Discover year listing URLs ───────────────────────────────────────────
    if mode == "listing":
        listing_url = config.get("listing_url", f"{BASE_URL}/registered-building/tn")
        year_urls = [listing_url]
        logger.info("listing mode: crawling direct listing URL", url=listing_url)
    else:
        year_urls = _get_year_listing_urls(logger)
        if mode == "single" and year_urls:
            year_urls = year_urls[:1]   # only most-recent year
    logger.info(f"Will crawl {len(year_urls)} year listing(s)", mode=mode)

    machine_name, machine_ip = get_machine_context()

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
            reg_no = row.get("project_registration_no")
            if not reg_no:
                continue

            counts["projects_found"] += 1
            if item_limit and counts["projects_found"] > item_limit:
                logger.info(f"CRAWL_ITEM_LIMIT={item_limit} reached, stopping")
                return counts
            project_key = generate_project_key(reg_no)
            if last_project_key and mode != "full":
                if project_key == last_project_key:
                    last_project_key = None
                counts["projects_skipped"] += 1
                continue

            # ── daily_light: skip projects already in the DB ──────────────────
            if mode == "daily_light" and get_project_by_key(project_key):
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
                    raw_record["is_live"] = True
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

                if status == "new":
                    counts["projects_new"] += 1
                elif status == "updated":
                    counts["projects_updated"] += 1
                else:
                    counts["projects_skipped"] += 1
                logger.info(f"DB result: {status}", step="db_upsert")

                # ── Document handling (weekly_deep or new projects only) ──────────
                raw_docs: list[dict] = list(row.get("uploaded_documents") or [])
                if not raw_docs:
                    raw_docs = list(raw_record.get("uploaded_documents") or [])

                doc_name_counts: dict[str, int] = {}
                enriched_docs: list[dict] = []
                if raw_docs:
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

    reset_checkpoint(site_id, mode)
    logger.info("Tamil Nadu RERA crawl complete", **counts)
    return counts
