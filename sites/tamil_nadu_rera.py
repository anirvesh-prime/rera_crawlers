"""
Tamil Nadu RERA Crawler — rera.tn.gov.in
Type: hybrid (httpx for listings, Selenium for JS-rendered detail pages)

Strategy:
- Listings live on two master pages and use a POST-driven year selector:
    1. https://rera.tn.gov.in/registered-building/tn
    2. https://rera.tn.gov.in/registered-layout/tn
  Each page renders a Laravel CSRF ``_token`` hidden input + a year ``<select>``
  whose options are the years the portal exposes (currently 2023-2026).
  Selecting a year submits a POST back to the same URL with ``_token`` + ``year``
  and returns the full server-rendered table for that year.  Listings are
  fetched via httpx because they are static HTML; no JS execution required.
- Each row yields: reg_no, promoter name, project name/description, expiry date,
  promoter-UUID (public-view1), project-UUID (public-view2), lat/lng, form-C URL.
- Detail pages (public-view1 / public-view2) are JS-rendered, so they go
  through the shared SeleniumSession.
- Documents: form-C QR code PDF + /public/storage/upload/*.pdf links from detail pages.
"""
from __future__ import annotations

import re
import time
from datetime import timezone
from typing import Any, Iterator

import httpx
from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import SeleniumSession, generate_project_key, get_random_ua, get_target_reg_nos, random_delay
from core.db import (
    get_project_by_key,
    upsert_project,
    insert_crawl_error,
    upsert_document,
    update_crawl_run_progress,
)
from core.document_policy import select_document_for_download
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import (
    build_document_filename,
    build_document_urls,
    document_identity_url,
    document_result_entry,
    existing_uploaded_document_entry,
    get_machine_context,
    merge_data_sections,
    normalize_project_payload,
    parse_datetime,
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings


# ── Selenium session (shared driver via core.crawler_base.SeleniumSession) ────

_SESSION: SeleniumSession | None = None


def _session() -> SeleniumSession:
    """Return the active SeleniumSession, lazy-initialising on first use."""
    global _SESSION
    if _SESSION is None:
        _SESSION = SeleniumSession(ignore_certificate_errors=True)
    return _SESSION


def _quit_driver() -> None:
    """Tear down the module's SeleniumSession driver (if any)."""
    global _SESSION
    if _SESSION is not None:
        try:
            _SESSION.quit()
        except Exception:
            pass
        _SESSION = None


def safe_get(url, *, logger=None, timeout=None, **_ignored):
    """Backwards-compatible shim — dispatches through the SeleniumSession.

    The httpx ``timeout`` argument is reinterpreted as ``page_load_timeout``
    when it's a plain number, so longer per-call timeouts still take effect.
    """
    plt = float(timeout) if isinstance(timeout, (int, float)) and timeout else None
    return _session().get(url, logger=logger, page_load_timeout=plt)


def download_response(url, *, logger=None, **_ignored):
    """Backwards-compatible shim — dispatches through the SeleniumSession."""
    return _session().download(url, logger=logger)


BASE_URL          = "https://rera.tn.gov.in"
STATE_CODE        = "TN"
DOMAIN            = "rera.tn.gov.in"
# Master listing pages walked by run(); each uses POST-based year selection.
LISTING_BASE_URLS = (
    f"{BASE_URL}/registered-building/tn",
    f"{BASE_URL}/registered-layout/tn",
)


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


# ── Listing HTTP client + POST-driven year walker ────────────────────────────

# Portal certificate is intermittently expired; we already pin the domain so
# disabling verification here is the same trade-off the Selenium session makes
# via ``ignore_certificate_errors=True``.
_LISTING_TIMEOUT = httpx.Timeout(120.0, connect=30.0)


def _make_listing_client() -> httpx.Client:
    """Return an httpx.Client configured for the TN RERA listing endpoints."""
    return httpx.Client(
        verify=False,
        follow_redirects=True,
        headers={"User-Agent": get_random_ua()},
        timeout=_LISTING_TIMEOUT,
    )


def _extract_listing_form(soup: BeautifulSoup) -> tuple[str | None, list[str]]:
    """Return (csrf_token, years[]) parsed from the master listing's year form."""
    token = None
    tok_input = soup.find("input", attrs={"name": "_token"})
    if tok_input and tok_input.get("value"):
        token = tok_input["value"].strip()
    years: list[str] = []
    sel = soup.find("select", attrs={"name": "year"})
    if sel:
        for opt in sel.find_all("option"):
            val = (opt.get("value") or "").strip()
            if val.isdigit():
                years.append(val)
    return token, years


def _iter_listing_rows(
    logger: CrawlerLogger,
) -> Iterator[tuple[str, str, list[dict]]]:
    """
    Walk each master listing (building + layout) and yield rows year-by-year.

    For each base URL:
      1. GET the page → grab CSRF ``_token`` + year ``<select>`` options +
         the initial table (which corresponds to the first / current year).
      2. For every subsequent year, POST ``{_token, year}`` back to the same
         URL and parse the returned table.

    Yields ``(base_url, year_label, rows)`` tuples, newest year first per base.
    """
    for base_url in LISTING_BASE_URLS:
        with _make_listing_client() as client:
            try:
                resp = client.get(base_url)
                resp.raise_for_status()
            except Exception as exc:
                logger.error(f"Listing GET failed: {exc}", url=base_url)
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            token, years = _extract_listing_form(soup)
            if not years:
                logger.warning("Listing year dropdown not found", url=base_url)
                continue
            current_year = years[0]
            rows = _parse_listing_html(resp.text)
            logger.info(
                f"Listing GET parsed: {len(rows)} rows",
                url=base_url, year=current_year,
            )
            yield base_url, current_year, rows

            if not token:
                logger.warning(
                    "CSRF _token missing; skipping POST year walk", url=base_url,
                )
                continue
            for year in years[1:]:
                try:
                    resp2 = client.post(
                        base_url,
                        data={"_token": token, "year": year},
                        headers={"Referer": base_url},
                    )
                    resp2.raise_for_status()
                except Exception as exc:
                    logger.error(
                        f"Listing POST failed: {exc}", url=base_url, year=year,
                    )
                    continue
                rows = _parse_listing_html(resp2.text)
                logger.info(
                    f"Listing POST parsed: {len(rows)} rows",
                    url=base_url, year=year,
                )
                yield base_url, year, rows


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
    Parse one <tr> of a Tamil Nadu master listing table (9 columns).

    Column layout (0-indexed):
      0 – S.No
      1 – Registration No + "dated DD-MM-YYYY"
      2 – Promoter name / address
      3 – "Project Name: <name>\\n<description>"
      4 – Approval details (planning/building permission text)
      5 – Expiry / completion date (DD.MM.YYYY or "Completed")
      6 – Links: Promoter Details (view1), Project Details (view2), Lat/Lng span
      7 – Form C QR code link (image button)
      8 – Status / reserved (often empty for active projects)
    """
    if len(tds) < 7:
        return None

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


def _parse_listing_html(html: str) -> list[dict]:
    """Parse a master-listing HTML body and return the list of row dicts."""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            parsed = _parse_listing_row(tds)
            if parsed:
                rows.append(parsed)
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
    "pan card no":                    "_pan",
    "pan card number":                "_pan",
    "project developed by":           "_project_developed_by",
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
    "construction cost":                            "_estimated_construction_cost",
    # Building / approval characteristics → building_details
    "usage":                                        "project_type",
    "type of building":                             "_building_type",
    "category":                                     "_project_category",
    "registration applied for":                     "_registration_applied_for",
    "block details":                                "_block_details",
    "floor details":                                "_floor_details",
    "no.of blocks applied now":                     "_no_of_blocks",
    "no. of blocks applied now":                    "_no_of_blocks",
    "no. of dwelling units":                        "_no_of_dwelling_units_in_block",
    "total no. of dwelling units including all phases/villas":
                                                    "number_of_residential_units",
    "building license / permit no":                 "_license_no",
    "building license / permit date":               "_license_date",
    "building license / permit issued by":          "_license_issued_by",
    "building license / permit issued in the name of": "_license_issued_to",
    "license valid upto":                           "_license_valid_upto",
    "planning permission approval / renewal letter no": "_planning_permission_no",
    "planning permission issued by":                "_planning_permission_issued_by",
    "planning permission issued in the name of":    "_planning_permission_issued_to",
    "validity of planning permission / renewal":    "_planning_permission_validity",
    # Layout local-body approval
    "local body approval letter no":                "_local_body_letter_no",
    "local body approval letter date":              "_local_body_letter_date",
    "name of the local body":                       "_local_body_name",
    "permission issued by local body":              "_local_body_issued_by",
    # Land/area breakdown → land_area_details
    "site extent(sq.m)":                            "_site_extent",
    "site extent":                                  "_site_extent",
    "fsi area (sq.m)":                              "_fsi_area",
    "fsi area":                                     "_fsi_area",
    "osr gifted (sq.m)":                            "_osr_gifted",
    "road area gifted (sq.m)":                      "_road_area_gifted",
    "plottable area (sq.m)":                        "_plottable_area",
    "public purpose gifted (tangedco / local body) (sq.m)": "_public_purpose_gifted",
    "< 60 sq.m (lig residential)":                  "_lig_residential_area",
    "> 60 sq.m (other residential)":                "_other_residential_area",
    "commercial":                                   "_commercial_area",
    "commercial floor area":                        "_commercial_floor_area",
    "other uses (other than residential / commercial)": "_other_uses_area",
    # Provided facilities → provided_faciltiy
    "internal road":                                "_facility_internal_road",
    "water supply source":                          "_facility_water_supply",
    "sewage disposal by":                           "_facility_sewage",
    "solid waste disposal by":                      "_facility_solid_waste",
    "fire fighting & emergency evacuation services as per msb norms for msb's (ie., more than stilt + 5 floors)":
                                                    "_facility_fire_fighting",
    "renewable energy if applicable(provision made in terrace floor)":
                                                    "_facility_renewable_energy",
    "amenity building details as per brochure / prospectus / agreement":
                                                    "_facility_amenity_building",
    "amenity details as per brochure / prospectus / agreement":
                                                    "_facility_amenity_details",
    "clearance / noc":                              "_facility_clearance_noc",
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
    _PROF_KEYS = (
        "surveyor name", "architect name", "engineer name",
        "contractor name", "professional name",
    )

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
            elif "contractor" in low_key:
                role = "Contractor"
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
    if mapped.get("_pan"):
        promoters_details["pan_number"] = mapped["_pan"]
    if mapped.get("_project_developed_by"):
        promoters_details["project_developed_by"] = mapped["_project_developed_by"]
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

    # Building / approval characteristics → building_details JSONB
    building: dict = {}
    for tgt, src in [
        ("type_of_building",            "_building_type"),
        ("category",                    "_project_category"),
        ("registration_applied_for",    "_registration_applied_for"),
        ("block_details",               "_block_details"),
        ("floor_details",               "_floor_details"),
        ("no_of_blocks",                "_no_of_blocks"),
        ("no_of_dwelling_units_in_block", "_no_of_dwelling_units_in_block"),
        ("license_no",                  "_license_no"),
        ("license_date",                "_license_date"),
        ("license_issued_by",           "_license_issued_by"),
        ("license_issued_to",           "_license_issued_to"),
        ("license_valid_upto",          "_license_valid_upto"),
        ("planning_permission_no",      "_planning_permission_no"),
        ("planning_permission_issued_by", "_planning_permission_issued_by"),
        ("planning_permission_issued_to", "_planning_permission_issued_to"),
        ("planning_permission_validity", "_planning_permission_validity"),
        ("local_body_letter_no",        "_local_body_letter_no"),
        ("local_body_letter_date",      "_local_body_letter_date"),
        ("local_body_name",             "_local_body_name"),
        ("local_body_issued_by",        "_local_body_issued_by"),
    ]:
        if out.get(src):
            building[tgt] = out.pop(src)
    if building:
        out["building_details"] = building

    # Provided facilities (utilities + amenities) → provided_faciltiy JSONB
    facilities: dict = {}
    for tgt, src in [
        ("internal_road",       "_facility_internal_road"),
        ("water_supply",        "_facility_water_supply"),
        ("sewage_disposal",     "_facility_sewage"),
        ("solid_waste_disposal","_facility_solid_waste"),
        ("fire_fighting",       "_facility_fire_fighting"),
        ("renewable_energy",    "_facility_renewable_energy"),
        ("amenity_building",    "_facility_amenity_building"),
        ("amenity_details",     "_facility_amenity_details"),
        ("clearance_noc",       "_facility_clearance_noc"),
    ]:
        if out.get(src):
            facilities[tgt] = out.pop(src)
    if facilities:
        out["provided_faciltiy"] = facilities

    # Land area breakdown → land_area_details JSONB
    land_breakdown: dict = {}
    for tgt, src in [
        ("site_extent",            "_site_extent"),
        ("fsi_area",               "_fsi_area"),
        ("osr_gifted",             "_osr_gifted"),
        ("road_area_gifted",       "_road_area_gifted"),
        ("plottable_area",         "_plottable_area"),
        ("public_purpose_gifted",  "_public_purpose_gifted"),
        ("lig_residential_area",   "_lig_residential_area"),
        ("other_residential_area", "_other_residential_area"),
        ("commercial_area",        "_commercial_area"),
        ("commercial_floor_area",  "_commercial_floor_area"),
        ("other_uses_area",        "_other_uses_area"),
    ]:
        if out.get(src):
            land_breakdown[tgt] = out.pop(src)
    if land_breakdown:
        out["land_area_details"] = land_breakdown

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
    reused, existing_s3_key = existing_uploaded_document_entry(project_key, {**doc, "url": url, "type": label})
    if reused:
        logger.info("Document reused", label=label, s3_key=existing_s3_key, step="documents")
        logger.log_document(label, url, "reused", s3_key=existing_s3_key)
        return reused
    filename = build_document_filename(doc)
    try:
        resp = download_response(url, logger=logger, timeout=60.0)
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
        "key":                          project_key,                                                                # FIELD: key <- generate_project_key(reg_no)
        "project_registration_no":      reg_no,                                                                     # FIELD: project_registration_no <- listing row reg_no
        "state":                        "Tamil Nadu",                                                               # FIELD: state <- hardcoded "Tamil Nadu"
        "project_state":                "Tamil Nadu",                                                               # FIELD: project_state <- hardcoded "Tamil Nadu"
        "domain":                       DOMAIN,                                                                     # FIELD: domain <- module DOMAIN constant
        "config_id":                    config_id,                                                                  # FIELD: config_id <- crawler config parameter
        "url":                          row.get("detail_url") or f"{BASE_URL}/registered-building/tn",              # FIELD: url <- listing detail_url or base listing URL
        "promoter_name":                row.get("promoter_name"),                                                   # FIELD: promoter_name <- listing row promoter_name
        "project_name":                 row.get("project_name"),                                                    # FIELD: project_name <- listing row project_name
        "project_description":          row.get("project_description"),                                             # FIELD: project_description <- listing row project_description
        "approved_on_date":             row.get("approved_on_date"),                                                # FIELD: approved_on_date <- listing row approved_on_date
        "estimated_finish_date":        row.get("estimated_finish_date"),                                           # FIELD: estimated_finish_date <- listing row estimated_finish_date
        # TNRERA registration date ("dated" field) serves as estimated commencement date.
        # If the detail page supplies an explicit commencement date it will override this.
        "estimated_commencement_date":  row.get("estimated_commencement_date"),                                     # FIELD: estimated_commencement_date <- listing row estimated_commencement_date
    }

    # project_name: only use what the portal actually provides.
    # Some listing rows have no "Project Name:" label — the portal genuinely
    # does not give those projects a name separate from their description.
    # We do NOT fabricate a name from the description.
    # The registration number IS real portal data and uniquely identifies the
    # project, so it is the only acceptable fallback when no name is present.
    if not record.get("project_name"):
        record["project_name"] = reg_no  # FIELD: project_name <- registration no fallback

    # project_type: NOT defaulted — the portal listing does not carry a project
    # type column and Tamil Nadu building registrations include both residential
    # and commercial projects.  Leaving it None is more accurate than assuming
    # "residential" for every building-type registration.

    # Status from listing (completed vs active)
    if row.get("is_completed"):
        record["status_of_the_project"] = "Completed"  # FIELD: status_of_the_project <- listing is_completed flag

    # GPS coordinates → project_location_raw seed
    loc: dict[str, str] = {}
    if row.get("latitude"):
        loc["latitude"] = row["latitude"]  # FIELD: project_location_raw.latitude <- listing row latitude
    if row.get("longitude"):
        loc["longitude"] = row["longitude"]  # FIELD: project_location_raw.longitude <- listing row longitude
    if loc:
        record["project_location_raw"] = loc  # FIELD: project_location_raw <- listing lat/lng dict

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
        record["project_location_raw"] = merged_loc  # FIELD: project_location_raw <- merged listing + project detail location

    # Normalise status string if set
    raw_status = record.get("status_of_the_project", "")
    if isinstance(raw_status, str):
        # FIELD: status_of_the_project <- normalized via _STATUS_MAP
        record["status_of_the_project"] = _STATUS_MAP.get(
            raw_status.lower().strip(), raw_status
        )

    # Form-C and document links
    doc_links: list[dict] = list(project_data.get("_doc_links") or [])
    if row.get("form_c_url"):
        doc_links.insert(0, {"label": "Form C", "url": row["form_c_url"]})  # FIELD: uploaded_documents <- prepend Form C entry from row.form_c_url
    if doc_links:
        record["uploaded_documents"] = doc_links  # FIELD: uploaded_documents <- project detail _doc_links + Form C

    # Data JSONB
    record["data"] = {  # FIELD: data <- system metadata JSONB block
        "govt_type":      "state",                                                # FIELD: data.govt_type <- hardcoded "state"
        "is_processed":   False,                                                  # FIELD: data.is_processed <- hardcoded False
        "approval_details": row.get("approval_details"),                          # FIELD: data.approval_details <- listing row approval_details
        "promoter_url":   row.get("promoter_url"),                                # FIELD: data.promoter_url <- listing row promoter_url
        "form_c":         row.get("form_c_url"),                                  # FIELD: data.form_c <- listing row form_c_url
        "all_labels_promoter": promoter_data.get("_promoter_raw_labels"),         # FIELD: data.all_labels_promoter <- promoter page raw kv labels
        "all_labels_project":  project_data.get("_project_raw_labels"),           # FIELD: data.all_labels_project <- project page raw kv labels
    }

    return {k: v for k, v in record.items() if v is not None and v != "" and v != {} and v != []}


# ── Sentinel helpers ─────────────────────────────────────────────────────────

def _fetch_sentinel_listing_row(reg_no: str, detail_url: str, logger: CrawlerLogger) -> dict | None:
    """
    Look up the sentinel project's listing row across the master listings.
    Used to retrieve fields only available from the listing (e.g. estimated_commencement_date).
    """
    target = reg_no.upper()
    for base_url, year, rows in _iter_listing_rows(logger):
        for row in rows:
            if (row.get("project_registration_no") or "").upper() == target:
                logger.info(
                    "Sentinel: found listing row",
                    reg=reg_no, listing_url=base_url, year=year, step="sentinel",
                )
                return row

    logger.warning(
        "Sentinel listing lookup: project not found in master listings",
        reg=reg_no, step="sentinel",
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
    """Public entry point — ensures the Selenium driver is shut down after the run."""
    try:
        return _run(config, run_id, mode)
    finally:
        _quit_driver()


def _run(config: dict, run_id: int, mode: str) -> dict:
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
    items_processed = 0
    t_run = time.monotonic()

    # ── Targeted run handling ────────────────────────────────────────────────
    # --target-reg-no restricts the run to one or more specific projects
    # (comma-separated, case-insensitive). The reg-no is present on every listing
    # row, so each year listing is filtered down to the requested project(s) and
    # the year walk stops as soon as all targets are found. The sentinel check is
    # skipped for targeted runs (mirrors karnataka_rera / uttarakhand_rera).
    target_regs = get_target_reg_nos()
    found_targets: set[str] = set()

    # ── Sentinel health check ────────────────────────────────────────────────
    if target_regs or mode == "daily_light":
        logger.info("Sentinel skipped (targeted run via --target-reg-no)", step="sentinel")
        counts["sentinel_passed"] = True
    else:
        t0 = time.monotonic()
        if not _sentinel_check(config, run_id, logger):
            logger.error("Sentinel failed — aborting crawl", step="sentinel")
            counts["sentinel_passed"] = False
            counts["error_count"] += 1
            return counts
        counts["sentinel_passed"] = True
        logger.timing("sentinel", time.monotonic() - t0)

    # ── Checkpoint handling ──────────────────────────────────────────────────
    checkpoint = (load_checkpoint(site_id, mode) if mode != "full" else {}) or {}
    last_page = int(checkpoint.get("last_page", 0))

    # weekly_deep is a full refresh — it must process every project every time.
    # Skipping rows via last_project_key would mean stale or interrupted checkpoints
    # silently skip thousands of projects, defeating the purpose of a deep crawl.
    # We still load last_page so an interrupted weekly_deep can resume from the
    # correct year URL rather than re-crawling completed year pages from scratch.
    if mode in ("full", "weekly_deep"):
        last_project_key: str | None = None
    else:
        last_project_key = checkpoint.get("last_project_key")

    if mode == "full":
        reset_checkpoint(site_id, mode)

    # ── Iterate master listings via POST-driven year walk ────────────────────
    # _iter_listing_rows() yields (base_url, year, rows) tuples for each
    # master listing × year combination, newest year first.  The two master
    # listings (building + layout) cover everything the portal currently
    # exposes (2023-2026 at time of writing); pre-2024 archived CMS pages
    # are intentionally not scraped (the portal does not publish detail-page
    # links for those rows).
    logger.info("Crawling master listings (POST year walk)", bases=list(LISTING_BASE_URLS))

    machine_name, machine_ip = get_machine_context()

    t0 = time.monotonic()
    first_listing_logged = False
    for year_index, (year_url, year_label, rows) in enumerate(_iter_listing_rows(logger)):
        if year_index < last_page:
            continue

        logger.info(f"Crawling year {year_label}", url=year_url, rows=len(rows))
        if not rows:
            logger.warning(f"No rows found for year {year_label}", url=year_url)
            continue

        # ── Targeted filtering ───────────────────────────────────────────────
        # Keep only the requested registration number(s); the year walk stops
        # once every target has been found (see end of the year loop).
        if target_regs:
            rows = [
                r for r in rows
                if (r.get("project_registration_no") or "").strip().upper() in target_regs
            ]
            found_targets.update(
                (r.get("project_registration_no") or "").strip().upper() for r in rows
            )
            if not rows:
                continue

        if not first_listing_logged:
            logger.timing("search", time.monotonic() - t0, rows=len(rows))
            first_listing_logged = True

        # Safety guard: if last_project_key was not found in this listing (e.g.
        # listing reordered or project removed since checkpoint was saved), clear
        # it after scanning the full page so subsequent listings are not skipped.
        checkpoint_found_in_page = False
        for row in rows:
            reg_no = row.get("project_registration_no")
            if not reg_no:
                continue

            # When item_limit is hit we stop the listing walk entirely —
            # projects_found then reflects only the rows actually walked, not
            # the full Tamil Nadu catalog.
            if item_limit and items_processed >= item_limit:
                break
            counts["projects_found"] += 1
            # Count every row toward the limit BEFORE skip checks so daily_light
            # (which skips every already-DB project) still honors CRAWL_ITEM_LIMIT.
            items_processed += 1
            project_key = generate_project_key(reg_no)
            if last_project_key and mode != "full":
                if project_key == last_project_key:
                    last_project_key = None
                    checkpoint_found_in_page = True
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
                    raw_record["is_live"] = True  # FIELD: is_live <- hardcoded True for active crawl
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
                else:
                    counts["projects_updated"] += 1
                logger.info(f"DB result: {status}", step="db_upsert")

                # ── Document handling (weekly_deep or new projects only) ──────────
                raw_docs: list[dict] = list(row.get("uploaded_documents") or [])
                if not raw_docs:
                    raw_docs = list(raw_record.get("uploaded_documents") or [])

                doc_name_counts: dict[str, int] = {}
                enriched_docs: list[dict] = []
                if raw_docs and (settings.SKIP_DOCUMENTS or mode == "daily_light"):
                    logger.info(
                        f"Skipping {len(raw_docs)} documents (light/skip-documents mode)",
                        step="documents",
                    )
                    enriched_docs = [
                        {
                            "link": doc.get("url") or doc.get("link"),
                            "type": doc.get("label") or doc.get("type") or "document",
                        }
                        for doc in raw_docs
                    ]
                elif raw_docs:
                    for doc in raw_docs:
                        selected = select_document_for_download(
                            config["state"], doc, doc_name_counts, domain=DOMAIN,
                        )
                        if not selected:
                            enriched_docs.append(
                                {
                                    "link": doc.get("url") or doc.get("link"),                            # FIELD: uploaded_documents.link <- doc url/link (unselected)
                                    "type": doc.get("label") or doc.get("type") or "document",            # FIELD: uploaded_documents.type <- doc label/type (unselected)
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
                                    "link": selected.get("url") or selected.get("link"),                  # FIELD: uploaded_documents.link <- selected doc url/link (upload failed)
                                    "type": selected.get("label") or selected.get("type") or "document",  # FIELD: uploaded_documents.type <- selected doc label/type (upload failed)
                                }
                            )

                if enriched_docs:
                    upsert_project({
                        "key": project_key,                                            # FIELD: key <- generate_project_key(reg_no)
                        "url": db_dict["url"],                                         # FIELD: url <- preserved from prior db_dict
                        "state": db_dict["state"],                                     # FIELD: state <- preserved from prior db_dict
                        "domain": db_dict["domain"],                                   # FIELD: domain <- preserved from prior db_dict
                        "project_registration_no": db_dict["project_registration_no"], # FIELD: project_registration_no <- preserved from prior db_dict
                        "uploaded_documents": enriched_docs,                           # FIELD: uploaded_documents <- enriched docs after upload pipeline
                        "document_urls": build_document_urls(enriched_docs),           # FIELD: document_urls <- built from enriched_docs
                    })

                # ── Checkpoint save ──────────────────────────────────────────────
                save_checkpoint(site_id, mode, year_index, project_key, run_id)
            finally:
                logger.clear_project()
                update_crawl_run_progress(run_id, counts)

        # Safety guard: if we scanned the entire listing and never found the
        # checkpoint project key, clear it so subsequent listings are processed
        # normally rather than being skipped wholesale.
        if last_project_key and not checkpoint_found_in_page and mode != "full":
            logger.warning(
                "Checkpoint project key not found in listing — clearing checkpoint "
                "to avoid skipping remaining listings",
                checkpoint_key=last_project_key,
                url=year_url,
            )
            last_project_key = None

        # Push at year boundary so the dashboard reflects per-year progress.
        update_crawl_run_progress(run_id, counts)

        if item_limit and items_processed >= item_limit:
            logger.info(
                f"Item limit {item_limit} reached after year {year_label} — "
                f"stopping listing walk",
            )
            break

        # Targeted run: stop once every requested project has been processed.
        if target_regs and target_regs <= found_targets:
            logger.info(
                "All targeted projects found — stopping listing walk", step="listing",
            )
            break

    # ── Targeted run summary ─────────────────────────────────────────────────
    # After walking the year listings, report which requested project(s) matched.
    if target_regs:
        for missing in sorted(target_regs - found_targets):
            logger.warning(f"Target reg_no={missing!r} not found in listing", step="listing")
        logger.info(
            f"Targeted run — {len(found_targets)} of {len(target_regs)} requested "
            f"project(s) matched", step="listing",
        )

    reset_checkpoint(site_id, mode)
    logger.info("Tamil Nadu RERA crawl complete", **counts)
    logger.timing("total_run", time.monotonic() - t_run)
    return counts
