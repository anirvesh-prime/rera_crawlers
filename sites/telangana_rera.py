"""
Telangana RERA Crawler — rerait.telangana.gov.in
Type: Playwright (ASP.NET search form + CAPTCHA)

Strategy:
- Submit the search form at /SearchList/Search with CAPTCHA solved via captcha_solver.
- Parse the server-rendered results table; paginate via ASP.NET __doPostBack.
- For each row: extract the encrypted q-param (PrintPreview URL) and the base64
  data_cert that encodes ProjectID / AppID / UserID.
- Navigate to the PrintPreview page with Playwright; parse HTML for all fields.
- Derive stable registration number from the detail page, falling back to TG-{AppID}.
- Download selected documents and upload to S3.
"""
from __future__ import annotations

import base64
import re
import time
from datetime import timezone
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse, unquote

from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.captcha_solver import captcha_to_text
from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import (
    PlaywrightSession,
    generate_project_key,
    get_legacy_ssl_context,
    random_delay,
    safe_get,
)
from core.db import get_project_by_key, upsert_project, insert_crawl_error, upsert_document
from core.document_policy import select_document_for_download
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import (
    build_document_filename,
    build_document_urls,
    clean_string,
    document_identity_url,
    document_result_entry,
    get_machine_context,
    merge_data_sections,
    normalize_project_payload,
    parse_datetime,
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL    = "https://rerait.telangana.gov.in"
SEARCH_URL  = f"{BASE_URL}/SearchList/Search"
DOMAIN      = "rerait.telangana.gov.in"
STATE       = "telangana"
STATE_CODE  = "TS"

_CERT_CHAR_D     = 52   # CharacterD value for registration certificate download
_PREVIEW_CHAR_D  = 87   # CharacterD value for project preview PDF

_PAGE_TIMEOUT_MS   = 90_000
_NAV_TIMEOUT_MS    = 120_000
_CAPTCHA_MAX_TRIES = 5

_CAPTCHA_SELECTORS = [
    "img[src*='captcha' i]",
    "img[src*='Captcha' i]",
    "canvas",
    "#captchaImage",
    "img[id*='captcha' i]",
]

# Regex to strip trailing colons / whitespace from label text
_LABEL_RE = re.compile(r"[:.\s]+$")

# ── Utility helpers ────────────────────────────────────────────────────────────

def _clean(val: Any) -> str | None:
    """Whitespace-normalised, None-safe string."""
    return clean_string(val)


def _label(raw: str | None) -> str:
    """Normalise a label extracted from HTML."""
    if not raw:
        return ""
    return _LABEL_RE.sub("", raw).strip().lower()


def _decode_data_cert(b64: str) -> dict[str, str]:
    """
    Decode a base64-encoded query string like
    ProjectID=…&Division=…&UserID=…&RoleID=…&AppID=…&Action=…&CharacterD=…&ExtAppID=
    into a plain dict.
    """
    try:
        decoded = base64.b64decode(b64 + "==").decode("utf-8", errors="replace")
        return dict(pair.split("=", 1) for pair in decoded.split("&") if "=" in pair)
    except Exception:
        return {}


def _build_cert_url(params: dict[str, str], char_d: int) -> str:
    """Rebuild a GetShowCertificateFileContent URL with a specific CharacterD."""
    p = {**params, "CharacterD": str(char_d), "ExtAppID": ""}
    qs = "&".join(f"{k}={v}" for k, v in p.items())
    b64 = base64.b64encode(qs.encode()).decode().rstrip("=")
    return f"{BASE_URL}/SearchList/GetShowCertificateFileContent?QueryStringID={b64}"


def _build_preview_url(params: dict[str, str], char_d: int) -> str:
    """Rebuild a GetshowFileApplicationPreviewFileContent URL."""
    p = {**params, "CharacterD": str(char_d), "ExtAppID": ""}
    qs = "&".join(f"{k}={v}" for k, v in p.items())
    b64 = base64.b64encode(qs.encode()).decode().rstrip("=")
    return f"{BASE_URL}//SearchList/GetshowFileApplicationPreviewFileContent/{b64}"


def _extract_print_preview_url(tag: Any) -> str | None:
    """Return the PrintPreview URL embedded in an onclick or href attribute."""
    for attr in ("onclick", "href"):
        val = tag.get(attr, "") if hasattr(tag, "get") else ""
        m = re.search(r"PrintPreview[^'\"]*q=[^'\"]+", val)
        if m:
            raw = m.group(0)
            if not raw.startswith("http"):
                raw = BASE_URL + "/" + raw.lstrip("/")
            return raw
    return None


# ── CAPTCHA + search form ─────────────────────────────────────────────────────

def _solve_captcha(page: Any, logger: CrawlerLogger) -> str | None:
    """
    Extract and solve the CAPTCHA from the search page.

    Telangana's CAPTCHA is served as an <img id="captchaImage"> whose src is a
    dynamic server endpoint (/SearchList/SearchCaptcha).  The generic
    extract_captcha_source_from_page() returns that URL directly for <img>
    elements, but the solver server cannot download a session-bound URL.

    Instead we draw the rendered image onto a canvas inside the browser to get
    a self-contained base64 data URL — identical to the Punjab approach.
    """
    try:
        # Wait for the CAPTCHA image to be fully loaded/rendered
        page.wait_for_function(
            """() => {
                const img = document.querySelector('#captchaImage');
                return img && img.complete && img.naturalWidth > 0;
            }""",
            timeout=15_000,
        )
        data_url: str | None = page.evaluate(
            """() => {
                const img = document.querySelector('#captchaImage');
                if (!img) return null;
                const canvas = document.createElement('canvas');
                canvas.width  = img.naturalWidth  || img.width  || 158;
                canvas.height = img.naturalHeight || img.height || 48;
                const ctx = canvas.getContext('2d');
                ctx.drawImage(img, 0, 0);
                const url = canvas.toDataURL('image/png');
                return (url && url !== 'data:,') ? url : null;
            }"""
        )
        if not data_url:
            logger.warning("CAPTCHA canvas extraction returned empty", step="captcha")
            return None
        solved = (captcha_to_text(data_url, default_captcha_source="eprocure") or "").strip()
        if solved:
            return solved
        logger.warning("CAPTCHA solver returned empty text", step="captcha")
        return None
    except Exception as exc:
        logger.warning(f"CAPTCHA extraction failed: {exc}", step="captcha")
        return None


def _submit_search(page: Any, logger: CrawlerLogger) -> bool:
    """
    Fill and submit the Telangana RERA search form with no filters.
    Returns True if the results table appeared within the timeout.
    """
    for attempt in range(1, _CAPTCHA_MAX_TRIES + 1):
        try:
            logger.info(f"CAPTCHA attempt {attempt}/{_CAPTCHA_MAX_TRIES}", step="captcha")
            page.wait_for_selector(
                ", ".join(_CAPTCHA_SELECTORS),
                state="visible",
                timeout=_PAGE_TIMEOUT_MS,
            )
            captcha_text = _solve_captcha(page, logger)
            if not captcha_text:
                logger.warning("CAPTCHA solver returned empty text", step="captcha")
                page.reload(wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
                continue

            captcha_input = None
            for sel in ("input[name*='captcha' i]", "input[id*='captcha' i]",
                        "input[placeholder*='captcha' i]", "#txtCaptcha", "input[type='text']"):
                try:
                    captcha_input = page.wait_for_selector(sel, timeout=3_000)
                    if captcha_input:
                        break
                except Exception:
                    pass

            if not captcha_input:
                logger.warning("CAPTCHA input field not found", step="captcha")
                page.reload(wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
                continue

            captcha_input.fill(captcha_text)

            for btn_sel in (
                "input[value*='Search' i]",
                "button[type='submit']",
                "input[type='submit']",
                "#btnSearch",
            ):
                try:
                    btn = page.query_selector(btn_sel)
                    if btn:
                        btn.click()
                        break
                except Exception:
                    pass

            try:
                page.wait_for_selector("table", timeout=30_000)
                logger.info("Search submitted successfully", step="search")
                return True
            except Exception:
                logger.warning("No results table after submit; retrying", step="search")
                page.reload(wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)

        except Exception as exc:
            logger.warning(f"Search attempt {attempt} error: {exc}", step="search")
            try:
                page.reload(wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
            except Exception:
                pass

    return False


def _goto_next_page(page: Any) -> bool:
    """
    Click the ASP.NET postback 'next page' link in the pager if present.
    Returns True if a next-page link was found and clicked.
    """
    try:
        # Common ASP.NET pager patterns: a link with text '>' or 'Next'
        next_link = page.query_selector("a:text-matches('^>$|^Next$|^»$', 'i')")
        if not next_link:
            return False
        next_link.click()
        page.wait_for_load_state("networkidle", timeout=30_000)
        return True
    except Exception:
        return False


def _get_total_pages(page: Any) -> int:
    """Try to parse total page count from pager HTML; returns 1 if not found."""
    try:
        html = page.content()
        soup = BeautifulSoup(html, "lxml")
        pager = soup.find(lambda t: t.name in ("tr", "td", "div")
                          and re.search(r"\bPage\b.*\bof\b", t.get_text(), re.I))
        if pager:
            m = re.search(r"of\s+(\d+)", pager.get_text(), re.I)
            if m:
                return int(m.group(1))
        # Count page-number links
        numbers = []
        for a in soup.find_all("a"):
            t = a.get_text(strip=True)
            if re.fullmatch(r"\d+", t):
                numbers.append(int(t))
        return max(numbers) if numbers else 1
    except Exception:
        return 1


# ── Listing table parsing ─────────────────────────────────────────────────────

def _parse_listing_rows(html: str) -> list[dict]:
    """
    Parse the search results HTML table.
    Each row yields a dict with PrintPreview URL, data_cert, AppID, ProjectID, etc.
    """
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict] = []
    seen_app_ids: set[str] = set()

    for table in soup.find_all("table"):
        tbody = table.find("tbody") or table
        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if not cells:
                continue

            pp_url: str | None = None
            raw_cert: str | None = None

            for tag in list(tr.find_all(["a", "button", "input", "span"])) + [tr]:
                if pp_url is None:
                    pp_url = _extract_print_preview_url(tag)
                for attr in ("href", "onclick", "data-url"):
                    v = str(tag.get(attr, "") or "") if hasattr(tag, "get") else ""
                    m = re.search(r"QueryStringID=([A-Za-z0-9+/=]+)", v)
                    if not m:
                        m = re.search(r"GetshowFileApplicationPreviewFileContent/([A-Za-z0-9+/=]+)", v)
                    if m and not raw_cert:
                        raw_cert = m.group(1)

            if not pp_url and not raw_cert:
                continue

            params: dict[str, str] = _decode_data_cert(raw_cert) if raw_cert else {}
            app_id  = params.get("AppID", "")
            proj_id = params.get("ProjectID", "")

            if app_id and app_id in seen_app_ids:
                continue
            if app_id:
                seen_app_ids.add(app_id)

            base_params = {k: v for k, v in params.items()
                           if k not in ("CharacterD", "ExtAppID")}

            texts = [_clean(c.get_text(separator=" ", strip=True)) for c in cells]
            texts = [t for t in texts if t]

            rows.append({
                "print_preview_url": pp_url,
                "data_cert": raw_cert,
                "cert_url": _build_cert_url(base_params, _CERT_CHAR_D) if base_params else None,
                "preview_pdf_url": _build_preview_url(base_params, _PREVIEW_CHAR_D) if base_params else None,
                "app_id": app_id,
                "project_id": proj_id,
                "user_id": params.get("UserID", ""),
                "division": params.get("Division", ""),
                "role_id": params.get("RoleID", ""),
                "listing_texts": texts,
            })

    return rows


# ── PrintPreview detail page parser ──────────────────────────────────────────

def _get_section(soup: BeautifulSoup, *header_patterns: str) -> BeautifulSoup:
    """
    Return the ``x_content`` div of the Bootstrap x_panel whose h2/h3 title
    matches any of the given regex patterns.  The Telangana PrintPreview uses
    ``<div class="x_panel"><div class="x_title"><h2>Section</h2>…</div>
    <div class="x_content">…data…</div></div>`` as its section structure.
    Falls back to the whole soup if no matching section is found.
    """
    for pattern in header_patterns:
        p = re.compile(pattern, re.I)
        h = soup.find(
            lambda t: t.name in ("h2", "h3", "h4") and p.search(t.get_text(strip=True))
        )
        if h:
            x_title = h.find_parent("div", class_=re.compile(r"x_title"))
            if x_title:
                x_content = x_title.find_next_sibling("div", class_=re.compile(r"x_content"))
                if x_content:
                    return x_content
    return soup


def _lv(soup: BeautifulSoup, *label_patterns: str) -> str | None:
    """
    Find a label whose text matches any of the given patterns (case-insensitive)
    and return the adjacent value cell's text.

    Supports three layouts:
    1. ``<th>/<td>`` table pairs.
    2. Bootstrap ``form-group`` col-div pairs used by the Telangana PrintPreview:
       ``<div class="form-group"><div class="col-…">Label</div>
         <div class="col-…">Value</div>…</div>``
    3. Col-div pairs NOT wrapped in a form-group (e.g. Built-Up Area Details):
       ``<div class="row"><div class="col-…"><label>Label</label></div>
         <div class="col-…">Value</div>…</div>``
    """
    for pattern in label_patterns:
        pat = re.compile(pattern, re.I)
        # 1. th/td/label/b/strong table-style pairs
        th = soup.find(
            lambda t: t.name in ("th", "td", "label", "b", "strong")
            and pat.search(t.get_text(strip=True))
        )
        if th:
            sib = th.find_next_sibling("td")
            if sib:
                return _clean(sib.get_text(separator=" ", strip=True))
            parent = th.parent
            if parent:
                sibs = parent.find_all("td")
                idx = sibs.index(th) if th in sibs else -1
                if idx >= 0 and idx + 1 < len(sibs):
                    return _clean(sibs[idx + 1].get_text(separator=" ", strip=True))
            # 3. Label inside a col-div → next sibling col-div holds the value
            col_parent = th.find_parent("div", class_=re.compile(r"\bcol"))
            if col_parent:
                next_col = col_parent.find_next_sibling("div", class_=re.compile(r"\bcol"))
                if next_col:
                    val = _clean(next_col.get_text(separator=" ", strip=True))
                    if val:
                        return val
        # 2. Bootstrap form-group col-div pairs
        for fg in soup.find_all("div", class_=re.compile(r"form-group")):
            cols = fg.find_all("div", class_=re.compile(r"\bcol"))
            for i in range(len(cols) - 1):
                lbl_text = cols[i].get_text(strip=True)
                if pat.search(lbl_text) and len(lbl_text) < 120:
                    val = _clean(cols[i + 1].get_text(separator=" ", strip=True))
                    if val:
                        return val
    return None


def _parse_label_value_section(container: Any) -> dict[str, str]:
    """Generic label-value extraction from a Bootstrap / table section."""
    result: dict[str, str] = {}
    if not container:
        return result
    for row in container.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) >= 2:
            lbl = _label(cells[0].get_text(strip=True))
            val = _clean(cells[1].get_text(separator=" ", strip=True))
            if lbl and val:
                result[lbl] = val
    # Bootstrap col layout: look for paired label/value divs
    for col in container.find_all(class_=re.compile(r"col")):
        txt = col.get_text(separator="\n", strip=True).split("\n")
        if len(txt) >= 2:
            lbl = _label(txt[0])
            val = _clean(" ".join(txt[1:]))
            if lbl and val:
                result.setdefault(lbl, val)
    return result


def _parse_table_rows(table: Any) -> list[dict[str, str]]:
    """
    Parse a generic HTML table into a list of row dicts.
    Header row (th) defines keys; subsequent tr rows define values.
    """
    if not table:
        return []
    rows = table.find_all("tr")
    if not rows:
        return []
    headers = [_label(th.get_text(strip=True)) for th in rows[0].find_all(["th", "td"])]
    records: list[dict[str, str]] = []
    for tr in rows[1:]:
        cells = tr.find_all(["td", "th"])
        row = {}
        for i, cell in enumerate(cells):
            if i < len(headers) and headers[i]:
                val = _clean(cell.get_text(separator=" ", strip=True))
                if val:
                    row[headers[i]] = val
        if row:
            records.append(row)
    return records


def _scrape_print_preview(soup: BeautifulSoup, row: dict) -> dict[str, Any]:
    """
    Extract all project fields from the PrintPreview page HTML.
    Maps Telangana portal labels to the canonical schema keys.

    The page uses Bootstrap x_panel sections with form-group col-div label/value
    pairs for most scalar fields, plus standard HTML tables for tabular data.
    We extract data from section-specific soup fragments to avoid confusion when
    the same label (e.g. "District") appears in both the Promoter and Project
    location sections.
    """
    # Section-specific soups (x_content of matching x_panel)
    proj_sec  = _get_section(soup, r"project\s*information")
    prom_sec  = _get_section(soup, r"promoter\s*information")
    addr_sec  = _get_section(soup, r"address\s*details")
    land_sec  = _get_section(soup, r"land\s*details")
    built_sec = _get_section(soup, r"built.up\s*area\s*details", r"built.*area.*detail")

    lv       = lambda *pats: _lv(soup, *pats)        # whole-doc fallback
    proj_lv  = lambda *pats: _lv(proj_sec, *pats)
    prom_lv  = lambda *pats: _lv(prom_sec, *pats)
    addr_lv  = lambda *pats: _lv(addr_sec, *pats)
    land_lv  = lambda *pats: _lv(land_sec, *pats)
    built_lv = lambda *pats: _lv(built_sec, *pats)

    data: dict[str, Any] = {}

    # ── Core fields (Project Information section) ─────────────────────────────
    data["project_name"]          = proj_lv(r"project\s*name")
    data["project_type"]          = proj_lv(r"project\s*type")
    data["status_of_the_project"] = proj_lv(r"project\s*status", r"\bstatus\b")
    data["project_registration_no"] = lv(
        r"registration\s*(no|number|cert)",
        r"rera\s*(cert|reg|no)",
        r"certificate\s*(no|number)",
    )
    data["approved_on_date"]      = proj_lv(r"approved\s*date")
    data["estimated_finish_date"] = proj_lv(
        r"revised\s*proposed\s*date",
        r"proposed\s*date.*completion",
        r"proposed.*completion",
    )
    data["actual_finish_date"]          = proj_lv(r"actual.*complet")
    data["estimated_commencement_date"] = proj_lv(r"commencement\s*date")

    # ── Location (Address Details section — project location, not promoter) ───
    district   = addr_lv(r"district")
    village    = addr_lv(r"village", r"mandal")
    locality   = addr_lv(r"locality")
    pin_code   = addr_lv(r"pin\s*code")
    state_name = addr_lv(r"\bstate\b") or "Telangana"

    raw_addr_parts = [p for p in [locality, district, state_name,
                                   f"Pincode: {pin_code}" if pin_code else None] if p]
    data["project_city"]      = district
    data["project_pin_code"]  = pin_code
    data["project_location_raw"] = {
        "state":       state_name,
        "district":    district,
        "village":     village,
        "locality":    locality,
        "pin_code":    pin_code,
        "raw_address": ", ".join(raw_addr_parts) if raw_addr_parts else None,
    }

    # ── Promoter (Promoter Information section) ───────────────────────────────
    # The page shows first-name ("Promoter Name") and last-name separately.
    first_name = prom_lv(r"promoter\s*name")
    last_name  = prom_lv(r"\blast\s*name\b", r"\bsurname\b")
    data["promoter_name"] = " ".join(filter(None, [first_name, last_name])) or None

    prom_house    = prom_lv(r"building\s*name", r"house\s*number", r"house\s*no")
    prom_locality = prom_lv(r"\blocality\b")
    prom_district = prom_lv(r"\bdistrict\b")
    prom_pin      = prom_lv(r"pin\s*code")
    prom_state    = prom_lv(r"^state$") or "Telangana"

    prom_raw_parts = [p for p in [prom_house, prom_locality, prom_district, prom_state,
                                   f"Pincode: {prom_pin}" if prom_pin else None] if p]
    data["promoter_address_raw"] = {
        "raw_address":            ", ".join(prom_raw_parts) if prom_raw_parts else None,
        "district":               prom_district,
        "locality":               prom_locality,
        "pin_code":               prom_pin,
        "state":                  prom_state,
        "house_no_building_name": prom_house,
    }
    phone = prom_lv(r"office\s*number", r"\bphone\b", r"\bmobile\b")
    email = prom_lv(r"e[-\s]?mail")
    data["promoter_contact_details"] = {k: v for k, v in {"phone": phone, "email": email}.items() if v}

    # The sample stores just the first-name fragment as promoters_details.name
    data["promoters_details"] = {"name": first_name} if first_name else None

    # ── Land / area ────────────────────────────────────────────────────────────
    # land_area: total land area from "Land Details" section
    # construction_area: approved built-up area from "Built-Up Area Details" section
    #   (distinct from Net Area in Land Details, which is land net of road widening)
    data["land_area"]         = land_lv(r"total\s*area", r"land\s*area")
    data["construction_area"] = (
        built_lv(r"approved.*built.*up", r"built[\s-]*up\s*area")
        or land_lv(r"approved.*built.*up", r"built[\s-]*up\s*area")
    )

    land_area_unit  = "Total Area(In sqmts)"
    const_area_unit = "Approved Built up Area (In Sqmts)"
    data["land_area_details"] = {
        "land_area":             data["land_area"],
        "land_area_unit":        land_area_unit,
        "construction_area":     data["construction_area"],
        "construction_area_unit": const_area_unit,
    } if data["land_area"] else None

    # ── Building / plot details table ─────────────────────────────────────────
    # Match plot tables specifically (has "Area of Plot" or "Proposed Number of Plots")
    # to avoid matching the Land Owner table which has a generic "Type" column.
    building_details: list[dict] = []
    for table in soup.find_all("table"):
        hdrs = [_label(th.get_text(strip=True)) for th in table.find_all("th")]
        hdrs_joined = " ".join(hdrs).lower()
        if re.search(r"number.*of.*plot|area.*of.*plot|proposed.*number.*plot", hdrs_joined):
            for r in _parse_table_rows(table):
                entry: dict[str, Any] = {"flat_type": "plot"}
                for h, v in r.items():
                    # Check booked/alloted BEFORE the generic number.*plot pattern
                    # so "number of plots booked/alloted/sold" → booking_detail,
                    # not no_of_plots.
                    if re.search(r"book|allot|sold", h, re.I):
                        entry["booking_detail"] = v
                    elif re.search(r"proposed.*number|number.*plot", h, re.I):
                        entry["no_of_plots"] = v
                    elif re.search(r"area.*of.*plot|plot.*area", h, re.I):
                        entry["total_area"] = v
                if len(entry) > 1:   # more than just flat_type
                    building_details.append(entry)
    data["building_details"] = building_details or None

    # ── Co-promoters (Land Owner / Investor table) ────────────────────────────
    # The table headers include "Promoter(Land Owner/ Investor)".
    # Use the "Promoter Name" column (not "Project Name") for the name.
    co_promoters: list[dict] = []
    for table in soup.find_all("table"):
        hdrs_text = " ".join(th.get_text(strip=True) for th in table.find_all("th")).lower()
        if re.search(r"land.*owner|promoter.*investor", hdrs_text, re.I):
            for r in _parse_table_rows(table):
                name = r.get("promoter name") or next(
                    (v for k, v in r.items()
                     if "promoter" in k.lower() and "name" in k.lower()), None
                )
                # Skip repeated header rows (ASP.NET GridView repeats headers)
                if name and not re.search(r"^promoter\s*name$", name, re.I):
                    co_promoters.append({"name": name})
    data["co_promoter_details"] = co_promoters or None

    # ── Professionals ─────────────────────────────────────────────────────────
    professionals: list[dict] = []
    for table in soup.find_all("table"):
        hdrs_text = " ".join(th.get_text(strip=True) for th in table.find_all("th")).lower()
        if re.search(r"architect|engineer|professional", hdrs_text, re.I):
            for r in _parse_table_rows(table):
                name = r.get("professional name") or r.get("name") or next(
                    (v for k, v in r.items() if "name" in k), None
                )
                role = r.get("professional type") or r.get("role") or next(
                    (v for k, v in r.items() if "type" in k or "role" in k), None
                )
                if name:
                    professionals.append({"name": name, "role": role or ""})
    data["professional_information"] = professionals or None

    # ── Construction progress ─────────────────────────────────────────────────
    # The "Project Details" x_panel has a table:
    #   Name | Proposed | Booked | WorkDone(In %)
    # The "Development Work" panel has:
    #   Common areas And Facilities | Available | Percent | Details
    progress: list[dict] = []
    for table in soup.find_all("table"):
        hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
        hdrs_joined = " ".join(hdrs).lower()
        if re.search(r"workdone|work.*done|booked|percent", hdrs_joined, re.I):
            for r in _parse_table_rows(table):
                entry: dict[str, Any] = {}
                for k, v in r.items():
                    kl = k.lower()
                    if re.search(r"^name$|^common\s*area|title|description|item", kl):
                        entry["title"] = v
                    elif re.search(r"remark|detail|status|available", kl):
                        entry["remarks"] = v
                    elif re.search(r"workdone|work.*done|percent|progress", kl):
                        entry["progress_percentage"] = v
                # Only include entries that have at least a remarks or progress value
                if entry and ("remarks" in entry or "progress_percentage" in entry):
                    progress.append(entry)
    data["construction_progress"] = progress or None

    # ── Status update ─────────────────────────────────────────────────────────
    data["status_update"] = {"booking_details": []}

    # ── Documents ─────────────────────────────────────────────────────────────
    docs: list[dict] = []
    seen_doc_urls: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(k in href for k in ("/GetUserDocumentFileContent/", "/Preview/", ".pdf")):
            full_url = urljoin(BASE_URL, href)
            if full_url not in seen_doc_urls:
                seen_doc_urls.add(full_url)
                label_text = _clean(a.get_text(strip=True)) or "Document"
                docs.append({"link": full_url, "type": label_text})
    data["_raw_docs"] = docs

    # ── Data blob (land area units) ───────────────────────────────────────────
    data["_land_area_unit"]   = land_area_unit
    data["_const_area_unit"]  = const_area_unit

    return data


# ── Document download + S3 upload ─────────────────────────────────────────────

def _handle_document(
    project_key: str,
    doc: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
    *,
    cookies: dict | None = None,
) -> dict | None:
    """Download one document, upload to S3, return a document_result_entry or None."""
    url = doc.get("link") or doc.get("url")
    if not url:
        return None
    label = doc.get("type", "document")
    filename = build_document_filename({**doc, "source_url": url})
    try:
        import httpx
        headers = {"User-Agent": "Mozilla/5.0", "Referer": BASE_URL}
        with httpx.Client(
            timeout=60.0,
            follow_redirects=True,
            verify=False,
            cookies=cookies or {},
        ) as client:
            resp = client.get(url, headers=headers)
            if not resp.is_success or len(resp.content) < 100:
                return None
            data = resp.content

        md5 = compute_md5(data)
        s3_key = upload_document(project_key, filename, data, dry_run=settings.DRY_RUN_S3)
        if not s3_key:
            return None
        s3_url = get_s3_url(s3_key)
        upsert_document(
            project_key=project_key,
            document_type=label,
            original_url=url,
            s3_key=s3_key,
            s3_bucket=settings.S3_BUCKET_NAME,
            file_name=filename,
            md5_checksum=md5,
            file_size_bytes=len(data),
        )
        logger.info("Document uploaded", label=label, s3_key=s3_key)
        return document_result_entry({**doc, "source_url": url, "type": label}, s3_url, filename)
    except Exception as exc:
        logger.warning("Document handling error", url=url, error=str(exc))
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(exc),
                           project_key=project_key, url=url)
        return None


def _build_uploaded_documents(row: dict, detail: dict) -> list[dict]:
    """
    Assemble the raw uploaded_documents list from listing-row URLs and
    document links found in the PrintPreview HTML.
    """
    docs: list[dict] = []
    seen: set[str] = set()

    def _add(link: str | None, doc_type: str) -> None:
        if not link or link in seen:
            return
        seen.add(link)
        docs.append({"link": link, "type": doc_type})

    # Certificate PDF (CharacterD=52)
    _add(row.get("cert_url"), "Registration Certificate 1")
    # Preview PDF (CharacterD=87)
    _add(row.get("preview_pdf_url"), "Rera Project Details")

    # Document links found on the PrintPreview HTML page
    for doc in detail.get("_raw_docs") or []:
        _add(doc.get("link"), doc.get("type", "Document"))

    return docs


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Telangana RERA.
    Loads state_projects_sample/telangana.json as the baseline, fetches the
    sentinel project's PrintPreview page via httpx, and verifies ≥ 80% field coverage.
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
        "state_projects_sample", "telangana.json",
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

    logger.info(f"Sentinel: fetching PrintPreview for {sentinel_reg}",
                url=detail_url, step="sentinel")
    try:
        resp = safe_get(detail_url, retries=2, logger=logger)
        if not resp:
            logger.error("Sentinel: failed to fetch PrintPreview", url=detail_url, step="sentinel")
            return False
        soup = BeautifulSoup(resp.text, "lxml")
        fresh = _scrape_print_preview(soup, {}) or {}
    except Exception as exc:
        logger.error(f"Sentinel: scrape error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", url=detail_url, step="sentinel")
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "telangana_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


# ── Main run() ────────────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    """
    Main entry point for the Telangana RERA crawler.

    Flow:
    1. Launch Playwright; navigate to /SearchList/Search.
    2. Solve CAPTCHA and submit search form (no filters → all registered projects).
    3. Parse listing rows from each page; paginate via ASP.NET postback.
    4. For each row: navigate to PrintPreview, parse detail HTML.
    5. Normalize → upsert to DB.
    6. Download selected documents → upload to S3.
    7. Checkpoint after each listing page.
    """
    site_id = config["id"]
    logger  = CrawlerLogger(site_id, run_id)
    counts  = dict(projects_found=0, projects_new=0, projects_updated=0,
                   projects_skipped=0, documents_uploaded=0, error_count=0)

    item_limit    = settings.CRAWL_ITEM_LIMIT or 0
    items_done    = 0
    delay_min, delay_max = config.get("rate_limit_delay", (2, 4))
    machine_name, machine_ip = get_machine_context()

    # ── Sentinel health check ────────────────────────────────────────────────
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counts["error_count"] += 1
        return counts

    checkpoint = load_checkpoint(site_id, mode)
    start_page = (checkpoint["last_page"] + 1) if checkpoint else 1
    if checkpoint:
        logger.info(f"Resuming from checkpoint page {start_page}")

    with PlaywrightSession(headless=True) as browser:
        # ── Open search page ─────────────────────────────────────────────────
        page = browser.new_page()
        try:
            logger.info("Navigating to Telangana RERA search page")
            page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
        except Exception as exc:
            logger.error(f"Search page failed to load: {exc}")
            insert_crawl_error(run_id, site_id, "HTTP_ERROR", str(exc), url=SEARCH_URL)
            return counts

        # ── Submit search form (solves CAPTCHA) ──────────────────────────────
        if not _submit_search(page, logger):
            insert_crawl_error(run_id, site_id, "CAPTCHA_FAILED",
                               "Could not submit search form after max tries")
            return counts

        # ── Determine total pages ────────────────────────────────────────────
        total_pages = _get_total_pages(page)
        max_pages   = settings.MAX_PAGES
        effective_end = (min(total_pages, start_page + max_pages - 1)
                         if max_pages else total_pages)
        logger.info(f"Total pages: {total_pages} | crawling up to {effective_end} | "
                    f"item_limit={item_limit or 'unlimited'}")

        # Skip listing pages before start_page by clicking "next"
        for _ in range(start_page - 1):
            if not _goto_next_page(page):
                break

        stop_all = False
        current_page = start_page

        while current_page <= effective_end and not stop_all:
            logger.info(f"Listing page {current_page}/{effective_end}")
            html  = page.content()
            rows  = _parse_listing_rows(html)
            logger.info(f"  {len(rows)} project rows on page {current_page}")

            for row in rows:
                if item_limit and items_done >= item_limit:
                    logger.info(f"Item limit {item_limit} reached")
                    stop_all = True
                    break

                app_id    = row.get("app_id") or row.get("project_id") or ""
                pp_url    = row.get("print_preview_url")
                stable_id = f"TG-APP-{app_id}" if app_id else None

                if not stable_id and not pp_url:
                    counts["error_count"] += 1
                    continue

                counts["projects_found"] += 1
                key = generate_project_key(stable_id or pp_url)
                logger.set_project(key=key, reg_no=stable_id, url=pp_url or SEARCH_URL,
                                   page=current_page)

                if mode == "daily_light" and get_project_by_key(key):
                    logger.info("Skipping — already in DB (daily_light)", step="skip")
                    counts["projects_skipped"] += 1
                    logger.clear_project()
                    continue

                try:
                    random_delay(delay_min, delay_max)

                    # ── Navigate to PrintPreview ──────────────────────────────
                    detail_data: dict[str, Any] = {}
                    if pp_url:
                        try:
                            logger.info("Navigating to PrintPreview", step="detail_fetch")
                            pp_page = browser.new_page()
                            pp_page.goto(pp_url, wait_until="domcontentloaded",
                                         timeout=_NAV_TIMEOUT_MS)
                            pp_page.wait_for_load_state("networkidle", timeout=30_000)
                            pp_html  = pp_page.content()
                            pp_soup  = BeautifulSoup(pp_html, "lxml")
                            detail_data = _scrape_print_preview(pp_soup, row)
                            pp_page.close()
                        except Exception as exc:
                            logger.warning(f"PrintPreview fetch failed: {exc}",
                                           step="detail_fetch")

                    # ── Determine registration number ─────────────────────────
                    reg_no = _clean(detail_data.get("project_registration_no")) or stable_id
                    if not reg_no:
                        reg_no = f"TG-APP-{app_id}" if app_id else f"TG-URL-{abs(hash(pp_url))}"
                    detail_data["project_registration_no"] = reg_no
                    final_key = generate_project_key(reg_no)

                    # ── Assemble document list ────────────────────────────────
                    raw_docs = _build_uploaded_documents(row, detail_data)
                    detail_data.pop("_raw_docs", None)

                    # ── Build data blob ────────────────────────────────────────
                    land_area_unit  = detail_data.pop("_land_area_unit", "Total Area(In sqmts)")
                    const_area_unit = detail_data.pop("_const_area_unit",
                                                      "Approved Built up Area (In Sqmts)")
                    base_params = {
                        "ProjectID": row.get("project_id", ""),
                        "Division":  row.get("division", ""),
                        "UserID":    row.get("user_id", ""),
                        "RoleID":    row.get("role_id", ""),
                        "AppID":     app_id,
                        "Action":    "SEARCH",
                    }
                    raw_cert = row.get("data_cert") or ""
                    doc_decoded = "&".join(f"{k}={v}" for k, v in base_params.items())

                    detail_data["data"] = {
                        "data_cert": raw_cert,
                        "govt_type": "state",
                        "doc_decoded": doc_decoded,
                        "is_processed": False,
                        "land_area_unit": land_area_unit,
                        "construction_area_unit": const_area_unit,
                    }
                    detail_data["land_area_details"] = {
                        "land_area": detail_data.get("land_area"),
                        "land_area_unit": land_area_unit,
                        "construction_area": detail_data.get("construction_area"),
                        "construction_area_unit": const_area_unit,
                    }

                    # ── Ensure required fields have fallbacks ─────────────────
                    if not _clean(detail_data.get("project_name")):
                        # Try to extract from listing texts (first long non-numeric text)
                        for txt in row.get("listing_texts") or []:
                            if txt and len(txt) > 4 and not txt.isdigit():
                                detail_data["project_name"] = txt
                                break
                        if not _clean(detail_data.get("project_name")):
                            detail_data["project_name"] = f"Telangana Project {app_id or proj_id}"

                    # ── Core metadata ─────────────────────────────────────────
                    detail_data["key"]              = final_key
                    detail_data["state"]            = STATE
                    detail_data["project_state"]    = STATE
                    detail_data["domain"]           = DOMAIN
                    detail_data["config_id"]        = config.get("config_id")
                    detail_data["crawl_machine_ip"] = machine_ip
                    detail_data["machine_name"]     = machine_name
                    detail_data["url"]              = pp_url or SEARCH_URL
                    detail_data["is_live"]          = False
                    detail_data["uploaded_documents"] = raw_docs or None

                    # ── Normalize + validate ──────────────────────────────────
                    try:
                        normalized = normalize_project_payload(
                            detail_data, config,
                            machine_name=machine_name, machine_ip=machine_ip,
                        )
                        record  = ProjectRecord(**normalized)
                        db_dict = record.to_db_dict()
                    except (ValidationError, ValueError) as ve:
                        logger.warning("Validation failed — using fallback", error=str(ve),
                                       step="normalize")
                        insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(ve),
                                           project_key=final_key, url=pp_url)
                        counts["error_count"] += 1
                        try:
                            detail_data["data"] = merge_data_sections(
                                detail_data.get("data"), {"validation_fallback": True})
                            db_dict = normalize_project_payload(
                                detail_data, config,
                                machine_name=machine_name, machine_ip=machine_ip,
                            )
                        except Exception:
                            logger.clear_project()
                            continue

                    # ── Upsert to DB ──────────────────────────────────────────
                    action = upsert_project(db_dict)
                    items_done += 1
                    if action == "new":       counts["projects_new"] += 1
                    elif action == "updated": counts["projects_updated"] += 1
                    else:                     counts["projects_skipped"] += 1
                    logger.info(f"DB result: {action}", step="db_upsert")

                    # ── Documents ─────────────────────────────────────────────
                    doc_name_counts: dict[str, int] = {}
                    uploaded_results: list[dict] = []
                    for doc in raw_docs:
                        selected = select_document_for_download(
                            STATE, doc, doc_name_counts, domain=DOMAIN)
                        if selected:
                            result = _handle_document(
                                db_dict["key"], selected, run_id, site_id, logger)
                            if result:
                                uploaded_results.append(result)
                                counts["documents_uploaded"] += 1

                    if uploaded_results:
                        upsert_project({
                            "key":                    db_dict["key"],
                            "url":                    db_dict.get("url"),
                            "state":                  STATE,
                            "domain":                 DOMAIN,
                            "project_registration_no": db_dict["project_registration_no"],
                            "uploaded_documents":     uploaded_results,
                            "document_urls":          build_document_urls(uploaded_results),
                        })

                except Exception as exc:
                    logger.exception("Project processing failed", exc, step="project_loop")
                    insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                                       project_key=key, url=pp_url)
                    counts["error_count"] += 1
                finally:
                    logger.clear_project()

            # ── Checkpoint after each listing page ────────────────────────────
            save_checkpoint(site_id, mode, current_page, None, run_id)

            # ── Paginate ──────────────────────────────────────────────────────
            if current_page < effective_end and not stop_all:
                random_delay(delay_min, delay_max)
                if not _goto_next_page(page):
                    logger.info("No next-page link found — stopping pagination")
                    break

            current_page += 1

    reset_checkpoint(site_id, mode)
    logger.info("Telangana RERA crawl finished", **counts)
    return counts
