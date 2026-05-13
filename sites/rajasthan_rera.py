"""
Rajasthan RERA Crawler — rera.rajasthan.gov.in
Type: Pure Playwright (Angular SPA listing + detail page HTML scraping)

Strategy:
- Phase 1: Use Playwright to navigate the Angular listing page
  (ProjectList?status=3) and enumerate all registered projects via
  DataTables HTML scraping.
- Phase 2: For each project, navigate to the detail page with Playwright,
  wait for the Angular SPA to fully render, then parse the rendered HTML
  with BeautifulSoup to extract structured fields.
- Documents: collect all anchor/link elements pointing to PDFs or
  downloadable files from the rendered detail page HTML.
- No direct REST API calls are made; all data is obtained by rendering
  the public-facing website through a real browser.
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timezone, timedelta

import httpx

from pydantic import ValidationError

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import download_response, generate_project_key, random_delay, safe_get
from core.db import get_project_by_key, upsert_project, insert_crawl_error, upsert_document
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
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings

# Rajasthan is in IST (UTC+5:30). /Date(ms)/ timestamps from the RERA API are
# stored as midnight IST values, so we must interpret them in IST and then
# write the result with a "+00:00" suffix (matching production convention).
_IST = timezone(timedelta(hours=5, minutes=30))


def _normalize_project_type(raw: str) -> str:
    """Normalize project type string to lowercase-hyphenated format.

    Examples: 'GROUP HOUSING' → 'group-housing', 'Residential' → 'residential'
    """
    return raw.strip().lower().replace(" ", "-")


def _normalize_date_str(val) -> str | None:
    """Normalize any date representation from the Rajasthan RERA APIs to the
    canonical ISO string ``YYYY-MM-DD HH:MM:SS+00:00`` (or ``+00:00`` suffix
    for existing ISO strings).  Returns *None* for invalid / empty / pre-epoch
    sentinel values so callers can skip them cleanly.

    Handled formats
    ---------------
    * ``/Date(<ms>)/``  — .NET JSON date; interpreted in IST (UTC+5:30)
    * ``dd-mm-yyyy``    — plain date string from listing / detail page
    * ``dd/mm/yyyy``    — alternate separator from listing page
    * ``YYYY-MM-DDTHH:MM:SS[.sss]`` — ISO with "T" separator (listing APPROVEDON)
    * ``YYYY-MM-DD HH:MM:SS``       — already-normalised (append ``+00:00``)
    """
    if val is None:
        return None
    v = str(val).strip()
    if v in ("", "null", "None", "0"):
        return None

    # /Date(ms)/ — .NET JSON format
    m = re.match(r"^/Date\((-?\d+)\)/$", v)
    if m:
        ms = int(m.group(1))
        if ms <= 0:
            return None  # sentinel for year 0001 or invalid
        try:
            dt = datetime.fromtimestamp(ms / 1000, tz=_IST)
            return dt.strftime("%Y-%m-%d %H:%M:%S") + "+00:00"
        except (ValueError, OSError):
            return None

    # dd-mm-yyyy  or  dd/mm/yyyy
    m = re.match(r"^(\d{2})[-/](\d{2})[-/](\d{4})$", v)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)} 00:00:00+00:00"

    # YYYY-MM-DDTHH:MM:SS[.sss…]  (ISO with 'T' separator)
    m = re.match(r"^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})", v)
    if m:
        return f"{m.group(1)} {m.group(2)}+00:00"

    # Already YYYY-MM-DD HH:MM:SS (with or without timezone suffix)
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", v):
        return v if "+" in v else v + "+00:00"

    # Unrecognised — return as-is so we don't silently discard data
    return v





def _format_inr(amount: float) -> str:
    """Format a float as Indian currency string: ₹X,XX,XX,XXX.XX"""
    rounded  = round(amount, 2)
    int_part = int(rounded)
    dec_str  = f"{rounded - int_part:.2f}"[1:]   # ".xx"
    s = str(int_part)
    if len(s) <= 3:
        return f"₹{s}{dec_str}"
    result, s = s[-3:], s[:-3]
    while s:
        result, s = s[-2:] + "," + result, s[:-2]
    return f"₹{result}{dec_str}"


BASE_URL         = "https://rera.rajasthan.gov.in"
STATE_CODE       = "RJ"
DOMAIN           = "rera.rajasthan.gov.in"
LISTING_PAGE_URL = f"{BASE_URL}/ProjectList?status=3"

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Playwright listing table field → schema field
_LIST_API_TO_FIELD: dict[str, str] = {
    "reg_no":         "project_registration_no",
    "project_name":   "project_name",
    "promoter_name":  "promoter_name",
    "project_type":   "project_type",
    "district":       "project_city",
    "application_no": "acknowledgement_no",
    "approved_on":    "approved_on_date",
    "status":         "status_of_the_project",
}


def _clean(text) -> str:
    """Strip and collapse whitespace (used by the Playwright listing scraper)."""
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def _is_real_document(resp) -> bool:
    """Return True only if the response body looks like an actual document (not a soft-404 HTML page)."""
    if resp is None:
        return False
    content_type = resp.headers.get("Content-Type", "").lower()
    chunk = resp.content[:8] if resp.content else b""
    if chunk.startswith(b"%PDF"):
        return True
    if "text/html" in content_type or "text/plain" in content_type:
        return False
    return len(chunk) > 0


def _resolve_relative_url(path: str) -> str:
    """Resolve a relative document path to an absolute URL using BASE_URL."""
    clean = path.replace("~/", "").replace("~\\", "").replace("../", "").replace("..\\", "")
    if not clean.startswith("/"):
        clean = f"/{clean}"
    return f"{BASE_URL}{clean}"


def _build_doc_url(path: str | None) -> str | None:
    """Return an absolute URL for a document path (relative or absolute)."""
    if not path or path == "0":
        return None
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return _resolve_relative_url(path)


def _extract_rj_table_rows(page) -> list[dict]:
    """
    Extract visible project rows from the Angular DataTables listing.
    Dynamically maps column indices from header text.
    """
    soup = BeautifulSoup(page.content(), "lxml")
    rows: list[dict] = []
    table = soup.select_one("table[datatable], table.dataTable, #project-list-table, table")
    if not table:
        return rows

    headers = [th.get_text(strip=True).lower() for th in table.select("thead th")]
    col_map: dict[str, int] = {}
    for i, h in enumerate(headers):
        if "district" in h:
            col_map["district"] = i
        elif "project name" in h and "no" not in h:
            col_map["project_name"] = i
        elif "project type" in h:
            col_map["project_type"] = i
        elif "promoter" in h:
            col_map["promoter_name"] = i
        elif "application" in h:
            col_map["application_no"] = i
        elif "registration" in h:
            col_map["reg_no"] = i
        elif "approved" in h or "approvedon" in h:
            col_map["approved_on"] = i
        elif "status" in h:
            col_map["status"] = i

    for tr in table.select("tbody tr"):
        cells = tr.select("td")
        if not cells:
            continue
        row: dict = {}
        for field, idx in col_map.items():
            if idx < len(cells):
                row[field] = cells[idx].get_text(strip=True)

        if row.get("reg_no"):
            rows.append(row)

    return rows


def _scrape_project_list_playwright(logger: CrawlerLogger) -> list[dict]:
    """
    Navigate the Rajasthan RERA Angular SPA listing page and extract all projects.
    Returns list of dicts with keys: reg_no, project_name, promoter_name,
    project_type, district, application_no, approved_on, status.
    """
    projects: list[dict] = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()
            page.goto(LISTING_PAGE_URL, timeout=60_000)

            # Wait for Angular DataTable to render
            try:
                page.wait_for_selector(
                    "table[datatable], table.dataTable, #project-list-table, table tbody tr",
                    timeout=30_000,
                )
            except Exception:
                logger.warning("DataTables table not found — listing may be empty")
                browser.close()
                return projects
            page.wait_for_load_state("networkidle", timeout=30_000)

            # Try to set DataTables page size to maximum (show all rows at once).
            # Attempt multiple strategies: value attribute, visible label, and
            # last-index fallback (the "All" option is often the last entry).
            _length_selector = (
                "select[name*='DataTables_Table'], select[name*='_length'], "
                "select.dt-length-select"
            )
            _set_page_size = False
            _size_attempts: list[dict] = [
                {"value": "-1"},    # DataTables standard "All" value
                {"label": "All"},   # visible label fallback
                {"value": "100"},
                {"value": "50"},
            ]
            for _kwargs in _size_attempts:
                try:
                    page.select_option(_length_selector, **_kwargs, timeout=5_000)
                    page.wait_for_timeout(3_000)
                    page.wait_for_load_state("networkidle", timeout=15_000)
                    _set_page_size = True
                    logger.info(f"DataTables page size set ({_kwargs})")
                    break
                except Exception:
                    pass
            if not _set_page_size:
                # Last resort: pick the last option (often "All" or the highest N)
                try:
                    page.evaluate(
                        """(sel) => {
                            const el = document.querySelector(sel);
                            if (el && el.options.length) {
                                el.selectedIndex = el.options.length - 1;
                                el.dispatchEvent(new Event('change', {bubbles: true}));
                            }
                        }""",
                        (
                            "select[name*='DataTables_Table'], select[name*='_length'], "
                            "select.dt-length-select"
                        ),
                    )
                    page.wait_for_timeout(3_000)
                    page.wait_for_load_state("networkidle", timeout=15_000)
                    _set_page_size = True
                    logger.info("DataTables page size set via JS last-option fallback")
                except Exception:
                    pass
            if not _set_page_size:
                logger.warning("Could not change DataTables page size — will paginate")

            # Extract rows from current view and paginate
            projects.extend(_extract_rj_table_rows(page))
            while True:
                try:
                    next_btn = page.locator(
                        "a.paginate_button.next:not(.disabled), "
                        "li.paginate_button.next:not(.disabled) a"
                    ).first
                    if not next_btn or not next_btn.is_visible():
                        break
                    next_btn.click()
                    page.wait_for_load_state("networkidle", timeout=15_000)
                    page.wait_for_timeout(1_000)
                    projects.extend(_extract_rj_table_rows(page))
                except Exception as e:
                    logger.warning(f"Pagination stopped: {e}")
                    break

            browser.close()
    except Exception as exc:
        logger.error(f"Playwright listing scrape failed: {exc}")

    logger.info(f"Rajasthan page inspection: found {len(projects)} projects")
    return projects


# ── HTML label → schema field map for the rendered detail page ────────────────
_DETAIL_LABEL_FIELD_MAP: dict[str, str] = {
    # Project basics
    "project name": "project_name",
    "name of the project": "project_name",
    "project title": "project_name",
    "registration no": "project_registration_no",
    "registration no.": "project_registration_no",
    "registration number": "project_registration_no",
    "rera registration no": "project_registration_no",
    "rera registration number": "project_registration_no",
    "promoter name": "promoter_name",
    "promoter details": "promoter_name",
    "name of promoter": "promoter_name",
    "developer name": "promoter_name",
    "project type": "project_type",
    "type of project": "project_type",
    "project category": "project_type",
    "district": "project_city",
    "district name": "project_city",
    "application no": "acknowledgement_no",
    "application no.": "acknowledgement_no",
    "application number": "acknowledgement_no",
    "acknowledgement no": "acknowledgement_no",
    "project status": "status_of_the_project",
    "status": "status_of_the_project",
    # Dates
    "approved on": "approved_on_date",
    "date of approval": "approved_on_date",
    "registration date": "approved_on_date",
    "date of registration": "submitted_date",
    "application date": "submitted_date",
    "submitted date": "submitted_date",
    "revised date of completion": "estimated_finish_date",
    "estimated finish date": "estimated_finish_date",
    "date of completion": "estimated_finish_date",
    "expected date of completion": "estimated_finish_date",
    "commencement date": "estimated_commencement_date",
    "actual commencement date": "actual_commencement_date",
    "actual finish date": "actual_finish_date",
    # Areas
    "plot area": "land_area",
    "land area": "land_area",
    "phase area": "land_area",
    "total land area": "land_area",
    "built up area": "construction_area",
    "builtup area": "construction_area",
    "total built up area": "construction_area",
    "construction area": "construction_area",
    # Units
    "total residential units": "number_of_residential_units",
    "number of residential units": "number_of_residential_units",
    "no. of residential units": "number_of_residential_units",
    "total commercial units": "number_of_commercial_units",
    "number of commercial units": "number_of_commercial_units",
    # Location
    "taluk": "taluk",
    "tehsil": "taluk",
    "tahsil": "taluk",
    "village": "village",
    "village name": "village",
    "locality": "locality",
    "pin code": "pin_code",
    "pincode": "pin_code",
    # Description
    "project description": "project_description",
    "description": "project_description",
}

_DOC_EXTENSIONS = frozenset({
    ".pdf", ".doc", ".docx", ".xlsx", ".xls", ".jpg", ".jpeg", ".png", ".zip",
})
_DOC_PATH_KEYWORDS = ("download", "/content/", "/uploads/", "getfile", "viewdoc", "getdocument")

# Anchor texts that carry no meaningful document-type information.
# When a link has one of these generic texts we fall back to the parent
# table-row's first cell (which contains the real document category).
_GENERIC_ANCHOR_TEXTS = frozenset({
    "view", "view photo", "download", "click here", "here", "open", "pdf", "",
})


def _extract_kv_from_html(soup: BeautifulSoup) -> dict[str, str]:
    """Extract key-value pairs from multiple Bootstrap/Angular HTML patterns."""
    kv: dict[str, str] = {}

    # Strategy 5 (highest priority): Rajasthan RERA 2.0 Angular pattern
    # <div class="details"><span class="label">…</span><span class="value">…</span></div>
    for div in soup.find_all("div", class_="details"):
        label_el = div.select_one(".label")
        value_el = div.select_one(".value")
        if not label_el or not value_el:
            continue
        label = _clean(label_el.get_text())
        # Exclude embedded <a> link text (e.g. "View" anchors inside the value span)
        value_parts = [
            t.strip() for t in value_el.strings
            if t.strip() and getattr(t.parent, "name", None) != "a"
        ]
        value = _clean(" ".join(value_parts))
        if label and value and len(label) < 80:
            kv.setdefault(label, value)

    # Strategy 1: <tr> with <th>label</th><td>value</td>
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) >= 2:
            label = _clean(cells[0].get_text())
            value = _clean(cells[1].get_text())
            if label and value and len(label) < 80:
                kv.setdefault(label, value)

    # Strategy 2: Bootstrap row with two-column label + value layout
    for row in soup.find_all(class_=re.compile(r"\brow\b")):
        cols = [c for c in row.children
                if hasattr(c, "get_text") and re.search(r"\bcol\b", " ".join(c.get("class", [])))]
        if len(cols) >= 2:
            label = _clean(cols[0].get_text())
            value = _clean(cols[1].get_text())
            if label and value and len(label) < 80:
                kv.setdefault(label, value)

    # Strategy 3: <label> element followed by sibling/parent-sibling with value
    for label_el in soup.find_all("label"):
        label_text = _clean(label_el.get_text())
        if not label_text or len(label_text) > 80:
            continue
        sibling = label_el.find_next_sibling()
        if not sibling:
            parent = label_el.parent
            sibling = parent.find_next_sibling() if parent else None
        if sibling and hasattr(sibling, "get_text"):
            value = _clean(sibling.get_text())
            if value:
                kv.setdefault(label_text, value)

    # Strategy 4: <dl><dt>label</dt><dd>value</dd></dl>
    for dl in soup.find_all("dl"):
        for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
            label = _clean(dt.get_text())
            value = _clean(dd.get_text())
            if label and value:
                kv.setdefault(label, value)

    return kv


def _parse_viewproject_html(soup: BeautifulSoup) -> dict:  # noqa: C901
    """Parse the full-detail ViewProject popup page (table-based layout).

    The popup at /ViewProject?id=...&type=U contains all the rich project data
    that is hidden behind the 'Updated project details' View link on the main
    ProjectDetail page. It uses <table> sections identified by a header row.
    """
    out: dict = {}

    def _tbl_heading(tbl) -> str:
        rows = tbl.find_all("tr")
        if rows:
            return _clean(rows[0].get_text()).lower()
        return ""

    def _find_table(*keywords: str):
        for tbl in soup.find_all("table"):
            h = _tbl_heading(tbl)
            if all(kw.lower() in h for kw in keywords):
                return tbl
        return None

    def _parse_kv_rows(tbl, skip: int = 1) -> dict:
        kv: dict = {}
        rows = tbl.find_all("tr")[skip:]
        for tr in rows:
            cells = [_clean(td.get_text()) for td in tr.find_all(["td", "th"])]
            if len(cells) >= 4:
                if cells[0]:
                    kv[cells[0]] = cells[1]
                if cells[2]:
                    kv[cells[2]] = cells[3]
            elif len(cells) == 2 and cells[0]:
                kv[cells[0]] = cells[1]
        return kv

    # ── Bank details ──────────────────────────────────────────────────────────
    bank_tbl = _find_table("detail of separate bank account")
    if bank_tbl:
        kv = _parse_kv_rows(bank_tbl)
        bank: dict = {}
        for k, v in kv.items():
            n = k.lower()
            if "bank name" in n:           bank["bank_name"]    = v
            elif "branch name" in n:       bank["branch"]        = v
            elif "ifsc" in n:              bank["IFSC"]          = v
            elif "a/c number" in n:        bank["account_no"]    = v
            elif "account holder" in n:    bank["account_name"]  = v
            elif "bank address" in n:      bank["address"]       = v
        if bank:
            out["bank_details"] = bank

    # ── Land area & units ─────────────────────────────────────────────────────
    land_tbl = _find_table("land details")
    if land_tbl:
        kv = _parse_kv_rows(land_tbl)
        for k, v in kv.items():
            n = k.lower()
            if ("total area" in n or "phase area" in n) and "land_area" not in out:
                try:
                    out["land_area"] = float(v.replace(",", ""))
                except (ValueError, TypeError):
                    pass
            if ("sanctioned number" in n or "number of apartments" in n) and v not in ("0", ""):
                try:
                    out.setdefault("number_of_residential_units", int(float(v)))
                except (ValueError, TypeError):
                    pass

    # ── Construction area ─────────────────────────────────────────────────────
    sba_tbl = _find_table("total built up area")
    if sba_tbl:
        kv = _parse_kv_rows(sba_tbl)
        for k, v in kv.items():
            if "built up area" in k.lower():
                try:
                    out["construction_area"] = float(v.replace(",", ""))
                except (ValueError, TypeError):
                    pass

    # ── Project location ──────────────────────────────────────────────────────
    loc_tbl = _find_table("location of project")
    if loc_tbl:
        kv = _parse_kv_rows(loc_tbl)
        loc: dict = {}
        _loc_map = {
            "state": "state", "district": "district",
            "tehsil": "taluk", "village": "village",
            "plot": "house_no_building_name", "khasra": "house_no_building_name",
            "ward": "ward", "street": "locality", "locality": "locality",
            "post office": "post_office", "pincode": "pin_code", "pin code": "pin_code",
        }
        for k, v in kv.items():
            if not v or v in ("-", "NA", ""):
                continue
            n = k.lower()
            for kw, field in _loc_map.items():
                if kw in n:
                    loc.setdefault(field, v)
                    break
        if loc:
            parts = [v for fld, v in loc.items() if fld not in ("state",) and v]
            loc["raw_address"] = ", ".join(parts)
            out["project_location_raw"] = loc

    # ── Project description (Remark) ──────────────────────────────────────────
    remark_tbl = _find_table("remark about project")
    if remark_tbl:
        rows = remark_tbl.find_all("tr")
        if len(rows) >= 2:
            desc = _clean(rows[1].get_text())
            if desc:
                out["project_description"] = desc

    # ── Project cost ──────────────────────────────────────────────────────────
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if not rows:
            continue
        hdrs = [_clean(c.get_text()).lower() for c in rows[0].find_all(["td", "th"])]
        if "title" in hdrs and "value" in hdrs:
            cost: dict = {}
            for tr in rows[1:]:
                cells = [_clean(td.get_text()) for td in tr.find_all(["td", "th"])]
                if len(cells) < 3 or not cells[1] or not cells[2]:
                    continue
                title, val = cells[1].lower(), cells[2]
                if "land cost" in title:               cost["cost_of_land"] = val
                elif "estimated" in title and "construction" in title:
                    cost["estimated_construction_cost"] = val
                elif "estimated" in title and "project" in title:
                    cost["estimated_project_cost"] = val
            if cost:
                out["project_cost_detail"] = cost
            break

    # ── Construction progress ─────────────────────────────────────────────────
    # The "Updates as of" ViewProject page has a table whose header row contains
    # both a "title" column and a "percentage" (or "%") column.
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if not rows:
            continue
        hdrs = [_clean(c.get_text()).lower() for c in rows[0].find_all(["td", "th"])]
        has_title = any("title" in h for h in hdrs)
        has_pct   = any("%" in h or "percentage" in h for h in hdrs)
        if not (has_title and has_pct):
            continue
        title_i = next((i for i, h in enumerate(hdrs) if "title" in h), None)
        pct_i   = next((i for i, h in enumerate(hdrs) if "%" in h or "percentage" in h), None)
        date_i  = next((i for i, h in enumerate(hdrs) if "date" in h), None)
        rem_i   = next((i for i, h in enumerate(hdrs) if "remark" in h), None)
        progress: list[dict] = []
        for tr in rows[1:]:
            cells = [_clean(td.get_text()) for td in tr.find_all(["td", "th"])]
            if not cells:
                continue
            entry: dict = {}
            if title_i is not None and title_i < len(cells) and cells[title_i]:
                entry["title"] = cells[title_i]
            if pct_i is not None and pct_i < len(cells) and cells[pct_i]:
                entry["progress_percentage"] = cells[pct_i]
            if date_i is not None and date_i < len(cells) and cells[date_i]:
                entry["date_of_reporting"] = cells[date_i]
            if rem_i is not None and rem_i < len(cells) and cells[rem_i]:
                entry["remarks"] = cells[rem_i]
            if entry.get("title"):
                progress.append(entry)
        if progress:
            out["construction_progress"] = progress
        break

    # ── Building / apartment details ──────────────────────────────────────────
    bldg_tbl = _find_table("building details")
    if bldg_tbl:
        rows = bldg_tbl.find_all("tr")
        hdrs: list[str] = []
        units: list[dict] = []
        for tr in rows:
            cells = [_clean(td.get_text()) for td in tr.find_all(["td", "th"])]
            if not cells or not any(cells):
                continue
            if "apartment type" in cells[0].lower() and not hdrs:
                hdrs = [c.lower() for c in cells]
                continue
            if hdrs and len(cells) >= 3:
                u: dict = {}
                for i, h in enumerate(hdrs[:len(cells)]):
                    v = cells[i] if i < len(cells) else ""
                    if not v or v == "View":
                        continue
                    if "apartment type" in h:       u["flat_type"]     = v
                    elif "block number" in h:       u["block_name"]    = v
                    elif "carpet area" in h:        u["carpet_area"]   = v
                    elif "balcony" in h:            u["balcony_area"]  = v
                    elif "terrace" in h:            u["open_area"]     = v
                    elif "proposed number" in h:    u["no_of_units"]   = v
                if u.get("flat_type") and u.get("carpet_area"):
                    units.append(u)
        if units:
            out["building_details"] = units

    # ── Professional information ───────────────────────────────────────────────
    # Tables with e-mail/name/contact-number headers, preceded by a role heading
    professionals: list[dict] = []
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if not rows:
            continue
        hdrs = [_clean(c.get_text()).lower() for c in rows[0].find_all(["td", "th"])]
        if "e-mail address" not in hdrs or "name" not in hdrs:
            continue
        ei = next((i for i, h in enumerate(hdrs) if "e-mail" in h), None)
        ni = next((i for i, h in enumerate(hdrs) if h == "name"), None)
        ai = next((i for i, h in enumerate(hdrs) if "address" in h), None)
        pi = next((i for i, h in enumerate(hdrs) if "contact number" in h), None)
        # Try to infer role from a preceding heading cell in the same table
        role_row = tbl.find_previous(["h5", "h4", "th", "caption"])
        role = _clean(role_row.get_text()) if role_row else ""
        for tr in rows[1:]:
            cells = [_clean(td.get_text()) for td in tr.find_all(["td", "th"])]
            if not cells:
                continue
            prof: dict = {}
            if ni is not None and ni < len(cells) and cells[ni]:
                prof["name"] = cells[ni]
            if ei is not None and ei < len(cells) and cells[ei]:
                prof["email"] = cells[ei]
            if ai is not None and ai < len(cells) and cells[ai]:
                prof["address"] = cells[ai]
            if pi is not None and pi < len(cells) and cells[pi]:
                prof["phone"] = cells[pi]
            if role:
                prof["role"] = role
            if prof.get("name"):
                professionals.append(prof)
    if professionals:
        out["professional_information"] = professionals

    # ── Members / partners ────────────────────────────────────────────────────
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if not rows:
            continue
        hdrs = [_clean(c.get_text()).lower() for c in rows[0].find_all(["td", "th"])]
        if "designation" in hdrs and "name" in hdrs:
            ni = next((i for i, h in enumerate(hdrs) if h == "name"), None)
            di = next((i for i, h in enumerate(hdrs) if "designation" in h), None)
            members: list[dict] = []
            for tr in rows[1:]:
                cells = [_clean(td.get_text()) for td in tr.find_all(["td", "th"])]
                if not cells:
                    continue
                m: dict = {}
                if ni is not None and ni < len(cells):
                    m["name"] = cells[ni]
                if di is not None and di < len(cells):
                    m["position"] = cells[di]
                if m.get("name"):
                    members.append(m)
            if members:
                out["members_details"] = members
            break

    # ── Promoter / organisation ───────────────────────────────────────────────
    org_tbl = _find_table("organization")
    if org_tbl:
        kv = _parse_kv_rows(org_tbl)
        promoter: dict = {}
        for k, v in kv.items():
            n = k.lower()
            if "organization name" in n:    promoter["name"]         = v
            elif "organization type" in n:  promoter["type_of_firm"] = v
        if promoter:
            out["promoters_details"] = promoter

    return out


def _parse_detail_docs(soup: BeautifulSoup) -> list[dict]:
    """Collect all document/download links from the rendered detail page HTML.

    For links whose anchor text is generic (e.g. "View", "View Photo") the
    function inspects the parent <tr> row and uses the first non-generic cell
    text as the document label — this is the pattern used on the ViewProject
    table-based page where document type is in cell[0] and the link is in the
    last cell.
    """
    docs: list[dict] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = str(a.get("href", "")).strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript"):
            continue
        hl = href.lower()
        is_doc = (
            any(hl.endswith(ext) for ext in _DOC_EXTENSIONS)
            or any(kw in hl for kw in _DOC_PATH_KEYWORDS)
        )
        if not is_doc:
            continue
        url = _build_doc_url(href)
        if not url or url in seen:
            continue
        seen.add(url)

        anchor_text = _clean(a.get_text()) or ""
        label = anchor_text

        # When anchor text is generic ("View", "Download" …), look for the
        # document category in the first non-generic cell of the parent <tr>.
        if label.lower() in _GENERIC_ANCHOR_TEXTS:
            parent_tr = a.find_parent("tr")
            if parent_tr:
                cells = [
                    _clean(td.get_text())
                    for td in parent_tr.find_all(["td", "th"])
                ]
                row_label = next(
                    (c for c in cells if c and c.lower() not in _GENERIC_ANCHOR_TEXTS),
                    "",
                )
                if row_label:
                    label = row_label

        if not label:
            label = "document"

        docs.append({"label": label, "url": url})
    return docs


def _try_expand_tabs(page) -> None:
    """Click through all inactive tabs/accordions to expose hidden content."""
    selectors = [
        "div.tab:not(.selected)",               # Rajasthan RERA 2.0 Angular tabs
        "li.nav-item a.nav-link:not(.active)",
        "a[data-toggle='tab']:not(.active)",
        ".nav-tabs li:not(.active) a",
        "[role='tab'][aria-selected='false']",
        ".accordion-toggle",
        ".panel-title a",
    ]
    for selector in selectors:
        try:
            tabs = page.locator(selector).all()
            for tab in tabs:
                try:
                    tab.click(timeout=2_000)
                    page.wait_for_timeout(800)
                except Exception:
                    pass
        except Exception:
            pass


def _parse_detail_html(soup: BeautifulSoup) -> dict:
    """Map extracted key-value pairs from the detail page HTML to schema fields."""
    out: dict = {}
    kv = _extract_kv_from_html(soup)

    for raw_label, raw_value in kv.items():
        norm = raw_label.lower().strip().rstrip(":").strip()
        field = _DETAIL_LABEL_FIELD_MAP.get(norm)
        if not field:
            continue
        val = str(raw_value).strip()
        if not val or val.lower() in ("", "null", "none", "n/a", "-", "--", "0"):
            continue
        if field.endswith("_date"):
            val = _normalize_date_str(val) or val
        elif field == "project_type":
            val = _normalize_project_type(val)
        elif field in ("number_of_residential_units", "number_of_commercial_units"):
            try:
                val = int(float(val))
            except (ValueError, TypeError):
                continue
        elif field in ("land_area", "construction_area"):
            try:
                val = float(val)
            except (ValueError, TypeError):
                continue
        out[field] = val

    return out


def _fetch_viewproject_html(page, logger: CrawlerLogger) -> str | None:
    """
    On the ProjectDetail page, find the 'Updated project details' (or
    'Project details as at time of registration') ViewProject link, resolve its
    fully encoded URL via JavaScript, then open it in a new tab and return the HTML.

    Using element.href (not getAttribute) gives the browser-resolved,
    correctly percent-encoded URL, avoiding double-encoding of base64 IDs.

    Returns the popup HTML string, or None if the link isn't found.
    """
    # Wait for Angular to finish rendering the detail divs
    page.wait_for_timeout(3_000)

    # Use JS to find the ViewProject href — element.href is already fully encoded.
    # When multiple "Updated project" links exist (one per periodic update), pick
    # the one whose surrounding text has the latest "as of DD/MM/YYYY" date.
    viewproject_url: str | None = page.evaluate("""() => {
        const links = Array.from(document.querySelectorAll('a[href*="ViewProject"]'));

        // Collect all "Updated project" links and parse their "as of" date.
        const DATE_RE = /as\\s+of\\s+(\\d{1,2})[\\/-](\\d{1,2})[\\/-](\\d{4})/i;
        let bestLink = null;
        let bestVal  = -1;

        for (const a of links) {
            const p = a.closest('div.details');
            if (!p || !p.textContent.includes('Updated project')) continue;
            const m = DATE_RE.exec(p.textContent);
            if (m) {
                // Build a numeric YYYYMMDD so we can compare without Date parsing.
                const val = parseInt(m[3]) * 10000
                          + parseInt(m[2]) * 100
                          + parseInt(m[1]);
                if (val > bestVal) { bestVal = val; bestLink = a; }
            } else if (!bestLink) {
                // No date found — keep as a fallback candidate.
                bestLink = a;
            }
        }

        // If no "Updated project" links at all, fall back to the first ViewProject link.
        const chosen = bestLink || links[0];
        return chosen ? chosen.href : null;
    }""")

    if not viewproject_url:
        logger.warning("ViewProject link not found on detail page", step="detail")
        return None

    logger.info(f"Fetching ViewProject page: {viewproject_url}", step="detail")
    try:
        popup_page = page.context.new_page()
        popup_page.goto(viewproject_url, timeout=60_000, wait_until="networkidle")
        popup_page.wait_for_timeout(2_000)
        html = popup_page.content()
        popup_page.close()
        logger.info("ViewProject page scraped", step="detail")
        return html
    except Exception as exc:
        logger.warning(f"ViewProject page fetch failed: {exc}", step="detail")
        return None


def _scrape_detail_playwright(
    page, project_ref: str, logger: CrawlerLogger
) -> tuple[dict, list[dict]]:
    """
    Scrape the currently-loaded project detail page (Angular SPA already rendered).
    Call _navigate_to_project_detail first to land on the right URL.

    Also clicks the 'Updated project details' View link to open the full-detail
    popup (/ViewProject) and merges that richer data into the result.

    Returns (data_dict, doc_links).
    """
    try:
        _try_expand_tabs(page)
        page.wait_for_timeout(1_000)
        soup = BeautifulSoup(page.content(), "lxml")
        data = _parse_detail_html(soup)
        docs = _parse_detail_docs(soup)

        # ── Open the full-detail popup and merge richer fields ────────────────
        popup_html = _fetch_viewproject_html(page, logger)
        if popup_html:
            popup_soup = BeautifulSoup(popup_html, "lxml")
            rich = _parse_viewproject_html(popup_soup)
            # Rich data overrides sparse surface-level data
            data.update(rich)
            # Also collect any doc links from the popup
            docs.extend(_parse_detail_docs(popup_soup))

        logger.info(
            f"Detail page scraped: {len(data)} fields, {len(docs)} docs",
            step="detail",
        )
        return data, docs
    except Exception as exc:
        logger.error(f"Detail page scrape failed: {exc}", step="detail")
        return {}, []


def _navigate_to_project_detail(page, reg_no: str, logger: CrawlerLogger) -> str:
    """
    Navigate to a project's detail page by using the Rajasthan RERA listing's
    Angular search form to filter by registration number, then clicking View.

    The listing uses three Angular form inputs (projectName, promoterName,
    registrationNo) rather than a DataTables global search box. Filling
    registrationNo and clicking the Search button triggers the filter.

    Returns the detail page URL on success, or empty string on failure.
    """
    # The reg_no extracted from the listing table may carry a date suffix such as
    # " (28/04/2026)".  The site's search input expects only the bare number, e.g.
    # "RAJ/P/2025/4508".  Strip any trailing parenthesised group before searching.
    search_reg_no = re.sub(r"\s*\([^)]*\)\s*$", "", reg_no).strip()

    try:
        page.goto(LISTING_PAGE_URL, timeout=60_000)
        page.wait_for_selector("table tbody tr", timeout=30_000)
        page.wait_for_load_state("networkidle", timeout=20_000)

        # Fill the registration-number search field and click Search
        try:
            reg_input = page.locator("input[name='registrationNo']").first
            reg_input.fill(search_reg_no)
            search_btn = page.locator(
                "button.btn-primary.w-100, button:has-text('Search')"
            ).first
            search_btn.click()
            page.wait_for_timeout(2_000)
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception as se:
            logger.warning(f"Registration search failed for {reg_no}: {se}")

        # Guard: confirm the table has real data rows before attempting to click.
        # An empty search returns a single "No data available" / "No records" row.
        try:
            first_row_text = page.locator("tbody tr").first.inner_text(timeout=5_000)
            if not first_row_text or any(
                phrase in first_row_text.lower()
                for phrase in ("no data", "no records", "no matching")
            ):
                logger.warning(f"No matching rows in listing for {reg_no!r} (searched: {search_reg_no!r})")
                return ""
        except Exception:
            pass  # can't confirm — proceed optimistically

        # Click the View button/link on the first visible matching row.
        # The element may be a <button> or an <a> tag depending on site version.
        view_locator = page.locator("tbody tr").first.locator(
            "button, a[href*='ProjectDetail'], a:has-text('View'), a:has-text('Details')"
        ).first
        view_locator.click(timeout=15_000)
        page.wait_for_load_state("networkidle", timeout=30_000)
        page.wait_for_timeout(1_500)

        detail_url = page.url
        if "ProjectDetail" in detail_url:
            return detail_url

        logger.warning(
            f"View click for {reg_no} did not reach ProjectDetail — got {detail_url}"
        )
        return ""
    except Exception as exc:
        logger.error(f"Navigation to detail failed for {reg_no}: {exc}")
        return ""




































def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Rajasthan RERA — Playwright-only.

    1. Navigates to the public listing page and searches for sentinel_registration_no.
    2. Clicks the View button to land on the project's detail page.
    3. Extracts fields via HTML scraping.
    4. Verifies ≥ 80% coverage against the stored baseline.
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
        "state_projects_sample", "rajasthan.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check",
                       path=sample_path, step="sentinel")
        return True

    logger.info(
        f"Sentinel: navigating to listing to find {sentinel_reg}", step="sentinel"
    )
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(user_agent=_UA)
            page = context.new_page()

            detail_url = _navigate_to_project_detail(page, sentinel_reg, logger)
            if not detail_url:
                logger.error(
                    "Sentinel: could not navigate to project detail page",
                    step="sentinel",
                )
                browser.close()
                insert_crawl_error(
                    run_id, config.get("id", "rajasthan_rera"),
                    "SENTINEL_FAILED", "Could not navigate to detail page",
                )
                return False

            logger.info(f"Sentinel: landed on {detail_url}", step="sentinel")
            fresh, _ = _scrape_detail_playwright(page, sentinel_reg, logger)
            browser.close()
    except Exception as exc:
        logger.error(f"Sentinel: Playwright failed — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted from detail page", step="sentinel")
        insert_crawl_error(
            run_id, config.get("id", "rajasthan_rera"),
            "SENTINEL_FAILED", "No data from detail page (Playwright)",
        )
        return False

    logger.info(f"Sentinel: checking coverage for {sentinel_reg}", step="sentinel")
    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "rajasthan_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


def _handle_document(project_key: str, doc: dict, run_id: int,
                     site_id: str, logger: CrawlerLogger,
                     client: httpx.Client | None = None) -> dict | None:
    """Download a document, upload to S3, persist to DB. Returns normalized document metadata or None."""
    url   = doc.get("url")
    label = doc.get("label", "document")
    if not url:
        return None
    reused, existing_s3_key = existing_uploaded_document_entry(project_key, {**doc, "url": url, "type": label})
    if reused:
        logger.info("Document reused", label=label, s3_key=existing_s3_key, step="documents")
        logger.log_document(label, url, "reused", s3_key=existing_s3_key)
        return reused
    filename = build_document_filename(doc)
    try:
        resp = download_response(url, logger=logger, timeout=15, client=client)
        if not resp or len(resp.content) < 100:
            return None
        content = resp.content
        md5     = compute_md5(content)
        s3_key  = upload_document(project_key, filename, content, dry_run=settings.DRY_RUN_S3)
        if s3_key is None:
            return None
        s3_url  = get_s3_url(s3_key)
        upsert_document(project_key=project_key, document_type=label, original_url=document_identity_url(doc) or url,
                        s3_key=s3_key, s3_bucket=settings.S3_BUCKET_NAME,
                        file_name=filename, md5_checksum=md5, file_size_bytes=len(content))
        logger.info("Document handled", label=label, s3_key=s3_key, step="documents")
        logger.log_document(label, url, "uploaded", s3_key=s3_key, file_size_bytes=len(content))
        return document_result_entry(doc, s3_url, filename)
    except Exception as e:
        logger.error(f"Document failed: {e}", url=url)
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(e),
                           project_key=project_key, url=url)
        return None


def run(config: dict, run_id: int, mode: str) -> dict:
    """Pure Playwright crawl: listing scrape + per-project detail page scraping."""
    logger   = CrawlerLogger(config["id"], run_id)
    site_id  = config["id"]
    counts   = dict(projects_found=0, projects_new=0, projects_updated=0,
                    projects_skipped=0, documents_uploaded=0, error_count=0)
    item_limit      = settings.CRAWL_ITEM_LIMIT or 0
    items_processed = 0
    machine_name, machine_ip = get_machine_context()
    t_run = time.monotonic()

    # ── Sentinel health check ────────────────────────────────────────────────
    t0 = time.monotonic()
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counts["error_count"] += 1
        return counts
    logger.timing("sentinel", time.monotonic() - t0)

    checkpoint       = load_checkpoint(site_id, mode) or {}
    resume_after_key = checkpoint.get("last_project_key")
    resume_pending   = bool(resume_after_key)

    # Phase 1: collect project list via Playwright listing scrape
    t0 = time.monotonic()
    listed_projects = _scrape_project_list_playwright(logger)
    if not listed_projects:
        return counts
    if item_limit:
        listed_projects = listed_projects[:item_limit]
        logger.info(f"Rajasthan: CRAWL_ITEM_LIMIT={item_limit} — {len(listed_projects)} projects")
    else:
        max_pages = settings.MAX_PAGES
        if max_pages:
            listed_projects = listed_projects[:max_pages * 50]
            logger.info(f"Rajasthan: limiting to {len(listed_projects)} projects (max_pages={max_pages})")
    counts["projects_found"] = len(listed_projects)
    logger.timing("search", time.monotonic() - t0, rows=len(listed_projects))

    # httpx session is used only for document downloads
    _timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0)
    session  = httpx.Client(timeout=_timeout, follow_redirects=True)

    # Phase 2: scrape each project detail page via Playwright (no API calls)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_UA)
        detail_page = context.new_page()

        for i, proj in enumerate(listed_projects):
            reg_no = proj.get("reg_no") or f"RJ-{i}"
            key    = generate_project_key(reg_no)
            if resume_pending:
                if key == resume_after_key:
                    resume_pending = False
                counts["projects_skipped"] += 1
                continue

            logger.set_project(key=key, reg_no=reg_no, url=LISTING_PAGE_URL, page=i)

            if mode == "daily_light" and get_project_by_key(key):
                logger.info("Skipping — already in DB (daily_light)", step="skip")
                counts["projects_skipped"] += 1
                logger.clear_project()
                continue

            detail_url = ""
            try:
                # Seed with listing-level fields from the HTML table
                data: dict = {}
                for list_f, schema_f in _LIST_API_TO_FIELD.items():
                    val = str(proj.get(list_f, "") or "").strip()
                    if val:
                        if schema_f.endswith("_date"):
                            val = _normalize_date_str(val) or val
                        elif schema_f == "project_type":
                            val = _normalize_project_type(val)
                        data[schema_f] = val

                # Navigate to detail page by clicking View from the listing
                detail_url = _navigate_to_project_detail(detail_page, reg_no, logger)

                # Scrape rich detail from the rendered detail page
                detail_fields: dict = {}
                doc_links: list[dict] = []
                if detail_url:
                    detail_fields, doc_links = _scrape_detail_playwright(
                        detail_page, reg_no, logger)
                # Detail page fields override listing fields (more authoritative)
                data.update(detail_fields)

                project_url = detail_url or LISTING_PAGE_URL
                data.update({
                    "key":              key,
                    "state":            config["state"],
                    "project_state":    "Rajasthan",
                    "domain":           DOMAIN,
                    "config_id":        config["config_id"],
                    "url":              project_url,
                    "is_live":          True,
                    "machine_name":     machine_name,
                    "crawl_machine_ip": machine_ip,
                })

                prod_data_fields: dict = {"govt_type": "state", "is_processed": False}
                if detail_url:
                    prod_data_fields["details_page"]           = detail_url
                    prod_data_fields["land_area_unit"]         = "In sq. meters"
                    prod_data_fields["construction_area_unit"] = "in sq. meters"

                _proj_type = data.get("project_type", "")
                if _proj_type:
                    prod_data_fields["type"] = _proj_type.replace("-", " ").title()

                _sub_date = data.get("submitted_date", "")
                _reg_no_for_temp = data.get("project_registration_no", reg_no)
                if _sub_date and _reg_no_for_temp:
                    try:
                        _dt = datetime.fromisoformat(_sub_date.replace("+00:00", ""))
                        prod_data_fields["temp"] = (
                            f"{_reg_no_for_temp} ({_dt.strftime('%d/%m/%Y')})"
                        )
                    except (ValueError, TypeError):
                        pass

                _pb_name    = data.get("promoter_name", "")
                _pb_contact = data.get("promoter_contact_details") or {}
                _pb_phone   = _pb_contact.get("phone", "")
                _pb_email   = _pb_contact.get("email", "")
                _promoter_block = [x for x in [_pb_name, _pb_phone, _pb_email] if x]
                if _promoter_block:
                    prod_data_fields["promoter_block"] = _promoter_block

                data["data"] = merge_data_sections(
                    prod_data_fields,
                    {"source": "playwright_html", "detail_url": detail_url},
                )

                logger.info("Normalizing and validating", step="normalize")
                try:
                    normalized = normalize_project_payload(
                        data, config, machine_name=machine_name, machine_ip=machine_ip)
                    record  = ProjectRecord(**normalized)
                    db_dict = record.to_db_dict()
                except (ValidationError, ValueError) as e:
                    logger.warning("Validation failed — raw fallback", error=str(e))
                    insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(e),
                                       project_key=key, url=detail_url, raw_data=data)
                    counts["error_count"] += 1
                    db_dict = normalize_project_payload(
                        {**data, "data": {"validation_fallback": True,
                                          "raw": data.get("data")}},
                        config, machine_name=machine_name, machine_ip=machine_ip,
                    )

                action = upsert_project(db_dict)
                items_processed += 1
                if action == "new":       counts["projects_new"] += 1
                elif action == "updated": counts["projects_updated"] += 1
                else:                     counts["projects_skipped"] += 1
                logger.info(f"DB result: {action}", step="db_upsert")

                if doc_links:
                    logger.info(f"Downloading {len(doc_links)} documents", step="documents")
                    uploaded_documents = []
                    doc_name_counts: dict[str, int] = {}
                    for doc in doc_links:
                        selected_doc = select_document_for_download(
                            config["state"], doc, doc_name_counts, domain=DOMAIN)
                        if selected_doc:
                            uploaded_doc = _handle_document(
                                key, selected_doc, run_id, site_id, logger, client=session)
                            if uploaded_doc:
                                uploaded_documents.append(uploaded_doc)
                                counts["documents_uploaded"] += 1
                            else:
                                uploaded_documents.append(
                                    {"link": doc.get("url"), "type": doc.get("label", "document")})
                        else:
                            uploaded_documents.append(
                                {"link": doc.get("url"), "type": doc.get("label", "document")})
                    if uploaded_documents:
                        upsert_project({
                            "key": db_dict["key"], "url": db_dict["url"],
                            "state": db_dict["state"], "domain": db_dict["domain"],
                            "project_registration_no": db_dict["project_registration_no"],
                            "uploaded_documents": uploaded_documents,
                            "document_urls": build_document_urls(uploaded_documents),
                        })

                if i % 100 == 0:
                    save_checkpoint(site_id, mode, i, key, run_id)
                random_delay(*config.get("rate_limit_delay", (1, 3)))

            except Exception as exc:
                logger.exception("Project processing failed", exc, step="project_loop",
                                 reg_no=reg_no)
                insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                                   project_key=key, url=detail_url or LISTING_PAGE_URL)
                counts["error_count"] += 1
            finally:
                logger.clear_project()

        browser.close()

    session.close()
    reset_checkpoint(site_id, mode)
    logger.info(f"Rajasthan RERA complete: {counts}")
    logger.timing("total_run", time.monotonic() - t_run)
    return counts
