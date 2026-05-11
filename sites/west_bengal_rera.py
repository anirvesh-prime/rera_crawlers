"""
West Bengal RERA Crawler — rera.wb.gov.in
Type: Playwright listing (DataTables JS API) + static httpx for detail pages

Strategy:
- The WB RERA listing page /district_project.php?dcode=0 uses DataTables with
  server-side or client-side rendering.  Python httpx is blocked by the site
  (Connection reset), so we use Playwright to load the page and extract all
  rows via the DataTables JavaScript API (which holds the complete dataset
  in memory even when only 10 rows are visible at a time).
- Each listing row: Sl No. | Old Reg No. | Project Name (link →procode) |
  Completion Date | Registration No. | Registration Date
- Detail pages: /project_details.php?procode=N
  Sections: PROJECT STATUS banner, Highlights, Residential Details, Facilities,
  Consultants, Promoter Details, Promoter and Officials table, Promoter's Document,
  Sanctioned Plan, Legal Title Deed, Other Project Documents,
  Authenticated Legal Document.
- Documents: anchors to doc.repository.semtwb.in, typed by section + row.
  Promoter's Document row 9 → "Plan of Development {n}" per file.
  Other Project Documents Sub Title → "Sanction Plan {n}" etc.
"""
from __future__ import annotations

import re
import time

from bs4 import BeautifulSoup, Tag
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, reset_checkpoint, save_checkpoint
from core.crawler_base import generate_project_key, get_legacy_ssl_context, random_delay, safe_get
from core.db import (
    get_project_by_key,
    insert_crawl_error,
    upsert_document,
    upsert_project,
)
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

LISTING_URL = "https://rera.wb.gov.in/district_project.php?dcode=0"
BASE_URL    = "https://rera.wb.gov.in"
DOMAIN      = "rera.wb.gov.in"

_PROCODE_RE = re.compile(r"procode=(\d+)", re.I)
_REG_RE     = re.compile(r"WBRERA/[A-Z]/[A-Z]+/\d{4}/\d+", re.I)
_AREA_RE    = re.compile(r"([\d,]+(?:\.\d+)?)\s*(sq\.?\s*m(?:tr)?|sqmt|sqmtr)?", re.I)
_DATE_RE    = re.compile(r"\d{2}-\d{2}-\d{4}")
_PLAN_DEV_KW = ("plan of development", "plan of development works")


# ── HTTP helper ────────────────────────────────────────────────────────────────
# WB RERA requires a legacy SSL context (UNSAFE_LEGACY_RENEGOTIATION_DISABLED).
# We create a single reusable httpx.Client with that context.
import httpx as _httpx

_SSL_CTX = get_legacy_ssl_context()
_CLIENT  = _httpx.Client(
    verify=_SSL_CTX,
    timeout=60.0,
    follow_redirects=True,
)


def _get(url: str, logger: CrawlerLogger, params: dict | None = None):
    return safe_get(url, logger=logger, timeout=60.0, params=params, client=_CLIENT)


# ── Listing page ───────────────────────────────────────────────────────────────

def _playwright_fetch_all_listing_rows(logger) -> list[dict]:
    """Use Playwright + DataTables JS API to fetch all WB RERA project rows.

    The site blocks Python httpx (Connection reset), but Chromium works fine.
    DataTables stores the full dataset in JS memory even when it only renders
    10 rows at a time, so we call dt.rows().data() to retrieve everything.

    Returns one dict per project (same format as ``_parse_listing_rows``).
    """
    from playwright.sync_api import sync_playwright

    rows: list[dict] = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(ignore_https_errors=True)
        page = ctx.new_page()
        try:
            page.goto(LISTING_URL, timeout=60_000, wait_until="networkidle")
            page.wait_for_timeout(3_000)

            # Extract all rows via DataTables JS API
            raw_rows = page.evaluate("""() => {
                if (typeof $ === 'undefined' || typeof $.fn.DataTable === 'undefined')
                    return null;
                const tables = $.fn.dataTable.tables();
                if (!tables || !tables.length) return null;
                const dt = $(tables[0]).DataTable();
                return dt.rows().data().toArray();
            }""")

            if not raw_rows:
                logger.warning("DataTables JS API returned no rows; "
                               "falling back to HTML tbody parsing")
                html = page.content()
                return _parse_listing_rows(BeautifulSoup(html, "lxml"))

            logger.info(f"DataTables JS API returned {len(raw_rows)} rows")

            for raw in raw_rows:
                # Each row is a list: [serial, old_reg_no, name_html, comp_date, reg_no, reg_date]
                if not isinstance(raw, list) or len(raw) < 5:
                    continue
                name_html = raw[2] if len(raw) > 2 else ""
                reg_no    = (raw[4] if len(raw) > 4 else "").strip()
                reg_date  = (raw[5] if len(raw) > 5 else "").strip()
                if not reg_no:
                    continue

                # Parse project name and procode from the HTML cell
                name_soup = BeautifulSoup(name_html, "lxml")
                a_tag = name_soup.find("a", href=True)
                project_name = a_tag.get_text(strip=True) if a_tag else None
                procode = None
                detail_url = None
                if a_tag:
                    m = _PROCODE_RE.search(a_tag["href"])
                    if m:
                        procode = m.group(1)
                        detail_url = f"{BASE_URL}/project_details.php?procode={procode}"

                row: dict = {
                    "project_registration_no": reg_no,
                    "project_name":            project_name,
                    "promoter_name":           None,
                    "status_of_the_project":   None,
                    "approved_on_date":        reg_date or None,
                    "detail_url":              detail_url,
                    "procode":                 procode,
                }
                rows.append(row)
        except Exception as e:
            logger.error(f"Playwright listing fetch failed: {e}")
        finally:
            browser.close()
    return rows


def _parse_listing_rows(soup: BeautifulSoup) -> list[dict]:
    """Return one dict per project from the DataTables listing table."""
    table = soup.find("table", id="projectDataTable")
    if not table:
        for t in soup.find_all("table"):
            ths = [th.get_text(strip=True).lower() for th in t.find_all("th")]
            if any("registration" in h for h in ths):
                table = t
                break
    if not table:
        return []

    header_row = table.find("tr")
    if not header_row:
        return []
    headers = [th.get_text(strip=True).lower()
               for th in header_row.find_all(["th", "td"])]

    def _idx(*candidates: str) -> int | None:
        for c in candidates:
            for i, h in enumerate(headers):
                if c in h:
                    return i
        return None

    name_idx   = _idx("project name", "name")
    reg_idx    = _idx("registration")
    dist_idx   = _idx("district")
    status_idx = _idx("status")
    prom_idx   = _idx("promoter")

    tbody = table.find("tbody") or table
    rows: list[dict] = []
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        def _cell(idx: int | None) -> str:
            if idx is None or idx >= len(tds):
                return ""
            return tds[idx].get_text(separator=" ", strip=True)

        project_name = None
        procode = None
        detail_url = None
        if name_idx is not None and name_idx < len(tds):
            a = tds[name_idx].find("a", href=True)
            if a:
                project_name = a.get_text(strip=True) or None
                m = _PROCODE_RE.search(a["href"])
                if m:
                    procode = m.group(1)
                    detail_url = f"{BASE_URL}/project_details.php?procode={procode}"
            if not project_name:
                project_name = tds[name_idx].get_text(separator=" ", strip=True) or None

        reg_no = _cell(reg_idx).strip()
        if not reg_no:
            for td in tds:
                txt = td.get_text(strip=True)
                if _REG_RE.match(txt):
                    reg_no = txt
                    break
        if not reg_no:
            continue

        district = _cell(dist_idx)
        row: dict = {
            "project_registration_no": reg_no,
            "project_name":            project_name,
            "promoter_name":           _cell(prom_idx) or None,
            "status_of_the_project":   _cell(status_idx) or None,
            "detail_url":              detail_url,
            "procode":                 procode,
        }
        if district:
            row["project_location_raw"] = {"raw_address": district}
        rows.append(row)
    return rows


# ── Detail page helpers ────────────────────────────────────────────────────────

def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _find_section_table(heading_el: Tag) -> Tag | None:
    """Return the first <table> following heading_el.

    Searches up to 12 direct siblings of the heading element, then falls back
    to searching siblings of the heading's parent and grandparent containers.
    This handles the new WB RERA layout where headings are wrapped in
    col-md-12 divs and the actual content tables are in sibling row divs.
    """
    def _search_siblings(start_el: Tag, limit: int = 12) -> Tag | None:
        sib = start_el.next_sibling
        for _ in range(limit):
            if sib is None:
                break
            if isinstance(sib, Tag):
                if sib.name == "table":
                    return sib
                t = sib.find("table")
                if t:
                    return t
            sib = sib.next_sibling
        return None

    # 1. Try direct siblings of the heading element
    t = _search_siblings(heading_el)
    if t:
        return t

    # 2. Try siblings of the heading's parent container div
    parent = heading_el.parent
    if parent and isinstance(parent, Tag):
        t = _search_siblings(parent)
        if t:
            return t

        # 3. Try siblings of the grandparent container div
        gp = parent.parent
        if gp and isinstance(gp, Tag):
            t = _search_siblings(gp, limit=6)
            if t:
                return t

    return None


def _table_rows_with_links(table: Tag) -> list[list[dict]]:
    """
    Return data rows (skipping the header) as lists of cell dicts {text, hrefs}.
    hrefs contains only anchors pointing to doc.repository.semtwb.in.
    """
    all_trs = table.find_all("tr")
    results: list[list[dict]] = []
    header_done = False
    for tr in all_trs:
        ths = tr.find_all("th")
        tds = tr.find_all("td")
        if ths and not header_done:
            header_done = True
            continue
        if not tds:
            header_done = True
            continue
        cells = []
        for td in tds:
            hrefs = [a["href"] for a in td.find_all("a", href=True)
                     if "doc.repository" in a.get("href", "")]
            cells.append({"text": td.get_text(separator=" ", strip=True), "hrefs": hrefs})
        results.append(cells)
    return results


def _table_rows(table: Tag) -> list[list[str]]:
    return [[c["text"] for c in row] for row in _table_rows_with_links(table)]


def _extract_area(raw: str) -> float | None:
    m = _AREA_RE.search(raw)
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except ValueError:
            pass
    return None


# ── Document extractors ────────────────────────────────────────────────────────

def _is_plan_of_dev(desc: str) -> bool:
    dl = desc.lower()
    return any(kw in dl for kw in _PLAN_DEV_KW)


def _extract_promoters_doc_links(table: Tag) -> list[dict]:
    """
    Promoter's Document table: Sl No. | File Description | File Name | Download
    • Rows 1-8, 10-13: type = "{sl_no} {full description}"
    • Row 9 (plan of development…): type = "Plan of Development {n}" per file
    """
    results: list[dict] = []
    plan_counter = 0
    seen: set[str] = set()

    for row_cells in _table_rows_with_links(table):
        if not row_cells:
            continue
        sl_text = row_cells[0]["text"].strip()
        if not sl_text.isdigit():
            continue
        description = row_cells[1]["text"] if len(row_cells) > 1 else ""

        doc_hrefs: list[str] = []
        for cell in row_cells:
            for href in cell["hrefs"]:
                if href not in seen:
                    seen.add(href)
                    doc_hrefs.append(href)

        if not doc_hrefs:
            continue

        if _is_plan_of_dev(description):
            for href in doc_hrefs:
                plan_counter += 1
                results.append({"url": href, "label": f"Plan of Development {plan_counter}"})
        else:
            doc_type = f"{sl_text} {description}".strip()
            for href in doc_hrefs:
                results.append({"url": href, "label": doc_type})

    return results


def _extract_numbered_section_links(table: Tag) -> list[dict]:
    """Generic numbered section (Sanctioned Plan, Legal Title Deed, etc.)."""
    results: list[dict] = []
    seen: set[str] = set()
    for row_cells in _table_rows_with_links(table):
        if not row_cells:
            continue
        sl_text = row_cells[0]["text"].strip()
        if not sl_text.isdigit():
            continue
        description = row_cells[1]["text"] if len(row_cells) > 1 else ""
        doc_type = f"{sl_text} {description}".strip() if description else sl_text
        for cell in row_cells:
            for href in cell["hrefs"]:
                if href not in seen:
                    seen.add(href)
                    results.append({"url": href, "label": doc_type})
    return results


def _extract_other_project_docs(table: Tag) -> list[dict]:
    """
    Other Project Documents: Sl No. | Title | Sub Title | Download.
    Sub Title → base type; files numbered per unique sub-title.
    """
    results: list[dict] = []
    type_counters: dict[str, int] = {}
    seen: set[str] = set()
    for row_cells in _table_rows_with_links(table):
        if len(row_cells) < 3:
            continue
        sub_title = row_cells[2]["text"].strip()
        if not sub_title:
            sub_title = row_cells[1]["text"].strip() or "Document"
        doc_hrefs: list[str] = []
        for cell in row_cells:
            for href in cell["hrefs"]:
                if href not in seen:
                    seen.add(href)
                    doc_hrefs.append(href)
        for href in doc_hrefs:
            type_counters[sub_title] = type_counters.get(sub_title, 0) + 1
            results.append({"url": href, "label": f"{sub_title} {type_counters[sub_title]}"})
    return results


# ── Main detail page parser ────────────────────────────────────────────────────

def _fetch_detail_html_playwright(url: str, logger: CrawlerLogger) -> str | None:
    """Fetch a WB RERA detail page via Playwright (httpx is blocked by the site)."""
    from playwright.sync_api import sync_playwright
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(ignore_https_errors=True)
            page = ctx.new_page()
            page.goto(url, timeout=60_000, wait_until="networkidle")
            html = page.content()
            browser.close()
            return html
    except Exception as e:
        logger.warning(f"Playwright detail fetch failed for {url}: {e}")
        return None


def _parse_detail_page(url: str, logger: CrawlerLogger) -> dict:
    """Fetch and parse a WB RERA project detail page. Returns a flat dict."""
    # Try httpx first; fall back to Playwright since site blocks Python SSL.
    html: str | None = None
    resp = _get(url, logger)
    if resp:
        html = resp.text
    else:
        html = _fetch_detail_html_playwright(url, logger)
    if not html:
        return {}
    soup = BeautifulSoup(html, "lxml")
    out: dict = {}
    raw: dict = {"source_url": url, "govt_type": "state"}

    # State is always West Bengal for this crawler
    out["project_state"] = "west bengal"

    # ── PROJECT NAME ─────────────────────────────────────────────────────────
    # The project name is in an <h2> inside the col-md-12 div that also
    # contains the ul.outerrera status banner.
    outerrera_ul = soup.find("ul", class_="outerrera")
    if outerrera_ul:
        name_container = outerrera_ul.parent
        if name_container:
            h2 = name_container.find("h2")
            if h2:
                out["project_name"] = _clean(h2.get_text())

    # ── PROJECT STATUS banner (ul.outerrera) ──────────────────────────────────
    # <ul class="outerrera">
    #   <li>PROJECT STATUS - <strong>Not Started </strong></li>
    #   <li>PROJECT ID: WBRERA/NPR-000629</li>
    #   <li>PROJECT COMPLETION DATE: DD-MM-YYYY</li>
    #   <li>EXTENSION COMPLETION DATE: NA</li>
    #   <li>RERA REGISTRATION NO.: WBRERA/P/.../NNNNN</li>
    # </ul>
    if outerrera_ul:
        status_block = _clean(outerrera_ul.get_text(separator=" "))
    else:
        # Fallback: search the full page text (may match scrolling news first)
        status_block = _clean(soup.get_text(separator=" "))

    # Parse from the full page text using anchored patterns (works with either source)
    page_text = _clean(soup.get_text(separator=" "))

    m = re.search(r"PROJECT STATUS\s*[-–]\s*(.+?)PROJECT ID", page_text, re.I)
    if m:
        out["status_of_the_project"] = _clean(m.group(1))

    m = re.search(r"PROJECT ID\s*:\s*(WBRERA/NPR-\d+)", page_text, re.I)
    if m:
        out["acknowledgement_no"] = m.group(1).strip()

    m = re.search(r"RERA REGISTRATION NO\.\s*:\s*(WBRERA/[A-Z]/[A-Z]+/\d{4}/\d+)",
                  page_text, re.I)
    if m:
        out["project_registration_no"] = m.group(1).strip()

    # Completion dates from the banner (skip the "NA" extension date)
    if outerrera_ul:
        dates = _DATE_RE.findall(status_block)
    else:
        dates = _DATE_RE.findall(page_text)
    if dates:
        out["estimated_finish_date"] = dates[0]
        out["actual_finish_date"]    = dates[0]
        raw["completion_date_raw"]   = dates[0]
        if len(dates) > 1 and dates[1].upper() != "NA":
            raw["extension_completion_date"] = dates[1]

    # Registration / approved date — not shown on detail page; will be injected
    # from the DataTables listing reg_date field in _sentinel_check / run().
    reg_date_m = re.search(
        r"Registration\s+Date\s*[:\-]?\s*(\d{2}-\d{2}-\d{4})", page_text, re.I
    )
    if reg_date_m:
        out["approved_on_date"] = reg_date_m.group(1)

    # ── RERA registration certificate (VIEW CERTIFICATE link) ─────────────────
    cert_counter = 0
    doc_links: list[dict] = []
    seen_urls: set[str] = set()

    def _add_doc(url_: str, label_: str) -> None:
        if url_ not in seen_urls:
            seen_urls.add(url_)
            doc_links.append({"url": url_, "label": label_})

    for a in soup.find_all("a", href=True):
        href = a["href"]
        txt  = a.get_text(strip=True).upper()
        if ("VIEW CERTIFICATE" in txt or "upcer" in href) and "doc.repository" in href:
            cert_counter += 1
            _add_doc(href, f"Rera Registration Certificate {cert_counter}")

    # ── Highlights ────────────────────────────────────────────────────────────
    # New layout (2025): <h3>Highlights</h3> followed by a <div class="ms_overviewList">
    # which contains <ul><li><span>Label<br/><b>Value</b></span></li></ul> items.
    hl_str = soup.find(string=re.compile(r"^Highlights", re.I))
    if hl_str:
        hl_heading = hl_str.parent  # h3 or h5 element
        # Find the ms_overviewList div (skip style sibling, grab first div sibling)
        ov_div = None
        sib = hl_heading.next_sibling
        for _ in range(5):
            if sib is None:
                break
            if isinstance(sib, Tag) and sib.name == "div":
                ov_div = sib
                break
            sib = sib.next_sibling

        if ov_div:
            for li in ov_div.find_all("li"):
                span = li.find("span")
                if not span:
                    continue
                # span text: "Label\nValue" (br separates them)
                parts = span.get_text(separator="|", strip=True).split("|")
                if len(parts) < 2:
                    continue
                label = parts[0].lower().strip()
                val   = parts[-1].strip()  # last part is the actual value
                if "land area" in label:
                    m = re.search(r"([\d,]+(?:\.\d+)?)", val)
                    if m:
                        try:
                            out["land_area"] = float(m.group(1).replace(",", ""))
                            raw["land_area_unit"] = "sq.mtr."
                        except ValueError:
                            pass
                elif "super built" in label or "total built" in label:
                    m = re.search(r"([\d,]+(?:\.\d+)?)", val)
                    if m:
                        try:
                            out["construction_area"] = float(m.group(1).replace(",", ""))
                            raw["construction_area_unit"] = "sq.mtr."
                        except ValueError:
                            pass
                elif "project type" in label:
                    out.setdefault("project_type", val)
        else:
            # Fallback: text-based extraction from heading's container
            h_text = _clean(hl_heading.get_text(separator=" "))
            la_m = re.search(r"Land\s+Area\s+([\d,]+(?:\.\d+)?)", h_text, re.I)
            if la_m:
                try:
                    out["land_area"] = float(la_m.group(1).replace(",", ""))
                    raw["land_area_unit"] = "sq.mtr."
                except ValueError:
                    pass
            pt_m = re.search(r"Project\s+Type\s+(\w+)", h_text, re.I)
            if pt_m:
                out.setdefault("project_type", pt_m.group(1).strip())

    # ── Residential Details ───────────────────────────────────────────────────
    # New layout (2025): data is in col-md-3 div siblings, not a table.
    # e.g. <div class="col-md-3">Land Area:<br>1021 sq.mtr.</div>
    #       <div class="col-md-3">Total Built Up Area:<br>2629 sq.mtr.</div>
    #       <div class="col-md-3">No. of Apartments:<br>31</div>
    res_h5 = soup.find("h5", string=re.compile(r"Residential Details", re.I))
    if res_h5:
        sib = res_h5.next_sibling
        for _ in range(20):
            if sib is None:
                break
            if isinstance(sib, Tag):
                # Stop at next section heading
                if sib.name in ("h3", "h4", "h5") and sib is not res_h5:
                    break
                if sib.name == "div":
                    text = sib.get_text(separator=":", strip=True)
                    if ":" in text:
                        label, _, val = text.partition(":")
                        label = label.lower().strip()
                        # Strip extra leading colons added by get_text separator
                        # when divs contain <br/> tags (e.g. "Land Area::1021")
                        val   = val.lstrip(":").strip()
                        if "land area" in label:
                            m = re.search(r"([\d,]+(?:\.\d+)?)", val)
                            if m:
                                try:
                                    out.setdefault("land_area",
                                                   float(m.group(1).replace(",", "")))
                                    raw.setdefault("land_area_unit", "sq.mtr.")
                                except ValueError:
                                    pass
                        elif "total built" in label or "built up" in label:
                            m = re.search(r"([\d,]+(?:\.\d+)?)", val)
                            if m:
                                try:
                                    out.setdefault("construction_area",
                                                   float(m.group(1).replace(",", "")))
                                    raw.setdefault("construction_area_unit", "sq.mtr.")
                                except ValueError:
                                    pass
                        elif any(kw in label for kw in (
                            "no. of apart", "no. of flat", "no. of unit",
                            "number of apart", "total unit", "total flat", "total apart",
                        )):
                            try:
                                out["number_of_residential_units"] = int(val.replace(",", ""))
                            except ValueError:
                                pass
                        elif "floor area" in label or "residential area" in label:
                            v = _extract_area(val)
                            if v is not None:
                                out["total_floor_area_under_residential"] = v
            sib = sib.next_sibling

        # Also try the old table approach if divs yielded nothing
        if "number_of_residential_units" not in out:
            res_table = _find_section_table(res_h5)
            if res_table:
                for row in _table_rows(res_table):
                    if len(row) < 2:
                        continue
                    label = row[0].lower()
                    val   = row[1]
                    if any(kw in label for kw in (
                        "total unit", "total flat", "total apart", "no. of apart",
                    )):
                        try:
                            out["number_of_residential_units"] = int(val.replace(",", ""))
                        except ValueError:
                            pass
                    elif "floor area" in label or "residential area" in label:
                        v = _extract_area(val)
                        if v is not None:
                            out["total_floor_area_under_residential"] = v

    # ── Location ──────────────────────────────────────────────────────────────
    # New layout (2025): <h3>Location</h3> followed by an <h5> with the full address.
    loc_heading = soup.find(["h3", "h5"], string=re.compile(r"^Location$", re.I))
    if loc_heading:
        addr_h5 = loc_heading.find_next("h5")
        if addr_h5:
            addr = _clean(addr_h5.get_text(separator=" "))
            if len(addr) > 5:
                out["project_location_raw"] = {"raw_address": addr}

    # ── Facilities ────────────────────────────────────────────────────────────
    provided_facility: list[dict] = []
    fac_h5 = soup.find("h5", string=re.compile(r"^Facilities$", re.I))
    if fac_h5:
        fac_table = _find_section_table(fac_h5)
        if fac_table:
            for row in _table_rows(fac_table):
                if row and row[0].strip():
                    provided_facility.append({"facility": row[0].strip(), "has_same_data": True})
        if not provided_facility:
            container = fac_h5.find_parent("div") or fac_h5
            for li in container.find_all("li"):
                txt = li.get_text(strip=True)
                if txt:
                    provided_facility.append({"facility": txt, "has_same_data": True})

    # ── Consultants → professional_information ────────────────────────────────
    professionals: list[dict] = []
    cons_h5 = soup.find("h5", string=re.compile(r"^Consultants$", re.I))
    if cons_h5:
        cons_table = _find_section_table(cons_h5)
        if cons_table:
            for row in _table_rows(cons_table):
                # Columns: Sl No. | Name | Address | Type
                if len(row) < 3:
                    continue
                name = row[1].strip()
                addr = row[2].strip() if len(row) > 2 else ""
                role = row[3].strip() if len(row) > 3 else ""
                if name:
                    prof: dict = {"name": name}
                    if role:
                        prof["role"] = role
                    if addr:
                        prof["address"] = addr
                    professionals.append(prof)


    # ── Promoter Details (Personal Information block) ────────────────────────
    # New layout (2025): <h5>Personal Information</h5> is in a col-md-12 div
    # inside a row div. The actual promoter content (company name, address, etc.)
    # is in the NEXT sibling of that row div (not the same col-md-12 container).
    promoter_address_raw: dict = {}
    promoters_details: dict = {}

    pi_h5 = soup.find("h5", string=re.compile(r"Personal Information", re.I))
    if pi_h5:
        # Try grandparent row sibling for the actual content div
        pi_parent = pi_h5.parent   # col-md-12
        pi_row    = pi_parent.parent if pi_parent else None  # row div
        content_div: Tag | None = None
        if pi_row and isinstance(pi_row, Tag):
            sib = pi_row.next_sibling
            for _ in range(5):
                if sib is None:
                    break
                if isinstance(sib, Tag):
                    content_div = sib
                    break
                sib = sib.next_sibling
        container = content_div or pi_h5.find_parent("div") or pi_h5
        pi_text = _clean(container.get_text(separator=" "))
        promoter_address_raw["registered_address"] = pi_text

        # Company name: text before "Company Type"
        parts = pi_text.split("Company Type")
        company_raw = parts[0].replace("Personal Information", "").strip()
        if company_raw:
            promoters_details["name"] = company_raw.upper()
            raw["temp_promoter"] = company_raw.upper()

        ct_m = re.search(r"Company\s+Type\s*[:\-]?\s*([^\n]+?)(?:Address|$)", pi_text, re.I)
        if ct_m:
            promoters_details["type_of_firm"] = _clean(ct_m.group(1))

        dist_m = re.search(r"District\s*[:\-]?\s*([\w\s]+?)(?:\s+Registration|\s*$)", pi_text, re.I)
        if dist_m:
            promoter_address_raw["district"] = _clean(dist_m.group(1))

        pin_m = re.search(r"Pincode\s*[:\-]?\s*(\d{6})", pi_text, re.I)
        if pin_m:
            promoter_address_raw["pin_code"] = pin_m.group(1)

        reg_m = re.search(r"Registration\s+No\.\s*[:\-]?\s*([^\n\s]+)", pi_text, re.I)
        if reg_m:
            promoters_details["registration_no"] = _clean(reg_m.group(1))

    # ── Promoter and Officials table ─────────────────────────────────────────
    # Columns: Sl No. | Promoter Name | Firm Name | Est.Year | Contact | Email ID | Address | Completed Projects
    officials_h5 = soup.find("h5", string=re.compile(r"Promoter and other officials", re.I))
    promoter_name_from_officials: str | None = None
    if officials_h5:
        officials_table = _find_section_table(officials_h5)
        if officials_table:
            emails: list[str] = []
            first_address: str | None = None
            exp_state: str | None = None
            for row in _table_rows(officials_table):
                if len(row) < 2:
                    continue
                pname   = row[1].strip() if len(row) > 1 else ""
                email   = row[5].strip() if len(row) > 5 else ""
                address = row[6].strip() if len(row) > 6 else ""
                completed = row[7].strip() if len(row) > 7 else ""
                if pname and not promoter_name_from_officials:
                    promoter_name_from_officials = pname
                if email:
                    emails.append(email)
                if address and not first_address:
                    first_address = address
                if completed and not exp_state:
                    exp_state = completed

            if emails:
                out["promoter_contact_details"] = {"email": " ".join(emails)}
            if first_address:
                promoter_address_raw["raw_address"] = first_address
            if exp_state:
                promoters_details["experience_state"] = exp_state

    if promoter_name_from_officials:
        out["promoter_name"] = promoter_name_from_officials
    if promoter_address_raw:
        out["promoter_address_raw"] = promoter_address_raw
    if promoters_details:
        out["promoters_details"] = promoters_details
    if provided_facility:
        out["provided_faciltiy"] = provided_facility   # preserve schema typo
    if professionals:
        out["professional_information"] = professionals

    # ── Document sections ─────────────────────────────────────────────────────
    # 2. Promoter's Document
    prom_doc_h5 = soup.find("h5", string=re.compile(r"Promoter'?s? Document", re.I))
    if prom_doc_h5:
        t = _find_section_table(prom_doc_h5)
        if t:
            for doc in _extract_promoters_doc_links(t):
                _add_doc(doc["url"], doc["label"])

    # 3. Sanctioned Plan, Layout Plan
    plan_h5 = soup.find("h5", string=re.compile(r"Sanctioned Plan.*Layout", re.I))
    if plan_h5:
        t = _find_section_table(plan_h5)
        if t:
            for doc in _extract_numbered_section_links(t):
                _add_doc(doc["url"], doc["label"])

    # 4. Legal Title Deed
    title_h5 = soup.find("h5", string=re.compile(r"legal title deed", re.I))
    if title_h5:
        t = _find_section_table(title_h5)
        if t:
            for doc in _extract_numbered_section_links(t):
                _add_doc(doc["url"], doc["label"])

    # 5. Project Completion Certificate
    comp_h5 = soup.find("h5", string=re.compile(r"Project Completion Certificate", re.I))
    if comp_h5:
        t = _find_section_table(comp_h5)
        if t:
            for doc in _extract_numbered_section_links(t):
                _add_doc(doc["url"], doc["label"])

    # 6. Other Project Documents (Sub Title → "Sanction Plan {n}" etc.)
    other_h5 = soup.find("h5", string=re.compile(r"Other Project Documents", re.I))
    if other_h5:
        t = _find_section_table(other_h5)
        if t:
            for doc in _extract_other_project_docs(t):
                _add_doc(doc["url"], doc["label"])

    # 7. Authenticated Legal Document
    auth_h5 = soup.find("h5", string=re.compile(r"Authenticated Legal Document", re.I))
    if auth_h5:
        t = _find_section_table(auth_h5)
        if t:
            for doc in _extract_numbered_section_links(t):
                _add_doc(doc["url"], doc["label"])

    out["_doc_links"] = doc_links
    # Also expose as uploaded_documents so the sentinel coverage check can verify
    # that documents are reachable (sentinel uses this field name).
    if doc_links:
        out["uploaded_documents"] = doc_links
    out["data"] = raw
    return out



# ── Document upload helper ────────────────────────────────────────────────────

def _handle_document(
    project_key: str, doc: dict, run_id: int, site_id: str, logger: CrawlerLogger,
) -> dict | None:
    url   = doc["url"]
    label = doc.get("label", "document")
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
        logger.info("Document uploaded", label=label, s3_key=s3_key, step="documents")
        logger.log_document(label, url, "uploaded", s3_key=s3_key, file_size_bytes=len(resp.content))
        return document_result_entry(doc, s3_url, fname)
    except Exception as e:
        logger.error(f"Doc failed for {project_key}: {e}")
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(e),
                           project_key=project_key, url=url)
        return None


# ── Main run() ────────────────────────────────────────────────────────────────

def _sentinel_find_procode(
    sentinel_reg: str, logger: CrawlerLogger
) -> "tuple[str | None, str | None]":
    """
    Use Playwright + the DataTables JS API on the WB RERA listing page to find
    the procode and registration date for the given registration number.

    Returns (procode, reg_date) on success, or (None, None) if not found /
    site unreachable.
    """
    from playwright.sync_api import sync_playwright

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(ignore_https_errors=True)
            page = ctx.new_page()
            page.goto(LISTING_URL, timeout=60_000, wait_until="networkidle")
            page.wait_for_timeout(3_000)

            # Check for 503 / maintenance page before going further
            title = page.title()
            if "503" in title or "unavailable" in title.lower():
                logger.warning(
                    f"Sentinel: listing page returned {title!r} — skipping",
                    step="sentinel",
                )
                browser.close()
                return None, None

            result = page.evaluate(
                """(targetReg) => {
                    if (typeof $ === 'undefined' || typeof $.fn.DataTable === 'undefined')
                        return null;
                    const tables = $.fn.dataTable.tables();
                    if (!tables || !tables.length) return null;
                    const dt = $(tables[0]).DataTable();
                    const allRows = dt.rows().data().toArray();
                    for (const row of allRows) {
                        // row layout: [serial, old_reg_no, name_html, comp_date, reg_no, reg_date]
                        if (Array.isArray(row) && row.length > 4 &&
                                String(row[4]).trim() === targetReg) {
                            const nameHtml = row[2] || '';
                            const m = nameHtml.match(/procode=(\\d+)/i);
                            const procode = m ? m[1] : null;
                            const regDate = row.length > 5 ? String(row[5]).trim() : '';
                            return {procode: procode, reg_date: regDate};
                        }
                    }
                    return null;
                }""",
                sentinel_reg,
            )
            browser.close()

            procode  = (result or {}).get("procode")
            reg_date = (result or {}).get("reg_date") or None

            if procode:
                logger.info(
                    f"Sentinel: found procode={procode} reg_date={reg_date!r} "
                    f"for {sentinel_reg}",
                    step="sentinel",
                )
            else:
                logger.warning(
                    f"Sentinel: {sentinel_reg!r} not found in DataTables listing",
                    step="sentinel",
                )
            return procode, reg_date

    except Exception as exc:
        logger.warning(f"Sentinel: listing search failed ({exc})", step="sentinel")
        return None, None


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for West Bengal RERA.

    Uses the listing page's DataTables JS API to look up the sentinel project
    by registration number, derives the correct detail URL from the live procode,
    then verifies ≥ 80% field coverage against state_projects_sample/west_bengal.json.
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
        "state_projects_sample", "west_bengal.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check",
                       path=sample_path, step="sentinel")
        return True

    # ── Find the live procode via listing-page search ──────────────────────────
    logger.info(
        f"Sentinel: searching listing page for {sentinel_reg}", step="sentinel"
    )
    procode, reg_date = _sentinel_find_procode(sentinel_reg, logger)

    if procode is None:
        # Site is down / project not found — skip rather than abort the crawl
        logger.warning(
            "Sentinel: could not locate project on listing page — skipping",
            step="sentinel",
        )
        return True

    detail_url = f"{BASE_URL}/project_details.php?procode={procode}"
    logger.info(f"Sentinel: scraping {sentinel_reg}", url=detail_url, step="sentinel")

    try:
        fresh = _parse_detail_page(detail_url, logger) or {}
    except Exception as exc:
        logger.error(f"Sentinel: scrape error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", url=detail_url, step="sentinel")
        return False

    # Inject listing-sourced fields that aren't available on the detail page alone.
    # approved_on_date comes from the DataTables reg_date column (registration date).
    if reg_date and not fresh.get("approved_on_date"):
        fresh["approved_on_date"] = reg_date

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "wb_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


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
    logger  = CrawlerLogger(config["id"], run_id)
    site_id = config["id"]
    counts  = dict(projects_found=0, projects_new=0, projects_updated=0,
                   projects_skipped=0, documents_uploaded=0, error_count=0)

    checkpoint = load_checkpoint(site_id, mode) or {}
    done_regs: set[str] = set(checkpoint.get("done_regs", []))
    item_limit = settings.CRAWL_ITEM_LIMIT or 0
    machine_name, machine_ip = get_machine_context()
    t_run = time.monotonic()

    # ── Sentinel health check ────────────────────────────────────────────────
    t0 = time.monotonic()
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counts["error_count"] += 1
        return counts
    logger.warning(f"Step timing [sentinel]: {time.monotonic()-t0:.2f}s", step="timing")

    # Use Playwright to fetch all rows via the DataTables JS API.
    # Direct httpx is blocked by the site (Connection reset by peer).
    t0 = time.monotonic()
    logger.info("Fetching WB RERA listing via Playwright + DataTables API")
    rows = _playwright_fetch_all_listing_rows(logger)
    if not rows:
        logger.error("No project rows found on listing page")
        insert_crawl_error(run_id, site_id, "PARSE_ERROR", "No rows parsed", url=LISTING_URL)
        counts["error_count"] += 1
        return counts

    counts["projects_found"] = len(rows)
    logger.info(f"Listing page: {len(rows)} projects found")
    logger.warning(f"Step timing [search]: {time.monotonic()-t0:.2f}s  rows={len(rows)}", step="timing")
    items_processed = 0

    for row in rows:
        if item_limit and items_processed >= item_limit:
            logger.info(f"CRAWL_ITEM_LIMIT={item_limit} reached, stopping")
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
                "project_state":           "west bengal",
                "project_registration_no": reg_no,
                "project_name":            row.get("project_name"),
                "promoter_name":           row.get("promoter_name"),
                "status_of_the_project":   row.get("status_of_the_project"),
                "domain":                  DOMAIN,
                "config_id":               config["config_id"],
                "url":                     row.get("detail_url") or LISTING_URL,
                "is_live":                 True,
                "machine_name":            machine_name,
                "crawl_machine_ip":        machine_ip,
            }
            if row.get("project_location_raw"):
                data["project_location_raw"] = row["project_location_raw"]
            if row.get("approved_on_date"):
                data["approved_on_date"] = row["approved_on_date"]

            doc_links: list[dict] = []
            if row.get("detail_url") and settings.SCRAPE_DETAILS:
                random_delay(*config.get("rate_limit_delay", (1, 3)))
                logger.info("Fetching detail page", step="detail_fetch")
                detail      = _parse_detail_page(row["detail_url"], logger)
                doc_links   = detail.pop("_doc_links", [])
                detail_data = detail.pop("data", {})

                for k, v in detail.items():
                    if v is not None and k != "project_registration_no":
                        data[k] = v

                la = data.get("land_area")
                ca = data.get("construction_area")
                if la or ca:
                    data["land_area_details"] = {
                        k: v for k, v in {
                            "land_area":              (str(int(la)) if la == int(la) else str(la)) if la else None,
                            "land_area_unit":         detail_data.get("land_area_unit", "sq.mtr."),
                            "construction_area":      ca,
                            "construction_area_unit": detail_data.get("construction_area_unit", "sq.mtr."),
                        }.items() if v is not None
                    }

                listing_row = {k: v for k, v in row.items() if k not in ("detail_url", "data")}
                data["data"] = merge_data_sections({"listing_row": listing_row}, detail_data)
                if row.get("detail_url"):
                    data["data"]["complete_html_url"] = [row["detail_url"]]
            else:
                listing_row = {k: v for k, v in row.items() if k not in ("detail_url", "data")}
                data["data"] = merge_data_sections({"listing_row": listing_row})

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
            if action == "new":         counts["projects_new"] += 1
            elif action == "updated":   counts["projects_updated"] += 1
            else:                       counts["projects_skipped"] += 1
            logger.info(f"DB result: {action}", step="db_upsert")

            uploaded_documents: list[dict] = []
            doc_name_counts: dict[str, int] = {}
            logger.info(f"Processing {len(doc_links)} documents", step="documents")
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

    reset_checkpoint(site_id, mode)
    logger.info(f"West Bengal RERA complete: {counts}")
    logger.warning(f"Step timing [total_run]: {time.monotonic()-t_run:.2f}s", step="timing")
    return counts
