"""
Uttar Pradesh RERA Crawler — www.up-rera.in
Type: httpx (listing) + Playwright (detail pages via __doPostBack)

Strategy:
- District-wise listing: GET frm_allprojectdistrictwise.aspx?districtname={district}
  The page is an ASP.NET WebForms GridView with all projects for that district in the
  initial HTML response (client-side DataTables — no server-side pagination).
  Columns: RegistrationNo, ProjectName, Promoter, District, ProjectType.
- "View Detail" buttons use __doPostBack to navigate to each project's full detail page.
  Playwright is used per-project to click the button and capture the resulting HTML.
- On the first detail navigation for a district, the resolved URL is cached so that
  subsequent projects in the same district can be fetched via direct httpx GET if the
  URL pattern is predictable (reduces Playwright overhead).
- Detail HTML is parsed with BeautifulSoup for structured field extraction.
- Documents: all PDF <a> links found on the detail page are collected, downloaded,
  and uploaded to S3 per the framework's document policy.
"""
from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import download_response, generate_project_key, random_delay, safe_get
from core.db import get_project_by_key, upsert_project, insert_crawl_error, upsert_document
from core.document_policy import select_document_for_download
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import (
    build_document_filename,
    build_document_urls,
    document_result_entry,
    get_machine_context,
    merge_data_sections,
    normalize_project_payload,
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings

# ── Constants ──────────────────────────────────────────────────────────────────
BASE_URL    = "https://www.up-rera.in"
DOMAIN      = "up-rera.in"
STATE       = "uttar pradesh"
STATE_CODE  = "UP"
PROJECT_STATE = "Uttar Pradesh"

_DISTRICT_LISTING_URL = BASE_URL + "/frm_allprojectdistrictwise.aspx?districtname={district}"
_FULL_DETAIL_URL      = BASE_URL + "/frm_view_project_details.aspx?id={project_id}"
_GRID_ID = "ctl00_ContentPlaceHolder1_GridView1"

# All 75 UP districts (names matching the UP RERA portal)
_UP_DISTRICTS: list[str] = [
    "Agra", "Aligarh", "Ambedkar Nagar", "Amethi", "Amroha", "Auraiya",
    "Ayodhya", "Azamgarh", "Baghpat", "Bahraich", "Ballia", "Balrampur",
    "Banda", "Barabanki", "Bareilly", "Basti", "Bijnor", "Budaun",
    "Bulandshahr", "Chandauli", "Chitrakoot", "Deoria", "Etah", "Etawah",
    "Farrukhabad", "Fatehpur", "Firozabad", "Gautam Buddha Nagar", "Ghaziabad",
    "Ghazipur", "Gonda", "Gorakhpur", "Hamirpur", "Hapur", "Hardoi", "Hathras",
    "Jalaun", "Jaunpur", "Jhansi", "Kannauj", "Kanpur Dehat", "Kanpur Nagar",
    "Kasganj", "Kaushambi", "Kushinagar", "Lakhimpur Kheri", "Lalitpur",
    "Lucknow", "Maharajganj", "Mahoba", "Mainpuri", "Mathura", "Mau", "Meerut",
    "Mirzapur", "Moradabad", "Muzaffarnagar", "Pilibhit", "Pratapgarh",
    "Prayagraj", "Raebareli", "Rampur", "Saharanpur", "Sambhal",
    "Sant Kabir Nagar", "Sant Ravidas Nagar", "Shahjahanpur", "Shamli",
    "Shravasti", "Siddharthnagar", "Sitapur", "Sonbhadra", "Sultanpur",
    "Unnao", "Varanasi",
]

_LISTING_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Max parallel workers for pre-fetching district listing pages
_MAX_LISTING_WORKERS = 8


# ── String helpers ─────────────────────────────────────────────────────────────

def _clean(text: Any) -> str | None:
    if text is None:
        return None
    s = re.sub(r"\s+", " ", str(text)).strip()
    return s or None


def _parse_float(text: Any) -> float | None:
    if text is None:
        return None
    m = re.search(r"[\d.]+", str(text).replace(",", ""))
    try:
        return float(m.group()) if m else None
    except (ValueError, TypeError):
        return None


def _parse_int(text: Any) -> int | None:
    if text is None:
        return None
    m = re.search(r"\d+", str(text).replace(",", ""))
    try:
        return int(m.group()) if m else None
    except (ValueError, TypeError):
        return None


def _parse_date(text: Any) -> str | None:
    """Normalize date strings from UP RERA (dd/mm/yyyy or dd-mm-yyyy) → ISO+00:00."""
    if not text:
        return None
    s = str(text).strip()
    if s in ("", "None", "null", "N/A", "--", "0"):
        return None
    m = re.match(r"^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})$", s)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)} 00:00:00+00:00"
    # Already YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s if "+" in s else s + "+00:00"
    return None



def _project_id_from_reg_no(reg_no: str) -> str | None:
    """Extract numeric project ID from 'UPRERAPRJ6734' → '6734'."""
    m = re.search(r"\d+$", reg_no)
    return str(int(m.group())) if m else None


def _fetch_full_detail_html(reg_no: str, logger: CrawlerLogger) -> tuple[str, str]:
    """Fetch the comprehensive project detail page via direct HTTP GET (no Playwright)."""
    project_id = _project_id_from_reg_no(reg_no)
    if not project_id:
        return "", ""
    url = _FULL_DETAIL_URL.format(project_id=project_id)
    resp = safe_get(url, headers=_LISTING_HEADERS, retries=3, timeout=60, logger=logger)
    if resp and resp.status_code == 200 and len(resp.text) > 1000:
        return resp.text, url
    return "", ""


# ── Listing scraper ────────────────────────────────────────────────────────────

def _fetch_district_listing(district: str, logger: CrawlerLogger) -> list[dict]:
    """
    GET the district listing page and parse all project rows from the GridView.
    Returns a list of stub dicts with keys:
      reg_no, project_name, promoter_name, district, project_type, row_index.
    """
    url = _DISTRICT_LISTING_URL.format(district=district)
    resp = safe_get(url, headers=_LISTING_HEADERS, retries=3, timeout=60, logger=logger)
    if not resp:
        logger.warning(f"Could not fetch district listing: {district}", url=url)
        return []
    html = resp.text
    if "servermaintenance" in html.lower() or "object moved" in html.lower():
        logger.warning(f"District {district} listing redirected to maintenance page", url=url)
        return []

    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", id=_GRID_ID)
    if not table:
        table = soup.find("table", attrs={"id": re.compile(r"GridView1", re.I)})
    if not table:
        logger.warning(f"GridView not found in district listing: {district}")
        return []

    rows: list[dict] = []
    seen_reg: set[str] = set()
    data_row_index = 0

    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue  # header row

        reg_no = project_name = promoter = dist_name = proj_type = None

        for span in tr.find_all("span", id=re.compile(r"lblRegistrationNo$", re.I)):
            reg_no = _clean(span.get_text())
        for span in tr.find_all("span", id=re.compile(r"lblProjectName$", re.I)):
            project_name = _clean(span.get_text())
        for span in tr.find_all("span", id=re.compile(r"lblPromoter$", re.I)):
            promoter = _clean(span.get_text(separator=", "))
        for span in tr.find_all("span", id=re.compile(r"lblDistrict$", re.I)):
            dist_name = _clean(span.get_text())
        for span in tr.find_all("span", id=re.compile(r"lblProjectType$", re.I)):
            proj_type = _clean(span.get_text())

        if not reg_no and len(cells) >= 5:
            reg_no = _clean(cells[0].get_text())
            project_name = project_name or _clean(cells[1].get_text())
            promoter = promoter or _clean(cells[2].get_text(separator=", "))
            dist_name = dist_name or _clean(cells[3].get_text())
            proj_type = proj_type or _clean(cells[4].get_text())

        if not reg_no or not re.match(r"UPRERAPRJ\d+", reg_no):
            continue
        if reg_no in seen_reg:
            continue
        seen_reg.add(reg_no)

        rows.append({
            "reg_no": reg_no,
            "project_name": project_name,
            "promoter_name": promoter,
            "district": dist_name or district,
            "project_type": proj_type,
            "row_index": data_row_index,
        })
        data_row_index += 1

    logger.info(f"District {district}: found {len(rows)} projects")
    return rows


# ── Playwright: fetch project detail HTML ─────────────────────────────────────

def _fetch_detail_html_playwright(
    district: str,
    reg_no: str,
    logger: CrawlerLogger,
    existing_url: str | None = None,
) -> tuple[str, str]:
    """
    Fetch detail HTML for a project. Strategy:
    1. If existing_url is a Projectsummary URL, fetch it directly (fastest,
       richest data).
    2. Otherwise navigate to the district listing page and click 'View Detail'
       to reach View_Registration_Details.aspx.
    Returns (detail_html, detail_url).
    """
    # If we already have a Projectsummary URL for this project, use it directly
    if existing_url and "Projectsummary" in existing_url:
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                ctx = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                    )
                )
                page = ctx.new_page()
                page.goto(existing_url, wait_until="domcontentloaded", timeout=60_000)
                try:
                    page.wait_for_load_state("networkidle", timeout=20_000)
                except Exception:
                    pass
                if "servermaintenance" not in page.url:
                    html = page.content()
                    browser.close()
                    return html, existing_url
                browser.close()
        except Exception as exc:
            logger.warning(f"Direct Projectsummary fetch failed for {reg_no}: {exc}")

    # Fall back to navigating via district listing → LnkView
    listing_url = _DISTRICT_LISTING_URL.format(district=district)
    detail_html = ""
    detail_url = listing_url

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                )
            )
            page = ctx.new_page()
            page.goto(listing_url, wait_until="domcontentloaded", timeout=60_000)

            try:
                page.wait_for_selector(f"table#{_GRID_ID}", timeout=20_000)
            except Exception:
                pass

            # Click the button in the row that contains our registration number
            clicked = page.evaluate(f"""
                (function() {{
                    var spans = document.querySelectorAll('span[id$="lblRegistrationNo"]');
                    for (var i = 0; i < spans.length; i++) {{
                        if (spans[i].textContent.trim() === '{reg_no}') {{
                            var row = spans[i].closest('tr');
                            if (row) {{
                                var btn = row.querySelector('a[id$="LnkView"]');
                                if (btn) {{ btn.click(); return true; }}
                            }}
                        }}
                    }}
                    return false;
                }})()
            """)

            if not clicked:
                logger.warning(f"Could not find View Detail button for {reg_no} in {district}")
                browser.close()
                return "", ""

            try:
                page.wait_for_load_state("networkidle", timeout=30_000)
            except Exception:
                page.wait_for_load_state("domcontentloaded", timeout=20_000)

            detail_url = page.url
            detail_html = page.content()
            browser.close()

    except Exception as exc:
        logger.error(f"Playwright detail fetch failed for {reg_no}: {exc}")

    return detail_html, detail_url


# ── Detail HTML parser ────────────────────────────────────────────────────────

def _extract_label_values(soup: BeautifulSoup) -> dict[str, str]:
    """
    Scan all <tr> pairs, <div class='row'> with col-* children, and
    <label>/<span> pairs. Returns a flat dict of {label_text: value_text}.

    The Projectsummary page uses <div class='row'> containing two col-sm-*
    children: the first holds the label, the second holds the value (which
    can be another <label>, a <span>, or plain text).
    """
    result: dict[str, str] = {}

    # Method 1: <div class="row"> + col-* children (Projectsummary page)
    for row_div in soup.find_all("div", class_=lambda c: c and "row" in c):
        cols = [
            d for d in row_div.find_all("div", recursive=False)
            if d.get("class") and any("col" in cls for cls in d.get("class", []))
        ]
        if len(cols) >= 2:
            key = _clean(cols[0].get_text(separator=" "))
            val = _clean(cols[1].get_text(separator=" "))
            if key and val and len(key) < 150:
                result.setdefault(key, val)

    # Method 2: <tr>/<td> pairs
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        i = 0
        while i < len(cells) - 1:
            label_text = _clean(cells[i].get_text(separator=" "))
            value_text = _clean(cells[i + 1].get_text(separator=" "))
            if label_text and value_text and len(label_text) < 150:
                result.setdefault(label_text, value_text)
            i += 2

    # Method 3: <label> + next sibling value element
    for label in soup.find_all("label"):
        key = _clean(label.get_text())
        if not key:
            continue
        sib = label.find_next_sibling(["span", "strong", "div", "td", "input"])
        if sib:
            val = _clean(sib.get_text(separator=" "))
            if val and val not in (key, ""):
                result.setdefault(key, val)

    return result


def _extract_building_details(soup: BeautifulSoup) -> list[dict]:
    """
    Parse the unit/apartment table from the UP RERA detail page.
    Expected columns: Floor No, Flat Type, Carpet Area, Balcony Area,
    Open Area, No. of Units, Block Name.
    """
    building_details: list[dict] = []
    # Look for table whose header contains "carpet area" or "flat type"
    for table in soup.find_all("table"):
        headers = [_clean(th.get_text()) for th in table.find_all("th")]
        if not headers:
            first_tr = table.find("tr")
            if first_tr:
                headers = [_clean(td.get_text()) for td in first_tr.find_all("td")]
        headers_lower = [h.lower() if h else "" for h in headers]
        if not any("carpet" in h or "flat type" in h for h in headers_lower):
            continue

        # Map column names to indices
        col = {}
        for i, h in enumerate(headers_lower):
            if "floor" in h:
                col["floor_no"] = i
            elif "flat type" in h or "unit type" in h or "apartment type" in h:
                col["flat_type"] = i
            elif "carpet" in h:
                col["carpet_area"] = i
            elif "balcony" in h or "varandah" in h:
                col["balcony_area"] = i
            elif "open" in h:
                col["open_area"] = i
            elif "no" in h and ("unit" in h or "apt" in h or "apartment" in h):
                col["no_of_units"] = i
            elif "block" in h or "tower" in h or "building" in h:
                col["block_name"] = i

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if not cells:
                continue
            entry: dict[str, str] = {}
            for field, idx in col.items():
                if idx < len(cells):
                    val = _clean(cells[idx].get_text())
                    if val and val not in ("", "-", "N/A"):
                        entry[field] = val
            if entry.get("carpet_area") or entry.get("flat_type"):
                if "open_area" not in entry:
                    entry["open_area"] = "0"
                building_details.append(entry)
        if building_details:
            break
    return building_details


def _parse_full_detail_page(html: str, reg_no: str, district: str) -> dict:  # noqa: C901
    """
    Parse frm_view_project_details.aspx — the comprehensive project detail page.

    Extracts bank details, lat/lon, land area, project cost, building/unit details,
    and dates from hidden input element values and data tables.

    Column mapping for grd_PlanDetails_ForAdmin (0-based):
      0=Sr.No  1=Block No  2=Floor Number  3=Flat type  4=Num units
      5=Carpet Area  6=Num Balcony  7=Balcony Area  8-11=Verandah/Garage  12=Open Parking
    """
    if not html:
        return {}

    soup = BeautifulSoup(html, "lxml")
    out: dict[str, Any] = {}

    def _inp(elem_id: str) -> str | None:
        """Return the value= attribute of a hidden input element (None if empty/placeholder)."""
        elem = soup.find("input", id=elem_id)
        if not elem:
            return None
        v = _clean(elem.get("value", ""))
        return v if v and v not in ("..", "-", "N/A") else None

    def _inp_raw(elem_id: str) -> str | None:
        """Return the raw value= attribute of a hidden input element (keeps '..' placeholders)."""
        elem = soup.find("input", id=elem_id)
        if not elem:
            return None
        return _clean(elem.get("value", "")) or None

    # ── Geographic location ───────────────────────────────────────────────────
    lat_str = _inp("ctl00_ContentPlaceHolder1_lblLat1")
    lon_str = _inp("ctl00_ContentPlaceHolder1_lblLong1")
    loc: dict[str, Any] = {"state": PROJECT_STATE, "district": district}
    if lat_str:
        try:
            loc["latitude"] = lat_str
            loc["processed_latitude"] = float(lat_str)
        except (ValueError, TypeError):
            pass
    if lon_str:
        try:
            loc["longitude"] = lon_str
            loc["processed_longitude"] = float(lon_str)
        except (ValueError, TypeError):
            pass
    if "latitude" in loc:
        out["project_location_raw"] = loc

    # ── Bank details ──────────────────────────────────────────────────────────
    bank: dict[str, str] = {}
    _acc_no   = _inp("ctl00_ContentPlaceHolder1_lblAccNo")
    _acc_name = _inp("ctl00_ContentPlaceHolder1_lblAccName")
    _bank_nm  = _inp("ctl00_ContentPlaceHolder1_lblBankName")
    _branch   = _inp("ctl00_ContentPlaceHolder1_lblBranchName")
    _ifsc     = _inp("ctl00_ContentPlaceHolder1_lblIFSCCode")
    if _acc_no:   bank["account_no"]   = _acc_no
    if _acc_name: bank["account_name"] = _acc_name
    if _bank_nm:  bank["bank_name"]    = _bank_nm
    if _branch:   bank["branch"]       = _branch
    if _ifsc:     bank["ifsc"]         = _ifsc
    if bank:
        out["bank_details"] = bank

    # ── Land area (Sq.mt.) ────────────────────────────────────────────────────
    _area = _inp("ctl00_ContentPlaceHolder1_lblTotalArea")
    if _area:
        land_val = _parse_float(_area)
        if land_val:
            out["land_area"] = land_val

    # ── Project cost (in Lacs → convert to rupees by × 1,00,000) ─────────────
    _cost = _inp("ctl00_ContentPlaceHolder1_lblProjectCost")
    if _cost:
        cost_val = _parse_float(_cost)
        if cost_val:
            out["project_cost_detail"] = {"total_project_cost": cost_val * 100_000.0}

    # ── Commencement / completion dates ───────────────────────────────────────
    _start = _inp("ctl00_ContentPlaceHolder1_lblStartDate")
    _end   = _inp("ctl00_ContentPlaceHolder1_lblEndDate")
    if _start and _start not in ("-", "0"):
        out["actual_commencement_date"] = _parse_date(_start)
    if _end and _end not in ("-", "0"):
        out["actual_finish_date"] = _parse_date(_end)

    # ── Building details & unit counts (grd_PlanDetails_ForAdmin) ────────────
    plan_table = soup.find("table", id="ctl00_ContentPlaceHolder1_grd_PlanDetails_ForAdmin")
    if plan_table:
        building_details: list[dict] = []
        residential_count = 0
        commercial_count  = 0
        rows = plan_table.find_all("tr")
        for tr in rows[1:]:  # skip header row
            cells = tr.find_all("td")
            if len(cells) < 8:
                continue

            def _cell(idx: int) -> str:
                return _clean(cells[idx].get_text()) or "" if idx < len(cells) else ""

            block_name   = _cell(1)
            floor_no     = _cell(2)
            flat_type    = _cell(3)
            no_of_units  = _cell(4)
            carpet_area  = _cell(5)
            balcony_area = _cell(7)
            open_area    = _cell(12) if len(cells) > 12 else "0"

            if not (flat_type or carpet_area):
                continue

            entry: dict[str, str] = {}
            if block_name:  entry["block_name"]   = block_name
            if floor_no:    entry["floor_no"]      = floor_no
            if flat_type:   entry["flat_type"]     = flat_type
            if no_of_units: entry["no_of_units"]   = no_of_units
            if carpet_area: entry["carpet_area"]   = carpet_area
            if balcony_area and balcony_area not in ("-", "N/A"):
                entry["balcony_area"] = balcony_area
            entry["open_area"] = open_area or "0"
            building_details.append(entry)

            # Count units by property type.
            # SHOP and PENTHOUSE are classified as commercial per UP RERA convention.
            try:
                n = int(no_of_units or "0")
                if (flat_type.upper() in ("SHOP", "PENTHOUSE")
                        or (block_name or "").upper() == "COMMERCIAL"):
                    commercial_count += n
                else:
                    residential_count += n
            except (ValueError, TypeError):
                pass

        if building_details:
            out["building_details"] = building_details
        if residential_count > 0:
            out["number_of_residential_units"] = residential_count
        if commercial_count > 0:
            out["number_of_commercial_units"] = commercial_count

    # ── Land detail (grdLadDetail) ────────────────────────────────────────────
    land_table = soup.find("table", id="ctl00_ContentPlaceHolder1_grdLadDetail")
    if land_table:
        rows = land_table.find_all("tr")
        for tr in rows[1:]:
            cells = tr.find_all("td")
            if len(cells) >= 4:
                plot_no    = _clean(cells[2].get_text()) or ""
                total_area = _clean(cells[3].get_text()) or ""
                if plot_no and total_area:
                    out["land_detail"] = {"plot_no": plot_no, "total_area": total_area}
                    break

    # ── Professional information ──────────────────────────────────────────────
    # Each professional's name is in a hidden input; their address is in
    # cells[3] of the same <tr> that contains the input element.
    def _prof_addr(inp_id: str) -> str | None:
        """Get the address from cells[3] of the row containing this input."""
        elem = soup.find("input", id=inp_id)
        if not elem:
            return None
        tr = elem.find_parent("tr")
        if not tr:
            return None
        cells = tr.find_all("td")
        return _clean(cells[3].get_text()) if len(cells) >= 4 else None

    professionals: list[dict] = []
    _cont_name = _inp_raw("ctl00_ContentPlaceHolder1_lblContractorName")
    _arch_name = _inp_raw("ctl00_ContentPlaceHolder1_lblArchName")
    _arch_lic  = _inp("ctl00_ContentPlaceHolder1_lblArchLicNo")
    _eng_name  = _inp_raw("ctl00_ContentPlaceHolder1_lblEnggName")

    _cont_addr = _prof_addr("ctl00_ContentPlaceHolder1_lblContractorName") or ".."
    _arch_addr = _prof_addr("ctl00_ContentPlaceHolder1_lblArchName")
    _eng_addr  = _prof_addr("ctl00_ContentPlaceHolder1_lblEnggName")

    if _cont_name:
        entry: dict = {"name": _cont_name, "role": "contractor", "address": _cont_addr}
        professionals.append(entry)
    if _arch_name:
        entry = {"name": _arch_name, "role": "architect"}
        if _arch_addr:
            entry["address"] = _arch_addr
        if _arch_lic:
            entry["liscence_no"] = _arch_lic  # preserve typo from original schema
        professionals.append(entry)
    if _eng_name:
        entry = {"name": _eng_name, "role": "structural engineer"}
        if _eng_addr:
            entry["address"] = _eng_addr
        professionals.append(entry)
    if professionals:
        out["professional_information"] = professionals

    return out


def _parse_detail_page(html: str, reg_no: str, district: str) -> dict:  # noqa: C901
    """
    Parse a UP RERA project detail page HTML and return a structured dict
    matching the projects table schema.
    """
    if not html:
        return {}

    soup = BeautifulSoup(html, "lxml")
    lv = _extract_label_values(soup)
    out: dict[str, Any] = {}

    # ── Registration and basic fields (also from listing stub) ─────────────────
    out["project_registration_no"] = reg_no

    # Lookup patterns for common fields (label text → schema field).
    # NOTE: "project type" is intentionally omitted — the Projectsummary page shows
    # the registration type ('New'/'Extension') rather than the property type
    # ('Residential'/'Commercial') that the listing provides.  Using the listing
    # value avoids incorrectly overwriting it.
    _FIELD_MAP = {
        "project name": "project_name",
        "name of project": "project_name",
        "promoter name": "promoter_name",
        "name of promoter": "promoter_name",
        "project status": "status_of_the_project",
        "pin code": "project_pin_code",
        "date of approval": "approved_on_date",
        "approved on": "approved_on_date",
        "actual commencement date": "actual_commencement_date",
        "actual completion date": "actual_finish_date",
        "last updated": "last_modified",
    }
    for raw_label, val in lv.items():
        label_lower = raw_label.lower().strip().rstrip(":")
        for pattern, schema_field in _FIELD_MAP.items():
            if pattern in label_lower:
                if schema_field not in out:
                    if schema_field.endswith("_date") or schema_field == "last_modified":
                        out[schema_field] = _parse_date(val)
                    else:
                        out[schema_field] = val
                break

    # Extract dates from the Projectsummary labels directly.
    # These are exposed as "Proposed Start Date" and "Declared Date Of Completion"
    # on the Projectsummary page.  Only extract actual_commencement/finish since
    # the sample stores them this way; the estimated_ variants stay None.
    for raw_label, val in lv.items():
        ll = raw_label.lower().strip().rstrip(":")
        if "proposed start date" in ll and not out.get("actual_commencement_date"):
            out["actual_commencement_date"] = _parse_date(val)

    # ── Land area ─────────────────────────────────────────────────────────────
    for raw_label, val in lv.items():
        if "land area" in raw_label.lower() or "plot area" in raw_label.lower():
            land_val = _parse_float(val)
            if land_val:
                out["land_area"] = land_val
                break

    # ── Project cost ──────────────────────────────────────────────────────────
    for raw_label, val in lv.items():
        ll = raw_label.lower()
        if "total cost" in ll or "total project cost" in ll or "estimated cost" in ll:
            cost_val = _parse_float(val)
            if cost_val:
                out["project_cost_detail"] = {"total_project_cost": cost_val}
                break

    # ── Residential / commercial units ────────────────────────────────────────
    for raw_label, val in lv.items():
        ll = raw_label.lower()
        if "residential unit" in ll or "total residential" in ll:
            u = _parse_int(val)
            if u:
                out.setdefault("number_of_residential_units", u)
        elif "commercial unit" in ll or "total commercial" in ll:
            u = _parse_int(val)
            if u:
                out.setdefault("number_of_commercial_units", u)

    # ── Promoter contact ──────────────────────────────────────────────────────
    contact: dict[str, str] = {}
    for raw_label, val in lv.items():
        ll = raw_label.lower()
        if "email" in ll and val and "@" in val:
            contact["email"] = val.strip()
        elif ("mobile" in ll or "phone" in ll or "promoter mobile" in ll) and re.search(r"\d{10}", val or ""):
            contact.setdefault("phone", re.search(r"\d{10}", val).group())
    # Also look directly in the HTML for email span (robust fallback)
    if "email" not in contact:
        for span in soup.find_all("label"):
            t = _clean(span.get_text())
            if t and "@" in t:
                contact["email"] = t
                break
    if contact:
        out["promoter_contact_details"] = contact

    # ── Promoter address ──────────────────────────────────────────────────────
    for raw_label, val in lv.items():
        ll = raw_label.lower()
        if "promoter" in ll and "address" in ll:
            out["promoter_address_raw"] = {"raw_address": val}
            break

    # ── Promoters details (name + type) ───────────────────────────────────────
    promoters: dict[str, str] = {}
    # Name from span#Lblpromotername or similar
    name_span = soup.find("span", id=re.compile(r"Lblpromotername|lblPromoterName", re.I))
    if name_span:
        promoters["name"] = _clean(name_span.get_text()) or ""
    for raw_label, val in lv.items():
        ll = raw_label.lower()
        if ll in ("applicant type", "type of firm", "type of applicant") and val:
            promoters["type_of_firm"] = val
    if promoters:
        out["promoters_details"] = promoters

    # ── Complaints / litigation ────────────────────────────────────────────────
    for raw_label, val in lv.items():
        ll = raw_label.lower()
        if "complaints in respect" in ll or "complaints against this project" in ll:
            m = re.search(r"\d+", val or "")
            if m:
                out["complaints_litigation_details"] = {"count": m.group()}
            break

    # ── actual_finish_date: last Extension Upto date ──────────────────────────
    ext_table = soup.find("table", id=re.compile(r"grd_extension", re.I))
    if ext_table:
        last_upto = None
        for tr in ext_table.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if len(cells) >= 3:
                upto_text = _clean(cells[2].get_text())
                # Strip descriptive text after date
                date_part = re.match(r"^\d{2}-\d{2}-\d{4}", upto_text or "")
                if date_part:
                    last_upto = date_part.group()
        if last_upto:
            out.setdefault("actual_finish_date", _parse_date(last_upto))

    # ── Bank details ──────────────────────────────────────────────────────────
    bank: dict[str, str] = {}
    for raw_label, val in lv.items():
        ll = raw_label.lower()
        if "bank name" in ll:
            bank["bank_name"] = val
        elif "branch" in ll and "bank" not in ll:
            bank["branch"] = val
        elif "account no" in ll or "account number" in ll:
            bank["account_no"] = val
        elif "account name" in ll or "account holder" in ll:
            bank["account_name"] = val
        elif "ifsc" in ll:
            bank["ifsc"] = val
    if bank:
        out["bank_details"] = bank

    # ── Location from district (known from listing) ───────────────────────────
    loc: dict[str, Any] = {
        "state": PROJECT_STATE,
        "district": district,
    }
    for raw_label, val in lv.items():
        ll = raw_label.lower().strip().rstrip(":")
        if "latitude" in ll:
            try:
                loc["latitude"] = val
                loc["processed_latitude"] = float(val)
            except (ValueError, TypeError):
                pass
        elif "longitude" in ll:
            try:
                loc["longitude"] = val
                loc["processed_longitude"] = float(val)
            except (ValueError, TypeError):
                pass
        elif ll in ("tehsil", "taluk", "tahsil"):
            loc["taluk"] = val
        elif ll == "district" and val:
            loc["district"] = val
    out["project_location_raw"] = loc

    # ── Building / unit details ───────────────────────────────────────────────
    building_details = _extract_building_details(soup)
    if building_details:
        out["building_details"] = building_details

    return out


# ── Document extractor ────────────────────────────────────────────────────────

def _extract_documents(soup: BeautifulSoup, detail_url: str) -> list[dict]:
    """
    Find all PDF links on the detail page and build document metadata dicts.
    """
    docs: list[dict] = []
    seen: set[str] = set()
    base = BASE_URL

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        # Skip empty, javascript, and anchor-only links
        if not href or href.startswith("#") or href.lower().startswith("javascript"):
            continue
        # Only collect PDF links or links with document-related paths
        lower_href = href.lower()
        is_doc = (
            ".pdf" in lower_href
            or "download" in lower_href
            or "document" in lower_href
            or "certificate" in lower_href
            or "upload" in lower_href
        )
        if not is_doc:
            continue

        abs_url = href if href.startswith("http") else urljoin(base, href)
        if abs_url in seen:
            continue
        seen.add(abs_url)

        label = _clean(a.get_text()) or "document"
        docs.append({"label": label, "url": abs_url, "source_url": abs_url})

    return docs


def _handle_document(
    project_key: str,
    doc: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
) -> dict | None:
    """Download, checksum, upload document to S3. Returns enriched doc dict or None."""
    url = doc.get("source_url") or doc.get("url") or ""
    if not url:
        return None

    resp = download_response(url, retries=2, timeout=30, logger=logger)
    if not resp or not resp.content:
        logger.warning(f"Document download failed: {url}")
        insert_crawl_error(run_id, site_id, "HTTP_ERROR",
                           f"Document download failed: {url}", project_key=project_key, url=url)
        return None

    content = resp.content
    if len(content) < 100:
        return None

    md5 = compute_md5(content)
    filename = build_document_filename(doc)
    try:
        s3_key = upload_document(project_key, filename, content)
        if not s3_key:
            return None
        s3_url = get_s3_url(s3_key)
        doc_type = doc.get("type") or doc.get("label") or "document"
        upsert_document(
            project_key=project_key,
            document_type=doc_type,
            original_url=url,
            s3_key=s3_key,
            s3_bucket=settings.S3_BUCKET_NAME,
            file_name=filename,
            md5_checksum=md5,
            file_size_bytes=len(content),
        )
        logger.info("Document uploaded", doc_type=doc_type, s3_key=s3_key, step="documents")
        logger.log_document(doc_type, url, "uploaded", s3_key=s3_key, file_size_bytes=len(content))
        return document_result_entry(doc, s3_url, filename)
    except Exception as exc:
        logger.warning(f"S3 upload failed for {url}: {exc}")
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED",
                           str(exc), project_key=project_key, url=url)
        return None


# ── Sentinel check ────────────────────────────────────────────────────────────

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Fetch the sentinel project's detail page and verify basic fields.
    Returns True if passed, False if failed (caller should abort crawl).
    """
    sentinel_reg = config.get("sentinel_registration_no", "")
    if not sentinel_reg:
        logger.warning("No sentinel_registration_no configured — skipping", step="sentinel")
        return True

    # The sentinel project's district is stored in config or inferred
    sentinel_district = config.get("sentinel_district", "Gautam Buddha Nagar")
    logger.info(f"Sentinel: fetching detail for {sentinel_reg}", step="sentinel")

    detail_html, detail_url = _fetch_detail_html_playwright(
        sentinel_district, sentinel_reg, logger
    )
    if not detail_html:
        logger.error("Sentinel: could not fetch detail page", step="sentinel")
        insert_crawl_error(run_id, config.get("id", "uttar_pradesh_rera"),
                           "SENTINEL_FAILED",
                           f"Could not fetch detail for sentinel {sentinel_reg}")
        return False

    parsed = _parse_detail_page(detail_html, sentinel_reg, sentinel_district)
    if not parsed.get("project_registration_no"):
        logger.error("Sentinel: registration number not found in detail", step="sentinel")
        insert_crawl_error(run_id, config.get("id", "uttar_pradesh_rera"),
                           "SENTINEL_FAILED",
                           f"Registration not found for sentinel {sentinel_reg}")
        return False

    logger.info(f"Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


# ── Main run function ─────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:  # noqa: C901
    """
    Entry point for the UP RERA crawler.

    Args:
        config:  site config dict from sites_config.SITES
        run_id:  crawl_runs.id for this run
        mode:    'daily_light' | 'weekly_deep'

    Returns:
        dict with keys: projects_found, projects_new, projects_updated,
                        projects_skipped, documents_uploaded, error_count
    """
    site_id = config.get("id", "uttar_pradesh_rera")
    logger = CrawlerLogger(site_id=site_id, run_id=run_id)
    delay_min, delay_max = config.get("rate_limit_delay", (2, 4))

    counts: dict[str, int] = {
        "projects_found": 0,
        "projects_new": 0,
        "projects_updated": 0,
        "projects_skipped": 0,
        "documents_uploaded": 0,
        "error_count": 0,
    }

    t_run = time.monotonic()

    # ── Sentinel check ─────────────────────────────────────────────────────────
    t0 = time.monotonic()
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting UP RERA crawl")
        return counts
    logger.timing("sentinel", time.monotonic() - t0)

    # ── Checkpoint / resume support ───────────────────────────────────────────
    checkpoint = load_checkpoint(site_id, mode) or {}
    resume_after_district = checkpoint.get("last_page", -1)
    resume_after_key = checkpoint.get("last_project_key")
    resume_pending = bool(resume_after_key)
    if resume_pending:
        logger.warning(
            f"Resuming {mode} from checkpoint: district_idx={resume_after_district}, "
            f"last_project_key={resume_after_key}. "
            f"All projects up to this checkpoint will be counted as skipped."
        )
    item_limit = settings.CRAWL_ITEM_LIMIT or 0

    machine_name, machine_ip = get_machine_context()

    # ── Iterate districts ─────────────────────────────────────────────────────
    t0 = time.monotonic()
    first_district_logged = False
    items_processed = 0

    # Pre-compute the districts that still need processing (respecting checkpoint)
    pending_districts = [
        (idx, dist) for idx, dist in enumerate(_UP_DISTRICTS)
        if not (resume_after_district >= 0 and idx < resume_after_district)
        and not (idx == resume_after_district and not resume_pending)
    ]

    # Fetch all pending district listings in parallel to overlap HTTP I/O
    logger.info(
        f"Pre-fetching {len(pending_districts)} district listings "
        f"(workers={_MAX_LISTING_WORKERS})",
        step="listing",
    )
    district_stubs: dict[int, list[dict]] = {}
    with ThreadPoolExecutor(max_workers=_MAX_LISTING_WORKERS) as pool:
        futures = {
            pool.submit(_fetch_district_listing, dist, logger): (idx, dist)
            for idx, dist in pending_districts
        }
        for future in as_completed(futures):
            idx, dist = futures[future]
            try:
                district_stubs[idx] = future.result()
            except Exception as exc:
                logger.error(
                    f"Listing fetch failed for {dist}: {exc}", step="listing"
                )
                district_stubs[idx] = []

    for district_idx, district in pending_districts:
        stubs = district_stubs.get(district_idx, [])
        counts["projects_found"] += len(stubs)
        logger.info(f"Processing district: {district} ({district_idx + 1}/{len(_UP_DISTRICTS)})")
        if not first_district_logged:
            logger.timing("search", time.monotonic() - t0, rows=len(stubs))
            first_district_logged = True

        for stub in stubs:
            if item_limit and items_processed >= item_limit:
                logger.info(f"CRAWL_ITEM_LIMIT={item_limit} reached", step="listing")
                break

            reg_no = stub["reg_no"]
            project_key = generate_project_key(reg_no)

            # Resume: skip projects already processed in a previous run.
            # The checkpoint key marks the LAST project fully processed, so it
            # is also skipped (not re-processed).  Count every resume-skip in
            # projects_skipped so the dashboard reflects what actually happened.
            if resume_pending:
                counts["projects_skipped"] += 1
                if project_key == resume_after_key:
                    resume_pending = False  # next project will be processed normally
                continue

            # ── Dedup check ────────────────────────────────────────────────────
            existing = get_project_by_key(project_key)
            if mode == "daily_light" and existing:
                counts["projects_skipped"] += 1
                save_checkpoint(site_id, mode, district_idx, project_key, run_id)
                continue

            items_processed += 1

            # ── Deep crawl: fetch detail page ──────────────────────────────────
            logger.info(f"Deep crawling project {reg_no}", district=district)
            existing_url = existing.get("url") if existing else None
            detail_html, detail_url = _fetch_detail_html_playwright(
                district, reg_no, logger, existing_url=existing_url
            )

            if not detail_html:
                logger.warning(f"No detail HTML for {reg_no}", url=detail_url)
                counts["error_count"] += 1
                insert_crawl_error(run_id, site_id, "EXTRACTION_FAILED",
                                   f"No detail HTML returned for {reg_no}",
                                   project_key=project_key,
                                   url=detail_url)
                save_checkpoint(site_id, mode, district_idx, project_key, run_id)
                random_delay(delay_min, delay_max)
                continue

            # ── Parse detail ───────────────────────────────────────────────────
            try:
                detail_data = _parse_detail_page(detail_html, reg_no, district)
            except Exception as exc:
                logger.error(f"Detail parse error for {reg_no}: {exc}")
                counts["error_count"] += 1
                insert_crawl_error(run_id, site_id, "EXTRACTION_FAILED",
                                   str(exc), project_key=project_key, url=detail_url)
                save_checkpoint(site_id, mode, district_idx, project_key, run_id)
                random_delay(delay_min, delay_max)
                continue

            # ── Fetch + parse comprehensive full-detail page ───────────────────
            # frm_view_project_details.aspx has bank details, lat/lon, area,
            # project cost, and full building unit table — not present on
            # the Projectsummary page fetched via Playwright above.
            full_detail_html, _ = _fetch_full_detail_html(reg_no, logger)
            if full_detail_html:
                try:
                    full_detail_data = _parse_full_detail_page(
                        full_detail_html, reg_no, district
                    )
                    # Deep-merge project_location_raw so that taluk from
                    # the Projectsummary page is preserved alongside the
                    # lat/lon extracted from the full-detail page.
                    detail_loc     = detail_data.get("project_location_raw") or {}
                    full_detail_loc = full_detail_data.pop("project_location_raw", None) or {}
                    if full_detail_loc:
                        full_detail_data["project_location_raw"] = {
                            **detail_loc, **full_detail_loc
                        }
                    # Overlay: full_detail_data takes precedence for fields it provides
                    for k, v in full_detail_data.items():
                        if v is not None:
                            detail_data[k] = v
                except Exception as exc:
                    logger.warning(f"Full detail parse error for {reg_no}: {exc}")

            # ── Merge listing stub fields into detail ─────────────────────────
            merged: dict[str, Any] = {
                "project_name":           stub.get("project_name"),
                "promoter_name":          stub.get("promoter_name"),
                # Use listing project_type ('Residential'/'Commercial') — not the
                # registration type ('New'/'Extension') from the Projectsummary page.
                "project_type":           stub.get("project_type"),
                "status_of_the_project":  "New",  # default for registered projects
            }
            # detail_data takes precedence except for fields already set above
            # that should not be overwritten (project_type).
            detail_copy = {k: v for k, v in detail_data.items() if v is not None}
            detail_copy.pop("project_type", None)  # preserve listing project_type
            merged.update(detail_copy)

            # ── Build full payload ─────────────────────────────────────────────
            raw_snapshot = merge_data_sections(
                {"source": "up_rera", "district": district, "detail": detail_data},
                {"listing_stub": stub},
            )
            payload: dict[str, Any] = {
                **merged,
                "key": project_key,
                "url": detail_url,
                "domain": DOMAIN,
                "state": STATE,
                "config_id": config.get("config_id"),
                "data": raw_snapshot,
                "is_live": True,
            }
            payload = normalize_project_payload(
                payload, config,
                machine_name=machine_name,
                machine_ip=machine_ip,
            )

            # ── Pydantic validation ────────────────────────────────────────────
            try:
                ProjectRecord(**{
                    k: v for k, v in payload.items()
                    if k in ProjectRecord.model_fields
                })
            except ValidationError as ve:
                logger.warning(f"Validation warning for {reg_no}: {ve}")

            # ── Upsert to DB ───────────────────────────────────────────────────
            try:
                action = upsert_project(payload)
                if action == "new":
                    counts["projects_new"] += 1
                elif action == "updated":
                    counts["projects_updated"] += 1
                else:
                    counts["projects_skipped"] += 1
                logger.info(f"DB result: {action}", step="db_upsert")
            except Exception as exc:
                logger.error(f"DB upsert failed for {reg_no}: {exc}")
                counts["error_count"] += 1
                insert_crawl_error(run_id, site_id, "EXTRACTION_FAILED",
                                   str(exc), project_key=project_key, url=detail_url)
                save_checkpoint(site_id, mode, district_idx, project_key, run_id)
                random_delay(delay_min, delay_max)
                continue

            # ── Documents ─────────────────────────────────────────────────────
            try:
                soup = BeautifulSoup(detail_html, "lxml")
                docs = _extract_documents(soup, detail_url)
                uploaded_docs: list[dict] = []
                doc_name_counts: dict[str, int] = {}
                for doc in docs:
                    selected = select_document_for_download(
                        STATE, doc, doc_name_counts, domain=DOMAIN
                    )
                    if not selected:
                        continue
                    result = _handle_document(
                        project_key, selected, run_id, site_id, logger
                    )
                    if result:
                        uploaded_docs.append(result)
                        counts["documents_uploaded"] += 1
                if uploaded_docs:
                    doc_urls = build_document_urls(uploaded_docs)
                    upsert_project({
                        "key": project_key,
                        "url": detail_url,
                        "domain": DOMAIN,
                        "state": STATE,
                        "uploaded_documents": uploaded_docs,
                        "document_urls": doc_urls,
                    })
            except Exception as exc:
                logger.warning(f"Document processing failed for {reg_no}: {exc}")

            save_checkpoint(site_id, mode, district_idx, project_key, run_id)
            random_delay(delay_min, delay_max)

        # Guard against a stale checkpoint whose key no longer exists in the
        # current listings (project removed / listing changed).  If we just
        # finished the resume district and resume_pending is still True, the
        # key was never found — reset it so we don't silently skip every
        # project in all remaining districts.
        if resume_pending and district_idx == resume_after_district:
            logger.warning(
                f"Checkpoint project key {resume_after_key!r} was not found in "
                f"district '{district}' listings — stale checkpoint cleared. "
                f"Remaining districts will be processed normally."
            )
            resume_pending = False

        if item_limit and items_processed >= item_limit:
            break

    # ── Crawl complete ─────────────────────────────────────────────────────────
    reset_checkpoint(site_id, mode)
    logger.info("UP RERA crawl complete", **counts)
    logger.timing("total_run", time.monotonic() - t_run)
    return counts
