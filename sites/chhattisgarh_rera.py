"""
Chhattisgarh RERA Crawler — rera.cgstate.gov.in
Type: static (httpx + BeautifulSoup — server-rendered ASP.NET WebForms)

Strategy:
- GET /Approved_project_List.aspx returns all ~2088 projects in one page.
  A JavaScript block embeds a JSON array of map-markers containing per-project
  lat/lon, registration number, district, tehsil, MyID and detail URL.
- Detail page: /Promoter_Reg_Only_View_Application_new.aspx?MyID={base64_id}
  served as static HTML with form fields, select dropdowns, textareas and
  grid tables for quarterly updates, building inventory, professional details
  and document links.
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup

from core.checkpoint import load_checkpoint, reset_checkpoint, save_checkpoint
from core.crawler_base import download_response, generate_project_key, random_delay, safe_get
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
from pydantic import ValidationError

LISTING_URL = "https://rera.cgstate.gov.in/Approved_project_List.aspx"
DETAIL_BASE  = "https://rera.cgstate.gov.in/Promoter_Reg_Only_View_Application_new.aspx"
BASE_URL     = "https://rera.cgstate.gov.in"
DOMAIN       = "rera.cgstate.gov.in"

_REG_RE   = re.compile(r"PCGRERA\w+")
_DATE_FMT = "%d-%m-%Y"   # e.g. "30-04-2022"
_DATE_FMT2 = "%d %b %Y"  # e.g. "20 Jun 2018"

_LISTING_TIMEOUT = httpx.Timeout(connect=20.0, read=180.0, write=30.0, pool=30.0)
_DETAIL_TIMEOUT = httpx.Timeout(connect=20.0, read=90.0, write=30.0, pool=30.0)
_DOCUMENT_TIMEOUT = httpx.Timeout(connect=20.0, read=120.0, write=30.0, pool=30.0)


# ── helpers ───────────────────────────────────────────────────────────────────

def _timeout_for_url(url: str) -> httpx.Timeout:
    if url == LISTING_URL:
        return _LISTING_TIMEOUT
    if "ProjectDocuments" in url:
        return _DOCUMENT_TIMEOUT
    return _DETAIL_TIMEOUT


def _get(url: str, logger: CrawlerLogger | None = None, **kw):
    kw.setdefault("timeout", _timeout_for_url(url))
    return safe_get(url, verify=False, logger=logger, **kw)


def _field(soup: BeautifulSoup, name: str) -> str:
    """Return value of an <input type=text> or <textarea> by its name attribute."""
    el = soup.find(["input", "textarea"], {"name": name})
    if el is None:
        return ""
    return (el.get("value") or el.get_text(strip=True) or "").strip()


def _select_val(soup: BeautifulSoup, name: str) -> str:
    sel = soup.find("select", {"name": name})
    if sel is None:
        return ""
    opt = sel.find("option", selected=True)
    return opt.get_text(strip=True) if opt else ""


def _span(soup: BeautifulSoup, span_id: str) -> str:
    el = soup.find("span", id=lambda x: x and x.endswith(span_id))
    return el.get_text(strip=True) if el else ""


def _parse_date(raw: str, fmt: str) -> str | None:
    raw = raw.strip()
    if not raw:
        return None
    try:
        dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        return str(dt)
    except ValueError:
        return None


def _abs_link(href: str) -> str:
    """Resolve a relative href like '../Content/...' to an absolute URL."""
    if href.startswith("http"):
        return href
    if href.startswith("../"):
        return BASE_URL + "/" + href[3:]
    if href.startswith("/"):
        return BASE_URL + href
    return BASE_URL + "/" + href


def _table_rows(soup: BeautifulSoup, table_id_suffix: str) -> list[list[str]]:
    """Return all rows (header + data) of a table identified by id suffix."""
    tbl = soup.find("table", id=re.compile(table_id_suffix + "$"))
    if not tbl:
        return []
    return [[td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
            for tr in tbl.find_all("tr")]



# ── Listing page ──────────────────────────────────────────────────────────────

def _parse_listing_map_data(html: str) -> list[dict]:
    """
    Extract the per-project marker objects embedded as a JavaScript array in the
    listing page.  Each object has the shape:
      {"title":"PROJECT NAME","location":"lat|lng","description":"...","development_type":"4"}
    The description contains the registration number and the detail-page URL
    with the Base64 MyID parameter.
    """
    # The markers are stored as a JS variable or directly inside a JSON-like structure
    pattern = re.compile(
        r'\[\s*\{["\']title["\'].*?\}\s*\]',
        re.DOTALL,
    )
    m = pattern.search(html)
    if not m:
        return []
    try:
        items = json.loads(m.group(0))
    except Exception:
        return []

    projects = []
    for item in items:
        desc = item.get("description", "")
        reg_m = _REG_RE.search(desc)
        if not reg_m:
            continue
        reg_no = reg_m.group(0)

        # Extract MyID from the anchor link in description
        my_id_m = re.search(r"MyID=([A-Za-z0-9+/=]+)", desc)
        my_id   = my_id_m.group(1) if my_id_m else None
        detail_url = f"{DETAIL_BASE}?MyID={my_id}" if my_id else None

        lat, lon = None, None
        loc = item.get("location", "")
        if "|" in loc:
            parts = loc.split("|")
            try:
                lat = float(parts[0].strip())
                lon = float(parts[1].strip())
            except ValueError:
                pass

        # Extract district and tahsil from description HTML
        district_m = re.search(r"District\s*:\s*([^<\n]+)", desc)
        tahsil_m   = re.search(r"Tahsil\s*:\s*([^<\n]+)", desc)
        addr_m     = re.search(r"Project Address\s*:\s*([^<\n]+)", desc)

        projects.append({
            "project_registration_no": reg_no,
            "project_name":            item.get("title", ""),
            "detail_url":              detail_url,
            "my_id":                   my_id,
            "latitude":                lat,
            "longitude":               lon,
            "district":  district_m.group(1).strip() if district_m else None,
            "tahsil":    tahsil_m.group(1).strip()   if tahsil_m   else None,
            "raw_address": addr_m.group(1).strip()   if addr_m     else None,
        })
    return projects


# ── Detail page ───────────────────────────────────────────────────────────────

def _parse_detail_page(detail_url: str, listing_row: dict, logger: CrawlerLogger | None) -> dict:
    resp = _get(detail_url, logger)
    if not resp:
        return {}
    soup = BeautifulSoup(resp.text, "lxml")
    out: dict = {}

    # ── Registration number ───────────────────────────────────────────────────
    reg_m = _REG_RE.search(resp.text)
    if reg_m:
        out["project_registration_no"] = reg_m.group(0)

    # ── Basic form fields ─────────────────────────────────────────────────────
    out["project_name"]  = _field(soup, "ctl00$ContentPlaceHolder1$txt_proj_name")
    out["project_type"]  = _field(soup, "ctl00$ContentPlaceHolder1$DropDownList5")
    out["project_state"] = _select_val(soup, "ctl00$ContentPlaceHolder1$State_Name") or "Chhattisgarh"

    raw_end = _field(soup, "ctl00$ContentPlaceHolder1$txtenddate")
    out["estimated_finish_date"] = _parse_date(raw_end, _DATE_FMT)
    # Also expose raw string so run() can use it for data["data"]["end_date"]
    # (storing the parsed datetime in that field loses information and causes
    # a format mismatch with the expected raw-string representation).
    out["_raw_end_date"] = raw_end

    ecoc = _field(soup, "ctl00$ContentPlaceHolder1$txtecoc")
    ecol = _field(soup, "ctl00$ContentPlaceHolder1$txtecol")
    if ecoc or ecol:
        out["project_cost_detail"] = {
            "estimated_construction_cost": ecoc,
            "cost_of_land":                ecol,
        }

    # ── Promoter / contact ────────────────────────────────────────────────────
    out["promoter_name"] = _field(soup, "ctl00$ContentPlaceHolder1$txt_p_name")
    preg_addr = _field(soup, "ctl00$ContentPlaceHolder1$txt_p_registeredaddress")
    if preg_addr:
        out["promoter_address_raw"] = {"registered_address": preg_addr}
    email = _field(soup, "ctl00$ContentPlaceHolder1$txt_pemail")
    phone = _field(soup, "ctl00$ContentPlaceHolder1$txt_pmobile")
    if email or phone:
        out["promoter_contact_details"] = {k: v for k, v in {"email": email, "phone": phone}.items() if v}
    firm_type = _select_val(soup, "ctl00$ContentPlaceHolder1$DropDownList1")
    promoter_name = out.get("promoter_name") or listing_row.get("project_name", "")
    out["promoters_details"] = {"name": promoter_name, "type_of_firm": firm_type}

    # ── Bank details ──────────────────────────────────────────────────────────
    bank = {
        "account_no":   _field(soup, "ctl00$ContentPlaceHolder1$TextBox2"),
        "account_name": _field(soup, "ctl00$ContentPlaceHolder1$TextBox3"),
        "bank_name":    _field(soup, "ctl00$ContentPlaceHolder1$TextBox4"),
        "branch":       _field(soup, "ctl00$ContentPlaceHolder1$TextBox5"),
        "IFSC":         _field(soup, "ctl00$ContentPlaceHolder1$TextBox6"),
    }
    if any(bank.values()):
        out["bank_details"] = {k: v for k, v in bank.items() if v}

    # ── Location (merge listing map data + detail page selects) ───────────────
    district = _select_val(soup, "ctl00$ContentPlaceHolder1$District_Name") or listing_row.get("district")
    taluk    = _select_val(soup, "ctl00$ContentPlaceHolder1$Tehsil_Name")   or listing_row.get("tahsil")
    raw_addr = _field(soup, "ctl00$ContentPlaceHolder1$AadharNumber")       or listing_row.get("raw_address", "")
    lat = listing_row.get("latitude")
    lon = listing_row.get("longitude")
    loc: dict = {"state": out["project_state"], "district": district, "taluk": taluk, "raw_address": raw_addr}
    if lat is not None:
        loc["latitude"]           = str(lat)
        loc["longitude"]          = str(lon)
        loc["processed_latitude"] = lat
        loc["processed_longitude"] = lon
    out["project_location_raw"] = {k: v for k, v in loc.items() if v}

    return out


def _parse_quarterly_data(soup: BeautifulSoup, resp_text: str) -> dict:
    """Parse quarterly tables to extract: status, building_details, construction_progress,
    professional_info, facility, land_detail, images, documents."""
    out: dict = {}

    # ── grid_status → project status + construction_progress + approved_on_date ──
    status_rows = _table_rows(soup, "ContentPlaceHolder1_grid_status")
    if len(status_rows) > 1:
        headers = status_rows[0]
        data    = status_rows[1:]
        # Approved on date = start date of first quarterly entry
        if data:
            first_from = data[0][0] if data[0] else ""
            out["approved_on_date"] = _parse_date(first_from, _DATE_FMT2)
        # Status = last row's Project Status column
        last = data[-1] if data else []
        if last:
            try: out["status_of_the_project"] = last[headers.index("Project Status")]
            except (ValueError, IndexError): pass
            try:
                to_date = last[1]
                pct     = last[headers.index("Completion(%)")]
                out["construction_progress"] = [{
                    "title":              "total_completion_percentage",
                    "date_of_reporting":  _parse_date(to_date, _DATE_FMT2),
                    "progress_percentage": pct,
                }]
            except (ValueError, IndexError):
                pass

    # ── grid_brief_plot → building_details ───────────────────────────────────
    single_rows = _table_rows(soup, "ContentPlaceHolder1_Single_grid_brief_plot")
    all_rows    = _table_rows(soup, "ContentPlaceHolder1_grid_brief_plot")
    building_details: list[dict] = []
    col_plot_group = "Plot Group"
    col_plots_type = "Plots Type"
    col_plot_area  = "Plot Area(Sq.Metre)"
    col_total      = "Total Plots"
    col_sold       = "Total Plot Sold/Booked"

    def _brief_row_to_detail(row: list[str], header: list[str]) -> dict | None:
        if not row or not any(row):
            return None
        try:
            return {
                "flat_name":     row[header.index(col_plot_group)],
                "flat_type":     row[header.index(col_plots_type)],
                "carpet_area":   row[header.index(col_plot_area)],
                "no_of_units":   "1",
                "booking_status": row[header.index(col_sold)],
            }
        except (ValueError, IndexError):
            return None

    # Current (single) row first
    if len(single_rows) > 1:
        h = single_rows[0]
        entry = _brief_row_to_detail(single_rows[-1], h)
        if entry:
            building_details.append(entry)

    # Header row then all historical rows
    if len(all_rows) > 1:
        h = all_rows[0]
        # Add column-names as a header entry (with "updated")
        building_details.append({
            "updated":       True,
            "flat_name":     col_plot_group,
            "flat_type":     col_plots_type,
            "carpet_area":   col_plot_area,
            "no_of_units":   "1",
            "booking_status": col_sold,
        })
        for row in all_rows[1:]:
            entry = _brief_row_to_detail(row, h)
            if entry:
                entry["updated"] = True
                building_details.append(entry)

    if building_details:
        out["building_details"] = building_details

    # ── number_of_residential_units from grid_brief_plot (Total Plots column) ─
    if len(all_rows) > 1:
        h = all_rows[0]
        try:
            last_data = all_rows[-1]
            total = last_data[h.index(col_total)]
            if total.isdigit():
                out["number_of_residential_units"] = int(total)
                out["_total_unit"] = total
        except (ValueError, IndexError):
            pass

    # ── land_detail ───────────────────────────────────────────────────────────
    total_plots = out.get("_total_unit")
    land_detail: dict = {}
    if total_plots:
        land_detail["no_of_plots"] = total_plots
    # Find encumbrance certificate link
    for a in soup.find_all("a", href=re.compile(r"ENUM_")):
        land_detail["encumbrance_certificate"] = _abs_link(a["href"])
        break
    if land_detail:
        out["land_detail"] = land_detail

    # ── Professional team details ─────────────────────────────────────────────
    professionals: list[dict] = []
    ca_stat_name   = _span(soup, "lbl_stat_ca_name")
    ca_stat_mem    = _span(soup, "lbl_stat_ca_mem_no")
    ca_qtr_name    = _span(soup, "lbl_quarter_ca_name")
    ca_qtr_mem     = _span(soup, "lbl_quarter_ca_mem_no")
    eng_name       = _span(soup, "lbl_engineer_name")
    eng_lic        = _span(soup, "lbl_engineer_lic_no")
    arch_name      = _span(soup, "lbl_arch_name")
    arch_lic       = _span(soup, "lbl_arch_lic_no")
    if ca_stat_name:
        professionals.append({"name": ca_stat_name, "role": "Chartered Accountant",
                               "registration_no": ca_stat_mem})
    if ca_qtr_name:
        professionals.append({"name": ca_qtr_name, "role": "Chartered Accountant",
                               "registration_no": ca_qtr_mem})
    if eng_name:
        professionals.append({"name": eng_name, "role": "Engineer", "registration_no": eng_lic})
    if arch_name:
        professionals.append({"name": arch_name, "role": "Architect", "registration_no": arch_lic})
    if professionals:
        out["professional_information"] = professionals

    # ── Provided facility (Single_gv_ProjectList Particulars column) ──────────
    gvp_rows = _table_rows(soup, "ContentPlaceHolder1_Single_gv_ProjectList")
    if len(gvp_rows) > 1:
        try:
            part_idx = gvp_rows[0].index("Particulars")
            parts = [r[part_idx] for r in gvp_rows[1:] if len(r) > part_idx and r[part_idx]]
            if parts:
                out["provided_faciltiy"] = {"facility": " ".join(parts)}
        except ValueError:
            pass

    # ── land_area_details (always include even when null values) ──────────────
    out["land_area_details"] = {"construction_area": None, "construction_area_unit": None}

    # ── status_update ─────────────────────────────────────────────────────────
    # plot_detail / building_details from grid_brief_plot rows
    plot_detail: list[dict] = []
    if len(all_rows) > 1:
        h = all_rows[0]
        try:
            idx_type  = h.index("Plots Type")
            idx_area  = h.index("Plot Area(Sq.Metre)")
            idx_sold  = h.index("Total Plot Sold/Booked")
            idx_total = h.index("Total Plots")
            idx_from  = 0
            idx_to    = 1
            for row in all_rows[1:]:
                if not row or not any(row):
                    continue
                plot_detail.append({
                    "flat_type":        row[idx_type]  if len(row) > idx_type  else "",
                    "carpet_area":      row[idx_area]  if len(row) > idx_area  else "",
                    "booked_flats":     row[idx_sold]  if len(row) > idx_sold  else "",
                    "completion_date":  row[idx_to]    if len(row) > idx_to    else "",
                    "total_available":  row[idx_total] if len(row) > idx_total else "",
                    "date_of_reporting": row[idx_from] if len(row) > idx_from  else "",
                })
        except ValueError:
            pass

    # construction_progress from gv_ProjectList (all quarters)
    gv_tbl = soup.find("table", id=re.compile(r"ContentPlaceHolder1_gv_ProjectList$"))
    gv_progress: list[dict] = []
    if gv_tbl:
        trs = gv_tbl.find_all("tr")
        if trs:
            gh = [th.get_text(strip=True) for th in trs[0].find_all(["th", "td"])]
            try:
                gi_from  = gh.index("From Date")
                gi_to    = gh.index("To Date")
                gi_part  = gh.index("Particulars")
                gi_prog  = gh.index("Progress Status(%)")
                gi_img   = gh.index("Image")
            except ValueError:
                gi_from = gi_to = gi_part = gi_prog = gi_img = -1
            for tr in trs[1:]:
                tds = tr.find_all("td")
                if not tds:
                    continue
                img_src = ""
                if gi_img >= 0 and gi_img < len(tds):
                    img_el = tds[gi_img].find("img")
                    if img_el:
                        img_src = _abs_link(img_el.get("src", ""))
                gv_progress.append({
                    "image":             img_src,
                    "title":             tds[gi_part].get_text(strip=True) if gi_part >= 0 and gi_part < len(tds) else "",
                    "date_of_reporting": tds[gi_from].get_text(strip=True) if gi_from >= 0 and gi_from < len(tds) else "",
                    "date_of_completion": tds[gi_to].get_text(strip=True)  if gi_to  >= 0 and gi_to  < len(tds) else "",
                    "progress_percentage": tds[gi_prog].get_text(strip=True) if gi_prog >= 0 and gi_prog < len(tds) else "",
                })

    status_update: dict = {}
    if plot_detail:
        status_update["plot_detail"]         = plot_detail
        status_update["building_details"]    = plot_detail   # duplicated per sample
    if gv_progress:
        status_update["construction_progress"] = gv_progress
    if status_update:
        out["status_update"] = status_update

    return out


def _parse_documents(soup: BeautifulSoup, resp_text: str) -> list[dict]:
    """
    Collect all document links from the detail page:
    1. Initial labeled docs (label text + adjacent anchor)
    2. Quarterly grid_docs table (CA/Engg/Arch per quarter)
    3. Quarterly grid_documents_amount_withdrawl table
    4. grid_status completion certificates
    """
    docs: list[dict] = []
    seen: set[str] = set()
    engg_count = arch_count = comp_count = reg_count = layout_count = spec_count = 0
    devteam_count = workplan_count = engg_cert_count = 0

    def _add(href: str, doc_type: str, dated_on: str | None = None):
        nonlocal engg_count, arch_count, comp_count, reg_count, layout_count
        nonlocal spec_count, devteam_count, workplan_count, engg_cert_count
        # Skip placeholder javascript: links (e.g. "NA" anchors in quarterly tables)
        if href.lower().startswith("javascript:"):
            return
        url = _abs_link(href)
        key = (url, doc_type, dated_on or "")
        if key in seen:
            return
        seen.add(key)
        entry: dict = {"link": url, "type": doc_type}
        if dated_on:
            entry["dated_on"] = dated_on
        docs.append(entry)

    # ── Completion certs from grid_status last row ────────────────────────────
    status_rows = _table_rows(soup, "ContentPlaceHolder1_grid_status")
    if len(status_rows) > 1:
        h       = status_rows[0]
        last    = status_rows[-1]
        from_dt = last[0] if last else ""
        try: arch_col = h.index("Completion Certificate by Architect")
        except ValueError: arch_col = -1
        try: ca_col = h.index("Completion Certificate by Competent Authority")
        except ValueError: ca_col = -1

        # Collect the raw <a> links from the last row of grid_status
        tbl = soup.find("table", id=re.compile(r"ContentPlaceHolder1_grid_status$"))
        if tbl:
            data_trs = tbl.find_all("tr")[1:]
            if data_trs:
                last_tr = data_trs[-1]
                tds     = last_tr.find_all("td")
                if arch_col >= 0 and arch_col < len(tds):
                    a = tds[arch_col].find("a", href=True)
                    if a:
                        comp_count += 1
                        _add(a["href"], f"Completion Certificate {comp_count}")
                if ca_col >= 0 and ca_col < len(tds):
                    a = tds[ca_col].find("a", href=True)
                    if a:
                        comp_count += 1
                        _add(a["href"], f"Completion Certificate {comp_count}")

    # ── Initial labeled documents ─────────────────────────────────────────────
    # Walk label → adjacent span with anchor
    _LABEL_TYPE_MAP = {
        "Registration Certificate": ("Registration Certificate", True),
        "Bank Account PassBook Front Page": ("Bank Account PassBook Front Page", False),
        "Fee Calculation Sheet": ("Fee Calculation Sheet", False),
        # The page uses both "Sanctioned Layout Plan" and "Layout Plan" depending on project
        "Layout Plan": ("Layout Plan", True),
        "Sanctioned Layout Plan": ("Layout Plan", True),
        # "Modified Layout Plan" on this site actually links to the Development
        # Permission file (DEVE_PER_COM_AUTH_…), NOT a separate layout plan.
        # Map it to the correct type so it de-dupes with the explicit
        # "Development Permission from competent authorities" label that follows.
        "Modified Layout Plan": ("Development Permission from competent authorities", False),
        "Project Specifications": ("Project Specifications", True),
        "Engineer Certificate": ("Engineer Certificate", True),
        # The page renders these with Title Case
        "Development team details": ("Development team details", True),
        "Development Team Details": ("Development team details", True),
        "Development work plan": ("Development work plan", True),
        "Development Work Plan": ("Development work plan", True),
        "Encumbrances on Land/Non-Encumbrances Certificate": ("Encumbrances on Land/Non-Encumbrances Certificate", False),
        "Search Report": ("Search Report", False),
        "Approval Letter of Town And Country Planning": ("Approval Letter of Town And Country Planning", False),
        "Development Permission from competent authorities": ("Development Permission from competent authorities", False),
        "Affidavit Cum Declaration": ("Affidavit Cum Declaration", False),
        "Undertaking by the Promoter for pending documents (ANNEX-08)": ("Undertaking by the Promoter for pending documents (ANNEX-08)", False),
        "Self Declaration by the Promoter for those documents which are not applicable (ANNEX-07)": ("Self Declaration by the Promoter for those documents which are not applicable (ANNEX-07)", False),
        "CA Certificate": ("CA Certificate", False),
    }
    _NUMBERED: dict[str, int] = {}

    def _type_with_count(base: str, numbered: bool) -> str:
        if not numbered:
            return base
        _NUMBERED[base] = _NUMBERED.get(base, 0) + 1
        return f"{base} {_NUMBERED[base]}"

    # Single-pass loop: process every label in HTML document order so that
    # variable-text labels (Brief Details, CA Certificate(For New Project…))
    # land in the correct position relative to LABEL_TYPE_MAP entries.
    # The old two-pass approach ran the variable-text sweep AFTER the map
    # sweep, placing those docs at the wrong index.
    for label in soup.find_all("label"):
        ltxt = label.get_text(strip=True)
        if ltxt in _LABEL_TYPE_MAP:
            base, numbered = _LABEL_TYPE_MAP[ltxt]
            nxt = label.find_next("a", href=re.compile(r"ProjectDocuments"))
            if nxt:
                _add(nxt["href"], _type_with_count(base, numbered))
        elif "CA Certificate(For New Project" in ltxt:
            a = label.find_next("a", href=re.compile(r"ProjectDocuments"))
            if a:
                _add(a["href"], ltxt)
        elif "Brief Details of Current Project" in ltxt:
            a = label.find_next("a", href=re.compile(r"ProjectDocuments"))
            if a:
                _add(a["href"], ltxt)

    # ── Quarterly grid_docs (all quarters) ────────────────────────────────────
    docs_tbl = soup.find("table", id=re.compile(r"ContentPlaceHolder1_grid_docs$"))
    if docs_tbl:
        trs = docs_tbl.find_all("tr")
        if trs:
            headers = [th.get_text(strip=True) for th in trs[0].find_all(["th", "td"])]
            try: ca_col   = headers.index("CA Certificate")
            except ValueError: ca_col = -1
            try: engg_col = headers.index("Engg Certificate")
            except ValueError: engg_col = -1
            try: arch_col = headers.index("Architect Certificate")
            except ValueError: arch_col = -1

            for tr in trs[1:]:
                tds = tr.find_all("td")
                from_dt = tds[0].get_text(strip=True) if tds else ""
                if ca_col >= 0 and ca_col < len(tds):
                    a = tds[ca_col].find("a", href=True)
                    if a:
                        _add(a["href"], "CA Certificate", from_dt)
                if engg_col >= 0 and engg_col < len(tds):
                    a = tds[engg_col].find("a", href=True)
                    if a:
                        engg_count += 1
                        _add(a["href"], f"Engg Certificate {engg_count}", from_dt)
                if arch_col >= 0 and arch_col < len(tds):
                    a = tds[arch_col].find("a", href=True)
                    if a:
                        arch_count += 1
                        _add(a["href"], f"Architect certificate {arch_count}", from_dt)

    # ── Quarterly amount-withdrawal docs ──────────────────────────────────────
    amt_tbl = soup.find("table", id=re.compile(r"ContentPlaceHolder1_grid_documents_amount_withdrawl$"))
    if amt_tbl:
        trs = amt_tbl.find_all("tr")
        if trs:
            headers = [th.get_text(strip=True) for th in trs[0].find_all(["th", "td"])]
            try: ca_col   = headers.index("CA Certificate")
            except ValueError: ca_col = -1
            try: engg_col = headers.index("Engg Certificate")
            except ValueError: engg_col = -1
            try: arch_col = headers.index("Architect Certificate")
            except ValueError: arch_col = -1

            for tr in trs[1:]:
                tds = tr.find_all("td")
                from_dt = tds[0].get_text(strip=True) if tds else ""
                if ca_col >= 0 and ca_col < len(tds):
                    a = tds[ca_col].find("a", href=True)
                    if a:
                        _add(a["href"], "CA Certificate", from_dt)
                if engg_col >= 0 and engg_col < len(tds):
                    a = tds[engg_col].find("a", href=True)
                    if a:
                        engg_count += 1
                        _add(a["href"], f"Engg Certificate {engg_count}", from_dt)
                if arch_col >= 0 and arch_col < len(tds):
                    a = tds[arch_col].find("a", href=True)
                    if a:
                        arch_count += 1
                        _add(a["href"], f"Architect certificate {arch_count}", from_dt)

    return docs


def _parse_images(soup: BeautifulSoup) -> list[str]:
    """Collect project images (amenity photos) from the detail page.

    Only collects from the Single_gv_ProjectList table (current/latest quarter
    amenity photos), which matches the 14-image sample exactly.  The full
    gv_ProjectList table contains images from ALL historical quarters and
    inflates the count ~5× with duplicates.
    """
    images: list[str] = []
    seen: set[str] = set()
    # Use only the single (latest-quarter) amenity-photo table
    table = soup.find("table", id=re.compile(r"ContentPlaceHolder1_Single_gv_ProjectList$"))
    if table is None:
        return images
    for img in table.find_all("img", src=True):
        src = img["src"].strip()
        lower = src.lower()
        if "ProjectDocuments" not in src:
            continue
        if any(t in lower for t in ("logo", "captcha", "icon", "banner")):
            continue
        url = _abs_link(src)
        if url not in seen:
            seen.add(url)
            images.append(url)
    return images


# ── Document upload ────────────────────────────────────────────────────────────

def _handle_document(project_key: str, doc: dict, run_id: int, site_id: str,
                     logger: CrawlerLogger) -> dict | None:
    url   = doc["link"]
    label = doc["type"]
    fname = build_document_filename({"url": url, "label": label})
    try:
        resp = download_response(
            url,
            logger=logger,
            timeout=_timeout_for_url(url),
            verify=False,
        )
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
            original_url=url,
            s3_key=s3_key,
            s3_bucket=settings.S3_BUCKET_NAME,
            file_name=fname,
            md5_checksum=md5,
            file_size_bytes=len(resp.content),
        )
        logger.info("Document uploaded", label=label, s3_key=s3_key, step="documents")
        logger.log_document(label, url, "uploaded", s3_key=s3_key, file_size_bytes=len(resp.content))
        return document_result_entry({"url": url, "label": label}, s3_url, fname)
    except Exception as e:
        logger.error(f"Doc failed for {project_key}: {e}")
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(e),
                           project_key=project_key, url=url)
        return None


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Chhattisgarh RERA.
    Full-flow check: re-scrapes the sentinel project's detail page (basic project
    + promoter fields) AND quarterly data (status, approved_on_date, residential
    units, documents), merges them (same as run()), and verifies ≥ 80% field
    coverage against the full baseline.
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
        "state_projects_sample", "chhattisgarh.json",
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
        # Pre-check connectivity: if the site is unreachable (transient timeout)
        # skip the sentinel rather than aborting the entire crawl run.
        _probe = _get(detail_url, logger)
        if not _probe:
            logger.warning(
                "Sentinel: detail page unreachable — likely transient network issue; "
                "skipping coverage check this run",
                url=detail_url, step="sentinel",
            )
            return True

        # ── Parse detail page (basic project + promoter fields) ───────────────
        detail = _parse_detail_page(detail_url, {}, logger) or {}

        # ── Parse quarterly data (status, approved_on_date, units, docs) ──────
        # Fetches the same URL again to get the quarterly tables (mirrors run())
        resp2 = _get(detail_url, logger)
        qdata: dict = {}
        if resp2:
            soup2 = BeautifulSoup(resp2.text, "lxml")
            qdata = _parse_quarterly_data(soup2, resp2.text)
        else:
            logger.warning("Sentinel: quarterly data fetch failed — proceeding with detail only",
                           step="sentinel")

        # Merge detail + quarterly (same as run())
        fresh = dict(detail)
        for k, v in qdata.items():
            if v is not None and not k.startswith("_"):
                fresh[k] = v

    except Exception as exc:
        logger.error(f"Sentinel: scrape error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", url=detail_url, step="sentinel")
        insert_crawl_error(run_id, config.get("id", "chhattisgarh_rera"),
                           "SENTINEL_FAILED",
                           "Sentinel page yielded no extractable fields (possible layout change)",
                           url=detail_url)
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "chhattisgarh_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


# ── Main run() ─────────────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    logger   = CrawlerLogger(config["id"], run_id)
    site_id  = config["id"]
    counts   = dict(projects_found=0, projects_new=0, projects_updated=0,
                    projects_skipped=0, documents_uploaded=0, error_count=0)

    checkpoint      = load_checkpoint(site_id, mode) or {}
    done_regs: set  = set(checkpoint.get("done_regs", []))
    item_limit      = settings.CRAWL_ITEM_LIMIT or 0
    machine_name, machine_ip = get_machine_context()
    t_run = time.monotonic()

    # ── Sentinel health check ────────────────────────────────────────────────
    t0 = time.monotonic()
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counts["error_count"] += 1
        return counts
    logger.timing("sentinel", time.monotonic() - t0)

    # ── Fetch listing page to get all project stubs ───────────────────────────
    t0 = time.monotonic()
    logger.info("Fetching listing page …")
    resp = _get(LISTING_URL, logger)
    if not resp:
        logger.error("Failed to fetch listing page")
        insert_crawl_error(run_id, site_id, "HTTP_ERROR", "Listing page failed", url=LISTING_URL)
        counts["error_count"] += 1
        return counts

    project_stubs = _parse_listing_map_data(resp.text)
    logger.info(f"Found {len(project_stubs)} project stubs from listing map data")
    counts["projects_found"] = len(project_stubs)
    logger.timing("search", time.monotonic() - t0, rows=len(project_stubs))

    items_processed = 0

    for stub in project_stubs:
        if item_limit and items_processed >= item_limit:
            logger.info(f"CRAWL_ITEM_LIMIT={item_limit} reached, stopping")
            reset_checkpoint(site_id, mode)
            return counts

        reg_no = stub.get("project_registration_no", "")
        if not reg_no:
            continue
        if reg_no in done_regs:
            counts["projects_skipped"] += 1
            continue

        key = generate_project_key(reg_no)
        detail_url = stub.get("detail_url") or ""
        logger.set_project(key=key, reg_no=reg_no, url=detail_url)

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
                "project_registration_no": reg_no,
                "project_name":            stub.get("project_name"),
                "domain":                  DOMAIN,
                "config_id":               config["config_id"],
                "url":                     detail_url or LISTING_URL,
                "is_live":                 True,
                "machine_name":            machine_name,
                "crawl_machine_ip":        machine_ip,
            }

            doc_links: list[dict] = []
            if detail_url and settings.SCRAPE_DETAILS:
                random_delay(*config.get("rate_limit_delay", (1, 3)))
                logger.info("Fetching detail page", step="detail_fetch")

                detail = _parse_detail_page(detail_url, stub, logger)
                for k, v in detail.items():
                    if v is not None and k != "project_registration_no":
                        data[k] = v

                # Quarterly data requires re-fetching soup (detail already done above)
                resp2 = _get(detail_url, logger)
                if resp2:
                    soup2 = BeautifulSoup(resp2.text, "lxml")
                    qdata = _parse_quarterly_data(soup2, resp2.text)
                    for k, v in qdata.items():
                        if v is not None and not k.startswith("_"):
                            data[k] = v

                    doc_links  = _parse_documents(soup2, resp2.text)
                    images     = _parse_images(soup2)
                    if images:
                        data["project_images"] = images

                    total_unit = qdata.get("_total_unit")
                    # Use the raw end-date string (e.g. "30-04-2022") rather
                    # than the parsed datetime so the stored value matches the
                    # site's original representation.
                    raw_end_date = data.pop("_raw_end_date", "") or data.get("estimated_finish_date", "")
                    data["data"] = merge_data_sections({
                        "govt_type":               "state",
                        "total_unit":              total_unit,
                        "construction_area_unit":  "sq.metre",
                        "end_date":                raw_end_date,
                    })

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
                                   project_key=key, url=detail_url, raw_data=data)
                counts["error_count"] += 1
                db_dict = normalize_project_payload(data, config,
                                                    machine_name=machine_name, machine_ip=machine_ip)

            # land_area_details must always be stored with its structure even
            # when both sub-values are null.  normalize_project_payload strips
            # {"construction_area": None, "construction_area_unit": None} to {}
            # → None via clean_json, so we inject it post-normalization.
            if not db_dict.get("land_area_details"):
                db_dict["land_area_details"] = {
                    "construction_area": None,
                    "construction_area_unit": None,
                }

            action = upsert_project(db_dict)
            items_processed += 1
            if action == "new":       counts["projects_new"] += 1
            elif action == "updated": counts["projects_updated"] += 1
            else:                     counts["projects_skipped"] += 1
            logger.info(f"DB result: {action}", step="db_upsert")

            # ── Document uploads ──────────────────────────────────────────────
            uploaded_documents: list[dict] = []
            doc_name_counts: dict[str, int] = {}
            for doc in doc_links:
                doc_for_policy = {"url": doc["link"], "label": doc["type"]}
                selected = select_document_for_download(
                    config["state"], doc_for_policy, doc_name_counts, domain=DOMAIN,
                )
                if selected:
                    doc_to_upload = {**doc, "type": selected.get("type", doc["type"])}
                    uploaded = _handle_document(db_dict["key"], doc_to_upload, run_id, site_id, logger)
                    if uploaded:
                        uploaded_documents.append(uploaded)
                        counts["documents_uploaded"] += 1
                    else:
                        uploaded_documents.append(doc)
                else:
                    uploaded_documents.append(doc)

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
            save_checkpoint(site_id, mode, 0, None, run_id)

        except Exception as exc:
            logger.exception("Project processing failed", exc, step="project_loop")
            insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                               project_key=key, url=detail_url)
            counts["error_count"] += 1
        finally:
            logger.clear_project()

    reset_checkpoint(site_id, mode)
    logger.info(f"Chhattisgarh RERA complete: {counts}")
    logger.timing("total_run", time.monotonic() - t_run)
    return counts
