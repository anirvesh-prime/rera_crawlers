"""
Punjab RERA Crawler — rera.punjab.gov.in/reraindex/publicview/projectinfo
Type: playwright (Playwright + Chromium)

Strategy:
- Navigate to listing page with a headless browser.
- Fill the CAPTCHA input with a dummy 6-char string — validation is client-side JS
  only; the server never verifies the CAPTCHA image text.
- Click Search → wait for AJAX response to populate #viewProjectPVList.
- DataTables is configured with ALL rows in the DOM (client-side pagination).
  A single search returns every matching project; we read directly from the DOM.
- Column headers seen on-screen: SNo | District Name | Project's Name |
  Promoter's Name | Registration Number | Registration Valid Upto Date | View Details.
- For each row: read hdnProjectID / hdnPromoterID / hdnPromoterType hidden inputs,
  click "View Details", wait for the Bootstrap modal to appear, extract every
  labeled <td> pair from the modal body — field names exactly as displayed.
"""
from __future__ import annotations

import base64
import json
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
import httpx
from playwright.sync_api import TimeoutError as PWTimeout, sync_playwright
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.captcha_solver import captcha_to_text
from core.crawler_base import generate_project_key, random_delay
from core.config import settings
from core.db import get_project_by_key, upsert_project, upsert_document, insert_crawl_error
from core.document_policy import select_document_for_download
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import (
    build_document_filename,
    build_document_urls,
    document_result_entry,
    get_machine_context,
    normalize_project_payload,
)
from core.s3 import compute_md5, upload_document, get_s3_url

LISTING_URL   = "https://rera.punjab.gov.in/reraindex/publicview/projectinfo"
SEARCH_URL    = "https://rera.punjab.gov.in/reraindex/PublicView/ProjectPVregdprojectInfo"
DETAIL_URL    = "https://rera.punjab.gov.in/reraindex/PublicView/ProjectViewDetails"
STATE_CODE    = "PB"
DOMAIN        = "rera.punjab.gov.in"

# CSS / text selectors (confirmed from live HTML)
SEL_CAPTCHA   = "#Input_RegdProject_CaptchaText"
SEL_SUBMIT    = "#btn_MapsProjectSubmit"
SEL_TABLE     = "table#dataTablePartialViewSearchRegdProject"
SEL_ROWS      = f"{SEL_TABLE} tbody tr"
SEL_VIEW_BTN  = "a#modalOpenerButtonRegdProject"
SEL_MODAL     = "#myModal"
SEL_MODAL_VIS = "#myModal.show"
SEL_CAPTCHA_IMG = "img.capcha-badge"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_modal_fields(page) -> dict:
    """
    Read every labeled pair from the visible modal body.
    Looks for <td>Label:</td><td>Value</td> rows — exactly what the user sees.
    """
    html  = page.inner_html(f"{SEL_MODAL} .modal-body")
    soup  = BeautifulSoup(html, "lxml")
    fields: dict = {}
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True).rstrip(":").strip()
            value = cells[1].get_text(separator=" ", strip=True)
            if label and value:
                fields[label] = value
    return fields


def _extract_modal_fields_html(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    fields: dict = {}
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True).rstrip(":").strip()
            value = cells[1].get_text(separator=" ", strip=True)
            if label and value:
                fields[label] = value
    return fields


# ── Comprehensive detail-page parser ─────────────────────────────────────────

_QTR_MAP: dict[str, str] = {
    "FirstQTR":  "QTR-I (January-March)",
    "SecondQTR": "QTR-II (April-June)",
    "ThirdQTR":  "QTR-III (July-September)",
    "FourthQTR": "QTR-IV (October-December)",
}

_BASE_URL = "https://rera.punjab.gov.in"


def _ws(text: str | None) -> str | None:
    """Collapse whitespace; return None if empty."""
    if not text:
        return None
    cleaned = re.sub(r"\s+", " ", str(text)).strip()
    return cleaned or None


def _find_by_label(soup: "BeautifulSoup", *labels: str) -> str | None:
    """
    Search all <td> elements for one whose normalized text matches any of
    *labels* (exact or prefix match after stripping trailing ':').
    Return the text of the immediately following sibling <td>.
    """
    for td in soup.find_all("td"):
        raw = _ws(td.get_text(separator=" "))
        if raw is None:
            continue
        raw = raw.rstrip(":")
        for label in labels:
            if raw == label or raw.startswith(label):
                sib = td.find_next_sibling("td")
                if sib:
                    return _ws(sib.get_text(separator=" "))
    return None


def _parse_detail_page(
    html: str,
    project_id: str,
    promoter_id: str,
    promoter_type: str,
    district: str | None = None,
) -> dict:
    """
    Parse the full Punjab project detail page into schema-compatible fields.
    Returns a dict ready to be merged into the crawler payload.
    """
    soup = BeautifulSoup(html, "lxml")
    result: dict = {}

    # ── Direct text / date fields ─────────────────────────────────────────────
    result["project_type"] = _find_by_label(soup, "Type of Project")
    result["status_of_the_project"] = _find_by_label(soup, "Project Status")
    result["actual_commencement_date"] = _find_by_label(soup, "Project Start Date")
    result["estimated_finish_date"] = _find_by_label(
        soup,
        "Proposed/ Expected Date of Project Completion as specified in Form B",
        "Proposed/ Expected Date of Project Completion",
    )
    result["project_description"] = _find_by_label(
        soup,
        "Specification Details of Proposed Project as per the Brochure/ Prospectus",
        "Specification Details",
    )

    # ── Project location raw (plot_no from khasra table + raw_address) ────────
    plot_no = None
    for table in soup.find_all("table"):
        hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
        if "Khasra Number" in " ".join(hdrs):
            for tr in table.find_all("tr")[1:2]:
                cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all("td")]
                if len(cells) >= 2:
                    plot_no = _ws(cells[1])
            break

    raw_addr = _find_by_label(soup, "Project Address")
    loc_raw: dict = {}
    if plot_no:
        loc_raw["plot_no"] = plot_no
    if raw_addr:
        loc_raw["raw_address"] = raw_addr
    if district:
        loc_raw["district"] = district
    if loc_raw:
        result["project_location_raw"] = loc_raw

    # ── Project cost ──────────────────────────────────────────────────────────
    cost_str = _find_by_label(soup, "Project Cost (in rupees)")
    if cost_str:
        cost_digits = re.sub(r"[^\d.]", "", cost_str.split("(")[0].strip())
        try:
            result["project_cost_detail"] = {"total_project_cost": float(cost_digits)}
        except (ValueError, TypeError):
            pass

    # ── Land / construction area ──────────────────────────────────────────────
    land_str = _find_by_label(
        soup,
        "Total Area of Land Proposed to be developed (in sqr mtrs)",
        "Total Area of Land Proposed to be developed",
    )
    if land_str:
        land_digits = re.sub(r"[^\d.]", "", land_str.split("(")[0].strip())
        try:
            land_val = float(land_digits)
            result["land_area"] = land_val
            result["construction_area"] = land_val
            result["land_area_details"] = {
                "land_area": land_val,
                "land_area_unit": "(in sqr mtrs)",
                "construction_area": land_val,
                "construction_area_unit": "(in sqr mtrs)",
            }
        except (ValueError, TypeError):
            pass

    # ── Promoter contact details ──────────────────────────────────────────────
    email: str | None = None
    phone: str | None = None
    for tr in soup.find_all("tr"):
        cells = tr.find_all("td")
        cell_texts = [_ws(c.get_text(separator=" ")) or "" for c in cells]
        if "Phone Number" in cell_texts:
            idx = cell_texts.index("Phone Number")
            if idx + 1 < len(cells):
                phone = _ws(cells[idx + 1].get_text(separator=" "))
            for i, ct in enumerate(cell_texts):
                if "Email" in ct and i + 1 < len(cells):
                    email = _ws(cells[i + 1].get_text(separator=" "))
            break
    if not email:
        email = _find_by_label(soup, "Email Address")
    if not phone:
        phone = _find_by_label(soup, "Phone Number")
    if email or phone:
        contact: dict = {}
        if email:
            contact["email"] = email
        if phone:
            contact["mobile no"] = phone
        result["promoter_contact_details"] = contact

    # ── Promoter experience ───────────────────────────────────────────────────
    exp_state = _find_by_label(
        soup,
        "Years of Experience of Promoter in Real Estate Development in Punjab",
    )
    exp_other = _find_by_label(
        soup,
        "Years of Experience of Promoter in Real Estate Development in Other states or UTs",
        "Years of Experience of Promoter in Real Estate Development in Other",
    )
    if exp_state or exp_other:
        result["promoters_details"] = {
            "experience_state": exp_state,
            "experience_outside_state": exp_other,
        }

    # ── Bank details ──────────────────────────────────────────────────────────
    bank_name   = _find_by_label(soup, "Bank Name")
    branch      = _find_by_label(soup, "Branch Name")
    acct_no     = _find_by_label(soup, "Bank Account Number")
    ifsc        = _find_by_label(soup, "Bank IFSC Code")
    acct_holder = _find_by_label(soup, "Account Holder Name")
    bank_addr   = _find_by_label(soup, "Bank Address")
    if bank_name:
        bank: dict = {"bank_name": bank_name}
        if branch:
            bank["branch"] = branch
        if acct_no:
            bank["account_no"] = acct_no
        if ifsc:
            bank["IFSC"] = ifsc
        if acct_holder:
            bank["account_name"] = acct_holder
        if bank_addr:
            bank["address"] = bank_addr
        result["bank_details"] = [bank]

    # ── Construction progress (internal + external infrastructure tables) ──────
    cp: list[dict] = []
    for table in soup.find_all("table"):
        hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
        hdr_str = " ".join(hdrs)
        if "Internal Infrastructure" in hdr_str:
            # Cols: Sr.No | Name | Details | Work Progress %
            for tr in table.find_all("tr")[1:]:
                cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all("td")]
                if len(cells) >= 4 and cells[1]:
                    cp.append({"title": _ws(cells[1]), "progress_percentage": _ws(cells[3])})
        elif "External Infrastructure" in hdr_str:
            # Cols: Sr.No | Name | Type | Details | Work Progress %
            for tr in table.find_all("tr")[1:]:
                cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all("td")]
                if len(cells) >= 5 and cells[1]:
                    cp.append({"title": _ws(cells[1]), "progress_percentage": _ws(cells[4])})
    if cp:
        result["construction_progress"] = cp

    # ── Building details (apartment/plot sub-table; uses <td> not <th> headers) ─
    def _area_str(raw: str) -> str | None:
        d = re.sub(r"[^\d.]", "", raw.split("(")[0].strip())
        return (d + " (in sqr mtrs)") if d else None

    bd: list[dict] = []
    for table in soup.find_all("table"):
        # Table uses <td> as column headers; check first non-empty row
        all_rows = table.find_all("tr")
        if not all_rows:
            continue
        hdr_cells = [td.get_text(strip=True) for td in all_rows[0].find_all("td")]
        hdr_str = " ".join(hdr_cells)
        if "Type of Apartment" not in hdr_str or "Carpet Area" not in hdr_str:
            continue
        # Cols: Sr.No | Type | Total | Sold | Carpet Area | Open Area | Balcony Area
        for tr in all_rows[1:]:
            cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all("td")]
            if len(cells) >= 5 and cells[0].isdigit():
                bd.append({
                    "flat_type":    _ws(cells[1]),
                    "no_of_units":  _ws(cells[2]),
                    "carpet_area":  _area_str(cells[4]) if len(cells) > 4 else None,
                    "open_area":    _area_str(cells[5]) if len(cells) > 5 else None,
                    "balcony_area": _area_str(cells[6]) if len(cells) > 6 else None,
                })
    if bd:
        result["building_details"] = bd

    # ── Professional information ──────────────────────────────────────────────
    prof_list: list[dict] = []
    for table in soup.find_all("table"):
        hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
        hdr_str = " ".join(hdrs)
        if "Name of Professional" in hdr_str and "Associated Consultant" in hdr_str:
            rows_tr = table.find_all("tr")[1:]
            i = 0
            while i < len(rows_tr):
                cells = [td.get_text(separator=" ", strip=True) for td in rows_tr[i].find_all("td")]
                if cells and cells[0].isdigit():
                    prof: dict = {}
                    if len(cells) > 1 and cells[1]:
                        prof["name"] = _ws(cells[1])
                    if len(cells) > 2 and cells[2]:
                        prof["role"] = _ws(cells[2])
                    if len(cells) > 4 and cells[4]:
                        prof["key_real_estate_projects"] = _ws(cells[4])
                    # Next row may have address (with embedded email / phone)
                    if i + 1 < len(rows_tr):
                        nc = [td.get_text(separator=" ", strip=True) for td in rows_tr[i + 1].find_all("td")]
                        addr_label = (nc[1] if len(nc) > 1 else "") + (nc[0] if nc else "")
                        if "Address" in addr_label:
                            addr_val = nc[2] if len(nc) > 2 else None
                            if addr_val:
                                # Extract email (pattern: "Email: foo@bar.com")
                                em = re.search(r"Email:\s*(\S+)", addr_val, re.IGNORECASE)
                                if em:
                                    prof["email"] = _ws(em.group(1))
                                # Extract phone (pattern: "Mobile/Landline Number: 9999999999")
                                ph = re.search(r"(?:Mobile|Landline)[^:]*:\s*([0-9]+)", addr_val, re.IGNORECASE)
                                if ph:
                                    prof["phone"] = _ws(ph.group(1))
                                # Strip email/phone suffix from address
                                clean_addr = re.sub(r"\s*Email:.*$", "", addr_val, flags=re.IGNORECASE | re.DOTALL)
                                clean_addr = _ws(clean_addr)
                                if clean_addr:
                                    prof["address"] = clean_addr
                            i += 1
                    if prof.get("name"):
                        prof_list.append(prof)
                i += 1
    if prof_list:
        result["professional_information"] = prof_list

    # ── Co-promoter details ───────────────────────────────────────────────────
    _QTR_LETTER: dict[str, str] = {"F": "FirstQTR", "S": "SecondQTR", "T": "ThirdQTR", "L": "FourthQTR"}
    for span in soup.find_all("span"):
        span_html = str(span)
        if "mailto:" not in span_html:
            continue
        span_text = span.get_text(separator="\n")
        lines = [ln.strip() for ln in span_text.splitlines() if ln.strip()]
        # Name is everything before the address line (first <br>)
        # Address follows name lines; email/mobile come after address
        name_parts: list[str] = []
        addr_parts: list[str] = []
        state = "name"
        co_email: str | None = None
        co_mobile: str | None = None
        _capture_mobile = False
        for ln in lines:
            if _capture_mobile:
                co_mobile = ln
                _capture_mobile = False
                continue
            if ln.startswith("E-Mail:"):
                state = "contact"
                continue
            if ln.startswith("Mobile Phone:"):
                val = ln.replace("Mobile Phone:", "").strip()
                if val:
                    co_mobile = val
                else:
                    _capture_mobile = True
                continue
            if state == "name" and re.match(r"[A-Z0-9 .\-]+$", ln):
                name_parts.append(ln)
            elif state == "name":
                state = "addr"
                addr_parts.append(ln)
            elif state == "addr":
                addr_parts.append(ln)
        # email from mailto link
        for a_tag in span.find_all("a", href=True):
            href = a_tag["href"]
            if href.startswith("mailto:"):
                co_email = a_tag.get_text(strip=True).replace("[at]", "@")
        co_name = _ws(" ".join(name_parts))
        co_addr = _ws(" ".join(addr_parts))
        if co_name:
            result["co_promoter_details"] = {
                "name": co_name,
                **({"email": co_email} if co_email else {}),
                **({"mobile": co_mobile} if co_mobile else {}),
                **({"present_address": co_addr} if co_addr else {}),
                "raw_data": span_html,
            }
        break

    # ── Complaints / litigation ───────────────────────────────────────────────
    # Parse litigation table first; fall back to label check
    lit_rows: list[dict] = []
    for table in soup.find_all("table"):
        hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
        hdr_str = " ".join(hdrs)
        if "Case Title" in hdr_str and "Case Number" in hdr_str:
            for tr in table.find_all("tr")[1:]:
                cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all("td")]
                if len(cells) >= 4 and cells[0].isdigit():
                    lit_rows.append({
                        "case_title": _ws(cells[2]),
                        "case_number": _ws(cells[3]),
                        "count": 1,
                    })
            break
    if lit_rows:
        result["complaints_litigation_details"] = lit_rows
    else:
        # No table rows; check the label — "Nil" means no litigation
        lit_val = _find_by_label(soup, "Litigation(s) related to Project")
        if lit_val is not None:
            # Use count=0 so the normalizer doesn't strip the empty dict
            result["complaints_litigation_details"] = [{"count": 0}]

    # ── Status update (quarterly progress reports) ────────────────────────────
    _QTR_LETTER_MAP: dict[str, str] = {
        "F": "FirstQTR", "S": "SecondQTR", "T": "ThirdQTR", "L": "FourthQTR",
    }
    _MONTH_MAP = {
        "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
        "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
    }

    def _parse_date_iso(date_str: str) -> str | None:
        """Convert DD-Mon-YYYY → YYYY-MM-DD 00:00:00+00:00."""
        m = re.match(r"(\d{1,2})-([A-Za-z]{3})-(\d{4})", date_str.strip())
        if not m:
            return None
        day, mon, year = m.group(1).zfill(2), m.group(2).capitalize(), m.group(3)
        mo = _MONTH_MAP.get(mon)
        return f"{year}-{mo}-{day} 00:00:00+00:00" if mo else None

    status_updates: list[dict] = []
    for table in soup.find_all("table"):
        hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
        hdr_str = " ".join(hdrs)
        if "Quarter Name" not in hdr_str or "Quarter Year" not in hdr_str:
            continue
        for tr in table.find_all("tr")[1:]:
            cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all("td")]
            if len(cells) < 5 or not cells[0].isdigit():
                continue
            quarter_name = _ws(cells[1]) or ""
            year = _ws(cells[2]) or ""
            ref_no = _ws(cells[3]) or ""
            date_str = _ws(cells[4]) or ""
            if not ref_no or len(ref_no) < 13:
                continue
            letter = ref_no[7] if len(ref_no) > 7 else ""
            qtr_code = _QTR_LETTER_MAP.get(letter.upper(), "")
            try:
                qu_id = int(ref_no[12:])
            except ValueError:
                continue
            base_params = (
                f"inProject_ID={project_id}&inPromoter_ID={promoter_id}"
                f"&inPromoterType={project_id}&inQUProject_ID={qu_id}"
                f"&inQUProject_DN={ref_no}&inQUProjectYear={year}&inQUProjectQTR={qtr_code}"
            )
            entry: dict = {
                "year": year,
                "ref_no": ref_no,
                "qpr_url": f"{_BASE_URL}/reraindex/PublicView/ProjectQuarterlyUpdateViewDetails?{base_params}",
                "quarter": quarter_name,
                "gallery_url": f"{_BASE_URL}/reraindex/PublicView/QuarterlyUpdatesGalleryImages?{base_params}",
            }
            iso = _parse_date_iso(date_str)
            if iso:
                entry["date_of_reporting"] = iso
            status_updates.append(entry)
        break
    if status_updates:
        result["status_update"] = status_updates

    # ── Uploaded documents ────────────────────────────────────────────────────
    uploaded: list[dict] = []
    seen_hrefs: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href or href.startswith("#") or href.startswith("javascript") or href.startswith("mailto"):
            continue
        href_fixed = href.replace("\\", "/")
        if not href_fixed.startswith("http"):
            href_fixed = urljoin(_BASE_URL, href_fixed)
        if href_fixed in seen_hrefs:
            continue
        seen_hrefs.add(href_fixed)

        parent_tr = a.find_parent("tr")
        doc_type: str | None = None
        dated_on: str | None = None
        if parent_tr:
            cells = [td.get_text(separator=" ", strip=True) for td in parent_tr.find_all("td")]
            # Structure: [Sr.No, Doc Type, Category, Date, Link]
            if len(cells) >= 2:
                candidate = _ws(cells[1])
                if candidate and not candidate.isdigit() and candidate != "--":
                    doc_type = candidate
            # date may be in cells[3]
            if len(cells) >= 4:
                date_candidate = _ws(cells[3])
                if date_candidate and date_candidate != "--" and not date_candidate.startswith("QU"):
                    dated_on = date_candidate

        # QTR links are captured in status_update; skip them here
        if "QuarterlyUpdates" in href_fixed:
            continue
        if "readwrite" not in href_fixed and "ApprovalDocument" not in href_fixed:
            continue

        doc: dict = {"link": href_fixed}
        if doc_type:
            doc["type"] = doc_type
        if dated_on:
            doc["dated_on"] = dated_on
        uploaded.append(doc)

    if uploaded:
        result["uploaded_documents"] = uploaded

    # ── data extras (project/promo IDs, units) ────────────────────────────────
    result["data"] = {
        "project_id":             project_id,
        "promo_id":               promoter_id,
        "promo_type":             promoter_type,
        "govt_type":              "state",
        "land_area_unit":         "(in sqr mtrs)",
        "construction_area_unit": "(in sqr mtrs)",
    }

    return {k: v for k, v in result.items() if v not in (None, "", [], {})}


def _close_modal(page) -> None:
    try:
        page.click(f"{SEL_MODAL} .close", timeout=3_000)
        page.wait_for_selector(SEL_MODAL_VIS, state="hidden", timeout=5_000)
    except PWTimeout:
        page.evaluate("$('#myModal').modal('hide')")
        time.sleep(0.5)


def _solve_search_captcha(page, logger: CrawlerLogger) -> str | None:
    try:
        page.wait_for_selector(SEL_CAPTCHA_IMG, state="visible", timeout=10_000)
        page.wait_for_function(
            """
            (selector) => {
                const img = document.querySelector(selector);
                return !!(img && img.complete && img.naturalWidth > 0);
            }
            """,
            arg=SEL_CAPTCHA_IMG,
            timeout=10_000,
        )
        image_source = page.evaluate(
            """
            (selector) => {
                const img = document.querySelector(selector);
                if (!img || !img.complete || !img.naturalWidth) return null;
                const canvas = document.createElement('canvas');
                canvas.width = img.naturalWidth;
                canvas.height = img.naturalHeight;
                const ctx = canvas.getContext('2d');
                if (!ctx) return null;
                ctx.drawImage(img, 0, 0);
                return canvas.toDataURL('image/png');
            }
            """,
            SEL_CAPTCHA_IMG,
        )
        if not image_source:
            logger.warning("Captcha image could not be converted to data URL", step="captcha")
            return None
        solved = (captcha_to_text(image_source, default_captcha_source="eprocure") or "").strip()
        if solved:
            logger.info(f"Captcha solved: {solved!r}", step="captcha")
            return solved
        logger.warning("Captcha solver returned empty text", step="captcha")
        return None
    except Exception as exc:
        logger.warning(f"Captcha solver failed: {exc}", step="captcha")
        return None


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Punjab RERA.
    Loads state_projects_sample/punjab.json as the baseline, fetches the sentinel
    project's detail page via httpx, and verifies ≥ 80% field coverage.
    """
    import json as _json
    import os as _os
    from urllib.parse import urlparse as _urlparse, parse_qs as _parse_qs
    from core.sentinel_utils import check_field_coverage

    sentinel_reg = config.get("sentinel_registration_no", "")
    if not sentinel_reg:
        logger.warning("No sentinel_registration_no configured — skipping", step="sentinel")
        return True

    sample_path = _os.path.join(
        _os.path.dirname(_os.path.dirname(__file__)),
        "state_projects_sample", "punjab.json",
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

    # Extract query params needed for _parse_detail_page
    qs = _parse_qs(_urlparse(detail_url).query)
    project_id  = (qs.get("inProject_ID") or [""])[0]
    promoter_id = (qs.get("inPromoter_ID") or [""])[0]
    promoter_type = (qs.get("inPromoterType") or [""])[0]

    logger.info(f"Sentinel: fetching detail for {sentinel_reg}", url=detail_url, step="sentinel")
    try:
        resp = safe_get(detail_url, retries=2, logger=logger)
        if not resp:
            logger.error("Sentinel: failed to fetch detail page", url=detail_url, step="sentinel")
            return False
        fresh = _parse_detail_page(resp.text, project_id, promoter_id, promoter_type) or {}
    except Exception as exc:
        logger.error(f"Sentinel: scrape error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", url=detail_url, step="sentinel")
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        from core.db import insert_crawl_error as _ice
        _ice(
            run_id, config.get("id", "punjab_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


def _solve_listing_captcha(session: httpx.Client, logger: CrawlerLogger) -> tuple[str | None, BeautifulSoup | None]:
    try:
        t_page = time.monotonic()
        resp = session.get(LISTING_URL)
        resp.raise_for_status()
        page_elapsed = time.monotonic() - t_page
        soup = BeautifulSoup(resp.text, "lxml")
        captcha_img = soup.select_one(SEL_CAPTCHA_IMG)
        if not captcha_img or not captcha_img.get("src"):
            logger.warning("Captcha image not found on listing page", step="captcha")
            return None, soup
        img_url = urljoin(LISTING_URL, captcha_img["src"])
        t_img = time.monotonic()
        img_resp = session.get(img_url)
        img_resp.raise_for_status()
        img_elapsed = time.monotonic() - t_img
        data_url = "data:image/png;base64," + base64.b64encode(img_resp.content).decode()
        t_solve = time.monotonic()
        solved = (captcha_to_text(data_url, default_captcha_source="eprocure") or "").strip()
        solve_elapsed = time.monotonic() - t_solve
        logger.info(
            f"Captcha timing: page={page_elapsed:.2f}s  img={img_elapsed:.2f}s  solver={solve_elapsed:.2f}s  total={page_elapsed+img_elapsed+solve_elapsed:.2f}s",
            step="captcha",
        )
        if solved:
            logger.info(f"Captcha solved: {solved!r}", step="captcha")
            return solved, soup
        logger.warning("Captcha solver returned empty text", step="captcha")
        return None, soup
    except Exception as exc:
        logger.warning(f"Captcha solve failed: {exc}", step="captcha")
        return None, None


def _search_projects(session: httpx.Client, logger: CrawlerLogger) -> list[dict]:
    for attempt in range(1, 21):
        captcha_text, soup = _solve_listing_captcha(session, logger)
        if not captcha_text or soup is None:
            continue

        token_input = soup.find("input", {"name": "__RequestVerificationToken"})
        payload = {
            "__RequestVerificationToken": token_input.get("value", "") if token_input else "",
            "Input_SearchOptionTabFlag": "1",
            "Input_AdvSearch_MoreOptionsFlag": "0",
            "Input_RegdProject_DistrictName": "",
            "Input_RegdProject_ProjectName": "",
            "Input_RegdProject_PromoterName": "",
            "Input_RegdProject_RERAnumberRegistration": "",
            "Input_RegdProject_CaptchaText": captcha_text,
        }
        try:
            resp = session.post(
                SEARCH_URL,
                data=payload,
                headers={
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": LISTING_URL,
                },
            )
            resp.raise_for_status()
        except Exception as exc:
            logger.warning(f"Search POST failed: {exc}", step="listing")
            continue

        if "Invalid Capcha Text" in resp.text:
            logger.warning(f"Captcha rejected on attempt {attempt}", step="captcha")
            continue

        rows = _parse_partial_rows(resp.text)
        if rows:
            logger.info(f"Search returned {len(rows)} rows", step="listing")
            return rows

    logger.error("Sentinel: no project rows appeared after search", step="sentinel")
    return []


def _parse_partial_rows(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict] = []
    for tr in soup.select(SEL_ROWS):
        cells = tr.find_all("td")
        if len(cells) < 6:
            continue
        hidden = {inp.get("class", [""])[0]: inp.get("value", "") for inp in tr.find_all("input")}
        row = {
            "district": cells[1].get_text(" ", strip=True),
            "project_name": cells[2].get_text(" ", strip=True),
            "promoter_name": cells[3].get_text(" ", strip=True),
            "project_registration_no": cells[4].get_text(" ", strip=True),
            "valid_upto": cells[5].get_text(" ", strip=True),
            "project_id": hidden.get("hdnProjectID", ""),
            "promoter_id": hidden.get("hdnPromoterID", ""),
            "promoter_type": hidden.get("hdnPromoterType", ""),
        }
        if row["project_registration_no"]:
            rows.append(row)
    return rows


def _fetch_detail_fields(session: httpx.Client, row: dict, logger: CrawlerLogger) -> dict:
    if not row.get("project_id"):
        return {}
    try:
        resp = session.get(
            DETAIL_URL,
            params={
                "inProject_ID": row.get("project_id"),
                "inPromoter_ID": row.get("promoter_id"),
                "inPromoterType": row.get("promoter_type"),
            },
            headers={"Referer": LISTING_URL},
        )
        resp.raise_for_status()
        return _parse_detail_page(
            resp.text,
            project_id=row.get("project_id", ""),
            promoter_id=row.get("promoter_id", ""),
            promoter_type=row.get("promoter_type", ""),
            district=row.get("district"),
        )
    except Exception as exc:
        logger.warning(f"Detail fetch failed: {exc}", step="detail")
        return {}


# ── Listing cache ────────────────────────────────────────────────────────────

_LISTING_CACHE_TTL = 4 * 3600  # reuse fetched rows for up to 4 hours


def _listing_cache_path() -> Path:
    return Path(settings.LOG_DIR) / "punjab_rera_listing_cache.json"


def _load_listing_cache(logger: CrawlerLogger) -> list[dict] | None:
    """Return cached listing rows if the file exists and is within TTL."""
    path = _listing_cache_path()
    try:
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > _LISTING_CACHE_TTL:
            logger.info(
                f"Listing cache expired ({age/3600:.1f}h old, TTL={_LISTING_CACHE_TTL/3600:.0f}h) — will re-fetch",
                step="listing",
            )
            return None
        rows = json.loads(path.read_text())
        logger.warning(
            f"Listing cache HIT: {len(rows)} rows ({age:.0f}s old) — skipping search fetch",
            step="listing",
        )
        return rows
    except Exception as exc:
        logger.warning(f"Listing cache read failed: {exc}", step="listing")
        return None


def _save_listing_cache(rows: list[dict], logger: CrawlerLogger) -> None:
    """Persist listing rows to disk so a restarted run can skip the fetch."""
    path = _listing_cache_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(rows))
        logger.info(f"Listing cache saved: {len(rows)} rows → {path}", step="listing")
    except Exception as exc:
        logger.warning(f"Listing cache write failed: {exc}", step="listing")


def _clear_listing_cache() -> None:
    """Remove the cache file after a successful run."""
    try:
        _listing_cache_path().unlink(missing_ok=True)
    except Exception:
        pass


# ── Document download + S3 upload ────────────────────────────────────────────

_DOC_CONNECT_TIMEOUT = 10.0   # seconds to establish TCP connection
_DOC_READ_TIMEOUT    = 30.0   # seconds between data chunks
_DOC_TOTAL_TIMEOUT   = 60.0   # hard cap: total download time in seconds
_DOC_MAX_BYTES       = 50 * 1024 * 1024  # 50 MB safety limit
_MAX_DOC_WORKERS     = 6      # parallel document download threads


def _handle_document(
    project_key: str,
    doc: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
    client: httpx.Client,
) -> dict | None:
    """Download one document, upload to S3, persist to DB. Returns enriched entry or None."""
    url = doc.get("link")
    label = doc.get("type") or "document"
    if not url:
        return None
    fname = build_document_filename({"url": url, "label": label})
    try:
        logger.info(f"Downloading document: {label}", url=url, step="documents")
        doc_timeout = httpx.Timeout(
            connect=_DOC_CONNECT_TIMEOUT,
            read=_DOC_READ_TIMEOUT,
            write=_DOC_READ_TIMEOUT,
            pool=10.0,
        )
        chunks: list[bytes] = []
        total_bytes = 0
        deadline_hit = threading.Event()

        def _abort_on_deadline(stream):
            deadline_hit.set()
            try:
                stream.close()
            except Exception:
                pass

        t_dl = time.monotonic()
        with client.stream(
            "GET", url,
            headers={"Referer": LISTING_URL},
            timeout=doc_timeout,
            follow_redirects=True,
        ) as stream:
            timer = threading.Timer(_DOC_TOTAL_TIMEOUT, _abort_on_deadline, args=(stream,))
            timer.start()
            try:
                for chunk in stream.iter_bytes(chunk_size=65536):
                    if deadline_hit.is_set():
                        raise TimeoutError(
                            f"Document download exceeded {_DOC_TOTAL_TIMEOUT}s total limit"
                        )
                    chunks.append(chunk)
                    total_bytes += len(chunk)
                    if total_bytes > _DOC_MAX_BYTES:
                        raise ValueError(
                            f"Document too large (>{_DOC_MAX_BYTES // (1024*1024)} MB), skipping"
                        )
            finally:
                timer.cancel()
        dl_elapsed = time.monotonic() - t_dl

        data = b"".join(chunks)
        if len(data) < 100:
            logger.warning("Document download empty or failed", url=url, step="documents")
            return None
        md5 = compute_md5(data)
        t_s3 = time.monotonic()
        s3_key = upload_document(project_key, fname, data, dry_run=settings.DRY_RUN_S3)
        s3_elapsed = time.monotonic() - t_s3
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
            file_size_bytes=len(data),
        )
        logger.warning(
            f"Doc timing [{label}]: download={dl_elapsed:.2f}s  s3={s3_elapsed:.2f}s"
            f"  total={dl_elapsed+s3_elapsed:.2f}s  size={len(data)//1024}KB",
            step="documents",
        )
        logger.info("Document uploaded", label=label, s3_key=s3_key, step="documents")
        logger.log_document(label, url, "uploaded", s3_key=s3_key, file_size_bytes=len(data))
        return document_result_entry({**doc, "url": url}, s3_url, fname)
    except Exception as exc:
        logger.warning(f"Document handling error: {exc}", url=url, step="documents")
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(exc),
                           project_key=project_key, url=url)
        return None


# ── Main entry point ──────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    logger = CrawlerLogger(config["id"], run_id)
    counters = dict(projects_found=0, projects_new=0, projects_updated=0,
                    projects_skipped=0, documents_uploaded=0, error_count=0)
    machine_name, machine_ip = get_machine_context()
    delay_range = config.get("rate_limit_delay", (2, 4))
    item_limit = settings.CRAWL_ITEM_LIMIT or 0
    t_run = time.monotonic()

    # ── Sentinel health check ────────────────────────────────────────────────
    t0 = time.monotonic()
    sentinel_ok = _sentinel_check(config, run_id, logger)
    logger.warning(f"Step timing [sentinel]: {time.monotonic()-t0:.2f}s", step="timing")
    if not sentinel_ok:
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counters["error_count"] += 1
        return counters

    site_id = config["id"]

    # ── Listing: try cache first, fall back to live fetch ────────────────────
    with httpx.Client(timeout=60.0, follow_redirects=True) as session:
        rows = _load_listing_cache(logger)
        if rows is None:
            t0 = time.monotonic()
            rows = _search_projects(session, logger)
            logger.warning(
                f"Step timing [search]: {time.monotonic()-t0:.2f}s  rows={len(rows)}",
                step="timing",
            )
            if not rows:
                return counters
            _save_listing_cache(rows, logger)
        else:
            logger.warning("Step timing [search]: 0.00s  (cache hit)", step="timing")

        # ── Resume: skip rows already processed in a previous (interrupted) run
        checkpoint = load_checkpoint(site_id, mode) or {}
        resume_after = checkpoint.get("last_project_key")
        if resume_after:
            original_count = len(rows)
            # Drop every row up to and including the last-completed key
            for i, row in enumerate(rows):
                if generate_project_key(row["project_registration_no"]) == resume_after:
                    rows = rows[i + 1:]
                    break
            logger.warning(
                f"Resuming: skipped {original_count - len(rows)} already-processed rows",
                step="checkpoint",
            )

        if item_limit:
            rows = rows[:item_limit]
            logger.info(
                f"Punjab: CRAWL_ITEM_LIMIT={item_limit} applied — processing {len(rows)} projects",
                step="listing",
            )
        counters["projects_found"] = len(rows)

        for idx, row in enumerate(rows):
            reg_no = row["project_registration_no"]
            key = generate_project_key(reg_no)

            # ── daily_light: skip projects already in the DB ──────────────────
            if mode == "daily_light" and get_project_by_key(key):
                counters["projects_skipped"] += 1
                continue

            logger.set_project(key=key, reg_no=reg_no, url=LISTING_URL, page=idx)
            try:
                try:
                    t0 = time.monotonic()
                    detail_fields = _fetch_detail_fields(session, row, logger)
                    logger.warning(
                        f"Step timing [detail_fetch]: {time.monotonic()-t0:.2f}s",
                        step="timing",
                    )
                    # Merge project_location_raw: detail page provides plot_no/address,
                    # listing row provides district as fallback.
                    loc_raw = detail_fields.pop("project_location_raw", {})
                    if not loc_raw.get("district") and row.get("district"):
                        loc_raw["district"] = row["district"]

                    detail_url = (
                        f"{DETAIL_URL}?inProject_ID={row.get('project_id')}"
                        f"&inPromoter_ID={row.get('promoter_id')}"
                        f"&inPromoterType={row.get('promoter_type')}"
                    )
                    payload: dict = {
                        "project_name": row.get("project_name"),
                        "promoter_name": row.get("promoter_name"),
                        "project_registration_no": reg_no,
                        **detail_fields,
                        "project_location_raw": loc_raw,
                        "domain": DOMAIN,
                        "url": detail_url,
                        "state": config.get("state", "Punjab"),
                        "is_live": True,
                    }

                    t0 = time.monotonic()
                    normalized = normalize_project_payload(
                        payload, config,
                        machine_name=machine_name,
                        machine_ip=machine_ip,
                    )
                    record = ProjectRecord(**normalized)
                    db_dict = record.to_db_dict()
                    logger.warning(
                        f"Step timing [normalize]: {time.monotonic()-t0:.2f}s",
                        step="timing",
                    )

                    t0 = time.monotonic()
                    status = upsert_project(db_dict)
                    logger.warning(
                        f"Step timing [db_upsert]: {time.monotonic()-t0:.2f}s  status={status}",
                        step="timing",
                    )

                    if status == "new":
                        counters["projects_new"] += 1
                    elif status == "updated":
                        counters["projects_updated"] += 1
                    else:
                        counters["projects_skipped"] += 1
                    logger.info(f"DB result: {status}", step="db_upsert")

                    # ── Document download + S3 upload ─────────────────────────
                    raw_docs: list[dict] = payload.get("uploaded_documents") or []
                    if raw_docs:
                        # Phase 1 (sequential): resolve names; build selected / skipped lists.
                        # select_document_for_download mutates doc_name_counts for dedup, so
                        # this must stay sequential to guarantee stable filenames.
                        t0 = time.monotonic()
                        doc_name_counts: dict[str, int] = {}
                        selected_pairs: list[tuple[dict, dict]] = []  # (original, selected)
                        skipped_entries: list[dict] = []
                        for doc in raw_docs:
                            selected = select_document_for_download(
                                config["state"], doc, doc_name_counts, domain=DOMAIN,
                            )
                            if selected:
                                selected_pairs.append((doc, selected))
                            else:
                                skipped_entries.append({
                                    "link": doc.get("link"),
                                    "type": doc.get("type", "document"),
                                })
                        logger.warning(
                            f"Step timing [doc_selection]: {time.monotonic()-t0:.2f}s"
                            f"  total={len(raw_docs)}  selected={len(selected_pairs)}"
                            f"  skipped={len(skipped_entries)}",
                            step="timing",
                        )

                        # Phase 2 (parallel): download + upload selected docs concurrently.
                        t0 = time.monotonic()
                        dl_results: list[dict | None] = [None] * len(selected_pairs)
                        with ThreadPoolExecutor(max_workers=_MAX_DOC_WORKERS) as pool:
                            future_to_idx = {
                                pool.submit(
                                    _handle_document,
                                    key, sel, run_id, config["id"], logger, session,
                                ): i
                                for i, (_orig, sel) in enumerate(selected_pairs)
                            }
                            for future in as_completed(future_to_idx):
                                i = future_to_idx[future]
                                try:
                                    dl_results[i] = future.result()
                                except Exception as exc:
                                    logger.warning(
                                        f"Document parallel error: {exc}", step="documents"
                                    )
                        logger.warning(
                            f"Step timing [doc_downloads]: {time.monotonic()-t0:.2f}s"
                            f"  workers={_MAX_DOC_WORKERS}  docs={len(selected_pairs)}",
                            step="timing",
                        )

                        # Phase 3: assemble final list in original order.
                        uploaded_documents: list[dict] = []
                        for (orig, _sel), result in zip(selected_pairs, dl_results):
                            if result:
                                uploaded_documents.append(result)
                                counters["documents_uploaded"] += 1
                            else:
                                uploaded_documents.append({
                                    "link": orig.get("link"),
                                    "type": orig.get("type", "document"),
                                })
                        uploaded_documents.extend(skipped_entries)

                        if uploaded_documents:
                            t0 = time.monotonic()
                            upsert_project({
                                "key": key,
                                "url": detail_url,
                                "state": db_dict["state"],
                                "domain": db_dict["domain"],
                                "project_registration_no": reg_no,
                                "uploaded_documents": uploaded_documents,
                                "document_urls": build_document_urls(uploaded_documents),
                            })
                            logger.warning(
                                f"Step timing [doc_db_upsert]: {time.monotonic()-t0:.2f}s",
                                step="timing",
                            )

                except ValidationError as exc:
                    counters["error_count"] += 1
                    logger.error(str(exc), step="validate")
                    insert_crawl_error(
                        run_id, config["id"], "VALIDATION_FAILED", str(exc),
                        project_key=key, url=LISTING_URL,
                    )
                except Exception as exc:
                    counters["error_count"] += 1
                    logger.error(str(exc), step="upsert")
                    insert_crawl_error(
                        run_id, config["id"], "CRAWLER_EXCEPTION", str(exc),
                        project_key=key, url=LISTING_URL,
                    )
            finally:
                # Checkpoint after every project so a restart can resume mid-list
                save_checkpoint(site_id, mode, 0, key, run_id)
                logger.clear_project()

            if idx > 0 and idx % 10 == 0:
                random_delay(*delay_range)

    # Clean up cache + checkpoint on successful completion
    _clear_listing_cache()
    reset_checkpoint(site_id, mode)
    logger.warning(
        f"Step timing [total_run]: {time.monotonic()-t_run:.2f}s",
        step="timing",
    )
    logger.info(f"Punjab RERA complete: {counters}", step="done")
    return counters
