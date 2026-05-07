"""
Madhya Pradesh RERA Crawler — rera.mp.gov.in
Type: static (httpx + BeautifulSoup — PHP site with AJAX listing)

Strategy:
- One GET to /project-all-loop.php?show=20&pagenum=1 returns ALL ~8,255 projects in
  a single HTML <table id="example"> (DataTables handles client-side pagination).
  The registration number is NOT present in the listing, so every project requires
  a detail-page fetch regardless of mode.
- Detail page: /view_project_details.php?id=<base64_id>
  Contains: project info, location, bank details, promoter info, consultants,
  unit inventory, project documents, quarterly progress reports (QPR), and photo gallery.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from bs4 import BeautifulSoup, Tag

from core.checkpoint import load_checkpoint, reset_checkpoint, save_checkpoint
from core.crawler_base import generate_project_key, random_delay, safe_get
from core.db import get_project_by_key, insert_crawl_error, upsert_document, upsert_project
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
from core.s3 import compute_md5, get_s3_url, upload_document
from core.config import settings
from pydantic import ValidationError

LISTING_AJAX_URL = "https://www.rera.mp.gov.in/project-all-loop.php"
DETAIL_BASE      = "https://www.rera.mp.gov.in/view_project_details.php"
BASE_URL         = "https://www.rera.mp.gov.in"
DOMAIN           = "rera.mp.gov.in"

_DATE_FMT  = "%d-%m-%Y"    # portal format: "25-07-2024"
_DATE_FMT2 = "%d %b %Y"    # e.g. "25 Jul 2024"


# ── helpers ───────────────────────────────────────────────────────────────────

def _get(url: str, logger: CrawlerLogger | None = None, **kw):
    return safe_get(url, verify=False, logger=logger, timeout=60.0, **kw)


def _clean(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def _parse_date(raw: str) -> str | None:
    raw = _clean(raw)
    if not raw:
        return None
    for fmt in (_DATE_FMT, _DATE_FMT2):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return str(dt)
        except ValueError:
            continue
    return None


def _row_val(soup: BeautifulSoup, label_text: str) -> str:
    """
    Find a <b>label_text</b> inside col-md-N and return the *immediately following*
    sibling value column's text.

    MP rows fall into two layouts:
      • Single label + single value  (e.g. "State :")
      • Two label+value pairs on one row (e.g. "District :" + "Tehsil :")

    For the two-pair case we must return the value that belongs to *this* label,
    not the last non-bold cell in the row (which could belong to a different label).
    We do this by finding the label's own column, then walking forward to the next
    sibling column that has no <b> child.
    """
    for b_tag in soup.find_all("b"):
        if _clean(b_tag.get_text()) == label_text:
            label_col = b_tag.find_parent("div", class_=re.compile(r"col-(?:md|sm)-\d"))
            if label_col is None:
                continue
            parent_row = label_col.find_parent("div", class_="row")
            if parent_row is None:
                continue
            cols = parent_row.find_all("div", class_=re.compile(r"col-(?:md|sm)-\d"), recursive=False)
            # Walk forward from the label column to find the immediately next value column
            found_label = False
            for col in cols:
                if col is label_col:
                    found_label = True
                    continue
                if found_label and not col.find("b"):
                    return _clean(col.get_text())
            break
    return ""


# ── Listing ────────────────────────────────────────────────────────────────────

def _fetch_listing(logger: CrawlerLogger | None) -> list[dict]:
    """
    Fetch the AJAX listing endpoint with default params.  The server ignores
    show/pagenum and returns all projects in one HTML fragment.
    """
    params = {
        "show": "20",
        "pagenum": "1",
        "search_txt": "",
        "search_dist": "",
        "search_tehs": "",
        "project_type_id": "",
    }
    headers = {
        "Referer": "https://www.rera.mp.gov.in/all-projects/",
        "X-Requested-With": "XMLHttpRequest",
    }
    resp = _get(LISTING_AJAX_URL, logger, params=params, headers=headers)
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table", id="example")
    if not table:
        return []

    stubs: list[dict] = []
    tbody = table.find("tbody")
    if not tbody:
        return []
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue
        link_tag = tds[5].find("a", href=True)
        if not link_tag:
            continue
        detail_url = link_tag["href"].strip()
        if not detail_url.startswith("http"):
            detail_url = BASE_URL + "/" + detail_url.lstrip("/")
        district_area = _clean(tds[3].get_text())
        district, area = ("", "")
        if " - " in district_area:
            parts = district_area.split(" - ", 1)
            district, area = parts[0].strip(), parts[1].strip()
        else:
            district = district_area
        stubs.append({
            "project_name":  _clean(tds[1].get_text()),
            "promoter_name": _clean(tds[2].get_text()),
            "district":      district,
            "area":          area,
            "status":        _clean(tds[4].get_text()),
            "detail_url":    detail_url,
        })
    return stubs


# ── Detail page parser ─────────────────────────────────────────────────────────

def _parse_detail(detail_url: str, stub: dict, logger: CrawlerLogger | None) -> dict:
    resp = _get(detail_url, logger)
    if not resp:
        return {}
    soup = BeautifulSoup(resp.text, "lxml")
    out: dict = {}

    # ── Project Information ───────────────────────────────────────────────────
    out["project_name"] = _row_val(soup, "Project Name :")
    out["project_type"] = _row_val(soup, "Project Type :")

    # Registration number: text directly next to the <b> tag, before the PDF links
    for b_tag in soup.find_all("b"):
        if _clean(b_tag.get_text()) == "Registration Number :":
            row = b_tag.find_parent("div", class_="row")
            if row:
                val_div = row.find("div", class_=re.compile(r"col-md-8"))
                if val_div:
                    # Registration number is the direct text (before anchors)
                    reg_text = _clean(
                        "".join(
                            t for t in val_div.strings
                            if t.strip() and not t.strip().startswith("\u00a0")
                        ).split()[0] if list(val_div.strings) else ""
                    )
                    # Also capture cert/order PDF links
                    links = val_div.find_all("a", href=True)
                    cert_links = [a["href"] for a in links if "title" in a.attrs]
                    out["_rera_cert_links"] = cert_links
                    if reg_text:
                        out["project_registration_no"] = reg_text
            break

    # Fallback reg_no from page text
    if not out.get("project_registration_no"):
        m = re.search(r"P-[A-Z]{2,5}-\d{2}-\d+", resp.text)
        if m:
            out["project_registration_no"] = m.group(0)

    # Status / approved date / contact email
    status_div = soup.find("a", class_="btn-success", attrs={"title": "Project Status"})
    if status_div:
        out["status_of_the_project"] = _clean(status_div.get_text())
    out["approved_on_date"]          = _parse_date(_row_val(soup, "Approved Date :"))
    out["estimated_commencement_date"] = _parse_date(_row_val(soup, "Proposed Start Date :"))
    out["estimated_finish_date"]     = _parse_date(_row_val(soup, "Proposed End Date :"))

    # Construction and land costs (in lacs → rupees)
    cost_dict: dict = {}
    cost_str = _row_val(soup, "Estimated Cost of Construction(in lacs) :")
    if cost_str:
        try:
            cost_dict["estimated_construction_cost"] = float(cost_str) * 100_000
        except ValueError:
            pass
    land_cost_str = _row_val(soup, "Estimated Cost of Land(in lacs) :")
    if land_cost_str:
        try:
            cost_dict["cost_of_land"] = float(land_cost_str) * 100_000
        except ValueError:
            pass
    if cost_dict:
        out["project_cost_detail"] = cost_dict

    # ── Project Location ───────────────────────────────────────────────────────
    state_val    = _row_val(soup, "State :")
    # Prefer value from the detail-page location box; fall back to listing stub
    district_val = ""
    tehsil_val   = ""
    address_val  = ""
    planning_val = ""

    # Location section uses col-md-3 label / col-md-3 or col-md-9 value.
    # District and Tehsil share one row — the fixed _row_val handles that correctly.
    loc_section = soup.find("div", class_="h3", string=re.compile(r"Project Location"))
    if loc_section:
        loc_box = loc_section.find_next("div", class_="box")
        if loc_box:
            rows_in_box = loc_box.find_all("div", class_="row")
            for row in rows_in_box:
                labels = [_clean(b.get_text()) for b in row.find_all("b")]
                cols   = row.find_all("div", recursive=False)
                # extract text from non-bold value columns in row order
                texts  = [_clean(c.get_text()) for c in cols if not c.find("b")]
                for i, lbl in enumerate(labels):
                    v = texts[i] if i < len(texts) else ""
                    if "District" in lbl:
                        # Location box is authoritative; fall back to stub only if empty
                        district_val = v or district_val
                    elif "Tehsil" in lbl:
                        tehsil_val = v
                    elif "Project Address" in lbl:
                        address_val = v
                    elif "Planning Area" in lbl:
                        planning_val = v

    # Fall back to listing stub if the location box had no district
    if not district_val:
        district_val = stub.get("district") or ""

    out["project_state"] = state_val or "Madhya Pradesh"
    loc: dict = {"state": out["project_state"]}
    if district_val:
        loc["district"] = district_val
    if tehsil_val:
        loc["taluk"] = tehsil_val
    if address_val:
        loc["raw_address"] = address_val
    if planning_val:
        loc["city"] = planning_val
        out["project_city"] = planning_val
    out["project_location_raw"] = loc

    # ── Bank Details ───────────────────────────────────────────────────────────
    bank: dict = {
        "account_no":   _row_val(soup, "Account Number :"),
        "bank_name":    _row_val(soup, "Bank Name :"),
        "branch":       _row_val(soup, "Branch Name :"),
        "account_name": _row_val(soup, "Account Name :"),
        "IFSC":         _row_val(soup, "IFSC Code :"),
    }
    if any(bank.values()):
        out["bank_details"] = {k: v for k, v in bank.items() if v}

    return out


def _parse_promoter(soup: BeautifulSoup) -> dict:
    """Parse Promoter Information and Co-Promoter sections."""
    out: dict = {}
    promo_section = soup.find("div", class_="h3", string=re.compile(r"Promoter Information"))
    if not promo_section:
        return out

    box = promo_section.find_next("div", class_=re.compile(r"col-md-12 box"))
    if box:
        name      = _row_val(box, "Name :")
        firm_type = _row_val(box, "Applicant Type :")
        address   = _row_val(box, "Address :")
        email     = _row_val(box, "Email :")
        phone     = _row_val(box, "Phone :")
        if name:
            out["promoter_name"] = name
        if address:
            out["promoter_address_raw"] = {"raw_address": address}
        contact = {k: v for k, v in {"email": email, "phone": phone}.items() if v}
        if contact:
            out["promoter_contact_details"] = contact
        out["promoters_details"] = {
            "name":           name,
            "type_of_firm":   firm_type,
            "promoters_details": [],
        }

    # Co-Promoter details
    co_promos: list[dict] = []
    for co_div in soup.find_all("div", class_="col-sm-9 box"):
        rows = co_div.find_all("div", class_="row")
        entry: dict = {}
        for row in rows:
            lbls = [_clean(b.get_text()) for b in row.find_all("b")]
            cols = [_clean(c.get_text()) for c in row.find_all("div", recursive=False)
                    if not c.find("b")]
            for i, lbl in enumerate(lbls):
                v = cols[i] if i < len(cols) else ""
                if "Name :" in lbl and "Father" not in lbl:
                    entry["name"] = v
                elif "Promoter Type" in lbl:
                    entry["promoter_type"] = v
                elif "Land Owner" in lbl:
                    entry["role"] = v
                elif "Email" in lbl:
                    entry["email"] = v
                elif "Address" in lbl:
                    entry["present_address"] = v
        if entry.get("name"):
            co_promos.append(entry)

    if co_promos and "promoters_details" in out:
        out["promoters_details"]["promoters_details"] = co_promos
    return out


def _parse_consultants(soup: BeautifulSoup) -> list[dict]:
    """Parse Consultants of Project table → professional_information."""
    section = soup.find("div", class_="h3", string=re.compile(r"Consultant"))
    if not section:
        return []
    tbl = section.find_next("table")
    if not tbl:
        return []
    rows = tbl.find_all("tr")
    if len(rows) < 2:
        return []
    result: list[dict] = []
    for tr in rows[1:]:
        tds = tr.find_all("td")
        if len(tds) >= 3:
            entry = {"name": _clean(tds[1].get_text()), "role": "Consultant",
                     "email": _clean(tds[2].get_text())}
            if entry["name"]:
                result.append(entry)
    return result


def _parse_building_details(soup: BeautifulSoup) -> list[dict]:
    """Parse Apartment/Plot inventory table → building_details."""
    section = soup.find("div", class_="h3", string=re.compile(r"Apartment"))
    if not section:
        return []
    tbl = section.find_next("table")
    if not tbl:
        return []
    details: list[dict] = []
    for tr in tbl.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        # header colspan rows (category headers like "Apartment", "Commercial Property")
        if len(tds) == 1:
            continue
        if len(tds) >= 6:
            flat_name   = _clean(tds[1].get_text())
            flat_type   = _clean(tds[3].get_text())
            carpet_area = _clean(tds[4].get_text())
            no_of_units = _clean(tds[5].get_text())
            entry: dict = {
                "updated":    True,
                "flat_name":  flat_name  or None,
                "flat_type":  flat_type  or None,
                "carpet_area": carpet_area or None,
                "no_of_units": no_of_units or None,
            }
            details.append(entry)
    return details


def _parse_unit_counts(soup: BeautifulSoup) -> tuple[int, int]:
    """
    Count total residential and commercial units from the apartment table.
    Uses the 'Type of Use' column (tds[2]): 'Residential' vs 'Non Residential'.
    Returns (number_of_residential_units, number_of_commercial_units).
    """
    section = soup.find("div", class_="h3", string=re.compile(r"Apartment"))
    if not section:
        return 0, 0
    tbl = section.find_next("table")
    if not tbl:
        return 0, 0
    residential = 0
    commercial = 0
    for tr in tbl.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue
        use_type = _clean(tds[2].get_text()).lower()
        try:
            units = int(_clean(tds[5].get_text()) or "0")
        except ValueError:
            continue
        if "residential" in use_type and "non" not in use_type:
            residential += units
        else:
            commercial += units
    return residential, commercial


def _parse_documents(soup: BeautifulSoup, rera_cert_links: list[str]) -> list[dict]:
    """
    Collect all document links from the detail page:
    1. Registration cert and order from the reg-number row.
    2. Project Documents table (numbered S.No. + Title rows; may have multiple links per row).
    """
    docs: list[dict] = []
    seen: set[str] = set()
    name_counters: dict[str, int] = {}

    def _add(href: str, doc_type: str) -> None:
        url = href.strip()
        if not url or url.lower().startswith("javascript"):
            return
        if not url.startswith("http"):
            url = BASE_URL + "/" + url.lstrip("/")
        key = (url, doc_type)
        if key in seen:
            return
        seen.add(key)
        docs.append({"link": url, "type": doc_type, "updated": True})

    # 1. Project Documents table
    doc_section = soup.find("div", class_="h3", string=re.compile(r"Project Documents"))
    if doc_section:
        tbl = doc_section.find_next("table", class_="doc-table")
        if tbl:
            rows = tbl.find_all("tr")
            for tr in rows[1:]:
                tds = tr.find_all("td")
                if len(tds) < 4:
                    continue
                sno       = _clean(tds[0].get_text())
                title     = _clean(tds[1].get_text())
                view_cell = tds[3]
                links_in_row = view_cell.find_all("a", href=True)
                if not links_in_row:
                    continue
                if len(links_in_row) == 1:
                    # Single link: type = "{sno} {title}" (matching sample numbering)
                    doc_type = f"{sno} {title}" if sno else title
                    _add(links_in_row[0]["href"], doc_type)
                else:
                    # Multiple links: type = "{title} {counter}"
                    base = title
                    for a in links_in_row:
                        name_counters[base] = name_counters.get(base, 0) + 1
                        doc_type = f"{base} {name_counters[base]}"
                        _add(a["href"], doc_type)

    # 2. RERA registration certificate and order — appended last to match expected ordering
    if rera_cert_links:
        cert_urls = ",".join(rera_cert_links)
        if cert_urls not in seen:
            seen.add(cert_urls)
            docs.append({"link": cert_urls, "type": "Rera Registration Certificate", "updated": True})
            docs.append({"link": cert_urls, "type": "Rera Order", "updated": True})
    return docs


def _parse_qpr(soup: BeautifulSoup) -> list[dict]:
    """
    Parse Quarterly Details table → list of QPR doc entries for status_update.qpr_docs.
    Columns: Quarter | Engineer Certificate | CA Certificate | Bank Statement
    """
    section = soup.find("div", class_="h3", string=re.compile(r"Quarterly Details"))
    if not section:
        return []
    tbl = section.find_next("table")
    if not tbl:
        return []
    qpr_docs: list[dict] = []
    col_map = {
        "Engineer Certificate":         "Engineer's Certificate",
        "CA Certificate":               "chartered accountant certificate",
        "Bank Statement":               "Bank Statement",
    }
    rows = tbl.find_all("tr")
    if len(rows) < 2:
        return []
    for tr in rows[1:]:
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        quarter = _clean(tds[1].get_text())
        for col_idx, (src_label, dest_type) in enumerate(col_map.items(), start=2):
            if col_idx >= len(tds):
                break
            a = tds[col_idx].find("a", href=True)
            if a:
                qpr_docs.append({
                    "link":    a["href"].strip(),
                    "type":    dest_type,
                    "quarter": quarter,
                })
    return qpr_docs


def _parse_images(soup: BeautifulSoup) -> list[str]:
    """Collect JPEG project images from the Photo Gallery flexslider."""
    images: list[str] = []
    seen: set[str] = set()
    gallery = soup.find("div", id="windowGallery")
    if not gallery:
        return []
    for a in gallery.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith((".jpeg", ".jpg", ".png")) and href not in seen:
            seen.add(href)
            images.append(href)
    return images


# ── Document upload ────────────────────────────────────────────────────────────

def _handle_document(
    project_key: str, doc: dict, run_id: int, site_id: str, logger: CrawlerLogger
) -> dict | None:
    """Download one document, upload to S3, return enriched entry or None on failure."""
    # A doc link may be comma-separated (RERA cert + order share same pair of PDFs).
    # Upload each URL individually; return the first successful entry.
    raw_urls = [u.strip() for u in doc.get("link", "").split(",") if u.strip()]
    label    = doc.get("type", "document")
    result   = None
    for url in raw_urls:
        fname = build_document_filename({"url": url, "label": label})
        try:
            resp = _get(url, logger)
            if not resp or len(resp.content) < 100:
                continue
            md5    = compute_md5(resp.content)
            s3_key = upload_document(project_key, fname, resp.content, dry_run=settings.DRY_RUN_S3)
            if s3_key is None:
                continue
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
            result = document_result_entry({"url": url, "label": label}, s3_url, fname)
            break  # one successful upload per doc entry is enough
        except Exception as exc:
            logger.error(f"Doc upload failed for {project_key}: {exc}")
            insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(exc),
                               project_key=project_key, url=url)
    return result


# ── Sentinel check ─────────────────────────────────────────────────────────────

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Madhya Pradesh RERA.
    Full-flow check: fetches the detail page once and runs ALL section parsers
    (_parse_detail, _parse_promoter, _parse_consultants, _parse_unit_counts),
    merges the results (same as run()), and verifies ≥ 80% field coverage against
    the full baseline.
    """
    import json as _json
    import os as _os
    from core.sentinel_utils import check_field_coverage

    sentinel_url    = config.get("sentinel_detail_url") or DETAIL_BASE
    sentinel_reg_no = config.get("sentinel_registration_no", "")
    if not sentinel_reg_no:
        logger.info("No sentinel configured, skipping sentinel check")
        return True

    sample_path = _os.path.join(
        _os.path.dirname(_os.path.dirname(__file__)),
        "state_projects_sample", "madhya_pradesh.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check",
                       path=sample_path, step="sentinel")
        return True

    logger.info(f"Sentinel: scraping {sentinel_reg_no}", url=sentinel_url, step="sentinel")
    try:
        # Fetch the detail page once and reuse the soup for all section parsers
        resp = _get(sentinel_url, logger)
        if not resp:
            logger.error("Sentinel: detail page unreachable", url=sentinel_url, step="sentinel")
            insert_crawl_error(run_id, config.get("id", "madhya_pradesh_rera"),
                               "SENTINEL_FAILED", "Detail page unreachable", url=sentinel_url)
            return False

        soup = BeautifulSoup(resp.text, "lxml")

        # Parse all sections (same as run())
        detail       = _parse_detail(sentinel_url, {}, logger) or {}
        promoter     = _parse_promoter(soup)
        consultants  = _parse_consultants(soup)
        res_units, com_units = _parse_unit_counts(soup)

        # Merge (matching run() ordering — detail first, then overrides)
        fresh = dict(detail)
        for k, v in promoter.items():
            if v is not None:
                fresh[k] = v
        if consultants:
            fresh["professional_information"] = consultants
        if res_units:
            fresh["number_of_residential_units"] = res_units
        if com_units:
            fresh["number_of_commercial_units"] = com_units

    except Exception as exc:
        logger.error(f"Sentinel: scrape error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", url=sentinel_url, step="sentinel")
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "madhya_pradesh_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg_no}",
        )
        return False

    logger.info("Sentinel check passed", sentinel_reg_no=sentinel_reg_no, step="sentinel")
    return True


# ── Main run() ─────────────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:  # noqa: C901
    logger   = CrawlerLogger(config["id"], run_id)
    site_id  = config["id"]
    counts   = dict(
        projects_found=0, projects_new=0, projects_updated=0,
        projects_skipped=0, documents_uploaded=0, error_count=0,
    )

    # ── Sentinel check ─────────────────────────────────────────────────────────
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counts["error_count"] += 1
        return counts

    checkpoint     = load_checkpoint(site_id, mode) or {}
    done_regs: set = set(checkpoint.get("done_regs", []))
    item_limit     = settings.CRAWL_ITEM_LIMIT or 0
    machine_name, machine_ip = get_machine_context()

    # ── Fetch listing ──────────────────────────────────────────────────────────
    logger.info("Fetching all-projects listing …")
    stubs = _fetch_listing(logger)
    if not stubs:
        logger.error("Listing returned no project stubs")
        insert_crawl_error(run_id, site_id, "SITE_STRUCTURE_CHANGED",
                           "Listing returned no rows",
                           url=LISTING_AJAX_URL)
        counts["error_count"] += 1
        return counts

    counts["projects_found"] = len(stubs)
    logger.info(f"Found {len(stubs)} project stubs in listing")
    items_processed = 0

    for stub in stubs:
        if item_limit and items_processed >= item_limit:
            logger.info(f"CRAWL_ITEM_LIMIT={item_limit} reached, stopping")
            save_checkpoint(site_id, mode, 0, None, run_id)
            return counts

        detail_url = stub.get("detail_url", "")
        if not detail_url:
            continue

        # Temporary key placeholder; real key computed after detail fetch
        temp_key = ""
        logger.set_project(key=temp_key, url=detail_url)

        try:
            random_delay(*config.get("rate_limit_delay", (1, 3)))
            logger.info("Fetching detail page", step="detail_fetch")
            resp_detail = _get(detail_url, logger)
            if not resp_detail:
                logger.warning("Detail page fetch failed, skipping")
                counts["error_count"] += 1
                logger.clear_project()
                continue

            soup = BeautifulSoup(resp_detail.text, "lxml")

            # ── Parse all sections ───────────────────────────────────────────
            detail = _parse_detail(detail_url, stub, logger)
            detail["_soup"] = soup  # pass soup along so sub-parsers don't re-parse

            reg_no = detail.get("project_registration_no", "")
            if not reg_no:
                logger.warning("No registration number found, skipping")
                counts["error_count"] += 1
                logger.clear_project()
                continue

            if reg_no in done_regs:
                counts["projects_skipped"] += 1
                logger.clear_project()
                continue

            key = generate_project_key(reg_no)
            logger.set_project(key=key, reg_no=reg_no, url=detail_url)

            if mode == "daily_light" and get_project_by_key(key):
                counts["projects_skipped"] += 1
                logger.info("Skipping — already in DB (daily_light)")
                done_regs.add(reg_no)
                logger.clear_project()
                continue

            # Promoter
            promoter_data = _parse_promoter(soup)
            consultants   = _parse_consultants(soup)
            building      = _parse_building_details(soup)
            res_units, com_units = _parse_unit_counts(soup)
            rera_certs    = detail.pop("_rera_cert_links", [])
            doc_links     = _parse_documents(soup, rera_certs)
            qpr_docs      = _parse_qpr(soup)
            images        = _parse_images(soup)
            detail.pop("_soup", None)

            # ── Assemble data dict ───────────────────────────────────────────
            data: dict = {
                "key":                     key,
                "state":                   config["state"],
                "config_id":               config["config_id"],
                "domain":                  DOMAIN,
                "url":                     detail_url,
                "is_live":                 True,
                "machine_name":            machine_name,
                "crawl_machine_ip":        machine_ip,
                # listing fields (fallback if detail parse returns empty strings)
                "project_name":            stub["project_name"],
                "promoter_name":           stub["promoter_name"],
                "status_of_the_project":   stub["status"],
                "project_registration_no": reg_no,
            }
            # detail overrides
            for k, v in detail.items():
                if v is not None and v != "" and not k.startswith("_"):
                    data[k] = v
            # promoter
            for k, v in promoter_data.items():
                if v is not None:
                    data[k] = v
            if consultants:
                data["professional_information"] = consultants
            if building:
                data["building_details"] = building
            if res_units:
                data["number_of_residential_units"] = res_units
            if com_units:
                data["number_of_commercial_units"] = com_units
            if images:
                data["project_images"] = images
            if qpr_docs:
                data["status_update"] = {"qpr_docs": qpr_docs}

            data["data"] = merge_data_sections({
                "govt_type":   "state",
                "START_PAGE":  "0",
                "is_processed": False,
            })

            # ── Normalize & validate ─────────────────────────────────────────
            logger.info("Normalizing", step="normalize")
            try:
                normalized = normalize_project_payload(
                    data, config, machine_name=machine_name, machine_ip=machine_ip,
                )
                record  = ProjectRecord(**normalized)
                db_dict = record.to_db_dict()
            except (ValidationError, ValueError) as exc:
                logger.warning("Validation failed — raw fallback", error=str(exc))
                insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(exc),
                                   project_key=key, url=detail_url, raw_data=data)
                counts["error_count"] += 1
                db_dict = normalize_project_payload(
                    data, config, machine_name=machine_name, machine_ip=machine_ip,
                )

            action = upsert_project(db_dict)
            items_processed += 1
            if action == "new":       counts["projects_new"] += 1
            elif action == "updated": counts["projects_updated"] += 1
            else:                     counts["projects_skipped"] += 1
            logger.info(f"DB result: {action}", step="db_upsert")

            # ── Document uploads ─────────────────────────────────────────────
            uploaded_documents: list[dict] = []
            doc_name_counts: dict[str, int] = {}
            for doc in doc_links:
                doc_for_policy = {"url": doc.get("link", ""), "label": doc.get("type", "")}
                selected = select_document_for_download(
                    config["state"], doc_for_policy, doc_name_counts, domain=DOMAIN,
                )
                if selected:
                    enriched_doc = {**doc, "type": selected.get("type", doc["type"])}
                    uploaded = _handle_document(db_dict["key"], enriched_doc, run_id, site_id, logger)
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
            save_checkpoint(site_id, mode, 0, reg_no, run_id)

        except Exception as exc:
            logger.exception("Project processing failed", exc, step="project_loop")
            insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                               project_key=key if key else None, url=detail_url)
            counts["error_count"] += 1
        finally:
            logger.clear_project()

    reset_checkpoint(site_id, mode)
    logger.info(f"Madhya Pradesh RERA complete: {counts}")
    return counts

