"""
Himachal Pradesh RERA Crawler — hprera.nic.in
Type: API (httpx) — dashboard-driven AJAX endpoints returning HTML fragments.

Strategy:
- Call GetMainContent to obtain the filter form's hidden field values.
- Submit the form to GetFilteredProjectsPV (GET, all districts/types selected)
  which returns ~19 MB of HTML containing:
    (a) HTML project cards in #reg-Projects with 'data-qs' encoded identifiers.
    (b) A <script> var markers = [...] JSON array with basic project metadata.
- For each registered project extract the 'data-qs' token and fetch five detail
  sections (Promoter, Project, Bank, AssociatedProfessionals, Documents).
- Normalize all fields, upsert to DB, and upload documents to S3.

No session cookies are required.  All detail endpoints accept GET with the same
'qs' token that appears in the listing HTML cards.
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlencode, quote

UTC = timezone.utc

import httpx
from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import download_response, generate_project_key, random_delay, safe_get
from core.db import get_project_by_key, upsert_project, insert_crawl_error, upsert_document
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
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://hprera.nic.in"
DOMAIN = "hprera.nic.in"
PUBLIC_DASHBOARD_URL = f"{BASE_URL}/PublicDashboard"
MAIN_CONTENT_URL = f"{BASE_URL}/PublicDashboard/GetMainContent"
LISTING_URL = f"{BASE_URL}/PublicDashboard/GetFilteredProjectsPV"
DETAIL_BASE = f"{BASE_URL}/Project/ProjectRegistration"

_AJAX_HEADERS = {
    "X-Requested-With": "XMLHttpRequest",
    "Referer": PUBLIC_DASHBOARD_URL,
    "Accept": "text/html,application/xhtml+xml",
}

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def _clean_doc_label(raw: str) -> str:
    """Extract document type name from a row label like '1. Jamabandhi* Uploaded on...'"""
    text = re.sub(r"^\d+\.\s*", "", raw.strip())
    text = re.sub(r"\s*Uploaded on.*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*Drawing\s*\d+.*", "", text, flags=re.IGNORECASE)
    # Normalise asterisk spacing: 'Name*' → 'Name *'
    text = re.sub(r"\s*\*\s*", " *", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_label_values(soup: BeautifulSoup) -> dict[str, str]:
    """Generic label→value extractor for HP RERA detail section HTML."""
    result: dict[str, str] = {}
    for tr in soup.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) >= 2:
            label = _clean(cells[0].get_text(separator=" "))
            val = _clean(cells[1].get_text(separator=" "))
            if label and val and len(label) < 120:
                result[label] = val
    return result


def _normalize_dd_mm_yyyy(val: str | None) -> str | None:
    """Convert 'DD/MM/YYYY' to 'YYYY-MM-DD 00:00:00+00:00', or return None."""
    if not val:
        return None
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", val.strip())
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)} 00:00:00+00:00"
    return None


def _parse_area(val: str | None) -> float | None:
    if not val:
        return None
    m = re.search(r"[\d.]+", val.replace(",", ""))
    try:
        return float(m.group()) if m else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Listing fetcher
# ---------------------------------------------------------------------------

def _get_form_data(client: httpx.Client) -> list[tuple[str, str]]:
    """Fetch the GetMainContent form and extract hidden field values."""
    resp = client.get(
        MAIN_CONTENT_URL,
        params={"ActiveTab": "tab_project_main"},
        headers={**_AJAX_HEADERS, "User-Agent": _UA},
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    form = soup.find("form")
    if not form:
        return []
    form_data: list[tuple[str, str]] = []
    for inp in form.find_all("input"):
        name = inp.get("name")
        itype = inp.get("type", "text")
        val = inp.get("value", "")
        if not name:
            continue
        # Skip checkbox inputs (we only send the companion hidden field with
        # value "false" for each checkbox, which the server treats as
        # "no filter applied → return all districts / types").
        if itype == "checkbox":
            continue
        form_data.append((name, val))
    return form_data


def _fetch_listing(
    client: httpx.Client,
    logger: CrawlerLogger,
) -> tuple[list[dict], dict[str, str]]:
    """
    Fetch all registered projects from the HP RERA public dashboard.

    Returns:
        markers  - list of dicts from the embedded JSON marker array (one per project)
        qs_map   - {registration_number: data-qs} mapping extracted from HTML cards
    """
    form_data = _get_form_data(client)
    resp = client.get(
        LISTING_URL,
        params=form_data,
        headers={**_AJAX_HEADERS, "User-Agent": _UA},
        timeout=120,
    )
    resp.raise_for_status()
    full_html = resp.text

    # ── Extract JSON markers (all projects) ─────────────────────────────────
    markers: list[dict] = []
    m = re.search(r"var markers\s*=\s*(\[.*?\]);", full_html, re.DOTALL)
    if m:
        try:
            markers = json.loads(m.group(1))
        except json.JSONDecodeError as exc:
            logger.warning(f"Failed to parse markers JSON: {exc}")
    else:
        logger.warning("Markers JSON not found in listing response")

    # ── Extract data-qs from HTML cards (registered projects tab only) ──────
    soup = BeautifulSoup(full_html, "lxml")
    qs_map: dict[str, str] = {}
    reg_tab = soup.find(id="reg-Projects")
    target = reg_tab if reg_tab else soup
    for a in target.find_all("a", attrs={"data-qs": True, "title": "View Application"}):
        reg_no = _clean(a.get_text())
        qs_val = a.get("data-qs", "").strip()
        if reg_no and qs_val:
            qs_map[reg_no] = qs_val

    logger.info(
        f"HP RERA listing: {len(markers)} markers, {len(qs_map)} qs tokens found"
    )
    return markers, qs_map


# ---------------------------------------------------------------------------
# Detail section fetchers
# ---------------------------------------------------------------------------

def _fetch_section(
    client: httpx.Client,
    section: str,
    qs: str,
    logger: CrawlerLogger,
) -> BeautifulSoup | None:
    """Fetch one detail section for the project identified by *qs*."""
    url = f"{DETAIL_BASE}/{section}"
    try:
        resp = client.get(
            url,
            params={"qs": qs, "UpdatedChangeDets": "N"},
            headers={**_AJAX_HEADERS, "User-Agent": _UA},
            timeout=30,
        )
        resp.raise_for_status()
        if "Page not found" in resp.text or len(resp.text) < 200:
            return None
        return BeautifulSoup(resp.text, "lxml")
    except Exception as exc:
        logger.warning(f"Section {section} fetch failed: {exc}")
        return None


# ---------------------------------------------------------------------------
# Section parsers
# ---------------------------------------------------------------------------

def _parse_promoter_details(soup: BeautifulSoup) -> dict:
    """Extract promoter fields from PromotorDetails_PreviewPV HTML."""
    lv = _parse_label_values(soup)
    norm = {k.lower().rstrip(":").strip(): v for k, v in lv.items()}

    contact: dict = {}
    phone = norm.get("mobile no.") or norm.get("mobile no") or norm.get("mobile")
    email = norm.get("email id") or norm.get("email")
    if phone and phone not in ("-NA-", "NA", "-"):
        contact["phone"] = phone
    if email and email not in ("-NA-", "NA", "-"):
        contact["email"] = email

    corr = norm.get("correspondence address") or ""
    perm = norm.get("permanent address") or norm.get("registered address") or ""
    addr_raw: dict = {}
    if corr and corr not in ("-NA-",):
        addr_raw["correspondence_address"] = corr
    if perm and perm not in ("-NA-",):
        addr_raw["registered_address"] = perm

    promoter_type = norm.get("promoter type") or ""
    name = norm.get("name") or ""

    return {
        "promoter_name": name or None,
        "promoter_contact_details": contact or None,
        "promoter_address_raw": addr_raw or None,
        "promoters_details": {"name": name, "type_of_firm": promoter_type} if promoter_type else None,
    }


def _parse_project_details(soup: BeautifulSoup) -> dict:
    """Extract project fields from ProjectDetails_PreviewPV HTML."""
    lv = _parse_label_values(soup)
    norm = {k.lower().rstrip(":").strip(): v for k, v in lv.items()}

    out: dict = {}

    # Project name — HP RERA detail page often carries this as "Project Name"
    pname = (
        norm.get("project name")
        or norm.get("name of project")
        or norm.get("name of the project")
    )
    if pname and pname not in ("-NA-", "NA", "N/A", "-"):
        out["project_name"] = pname

    # Status
    status = norm.get("project status") or norm.get("status")
    if status:
        out["status_of_the_project"] = status

    # Description
    desc = norm.get("project description") or norm.get("description")
    if desc:
        out["project_description"] = desc

    # Areas
    land_raw = norm.get("total land area")
    built_raw = norm.get("total built-up area") or norm.get("total built up area")
    land = _parse_area(land_raw)
    built = _parse_area(built_raw)
    if land is not None:
        out["land_area"] = land
    if built is not None:
        out["construction_area"] = built

    # Land area details — always use 2 decimal places for consistency
    land_unit = "sq m"
    if land is not None or built is not None:
        out["land_area_details"] = {
            "land_area": f"{land:.2f}" if land is not None else "0.00",
            "land_area_unit": land_unit,
            "construction_area": f"{built:.2f}" if built is not None else "0.00",
            "construction_area_unit": land_unit,
        }

    # Costs — strip any trailing "lakh" the site may already include before
    # appending the unit suffix, preventing "0.00 lakh lakh" duplication.
    def _cost_val(raw: str) -> str:
        cleaned = _clean(raw)
        cleaned = re.sub(r"\s*lakh\s*$", "", cleaned, flags=re.IGNORECASE).strip()
        return f"{cleaned} lakh"

    est_cost = norm.get("estimated contruction cost") or norm.get("estimated construction cost", "")
    land_cost = norm.get("land cost", "")
    total_cost = norm.get("total cost", "")
    cost_detail: dict = {}
    if est_cost:
        cost_detail["estimated_construction_cost"] = _cost_val(est_cost)
    if land_cost:
        cost_detail["cost_of_land"] = _cost_val(land_cost)
    if total_cost:
        cost_detail["total_project_cost"] = _cost_val(total_cost)
    if cost_detail:
        out["project_cost_detail"] = cost_detail

    # Dates
    commence_raw = (
        norm.get("proposed date of commencement")
        or norm.get("date of commencement")
        or norm.get("commencement date")
        or norm.get("proposed commencement date")
        or norm.get("estimated commencement date")
    )
    if commence_raw:
        out["estimated_commencement_date"] = _normalize_dd_mm_yyyy(commence_raw) or commence_raw

    # Land detail
    khata = norm.get("khata no.") or norm.get("khata no")
    mouza = norm.get("mohal/mauza no.") or norm.get("mohal/mauza no") or norm.get("mauza")
    land_det: dict = {}
    if khata and khata not in ("-NA-",):
        land_det["khata_no"] = khata
    if mouza and mouza not in ("-NA-",):
        land_det["mouza"] = mouza
    if land_det:
        out["land_detail"] = land_det

    # Location raw
    loc: dict = {}
    tehsil = norm.get("tehsil") or norm.get("taluk")
    khasra = norm.get("khasra no.") or norm.get("khasra no")
    # Locality: try explicit field first, then fall back to Mohal/Mauza No.
    # (HP RERA site uses "Mohal/Mauza No." as the closest equivalent to locality)
    locality = (
        norm.get("locality")
        or norm.get("village/locality")
        or norm.get("village")
        or norm.get("mohal/mauza no.")
        or norm.get("mohal/mauza no")
        or norm.get("mauza")
    )
    address = norm.get("address")
    lat = norm.get("latitude")
    lon = norm.get("longitude")
    if tehsil:
        loc["taluk"] = tehsil
    if khasra:
        loc["plot_no"] = khasra
    if locality and locality not in ("-NA-", "NA", "-"):
        loc["locality"] = locality
    if address:
        loc["raw_address"] = address
    if lat:
        loc["latitude"] = lat + " deg" if "deg" not in lat else lat
    if lon:
        loc["longitude"] = lon + " deg" if "deg" not in lon else lon
    if loc:
        out["project_location_raw"] = loc

    # Completion period → data field
    comp_years = norm.get("period of completion", "")
    # e.g. "6 years 0 months" or parsed from separate cells
    yr_m = re.search(r"(\d+)\s*years?\s*(\d+)\s*months?", comp_years, re.I)
    if not yr_m:
        # Try from lv directly (label may have 'Period of Completion' and value '6 years 0 months')
        yr_m = re.search(r"(\d+)\s*years?\s*(\d+)\s*months?",
                         " ".join(lv.values()), re.I)
    if yr_m:
        out["_completion_year"] = yr_m.group(1)
        out["_completion_month"] = yr_m.group(2)

    return out


def _parse_bank_details(soup: BeautifulSoup) -> dict | None:
    """Extract bank_details from BankDetails_PreviewPV HTML."""
    lv = _parse_label_values(soup)
    norm = {k.lower().rstrip(":").strip(): v for k, v in lv.items()}
    bank: dict = {}
    mapping = {
        "account holder name": "account_name",
        "bank name": "bank_name",
        "branch name": "branch",
        "branch address": "address",
        "account no.": "account_no",
        "account no": "account_no",
        "ifsc code": "IFSC",
        "ifsc": "IFSC",
    }
    for label, field in mapping.items():
        val = norm.get(label, "")
        if val and val not in ("-NA-", "NA", "N/A", "-"):
            bank[field] = val
    return bank or None


def _parse_professionals(soup: BeautifulSoup) -> list[dict] | None:
    """Extract professional_information from AssociatedVendors_PreviewPV HTML."""
    professionals: list[dict] = []

    role_headings = {
        "project architects": "Architect",
        "project engineers": "Engineer",
        "project chartered accountants": "Chartered Accountant",
        "project contractors": "Contractor",
        "project agents": "Agent",
    }

    # Each professional category is a block with a heading div + table.
    # HTML nesting: outer col-12 > inner row > innermost col-12 > heading-div
    # The <table> is a child of the outer col-12 (sibling of inner row).
    for heading_div in soup.find_all("div", class_=re.compile(r"fw-600")):
        heading_text = _clean(heading_div.get_text()).lower()
        current_role = next(
            (role for key, role in role_headings.items() if key in heading_text),
            None,
        )
        if not current_role:
            continue
        # Navigate: innermost col-12 → inner row → outer col-12
        innermost_col = heading_div.find_parent("div", class_=re.compile(r"col-"))
        if not innermost_col:
            continue
        inner_row = innermost_col.parent  # the inner .row div
        if not inner_row:
            continue
        # table is a sibling of inner_row inside the outer col-12
        table = inner_row.find_next_sibling("table")
        if not table:
            # fallback: search outer col-12
            outer_col = innermost_col.find_parent("div", class_=re.compile(r"col-"))
            table = outer_col.find("table") if outer_col else None
        if not table:
            continue
        for tr in table.select("tbody tr"):
            cells = tr.find_all("td")
            if len(cells) < 5:
                continue
            # col 1: name + reg_no  col 3: established + key_projects
            # col 4: phone + email  col 5: address
            name_cell = cells[1]
            name_span = name_cell.find("span", class_="no-wrap")
            name = _clean(name_span.get_text() if name_span else name_cell.get_text())
            if not name:
                continue

            other_cell = cells[3]
            established = ""
            key_projects = "0"
            for span in other_cell.find_all("span", class_="no-wrap"):
                txt = span.get_text(separator=" ", strip=True)
                em = re.search(r"Established\s*:\s*(\d+)", txt, re.I)
                kpm = re.search(r"Key Projects\s*:\s*(\d+)", txt, re.I)
                if em:
                    established = em.group(1)
                if kpm:
                    key_projects = kpm.group(1)

            contact_cell = cells[4]
            phone = ""
            email = ""
            for span in contact_cell.find_all("span", class_="no-wrap"):
                txt = _clean(span.get_text())
                if re.match(r"[\d\s\-+]{7,}", txt):
                    phone = txt
                elif "@" in txt:
                    email = txt

            address = _clean(cells[5].get_text()) if len(cells) > 5 else ""

            prof: dict = {"name": name, "role": current_role}
            if phone:
                prof["phone"] = phone
            if email:
                prof["email"] = email
            if address:
                prof["address"] = address
            if key_projects:
                prof["key_real_estate_projects"] = key_projects
            prof["updated"] = True
            professionals.append(prof)

    return professionals or None


def _extract_documents(soup: BeautifulSoup) -> list[dict]:
    """Extract uploaded document records from Documents_PreviewPV HTML.

    Captures both uploaded docs (with a ViewOpenFile link) and placeholder
    entries (listed doc type but not yet uploaded — no link).
    """
    docs: list[dict] = []
    seen_doc_keys: set[tuple[str, str]] = set()
    seen_placeholder_types: set[str] = set()

    def _doc_label(base: str, context: str, needs_context: bool) -> str:
        context = _clean(context)
        if needs_context and context:
            return f"{base} ({context})"
        return base

    def _cell_context(cell, header_label: str) -> str:
        header_label = _clean(header_label)
        if header_label:
            return header_label
        cell_text = _clean(cell.get_text(separator=" "))
        drawing_match = re.search(r"\bDrawing\s+\d+\b", cell_text, re.I)
        if drawing_match:
            return drawing_match.group(0)
        year_match = re.search(r"\bYear\s+\d+\b", cell_text, re.I)
        if year_match:
            return year_match.group(0)
        return ""

    for table in soup.find_all("table"):
        header_cells = table.find("tr")
        header_labels: list[str] = []
        if header_cells:
            header_labels = [
                _clean(cell.get_text(separator=" "))
                for cell in header_cells.find_all(["th", "td"])
            ]

        for tr in table.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < 2:
                continue

            # Rows must start with a serial number cell ("1.", "2.", …) so that
            # header / sub-heading rows are skipped automatically.
            serial_text = _clean(cells[0].get_text())
            if not re.match(r"^\d+\.?$", serial_text):
                continue

            raw_label = _clean(cells[1].get_text(separator=" "))
            doc_type = _clean_doc_label(raw_label) if raw_label else ""
            if not doc_type:
                continue

            uploaded = False
            doc_cells = cells[2:]
            multi_slot_row = len(doc_cells) > 1
            header_offset = max(0, len(cells) - len(header_labels))
            for cell_idx, cell in enumerate(doc_cells, start=2):
                header_idx = cell_idx - header_offset
                header_label = header_labels[header_idx] if 0 <= header_idx < len(header_labels) else ""
                cell_context = _cell_context(cell, header_label)
                anchors = cell.find_all("a", href=re.compile(r"ViewOpenFile", re.I))
                if not anchors:
                    continue

                uploaded = True
                for anchor_idx, link_tag in enumerate(anchors, start=1):
                    href = link_tag.get("href", "").strip()
                    if not href:
                        continue
                    full_url = href if href.startswith("http") else BASE_URL + href
                    label = _doc_label(doc_type, cell_context, multi_slot_row)
                    if len(anchors) > 1:
                        label = f"{label} #{anchor_idx}"
                    doc_key = (label, full_url)
                    if doc_key in seen_doc_keys:
                        continue
                    seen_doc_keys.add(doc_key)
                    docs.append({"type": label, "link": full_url, "updated": True})

            if not uploaded and doc_type not in seen_placeholder_types:
                seen_placeholder_types.add(doc_type)
                docs.append({"type": doc_type})

    return docs



def _parse_inventory(soup: BeautifulSoup) -> dict:
    """Extract unit counts from InventoryDetails_PreviewPV HTML."""
    result: dict = {}
    residential_total = 0
    commercial_total = 0

    _RES_KEYWORDS = {"apartment", "flat", "villa", "plot", "residential", "studio", "dwelling"}
    _COM_KEYWORDS = {"commercial", "shop", "office", "retail", "showroom"}

    for tr in soup.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 2:
            continue
        label = _clean(cells[0].get_text()).lower()
        # Look for the total/sanctioned column (usually col index 1 or 2)
        for idx in range(1, min(4, len(cells))):
            raw_val = _clean(cells[idx].get_text())
            parsed = _parse_area(raw_val)
            if parsed is not None and parsed > 0:
                val = int(parsed)
                if any(k in label for k in _COM_KEYWORDS):
                    commercial_total += val
                elif any(k in label for k in _RES_KEYWORDS) or "unit" in label or "no." in label:
                    residential_total += val
                break
        # If it's a "total" row that doesn't match specific types, use it
        if "total" in label and not any(
            k in label for k in _RES_KEYWORDS | _COM_KEYWORDS
        ):
            for idx in range(1, min(4, len(cells))):
                raw_val = _clean(cells[idx].get_text())
                parsed = _parse_area(raw_val)
                if parsed is not None and parsed > 0:
                    result["total_units"] = int(parsed)
                    break

    if residential_total:
        result["residential_units"] = residential_total
    if commercial_total:
        result["commercial_units"] = commercial_total
    if "total_units" not in result and residential_total:
        result["total_units"] = residential_total + commercial_total
    return result


def _handle_document(
    project_key: str,
    doc: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
    client: httpx.Client | None = None,
) -> dict | None:
    """Download a document, upload to S3, persist to DB. Returns enriched doc dict or None."""
    url = doc.get("url") or doc.get("link")
    label = doc.get("label") or doc.get("type") or "document"
    if not url:
        return None
    # Normalize doc dict keys for build_document_filename
    doc_for_fn = {"url": url, "label": label, **doc}
    filename = build_document_filename(doc_for_fn)
    try:
        resp = download_response(url, logger=logger, timeout=30, client=client)
        if not resp or len(resp.content) < 100:
            return None
        content = resp.content
        md5 = compute_md5(content)
        s3_key = upload_document(project_key, filename, content, dry_run=settings.DRY_RUN_S3)
        if s3_key is None:
            return None
        s3_url = get_s3_url(s3_key)
        logger.info("Document uploaded", label=label, s3_key=s3_key, step="documents")
        logger.log_document(label, url, "uploaded", s3_key=s3_key, file_size_bytes=len(content))
        return {
            "type": label,
            "link": url,
            "s3_link": s3_url,
            "updated": True,
        }
    except Exception as exc:
        logger.warning(f"Document download failed [{label}]: {exc}")
        return None


def _sentinel_check(
    config: dict,
    run_id: int,
    logger: CrawlerLogger,
    *,
    markers: list[dict],
    qs_map: dict[str, str],
    client: httpx.Client,
) -> bool:
    """
    Data-quality sentinel for Himachal Pradesh RERA.

    Accepts the already-fetched listing data (markers + qs_map) and the open
    httpx.Client from run() so the ~19 MB listing is downloaded only once.
    Fetches ALL 6 detail sections for the sentinel project and verifies
    ≥ 80% field coverage against the full baseline.
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
        "state_projects_sample", "himachal_pradesh.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check",
                       path=sample_path, step="sentinel")
        return True

    logger.info(f"Sentinel: using pre-fetched listing to find {sentinel_reg}", step="sentinel")
    try:
        qs = qs_map.get(sentinel_reg, "")
        if not qs:
            logger.warning(f"Sentinel: {sentinel_reg!r} not found in listing qs_map — skipping",
                           step="sentinel")
            return True

        # Extract fields from listing marker (project_name, project_type, etc. live here)
        marker_by_reg: dict[str, dict] = {}
        for _mk in markers:
            _r = (
                _mk.get("reg_no") or _mk.get("RegistrationNo") or _mk.get("reg")
                or _mk.get("RegNo") or ""
            ).strip()
            if _r:
                marker_by_reg[_r] = _mk
        _marker = marker_by_reg.get(sentinel_reg, {})

        def _m(*keys: str) -> str:
            for k in keys:
                v = _marker.get(k)
                if v:
                    return str(v).strip()
            return ""

        # Fetch ALL 6 sections using the already-open client from run()
        proj_soup = _fetch_section(client, "ProjectDetails_PreviewPV",     qs, logger)
        p_soup    = _fetch_section(client, "PromotorDetails_PreviewPV",    qs, logger)
        bank_soup = _fetch_section(client, "BankDetails_PreviewPV",        qs, logger)
        prof_soup = _fetch_section(client, "AssociatedVendors_PreviewPV",  qs, logger)
        docs_soup = _fetch_section(client, "Documents_PreviewPV",          qs, logger)
        inv_soup  = _fetch_section(client, "InventoryDetails_PreviewPV",   qs, logger)

        proj_data    = _parse_project_details(proj_soup)  if proj_soup  else {}
        promoter     = _parse_promoter_details(p_soup)    if p_soup     else {}
        bank         = _parse_bank_details(bank_soup)     if bank_soup  else None
        professionals = _parse_professionals(prof_soup)   if prof_soup  else None
        docs         = _extract_documents(docs_soup)      if docs_soup  else []
        inventory    = _parse_inventory(inv_soup)         if inv_soup   else {}

        # Merge (same ordering as run())
        fresh = {**promoter, **proj_data}
        if bank:
            fresh["bank_details"] = bank
        if professionals:
            fresh["professional_information"] = professionals
        if docs:
            fresh["uploaded_documents"] = docs
        if inventory:
            fresh.update(inventory)

        # Seed listing-marker fields (same as run() lines 849-888)
        m_name     = _m("ProjectName", "title", "name")
        m_type     = _m("ProjectTypeNm", "ProjectType", "projectType", "type")
        m_validity = _m("ValidUpto", "validupto", "valid_upto")
        if m_name and not fresh.get("project_name"):
            fresh["project_name"] = m_name
        if m_type and not fresh.get("project_type"):
            fresh["project_type"] = m_type
        if m_validity and not fresh.get("estimated_finish_date"):
            fresh["estimated_finish_date"] = _normalize_dd_mm_yyyy(m_validity) or m_validity
        # Derive estimated_commencement_date from finish date minus period of completion
        comp_year_s = fresh.pop("_completion_year", None)
        comp_month_s = fresh.pop("_completion_month", None)
        if (
            not fresh.get("estimated_commencement_date")
            and m_validity
            and (comp_year_s or comp_month_s)
        ):
            try:
                from dateutil.relativedelta import relativedelta
                finish_str = _normalize_dd_mm_yyyy(m_validity)
                if finish_str:
                    finish_dt = datetime(
                        int(finish_str[:4]),
                        int(finish_str[5:7]),
                        int(finish_str[8:10]),
                        tzinfo=UTC,
                    )
                    delta = relativedelta(
                        years=int(comp_year_s or 0),
                        months=int(comp_month_s or 0),
                    )
                    commence_dt = finish_dt - delta
                    fresh["estimated_commencement_date"] = (
                        commence_dt.strftime("%Y-%m-%d 00:00:00+00:00")
                    )
            except Exception:
                pass
        # number_of_residential_units falls back to inventory
        if not fresh.get("number_of_residential_units"):
            flats = inventory.get("total_units") or inventory.get("residential_units")
            if flats:
                fresh["number_of_residential_units"] = flats

        # project_state is injected from config in run(), not scraped from the portal
        fresh["project_state"] = config.get("state", "Himachal Pradesh")

    except Exception as exc:
        logger.error(f"Sentinel: fetch/parse error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", step="sentinel")
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "himachal_pradesh_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(config: dict, run_id: int, mode: str) -> dict:
    """HP RERA: static AJAX crawl — listing + five detail sections per project."""
    logger = CrawlerLogger(config["id"], run_id)
    site_id = config["id"]
    counts = dict(
        projects_found=0, projects_new=0, projects_updated=0,
        projects_skipped=0, documents_uploaded=0, error_count=0,
    )
    item_limit = settings.CRAWL_ITEM_LIMIT or 0
    items_processed = 0
    machine_name, machine_ip = get_machine_context()
    t_run = time.monotonic()

    checkpoint = load_checkpoint(site_id, mode) or {}
    resume_after_key = checkpoint.get("last_project_key")
    resume_pending = bool(resume_after_key)

    _timeout = httpx.Timeout(connect=15.0, read=120.0, write=10.0, pool=5.0)

    with httpx.Client(
        timeout=_timeout, follow_redirects=True, headers={"User-Agent": _UA}
    ) as client:
        # Warm up session / obtain cookies
        client.get(PUBLIC_DASHBOARD_URL)

        # ── Phase 1: Fetch listing ────────────────────────────────────────────
        t0 = time.monotonic()
        markers, qs_map = _fetch_listing(client, logger)
        logger.timing("search", time.monotonic() - t0, rows=len(qs_map))

        # ── Sentinel health check (uses already-fetched listing) ─────────────
        t0 = time.monotonic()
        if not _sentinel_check(config, run_id, logger,
                               markers=markers, qs_map=qs_map, client=client):
            logger.error("Sentinel failed — aborting crawl", step="sentinel")
            counts["error_count"] += 1
            return counts
        logger.timing("sentinel", time.monotonic() - t0)

        # Build marker lookup by registration number (try multiple key names)
        marker_by_reg: dict[str, dict] = {}
        for m in markers:
            reg = (
                m.get("reg_no") or m.get("RegistrationNo") or m.get("reg")
                or m.get("RegNo") or ""
            ).strip()
            if reg:
                marker_by_reg[reg] = m

        all_reg_nos = list(qs_map.keys())
        counts["projects_found"] = len(all_reg_nos)
        if item_limit:
            all_reg_nos = all_reg_nos[:item_limit]
            logger.info(f"HP RERA: CRAWL_ITEM_LIMIT={item_limit} — {len(all_reg_nos)} projects")

        # ── Phase 2: Process each project ────────────────────────────────────
        for i, reg_no in enumerate(all_reg_nos):
            qs = qs_map[reg_no]
            key = generate_project_key(reg_no)
            detail_url = f"{DETAIL_BASE}/GetApplicationPreviewPV?qs={qs}&UpdatedChangeDets=Y"

            if resume_pending:
                if key == resume_after_key:
                    resume_pending = False
                counts["projects_skipped"] += 1
                continue

            logger.set_project(key=key, reg_no=reg_no, url=detail_url, page=i)

            if mode == "daily_light" and get_project_by_key(key):
                logger.info("Skipping — already in DB (daily_light)", step="skip")
                counts["projects_skipped"] += 1
                logger.clear_project()
                continue

            try:
                marker = marker_by_reg.get(reg_no, {})

                # Fetch all detail sections
                p_soup = _fetch_section(client, "PromotorDetails_PreviewPV", qs, logger)
                proj_soup = _fetch_section(client, "ProjectDetails_PreviewPV", qs, logger)
                bank_soup = _fetch_section(client, "BankDetails_PreviewPV", qs, logger)
                prof_soup = _fetch_section(client, "AssociatedVendors_PreviewPV", qs, logger)
                docs_soup = _fetch_section(client, "Documents_PreviewPV", qs, logger)
                inv_soup = _fetch_section(client, "InventoryDetails_PreviewPV", qs, logger)

                random_delay(*config.get("rate_limit_delay", (1, 3)))

                # Parse sections
                promoter = _parse_promoter_details(p_soup) if p_soup else {}
                proj_data = _parse_project_details(proj_soup) if proj_soup else {}
                bank = _parse_bank_details(bank_soup) if bank_soup else None
                professionals = _parse_professionals(prof_soup) if prof_soup else None
                docs = _extract_documents(docs_soup) if docs_soup else []
                inventory = _parse_inventory(inv_soup) if inv_soup else {}

                # Pull internal completion year/month out of proj_data
                data_extras: dict = {
                    "govt_type": "state",
                    "arrived_date": datetime.now(tz=UTC).isoformat(),
                }
                comp_year = proj_data.pop("_completion_year", None)
                comp_month = proj_data.pop("_completion_month", None)
                if comp_year:
                    data_extras["completion_year"] = comp_year
                if comp_month:
                    data_extras["completion_month"] = comp_month

                # Merge location: prefer parsed detail, fill gaps from marker
                loc: dict = proj_data.pop("project_location_raw", None) or {}

                def _m(*keys: str) -> str:
                    """Get first non-empty value from marker using given key names."""
                    for k in keys:
                        v = str(marker.get(k) or "").strip()
                        if v and v not in ("-NA-", "NA", "None"):
                            return v
                    return ""

                # Real marker keys (from API inspection):
                # Latitude, Longitude, Address, MobileNo, EmailId, ProjectName,
                # ProjectTypeNm, PromoterName, ValidUpto, RegDate, Area
                lat_str = _m("Latitude", "lat", "latitude")
                lng_str = _m("Longitude", "lng", "longitude")
                addr_str = _m("Address", "address")
                m_area = _m("Area", "area")

                if not loc.get("latitude") and lat_str:
                    loc["latitude"] = lat_str + " deg" if "deg" not in lat_str else lat_str
                if not loc.get("longitude") and lng_str:
                    loc["longitude"] = lng_str + " deg" if "deg" not in lng_str else lng_str
                if not loc.get("raw_address") and addr_str:
                    loc["raw_address"] = addr_str

                # Basic fields from marker
                m_name = _m("ProjectName", "title", "name")
                m_type = _m("ProjectTypeNm", "ProjectType", "projectType", "type")
                m_promoter = _m("PromoterName", "promoter", "promoter_name")
                m_validity = _m("ValidUpto", "validupto", "valid_upto")
                m_reg_date = _m("RegDate")

                # Flats count: prefer inventory, fall back to marker
                flats_count = inventory.get("total_units") or inventory.get("residential_units")
                if flats_count:
                    data_extras["flats"] = str(flats_count)

                # Promoter contact fallback from marker (MobileNo / EmailId)
                promoter_contact = promoter.get("promoter_contact_details") or {}
                m_phone = _m("MobileNo", "Phone", "mobile")
                m_email = _m("EmailId", "Email", "email")
                if not promoter_contact.get("phone") and m_phone:
                    promoter_contact["phone"] = m_phone
                if not promoter_contact.get("email") and m_email:
                    promoter_contact["email"] = m_email

                # Construction area from marker if not found in project details
                if m_area and proj_data.get("construction_area") is None:
                    proj_data["construction_area"] = _parse_area(m_area)

                # Approved date from marker RegDate
                if m_reg_date and not proj_data.get("approved_on_date"):
                    proj_data["approved_on_date"] = _normalize_dd_mm_yyyy(m_reg_date) or m_reg_date

                # Assemble payload
                payload: dict = {
                    "key": key,
                    "project_registration_no": reg_no,
                    "project_name": m_name or None,
                    "project_type": m_type or None,
                    "promoter_name": promoter.get("promoter_name") or m_promoter or None,
                    "promoter_address_raw": promoter.get("promoter_address_raw"),
                    "promoter_contact_details": promoter_contact or None,
                    "promoters_details": promoter.get("promoters_details"),
                    "state": config["state"],
                    "project_state": config["state"],
                    "domain": DOMAIN,
                    "config_id": config.get("config_id"),
                    "url": PUBLIC_DASHBOARD_URL,
                    "is_live": True,
                    "machine_name": machine_name,
                    "crawl_machine_ip": machine_ip,
                    "data": data_extras,
                }

                # Overlay project detail fields (don't overwrite keys already set)
                for fld, val in proj_data.items():
                    if val is not None and payload.get(fld) is None:
                        payload[fld] = val

                if loc:
                    payload["project_location_raw"] = loc
                if bank:
                    payload["bank_details"] = bank
                if professionals:
                    payload["professional_information"] = professionals

                res_units = inventory.get("residential_units")
                com_units = inventory.get("commercial_units")
                # If residential_units wasn't classified by keyword matching, fall
                # back to flats_count (total_units / residential_units from inventory).
                if res_units:
                    payload["number_of_residential_units"] = res_units
                elif flats_count:
                    payload["number_of_residential_units"] = flats_count
                if com_units:
                    payload["number_of_commercial_units"] = com_units

                if docs:
                    payload["uploaded_documents"] = docs

                # Date: validity from marker as estimated_finish_date fallback
                if m_validity and not payload.get("estimated_finish_date"):
                    payload["estimated_finish_date"] = (
                        _normalize_dd_mm_yyyy(m_validity) or m_validity
                    )

                # Derive estimated_commencement_date = estimated_finish_date minus
                # the Period of Completion (years + months) when not explicitly scraped.
                if (
                    not payload.get("estimated_commencement_date")
                    and m_validity
                    and (comp_year or comp_month)
                ):
                    try:
                        from dateutil.relativedelta import relativedelta
                        finish_str = _normalize_dd_mm_yyyy(m_validity)
                        if finish_str:
                            finish_dt = datetime(
                                int(finish_str[:4]),
                                int(finish_str[5:7]),
                                int(finish_str[8:10]),
                                tzinfo=UTC,
                            )
                            delta = relativedelta(
                                years=int(comp_year or 0),
                                months=int(comp_month or 0),
                            )
                            commence_dt = finish_dt - delta
                            payload["estimated_commencement_date"] = (
                                commence_dt.strftime("%Y-%m-%d 00:00:00+00:00")
                            )
                    except Exception:
                        pass

                # Fallback: use registration number as project_name when the
                # portal provides no name in either the listing marker or the
                # detail page.  The reg number uniquely identifies the project
                # and satisfies the required-field constraint without skipping
                # an otherwise valid record.
                if not payload.get("project_name"):
                    logger.warning(
                        "project_name missing — falling back to registration number",
                        reg_no=reg_no, step="normalize",
                    )
                    payload["project_name"] = reg_no

                # Normalize and validate
                logger.info("Normalizing and validating", step="normalize")
                try:
                    normalized = normalize_project_payload(
                        payload, config,
                        machine_name=machine_name, machine_ip=machine_ip,
                    )
                    record = ProjectRecord(**normalized)
                    db_dict = record.to_db_dict()
                except (ValidationError, ValueError) as exc:
                    logger.warning("Validation failed — raw fallback", error=str(exc))
                    insert_crawl_error(
                        run_id, site_id, "VALIDATION_FAILED", str(exc),
                        project_key=key, url=detail_url, raw_data=payload,
                    )
                    counts["error_count"] += 1
                    db_dict = normalize_project_payload(
                        {**payload, "data": merge_data_sections(
                            payload.get("data"), {"validation_fallback": True}
                        )},
                        config, machine_name=machine_name, machine_ip=machine_ip,
                    )

                action = upsert_project(db_dict)
                items_processed += 1
                if action == "new":
                    counts["projects_new"] += 1
                else:
                    counts["projects_updated"] += 1
                logger.info(f"DB result: {action}", step="db_upsert")

                # Documents are not downloaded for HP RERA.

                if i % 50 == 0:
                    save_checkpoint(site_id, mode, i, key, run_id)

            except Exception as exc:
                logger.exception(
                    "Project processing failed", exc,
                    step="project_loop", reg_no=reg_no,
                )
                insert_crawl_error(
                    run_id, site_id, "PROJECT_ERROR", str(exc),
                    project_key=key, url=detail_url,
                )
                counts["error_count"] += 1
            finally:
                logger.clear_project()

    reset_checkpoint(site_id, mode)
    logger.info(f"HP RERA crawl complete: {counts}")
    logger.timing("total_run", time.monotonic() - t_run)
    return counts
