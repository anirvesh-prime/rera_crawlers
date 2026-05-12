"""
Bihar RERA Crawler — rera.bihar.gov.in/RegisteredPP.aspx
Type: httpx listing + Playwright detail (popup capture)

Strategy:
- The listing page is a server-side ASP.NET GridView paginated via ASP.NET postbacks.
- Listing pagination is driven with Playwright because direct httpx page-turn POSTs
  intermittently fail with server-side ViewState / cluster mismatches.
- Each project name is a link that triggers __doPostBack('...', 'PrintIndicator$N'),
  which opens a popup window at Filanprint.aspx?id=RERAP...
- Playwright clicks each listing row and captures the popup URL via
  context.expect_page().
- The Filanprint detail page is a plain GET request, parsed with httpx + BeautifulSoup.
- Fields from listing: project_name, project_registration_no, promoter_name,
    project_location_raw (address), submitted_date.
- Additional fields from detail: project_type, status_of_the_project,
    estimated_commencement_date, estimated_finish_date, land_area, construction_area,
    project_description, project_location_raw (+ lat/lng/district/plot),
    bank_details, promoter_contact_details, members_details,
    professional_information, building_details.
"""
from __future__ import annotations

import re
import time

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from pydantic import ValidationError

from core.checkpoint import reset_checkpoint
from core.config import settings
from core.crawler_base import download_response, generate_project_key, random_delay, safe_get, safe_post
from core.db import get_project_by_key, upsert_project, upsert_document, insert_crawl_error
from core.document_policy import select_document_for_download
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import (
    build_document_filename,
    existing_uploaded_document_entry,
    get_machine_context,
    merge_data_sections,
    normalize_project_payload,
)
from core.s3 import compute_md5, upload_document, get_s3_url

LISTING_URL  = "https://rera.bihar.gov.in/RegisteredPP.aspx"
FILANPRINT   = "https://rera.bihar.gov.in/Filanprint.aspx"
STATE_CODE   = "BR"
DOMAIN       = "rera.bihar.gov.in"
# ASP.NET GridView control ID
_GRID_ID     = "ContentPlaceHolder1_GV_Building"
_GRID_TARGET = "ctl00$ContentPlaceHolder1$GV_Building"


# ── Playwright: collect Filanprint popup URLs ──────────────────────────────────

def _collect_listing_pages(
    logger: CrawlerLogger,
    max_items: int | None = None,
    max_pages: int | None = None,
    capture_detail_urls: bool = True,
) -> list[dict]:
    """
    Use Playwright to traverse listing pages and capture aligned detail popup URLs.

    Strategy:
    - Maintain a single persistent listing page and navigate it forward page by page.
    - Parse each listing page's row data from the rendered HTML.
    - Within each listing page, click projects sequentially without reloading:
      the __doPostBack postback that opens the popup reloads the listing in-place
      while preserving the current page number in ViewState, so subsequent project
      clicks on the same listing page work immediately after the popup closes.
    - To advance to the next listing page, find the Page$N pager link (or the
      "..." overflow link) and click it.

    Returns a list of page payloads:
      {"page": <page_no>, "rows": [...], "detail_urls": [...]}
    where detail_urls is positionally aligned with rows.

    max_items: if set, stop after collecting this many URLs (for CRAWL_ITEM_LIMIT).
    """
    links_sel = f"table#{_GRID_ID} tr td:first-child a"
    pages: list[dict] = []
    items_seen = 0

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context()
            listing_page = ctx.new_page()
            listing_page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=30_000)
            current_listing_pg = 1

            while True:
                soup = BeautifulSoup(listing_page.content(), "lxml")
                rows = _parse_page_rows(soup)
                if not rows:
                    logger.info(
                        f"Playwright: listing page {current_listing_pg} has no parsed rows — stopping",
                        step="detail_collect",
                    )
                    break

                if max_items:
                    remaining = max_items - items_seen
                    rows = rows[:remaining]
                if not rows:
                    break

                # ── Identify project links on this listing page ───────────────
                link_texts: list[str] = listing_page.eval_on_selector_all(
                    links_sel, "els => els.map(e => e.innerText.trim())"
                )
                project_indices = [
                    i for i, t in enumerate(link_texts)
                    if t and not t.isdigit()
                    and t not in ("...", "Next", "Prev", "Previous", "First", "Last")
                ]

                project_indices = project_indices[:len(rows)]
                if not project_indices:
                    logger.info(
                        f"Playwright: listing page {current_listing_pg} has no project links — stopping",
                        step="detail_collect",
                    )
                    break

                detail_urls: list[str | None]
                if capture_detail_urls:
                    logger.info(
                        f"Playwright: listing page {current_listing_pg},"
                        f" collecting {len(project_indices)} detail URLs for {len(rows)} rows",
                        step="detail_collect",
                    )
                    if len(project_indices) != len(rows):
                        logger.warning(
                            f"Playwright: row/link mismatch on page {current_listing_pg}:"
                            f" rows={len(rows)} links={len(project_indices)}",
                            step="detail_collect",
                        )

                    # ── Click each project; listing page stays alive between clicks ─
                    detail_urls = []
                    for rank, idx in enumerate(project_indices):
                        name = link_texts[idx]
                        try:
                            with ctx.expect_page(timeout=15_000) as popup_info:
                                listing_page.eval_on_selector_all(
                                    links_sel, f"els => els[{idx}].click()"
                                )
                            popup = popup_info.value
                            popup.wait_for_load_state("domcontentloaded", timeout=15_000)
                            url = popup.url
                            popup.close()
                            # Postback already reloaded the listing page in-place;
                            # wait for it to settle before the next click.
                            listing_page.wait_for_load_state("domcontentloaded", timeout=15_000)

                            if "Filanprint.aspx" in url:
                                detail_urls.append(url)
                                logger.info(
                                    f"  [pg{current_listing_pg}:{rank}] {name!r} → {url}",
                                    step="detail_collect",
                                )
                            else:
                                detail_urls.append(None)
                                logger.warning(
                                    f"  [pg{current_listing_pg}:{rank}] {name!r}: unexpected URL {url}",
                                    step="detail_collect",
                                )
                        except Exception as e:
                            detail_urls.append(None)
                            logger.warning(
                                f"  [pg{current_listing_pg}:{rank}] {name!r}: popup failed — {e}",
                                step="detail_collect",
                            )

                    if len(detail_urls) < len(rows):
                        detail_urls.extend([None] * (len(rows) - len(detail_urls)))
                else:
                    logger.info(
                        f"Playwright: listing page {current_listing_pg}, parsed {len(rows)} rows",
                        step="detail_collect",
                    )
                    detail_urls = [None] * len(rows)

                pages.append({
                    "page": current_listing_pg,
                    "rows": rows,
                    "detail_urls": detail_urls,
                })
                items_seen += len(rows)

                if (max_items and items_seen >= max_items) or (max_pages and current_listing_pg >= max_pages):
                    break

                # ── Navigate to the next listing page ─────────────────────────
                next_pg = current_listing_pg + 1
                next_href = (
                    f"javascript:__doPostBack('{_GRID_TARGET}','Page${next_pg}')"
                )
                next_link = listing_page.query_selector(f'a[href="{next_href}"]')
                if next_link:
                    next_link.click()
                    listing_page.wait_for_load_state("domcontentloaded", timeout=15_000)
                    current_listing_pg = next_pg
                else:
                    # "..." overflow link — find any pager link pointing to next_pg
                    pager_info: list[dict] = listing_page.eval_on_selector_all(
                        f"table#{_GRID_ID} tr:last-child a",
                        "els => els.map(e => ({text: e.innerText.trim(), href: e.getAttribute('href')}))",
                    )
                    overflow = next(
                        (
                            info for info in pager_info
                            if info.get("href", "").endswith(f"'Page${next_pg}')")
                        ),
                        None,
                    )
                    if overflow:
                        listing_page.click(f'a[href="{overflow["href"]}"]')
                        listing_page.wait_for_load_state("domcontentloaded", timeout=15_000)
                        current_listing_pg = next_pg
                    else:
                        logger.info(
                            f"Playwright: no link to page {next_pg} — all pages collected",
                            step="detail_collect",
                        )
                        break

            listing_page.close()
            browser.close()
    except Exception as e:
        logger.error(f"Playwright detail-url collection failed: {e}", step="detail_collect")
    return pages


def _collect_detail_urls(logger: CrawlerLogger, max_items: int | None = None) -> list[str | None]:
    """Backward-compatible wrapper that flattens Playwright listing page payloads."""
    pages = _collect_listing_pages(logger, max_items=max_items)
    return [url for payload in pages for url in payload["detail_urls"]]


# ── Detail page parser ────────────────────────────────────────────────────────

def _kv_table(table: BeautifulSoup) -> dict[str, str]:
    """Flatten a label ':' value ASP.NET table into a dict (lowercase keys)."""
    out: dict[str, str] = {}
    for row in table.find_all("tr"):
        cells = [c.get_text(separator=" ", strip=True) for c in row.find_all(["td", "th"])]
        i = 0
        while i + 2 < len(cells):
            if cells[i + 1] == ":" and cells[i] and cells[i + 2] not in (":", ""):
                out[cells[i].lower().strip()] = cells[i + 2].strip()
                i += 3
            else:
                i += 1
    return out


def _parse_detail_page(html: str) -> dict:
    """Extract all available fields from Filanprint.aspx detail HTML."""
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")

    # ── Promoter detail (Table 2: app_no, type, name, PAN, mobile, email) ─────
    prom_kv: dict[str, str] = {}
    if len(tables) > 2:
        prom_kv = _kv_table(tables[2])

    # ── Contact / address (Table 3) ───────────────────────────────────────────
    contact_kv: dict[str, str] = {}
    if len(tables) > 3:
        contact_kv = _kv_table(tables[3])

    # ── Org members (GV_Member table: header + data rows) ────────────────────
    members: list[dict] = []
    members_table = soup.find("table", id="GV_Member")
    if members_table:
        rows = members_table.find_all("tr")
        if len(rows) > 1:
            headers = [c.get_text(strip=True).lower() for c in rows[0].find_all(["td", "th"])]
            for row in rows[1:]:
                cells = [c.get_text(separator=" ", strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) >= len(headers):
                    raw = dict(zip(headers, cells))
                    member: dict = {
                        "name":     raw.get("name", ""),
                        "position": raw.get("designation", ""),
                        "phone":    raw.get("mobile no.", ""),
                        # address column is not included: the schema's members_details
                        # field does not store raw_address for Bihar directors/members.
                    }
                    # Photo: try to extract img src from the Image cell
                    img_cell_idx = headers.index("image") if "image" in headers else -1
                    if img_cell_idx >= 0 and img_cell_idx < len(row.find_all(["td", "th"])):
                        img_tag = row.find_all(["td", "th"])[img_cell_idx].find("img")
                        if img_tag and img_tag.get("src"):
                            src = img_tag["src"].lstrip("/")
                            member["photo"] = f"https://{DOMAIN}/{src}"
                    members.append({k: v for k, v in member.items() if v})

    # ── Project info (Table 7) ────────────────────────────────────────────────
    proj_kv: dict[str, str] = {}
    if len(tables) > 7:
        proj_kv = _kv_table(tables[7])

    # ── Project location (Table 8) ────────────────────────────────────────────
    loc_kv: dict[str, str] = {}
    if len(tables) > 8:
        loc_kv = _kv_table(tables[8])

    # ── Bank account (Table 14) ───────────────────────────────────────────────
    bank_kv: dict[str, str] = {}
    if len(tables) > 14:
        bank_kv = _kv_table(tables[14])

    # ── Vendors / professionals (GV_Vendor table: header + data rows) ────────
    vendors: list[dict] = []
    vendors_table = soup.find("table", id="GV_Vendor")
    if vendors_table:
        rows = vendors_table.find_all("tr")
        if len(rows) > 1:
            headers = [c.get_text(strip=True).lower() for c in rows[0].find_all(["td", "th"])]
            for row in rows[1:]:
                cells = [c.get_text(separator=" ", strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) >= 2:
                    raw = dict(zip(headers, cells))
                    vendor = {
                        "name":                    raw.get("name", ""),
                        "role":                    raw.get("vendor type", ""),
                        "address":                 raw.get("address", ""),
                        "effective_date":          raw.get("year of establishment", ""),
                        "key_real_estate_projects": raw.get("key project", ""),
                    }
                    vendors.append({k: v for k, v in vendor.items() if v})

    # ── Building details (GV_Building table: header + data rows) ─────────────
    buildings: list[dict] = []
    buildings_table = soup.find("table", id="GV_Building")
    if buildings_table:
        rows = buildings_table.find_all("tr")
        if len(rows) > 1:
            headers = [c.get_text(strip=True).lower() for c in rows[0].find_all(["td", "th"])]
            for row in rows[1:]:
                cells = [c.get_text(separator=" ", strip=True) for c in row.find_all(["td", "th"])]
                if any(cells):
                    raw = dict(zip(headers, cells))
                    block = raw.get("name of building", "")
                    # The site renders floors as "G+17" in "Sanctioned No of Floor";
                    # older pages may have a plain numeric "no of floors" column.
                    floors = (
                        raw.get("sanctioned no of floor", "")
                        or raw.get("no of floors", "")
                    ).strip()
                    if block and floors and floors not in ("0", ""):
                        if re.match(r"^G\+\d+$", floors, re.IGNORECASE):
                            # Already "G+17" format — append directly
                            block = f"{block} {floors.upper()}"
                        else:
                            try:
                                block = f"{block} G+{int(float(floors))}"
                            except (ValueError, TypeError):
                                pass
                    building = {
                        "block_name":   block,
                        "flat_type":    raw.get("type of apartment", ""),
                        "no_of_units":  raw.get("no of apartment", ""),
                        "carpet_area":  raw.get("carpet area", ""),
                        "balcony_area": raw.get("area of exclusive balcony", ""),
                        "open_area":    raw.get("area of exclusive open terrace", ""),
                    }
                    buildings.append({k: v for k, v in building.items() if v})

    # ── Assemble output ───────────────────────────────────────────────────────
    def _f(kv: dict, *keys: str) -> str:
        """Return first matching key value (case-insensitive)."""
        for k in keys:
            v = kv.get(k.lower(), "")
            if v:
                return v
        return ""

    # Pin code comes from the contact/address table (promoter's office PIN).
    # It is stored at the top-level project_pin_code field but NOT inside
    # project_location_raw, which describes the project site — not the promoter.
    # The location table itself has no PIN code field on Bihar RERA.
    pin_code = (
        _f(loc_kv, "pin code", "pincode", "pin", "zip")
        or _f(contact_kv, "pin code", "pincode", "pin code")
    )
    loc_raw: dict = {
        "address":  _f(loc_kv, "project address"),
        "district": _f(loc_kv, "district"),
        "plot_no":  _f(loc_kv, "khesra no./plot no."),
        "taluk":    _f(loc_kv, "mauja") or _f(loc_kv, "anchal"),  # Mauja = revenue hamlet used as taluk in Bihar
        "village":  _f(loc_kv, "village"),
        # city/town is already captured at the top-level project_city field;
        # do not duplicate it inside project_location_raw.
        # pin_code is the promoter's office PIN, not the project location PIN;
        # it is stored only at the top-level project_pin_code field.
    }
    try:
        lat_raw = _f(loc_kv, "latitude of end point of the plot")
        lng_raw = _f(loc_kv, "longitude of end point of the plot")
        lat = float(lat_raw)
        lng = float(lng_raw)
        if lat and lng:
            loc_raw["latitude"]           = str(lat)
            loc_raw["longitude"]          = str(lng)
            loc_raw["processed_latitude"]  = lat
            loc_raw["processed_longitude"] = lng
    except ValueError:
        pass

    # ── Passbook URL for scan_copy_of_cheque ──────────────────────────────────
    passbook_url: str | None = None
    for a in soup.find_all("a", href=True):
        href_pb: str = a["href"]
        # href may be "PassBook/..." or "/PassBook/..." (no leading slash on older pages)
        if "PassBook/" in href_pb and ".pdf" in href_pb.lower():
            passbook_url = (
                href_pb if href_pb.startswith("http")
                else f"https://{DOMAIN}/{href_pb.lstrip('/')}"
            )
            break

    bank: dict = {}
    if any(bank_kv.values()):
        bank = {
            "bank_name":  _f(bank_kv, "bank name"),
            "branch":     _f(bank_kv, "branch name"),
            "account_no": _f(bank_kv, "account no."),
            "IFSC":       _f(bank_kv, "ifsc code"),
        }
        if passbook_url:
            bank["scan_copy_of_cheque"] = passbook_url

    contact: dict = {}
    mobile    = _f(prom_kv, "mobile number")
    email     = _f(prom_kv, "e-mail")
    telephone = _f(contact_kv, "telephone no.")
    if mobile or email:
        contact = {"phone": mobile, "email": email}
        if telephone and telephone not in ("0", "0000", "N/A"):
            contact["telephone_no"] = telephone

    addr: dict = {}
    reg_addr = _f(contact_kv, "register address")
    if reg_addr:
        addr = {"address": reg_addr}

    def _safe_float(val: str) -> float | None:
        try:
            v = float(val.replace(",", ""))
            return v if v > 0 else None
        except (ValueError, AttributeError):
            return None

    # ── Promoter entity details → normalized promoters_details field ──────────
    prom_details: dict = {
        "type_of_firm":    _f(prom_kv, "promoter type"),
        "pan_no":          _f(prom_kv, "pan number"),
        "registration_no": _f(prom_kv, "company registration no /deed no."),
    }
    prom_details = {k: v for k, v in prom_details.items() if v}

    # ── Document links ────────────────────────────────────────────────────────
    # Strategy:
    #   1. Use the GV_Doc table (new format) for proper Document Type labels on
    #      All_Document PDFs — the link text is a raw Windows path, not a label.
    #      Skip rows with no href (e.g. "Brochure of Current Project").
    #   2. After GV_Doc, do a supplemental scan for any PDF anchors elsewhere on
    #      the page (e.g. "Mutation(Correction-Slip)" rows) that were not captured
    #      by GV_Doc.  These use a simple [Label | View] row pattern.
    #   3. Append the Registration Certificate at the END from its anchor href.
    #      This matches the sample order: GV_Doc docs first, Reg Cert last.
    #   4. Fall back to anchor-href scan when GV_Doc is absent (old page format).
    docs: list[dict] = []

    # Registration Certificate — collected here, appended after GV_Doc rows
    reg_cert_doc: dict | None = None
    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        if "Registration_Certificate/" in href and ".pdf" in href.lower():
            full_url = href if href.startswith("http") else f"https://{DOMAIN}/{href.lstrip('/')}"
            reg_cert_doc = {"link": full_url, "type": "Registration Certificate"}
            break  # only one reg cert

    # GV_Doc table: proper document type → file path mapping
    doc_table = soup.find("table", id="GV_Doc")
    if doc_table:
        for row in doc_table.find_all("tr")[1:]:   # skip header
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            doc_type = cells[0].get_text(strip=True)
            # The "Link for file" cell may contain an <a> or just the raw Windows path text
            a_tag = cells[2].find("a", href=True)
            raw_path = a_tag["href"] if a_tag else cells[2].get_text(strip=True)
            # Convert Windows path  ~\All_Document\...pdf  →  All_Document/...pdf
            raw_path = raw_path.replace("~\\", "").replace("\\", "/").lstrip("/")
            if not raw_path or not doc_type:
                continue
            full_url = raw_path if raw_path.startswith("http") else f"https://{DOMAIN}/{raw_path}"
            docs.append({"link": full_url, "type": doc_type})

        # ── Supplemental scan: catch PDF anchors outside GV_Doc ───────────────
        # Bihar RERA pages sometimes contain extra documents (e.g. Mutation /
        # Correction-Slip) in simple [Label | View] rows that sit outside GV_Doc.
        # When GV_Doc is present the else-branch below is skipped, so we do a
        # targeted second pass here to pick those up.
        seen_urls = {d["link"] for d in docs}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" not in href.lower():
                continue
            if "Registration_Certificate/" in href or "PassBook/" in href:
                continue  # handled separately
            raw_path = href.replace("~\\", "").replace("\\", "/").lstrip("/")
            full_url = raw_path if raw_path.startswith("http") else f"https://{DOMAIN}/{raw_path}"
            if full_url in seen_urls:
                continue  # already captured via GV_Doc
            # Derive label from the sibling cell in the parent <tr> (if any).
            row = a.find_parent("tr")
            if row:
                sibling_texts = [
                    c.get_text(strip=True)
                    for c in row.find_all(["td", "th"])
                    if c.get_text(strip=True) not in ("", "View", "Download")
                ]
                doc_type = sibling_texts[0] if sibling_texts else "Project Document"
            else:
                doc_type = a.get_text(separator=" ", strip=True) or "Project Document"
            docs.append({"link": full_url, "type": doc_type})
            seen_urls.add(full_url)
    else:
        # Fallback for older page format — scan all PDF anchors
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" not in href.lower():
                continue
            if "/Registration_Certificate/" in href or "/PassBook/" in href:
                continue  # handled separately
            full_url = href if href.startswith("http") else f"https://{DOMAIN}/{href.lstrip('/')}"
            raw_label = a.get_text(separator=" ", strip=True)
            doc_type = raw_label or "Project Document"
            docs.append({"link": full_url, "type": doc_type})

    # Append Registration Certificate at the end (matches sample ordering)
    if reg_cert_doc:
        docs.append(reg_cert_doc)

    # Keep raw strings so trailing zeros (e.g. "8102.60") are preserved
    land_area_raw    = _f(proj_kv, "total area of land (sq mt)")
    land_area_val    = _safe_float(land_area_raw)
    # "Total Builtup Area" is the correct total; "Total Covered Area" is a per-unit figure
    const_area_raw   = _f(proj_kv, "total builtup area (sq. mtr.)")
    const_area_val   = _safe_float(const_area_raw)
    covered_area_val = _safe_float(_f(proj_kv, "total covered area (sq mtr)"))
    land_area_details: dict | None = None
    if land_area_val or const_area_val:
        land_area_details = {
            "land_area": land_area_raw if land_area_raw else None,
            "land_area_unit": "Total Area of Land (Sq mt)",
            "construction_area": const_area_raw if const_area_raw else None,
            "construction_area_unit": "Total Builtup Area (Sq. Mtr.)",
        }

    out: dict = {
        # project info
        "project_type":              _f(proj_kv, "project type"),
        "status_of_the_project":     _f(proj_kv, "project status"),
        "project_description":       _f(proj_kv, "project description"),
        "estimated_commencement_date": _f(proj_kv, "project start date"),
        "estimated_finish_date":     _f(proj_kv, "project end date"),
        "land_area":                 land_area_val,
        "construction_area":         const_area_val,
        "land_area_details":         land_area_details,
        "total_floor_area_under_residential": covered_area_val if covered_area_val and not const_area_val else None,
        # location
        "project_location_raw": {k: v for k, v in loc_raw.items() if v},
        "project_city":          _f(loc_kv, "city/town"),
        "project_pin_code":      pin_code or None,
        # promoter / contact
        "promoter_contact_details": contact or None,
        "promoter_address_raw":     addr or None,
        # promoter entity metadata
        "promoters_details": prom_details or None,
        # structured
        "bank_details":          bank or None,
        "members_details":       members or None,
        "professional_information": vendors or None,
        "building_details":      buildings or None,
        # documents
        "uploaded_documents": docs or None,
        # project cost — values must be strings per schema
        "project_cost_detail": {
            k: str(v) for k, v in {
                "estimated_construction_cost": _safe_float(_f(proj_kv, "estimated cost of development (in lakh)")),
                "cost_of_land":               _safe_float(_f(proj_kv, "estimated cost of land (in lakh)")),
            }.items() if v is not None
        },
        # data JSONB — only Bihar-allowed keys: link, type, govt_type, land_area_unit, construction_area_unit
        "data": {
            "govt_type":              "state",
            "land_area_unit":         "Total Area of Land (Sq mt)" if land_area_val else None,
            "construction_area_unit": "Total Builtup Area (Sq. Mtr.)" if const_area_val else None,
        },
    }
    # Inject registration certificate link+type into data if available
    reg_cert_url = next(
        (d["link"] for d in docs if "/Registration_Certificate/" in d.get("link", "")),
        None,
    )
    if reg_cert_url:
        out["data"]["link"] = reg_cert_url
        out["data"]["type"] = "Registration Certificate"
    # Strip None-valued keys from nested dicts
    out["project_cost_detail"] = {k: v for k, v in out["project_cost_detail"].items() if v is not None}
    return out


# ── Sentinel ──────────────────────────────────────────────────────────────────

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Bihar RERA.
    Full-flow check: fetches the listing page (structural verification + listing-level
    fields) and the Filanprint detail page from the baseline URL, merges both, and
    verifies ≥ 80% field coverage against the full baseline.
    """
    import json as _json
    import os as _os
    from core.sentinel_utils import check_field_coverage

    sentinel_reg = config.get("sentinel_registration_no", "")

    sample_path = _os.path.join(
        _os.path.dirname(_os.path.dirname(__file__)),
        "state_projects_sample", "bihar.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check",
                       path=sample_path, step="sentinel")
        return True

    # ── Step 1: Verify listing page is reachable and has expected structure ────
    resp = safe_get(LISTING_URL, retries=2, logger=logger, verify=False)
    if not resp:
        logger.error("Sentinel: listing page unreachable", step="sentinel")
        insert_crawl_error(run_id, config["id"], "SENTINEL_FAILED",
                           "Listing page unreachable", url=LISTING_URL)
        return False

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table", id=_GRID_ID)
    if not table or not table.find("tr"):
        logger.error("Sentinel: project table not found in response", step="sentinel")
        insert_crawl_error(run_id, config["id"], "SENTINEL_FAILED",
                           "Project table not found", url=LISTING_URL)
        return False

    data_rows = _parse_page_rows(soup)
    if not data_rows:
        logger.error("Sentinel: GridView table has no data rows — site structure may have changed",
                     step="sentinel")
        insert_crawl_error(run_id, config["id"], "SENTINEL_FAILED",
                           "No data rows on listing page", url=LISTING_URL)
        return False

    # Find sentinel project row; fall back to first row for structural check
    listing_row = next(
        (r for r in data_rows
         if r.get("project_registration_no", "").upper() == sentinel_reg.upper()),
        data_rows[0],
    )
    fresh = dict(listing_row)

    # ── Step 2: Discover Filanprint detail URL via Playwright ─────────────────
    # The sentinel project is always on page 1 of the listing (data_rows).
    # Find its row index first so we only collect that many popup URLs instead
    # of crawling all pages (which would take minutes / hours).
    logger.info(f"Sentinel: discovering Filanprint URL via Playwright", step="sentinel")
    detail_url: str = ""
    try:
        sentinel_row_idx = next(
            (i for i, r in enumerate(data_rows)
             if r.get("project_registration_no", "").upper() == sentinel_reg.upper()),
            0,  # fallback: use first row
        )
        # Collect only enough URLs to reach the sentinel row on page 1
        sentinel_url_list = _collect_detail_urls(logger, max_items=sentinel_row_idx + 1)
        if sentinel_row_idx < len(sentinel_url_list) and sentinel_url_list[sentinel_row_idx]:
            detail_url = sentinel_url_list[sentinel_row_idx]
        if not detail_url:
            # Fallback: use any valid Filanprint URL collected
            detail_url = next((u for u in sentinel_url_list if u and "Filanprint.aspx" in u), "")
            if detail_url:
                logger.warning("Sentinel: exact project not found, using first available Filanprint URL",
                               step="sentinel")
    except Exception as exc:
        logger.warning(f"Sentinel: Playwright discovery failed — {exc}", step="sentinel")

    if detail_url and "Filanprint.aspx" in detail_url:
        logger.info(f"Sentinel: fetching Filanprint detail for {sentinel_reg}",
                    url=detail_url, step="sentinel")
        try:
            detail_resp = safe_get(detail_url, retries=2, logger=logger, verify=False)
            if detail_resp:
                detail_extra = _parse_detail_page(detail_resp.text)
                if detail_extra:
                    fresh.update(detail_extra)
                    logger.info("Sentinel: detail page parsed successfully", step="sentinel")
                else:
                    logger.warning("Sentinel: detail page yielded no fields", step="sentinel")
            else:
                logger.warning("Sentinel: Filanprint detail page unreachable — "
                               "using listing fields only", url=detail_url, step="sentinel")
        except Exception as exc:
            logger.warning(f"Sentinel: detail fetch error — {exc}", step="sentinel")
    else:
        logger.warning("Sentinel: could not discover Filanprint URL — "
                       "detail page coverage check skipped", step="sentinel")

    # project_state is injected from config in run(), not scraped from the page
    fresh["project_state"] = config.get("state", "Bihar").title()

    logger.info(f"Sentinel: checking coverage for {sentinel_reg}", step="sentinel")
    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "bihar_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


# ── Document processing ────────────────────────────────────────────────────────

def _process_documents(
    project_key: str,
    documents: list[dict],
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
) -> tuple[list[dict], int]:
    """Download, MD5-check, and upload each document PDF to S3.

    Follows the spec's process_documents() pattern:
      for each PDF: download → compute_md5 → compare with rera_project_documents
                  → upload to S3 if new/changed → update rera_project_documents row

    Returns:
        enriched_documents: same list with 's3_link' injected for uploaded docs
        upload_count: number of documents actually uploaded (new or changed)
    """
    enriched: list[dict] = []
    upload_count = 0
    doc_name_counts: dict[str, int] = {}

    for doc in documents:
        selected = select_document_for_download("bihar", doc, doc_name_counts, domain=DOMAIN)
        if not selected:
            enriched.append(doc)
            continue

        url = selected.get("link") or selected.get("url")
        doc_type = selected.get("type", "document")
        if not url:
            enriched.append(selected)
            continue

        reused, existing_s3_key = existing_uploaded_document_entry(
            project_key, {**selected, "link": url}
        )
        if reused:
            logger.info(f"Document reused: {doc_type!r}", s3_key=existing_s3_key, step="documents")
            logger.log_document(doc_type, url, "reused", s3_key=existing_s3_key)
            enriched.append(reused)
            continue

        filename = build_document_filename(selected)

        try:
            resp = download_response(url, logger=logger, timeout=60.0, verify=False)
            if not resp or len(resp.content) < 100:
                enriched.append(selected)
                logger.warning(f"Document download failed or too small: {url}", step="documents")
                continue

            data = resp.content
            md5 = compute_md5(data)
            s3_key = upload_document(
                project_key, filename, data, dry_run=settings.DRY_RUN_S3
            )
            if s3_key is None:
                enriched.append(selected)
                logger.warning(f"S3 upload returned None for {url}", step="documents")
                continue

            s3_url = get_s3_url(s3_key)
            upsert_document(
                project_key=project_key,
                document_type=doc_type,
                original_url=url,
                s3_key=s3_key,
                s3_bucket=settings.S3_BUCKET_NAME,
                file_name=filename,
                md5_checksum=md5,
                file_size_bytes=len(data),
            )
            enriched.append({**selected, "link": url, "s3_link": s3_url, "updated": True})
            upload_count += 1
            logger.info(f"Document uploaded: {doc_type!r}", s3_key=s3_key, step="documents")
            logger.log_document(doc_type, url, "uploaded", s3_key=s3_key, file_size_bytes=len(data))

        except Exception as exc:
            enriched.append(selected)
            logger.error(f"Document processing error: {exc}", url=url, step="documents")
            insert_crawl_error(
                run_id, site_id, "S3_UPLOAD_FAILED", str(exc),
                url=url, project_key=project_key,
            )

    return enriched, upload_count


# ── Parsing ───────────────────────────────────────────────────────────────────

def _extract_form_fields(soup: BeautifulSoup) -> dict:
    """Pull all hidden ASP.NET form fields needed for __doPostBack POSTs."""
    fields = {}
    for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION",
                 "__LASTFOCUS", "__EVENTTARGET", "__EVENTARGUMENT"):
        tag = soup.find("input", {"name": name})
        fields[name] = tag["value"] if tag and tag.get("value") else ""
    return fields


def _has_next_page(soup: BeautifulSoup, current_page: int) -> bool:
    """Return True if the GridView pager has a link for page current_page+1 or a '...' overflow."""
    table = soup.find("table", id=_GRID_ID)
    if not table:
        return False
    pager_row = table.find_all("tr")[-1]
    for a in pager_row.find_all("a"):
        txt = a.get_text(strip=True)
        if txt == "..." or (txt.isdigit() and int(txt) > current_page):
            return True
    return False


def _parse_page_rows(soup: BeautifulSoup) -> list[dict]:
    """Extract project rows from a single listing page (skips header and pager rows).

    Handles both source variants seen on Bihar RERA:
    - direct <table> > <tr> rows in raw HTTP responses
    - <table> > <tbody> > <tr> rows in browser-rendered DOM snapshots
    The selector remains scoped to direct children so the nested pager table is
    never traversed.
    """
    rows = soup.select(f"table#{_GRID_ID} > tr, table#{_GRID_ID} > tbody > tr")
    if not rows:
        return []
    projects = []
    for row in rows:
        cells = row.find_all("td", recursive=False)
        if len(cells) < 5:
            continue   # header row has <th>, pager row has 1 colspan <td>
        reg_no = cells[1].get_text(strip=True)
        # Skip pager rows that leaked through (should not happen with direct selector)
        if not reg_no or reg_no.isdigit():
            continue
        projects.append({
            "project_name":            cells[0].get_text(separator=" ", strip=True),
            "project_registration_no": reg_no,
            "promoter_name":           cells[2].get_text(separator=" ", strip=True),
            "project_location_raw":    {"address": cells[3].get_text(separator=" ", strip=True)},
            "submitted_date":          cells[4].get_text(strip=True),
        })
    return projects


def _fetch_page(page: int, form_fields: dict, logger: CrawlerLogger) -> BeautifulSoup | None:
    """POST to navigate to an ASP.NET GridView page via __doPostBack."""
    payload = {
        **form_fields,
        "__EVENTTARGET":   _GRID_TARGET,
        "__EVENTARGUMENT": f"Page${page}",
    }
    resp = safe_post(LISTING_URL, data=payload, retries=3, logger=logger, verify=False)
    if not resp:
        return None
    return BeautifulSoup(resp.text, "lxml")


# ── Main entry point ──────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    logger = CrawlerLogger(config["id"], run_id)
    counters = dict(projects_found=0, projects_new=0, projects_updated=0,
                    projects_skipped=0, documents_uploaded=0, error_count=0)
    machine_name, machine_ip = get_machine_context()
    item_limit    = settings.CRAWL_ITEM_LIMIT or 0  # 0 = unlimited
    items_processed = 0
    t_run = time.monotonic()

    # ── Sentinel health check ────────────────────────────────────────────────
    t0 = time.monotonic()
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counters["error_count"] += 1
        return counters
    logger.timing("sentinel", time.monotonic() - t0)

    max_pages    = settings.MAX_PAGES
    delay_range  = config.get("rate_limit_delay", (2, 4))

    # ── Step 1: Collect paginated listing rows + popup URLs via Playwright ──
    t0 = time.monotonic()
    logger.info(
        f"Collecting paginated listing data via Playwright"
        f" (item_limit={item_limit or 'unlimited'}, max_pages={max_pages or 'unlimited'})",
        step="detail_collect",
    )
    listing_pages = _collect_listing_pages(
        logger,
        max_items=item_limit or None,
        max_pages=max_pages or None,
    )
    logger.info(
        f"Collected {sum(len(p['rows']) for p in listing_pages)} listing rows across"
        f" {len(listing_pages)} page(s);"
        f" {sum(1 for p in listing_pages for u in p['detail_urls'] if u)} detail URLs captured",
        step="detail_collect",
    )
    if not listing_pages:
        logger.error("Failed to collect any listing pages", step="listing")
        insert_crawl_error(run_id, config["id"], "HTTP_ERROR", "listing pagination failed", LISTING_URL)
        counters["error_count"] += 1
        return counters

    stop_all     = False
    logger.warning(
        f"Step timing [search]: {time.monotonic()-t0:.2f}s"
        f"  pages={len(listing_pages)}"
        f"  rows={sum(len(p['rows']) for p in listing_pages)}",
        step="timing",
    )

    for payload in listing_pages:
        if stop_all:
            break
        current_page = payload["page"]
        rows = payload["rows"]
        detail_urls = payload["detail_urls"]
        counters["projects_found"] += len(rows)
        logger.info(f"Page {current_page}: {len(rows)} projects", step="listing")

        for row_idx, raw in enumerate(rows):
            if item_limit and items_processed >= item_limit:
                logger.info(f"Item limit {item_limit} reached — stopping", step="listing")
                stop_all = True
                break
            reg_no = raw.get("project_registration_no", "").strip()
            if not reg_no:
                counters["error_count"] += 1
                continue

            key = generate_project_key(reg_no)
            detail_url: str = ""
            if row_idx < len(detail_urls) and detail_urls[row_idx]:
                detail_url = detail_urls[row_idx]

            # ── daily_light: skip projects already in the DB ──────────────────
            if mode == "daily_light" and get_project_by_key(key):
                counters["projects_skipped"] += 1
                continue

            logger.set_project(
                key=key,
                reg_no=reg_no,
                url=detail_url or LISTING_URL,
                page=current_page,
            )
            try:

                # ── Step 3: Fetch & parse detail page ────────────────────────────
                # Use global positional alignment: detail_url_list[global_idx]
                # matches the listing row at (listing_page, row_idx).
                detail_extra: dict = {}
                if detail_url:
                    detail_resp = safe_get(detail_url, retries=2, logger=logger, verify=False)
                    if detail_resp and "Invalid Project ID" not in detail_resp.text:
                        detail_extra = _parse_detail_page(detail_resp.text)
                        logger.info(f"Detail parsed for {reg_no!r}", step="detail")
                    elif detail_resp:
                        logger.warning(
                            f"Detail page returned 'Invalid Project ID' for {reg_no!r}",
                            step="detail",
                        )
                    else:
                        logger.warning(f"Detail fetch failed for {reg_no!r}", step="detail")
                else:
                    logger.warning(
                        f"No detail URL for listing page {current_page} row {row_idx} ({reg_no!r})",
                        step="detail",
                    )

                # ── Step 4: Merge listing + detail, upsert ───────────────────────
                try:
                    # listing fields win for the core identifiers; detail fills gaps
                    merged: dict = {
                        **detail_extra,
                        # listing fields always take priority for these
                        "project_name":            raw["project_name"],
                        "project_registration_no": reg_no,
                        "promoter_name":           raw["promoter_name"],
                        "submitted_date":          raw["submitted_date"],
                        # merge location: detail loc_raw is richer; listing address as fallback
                        "project_location_raw": {
                            **raw.get("project_location_raw", {}),
                            **detail_extra.get("project_location_raw", {}),
                            "state": config.get("state", "bihar").title(),
                        },
                        "project_state": config.get("state", "bihar").title(),
                        "domain": DOMAIN,
                        # Store the canonical base domain as url (matches sample).
                        # The Filanprint detail URL is a session-specific popup URL
                        # that is not stable across crawls.
                        "url":    f"https://{DOMAIN}",
                        "state":  config.get("state", "Bihar"),
                        "is_live": True,
                        # merge data sub-dicts
                        "data": merge_data_sections(
                            detail_extra.get("data"),
                            {"listing_address": raw.get("project_location_raw", {}).get("address", "")},
                        ),
                    }
                    # Remove None values to avoid overwriting good DB data with nulls
                    merged = {k: v for k, v in merged.items() if v is not None}

                    normalized = normalize_project_payload(
                        merged, config,
                        machine_name=machine_name,
                        machine_ip=machine_ip,
                    )
                    record  = ProjectRecord(**normalized)
                    db_dict = record.to_db_dict()
                    status  = upsert_project(db_dict)
                    items_processed += 1

                    if status == "new":
                        counters["projects_new"] += 1
                        logger.info(f"New project: {reg_no}", step="upsert")
                    elif status == "updated":
                        counters["projects_updated"] += 1
                        logger.info(f"Updated: {reg_no}", step="upsert")
                    else:
                        counters["projects_skipped"] += 1

                    # ── Step 5: Process documents (weekly_deep or new projects) ──
                    # Spec §13: process_documents() → download → md5 → S3 upload
                    uploaded_docs = detail_extra.get("uploaded_documents") or []
                    if uploaded_docs:
                        enriched_docs, doc_count = _process_documents(
                            key, uploaded_docs, run_id, config["id"], logger,
                        )
                        counters["documents_uploaded"] += doc_count
                        if doc_count:
                            # Write enriched uploaded_documents + derived document_urls back
                            doc_urls = [
                                {"link": d["s3_link"], "type": d.get("type")}
                                for d in enriched_docs if d.get("s3_link")
                            ]
                            upsert_project({
                                "key": key,
                                "uploaded_documents": enriched_docs,
                                "document_urls": doc_urls,
                            })

                except ValidationError as exc:
                    counters["error_count"] += 1
                    logger.error(f"Validation error for {reg_no}: {exc}", step="validate")
                    insert_crawl_error(run_id, config["id"], "VALIDATION_FAILED", str(exc),
                                       project_key=key, url=detail_url or LISTING_URL)
                except Exception as exc:
                    counters["error_count"] += 1
                    logger.error(f"Unexpected error for {reg_no}: {exc}", step="upsert")
                    insert_crawl_error(run_id, config["id"], "CRAWLER_EXCEPTION", str(exc),
                                       project_key=key, url=detail_url or LISTING_URL)
            finally:
                logger.clear_project()

            random_delay(*delay_range)

    reset_checkpoint(config["id"], mode)
    logger.info(f"Bihar RERA complete: {counters}", step="done")
    logger.timing("total_run", time.monotonic() - t_run)
    return counters
