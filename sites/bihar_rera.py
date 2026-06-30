"""
Bihar RERA Crawler — rera.bihar.gov.in/RegisteredPP.aspx
Type: httpx listing + Selenium detail (popup capture)

Strategy:
- The listing page is a server-side ASP.NET GridView paginated via ASP.NET postbacks.
- Listing pagination is driven with Selenium because direct httpx page-turn POSTs
  intermittently fail with server-side ViewState / cluster mismatches.
- Each project name is a link that triggers __doPostBack('...', 'PrintIndicator$N'),
  which opens a popup window at Filanprint.aspx?id=RERAP...
- Selenium clicks each listing row and captures the popup URL via
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

import base64
import re
import time
from datetime import datetime
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.checkpoint import reset_checkpoint
from core.config import settings
from core.crawler_base import (
    SeleniumSession,
    SeleniumTimeout,
    generate_project_key,
    get_target_reg_nos,
    page_adapter,
    random_delay,
)
from core.db import (
    get_project_by_key,
    upsert_project,
    upsert_document,
    insert_crawl_error,
    update_crawl_run_progress,
)
from core.details_pool import get_detail_workers, process_details
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
# QPR (Quarterly Progress Report) listing page — keyed on the project's <rid>-<N> id.
QRCODE_URL   = "https://rera.bihar.gov.in/QRCODE.aspx"
STATE_CODE   = "BR"
DOMAIN       = "rera.bihar.gov.in"
# ASP.NET GridView control ID
_GRID_ID     = "ContentPlaceHolder1_GV_Building"
_GRID_TARGET = "ctl00$ContentPlaceHolder1$GV_Building"


# ── SeleniumSession wiring ────────────────────────────────────────────────────

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
    """Backwards-compatible shim — dispatches through the SeleniumSession."""
    plt = float(timeout) if isinstance(timeout, (int, float)) and timeout else None
    return _session().get(url, logger=logger, page_load_timeout=plt)


def safe_post(url, *, data=None, headers=None, logger=None, **_ignored):
    """Backwards-compatible shim — dispatches through the SeleniumSession."""
    return _session().post(url, data=data, headers=headers, logger=logger)


def download_response(url, *, method="GET", data=None, headers=None,
                      logger=None, **_ignored):
    """Backwards-compatible shim — dispatches through the SeleniumSession."""
    return _session().download(url, method=method, data=data, headers=headers, logger=logger)


# ── Selenium-driven listing walker (was Selenium) ────────────────────────────

def _collect_listing_pages(
    logger: CrawlerLogger,
    max_items: int | None = None,
    max_pages: int | None = None,
    capture_detail_urls: bool = True,
    on_progress=None,
    on_candidate=None,
    target_regs: set[str] | None = None,
    found_targets: set[str] | None = None,
) -> list[dict]:
    """
    Use Selenium to traverse listing pages and capture aligned detail popup URLs.

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

    max_items: when set, stop after collecting this many URLs (for CRAWL_ITEM_LIMIT).
        Bihar's listing requires Selenium pagination with per-row popup
        clicks to surface detail URLs, so a full count-only walk would take
        many minutes; honouring the cap short-circuits the walk and
        projects_found reflects only the rows actually collected.

    on_candidate: optional callback fired per-row immediately after the popup is
        clicked and the detail URL/HTML is captured (but before closing). Signature:
            on_candidate(raw: dict, detail_url: str, detail_html: str | None, page: int)
        Enables inline parse+upsert during the listing walk so the dashboard's
        per-project counters climb in real time rather than only after Phase B.
    """
    links_sel = f"table#{_GRID_ID} tr td:first-child a"
    pages: list[dict] = []
    items_seen = 0

    try:
        listing_page = page_adapter(_session())
        ctx = listing_page.context
        listing_page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=30_000)
        current_listing_pg = 1

        while True:
                soup = BeautifulSoup(listing_page.content(), "lxml")
                rows = _parse_page_rows(soup)
                if not rows:
                    logger.info(
                        f"Selenium: listing page {current_listing_pg} has no parsed rows — stopping",
                        step="detail_collect",
                    )
                    break

                # Cap the number of rows for which we capture detail URLs (the
                # expensive popup-click step), but always keep the full row list
                # so projects_found counts every project on the page.
                rows_for_detail = rows
                if max_items:
                    remaining = max(0, max_items - items_seen)
                    rows_for_detail = rows[:remaining]

                # ── Identify project links on this listing page ───────────────
                link_texts: list[str] = listing_page.eval_on_selector_all(
                    links_sel, "els => els.map(e => e.innerText.trim())"
                )
                project_indices = [
                    i for i, t in enumerate(link_texts)
                    if t and not t.isdigit()
                    and t not in ("...", "Next", "Prev", "Previous", "First", "Last")
                ]

                project_indices = project_indices[:len(rows_for_detail)]

                # ── Targeted filtering ─────────────────────────────────────────
                # --target-reg-no restricts the walk to specific project(s).
                # Filter this page's rows (and the positionally-aligned project
                # links) down to the requested reg-no(s); the walk stops once
                # every target has been found (see end of the page loop).
                if target_regs:
                    keep = [
                        rank for rank, r in enumerate(rows_for_detail)
                        if (r.get("project_registration_no") or "").strip().upper()
                        in target_regs
                    ]
                    rows_for_detail = [rows_for_detail[rank] for rank in keep]
                    project_indices = [
                        project_indices[rank] for rank in keep
                        if rank < len(project_indices)
                    ]
                    rows = rows_for_detail
                    if found_targets is not None:
                        found_targets.update(
                            (r.get("project_registration_no") or "").strip().upper()
                            for r in rows_for_detail
                        )

                if rows_for_detail and not project_indices:
                    logger.info(
                        f"Selenium: listing page {current_listing_pg} has no project links — stopping",
                        step="detail_collect",
                    )
                    break

                detail_urls: list[str | None]
                if capture_detail_urls and rows_for_detail:
                    logger.info(
                        f"Selenium: listing page {current_listing_pg},"
                        f" collecting {len(project_indices)} detail URLs for {len(rows)} rows",
                        step="detail_collect",
                    )
                    if len(project_indices) != len(rows):
                        logger.warning(
                            f"Selenium: row/link mismatch on page {current_listing_pg}:"
                            f" rows={len(rows)} links={len(project_indices)}",
                            step="detail_collect",
                        )

                    # ── Click each project; listing page stays alive between clicks ─
                    detail_urls = []
                    for rank, idx in enumerate(project_indices):
                        name = link_texts[idx]
                        captured_url: str | None = None
                        captured_html: str | None = None
                        try:
                            with ctx.expect_page(timeout=15_000) as popup_info:
                                listing_page.eval_on_selector_all(
                                    links_sel, f"els => els[{idx}].click()"
                                )
                            popup = popup_info.value
                            popup.wait_for_load_state("domcontentloaded", timeout=15_000)
                            url = popup.url
                            # Capture the popup's rendered HTML before closing so
                            # downstream inline processing can parse it without a
                            # second Selenium GET (which would navigate the
                            # listing tab away).
                            if "Filanprint.aspx" in url:
                                try:
                                    captured_html = popup.content()
                                except Exception as exc:
                                    logger.warning(
                                        f"  [pg{current_listing_pg}:{rank}] {name!r}:"
                                        f" popup HTML capture failed — {exc}",
                                        step="detail_collect",
                                    )
                            popup.close()
                            # Postback already reloaded the listing page in-place;
                            # wait for it to settle before the next click.
                            listing_page.wait_for_load_state("domcontentloaded", timeout=15_000)

                            if "Filanprint.aspx" in url:
                                captured_url = url
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

                        # Fire the per-candidate callback immediately so parse +
                        # upsert can run interleaved with the listing walk.
                        if on_candidate is not None and rank < len(rows_for_detail):
                            try:
                                on_candidate(
                                    rows_for_detail[rank],
                                    captured_url or "",
                                    captured_html,
                                    current_listing_pg,
                                )
                            except Exception as exc:
                                logger.warning(
                                    f"  [pg{current_listing_pg}:{rank}] on_candidate raised: {exc}",
                                    step="detail_collect",
                                )

                    if len(detail_urls) < len(rows):
                        detail_urls.extend([None] * (len(rows) - len(detail_urls)))
                else:
                    logger.info(
                        f"Selenium: listing page {current_listing_pg}, parsed {len(rows)} rows",
                        step="detail_collect",
                    )
                    detail_urls = [None] * len(rows)
                    # daily_light / count-only walk: still invoke the candidate
                    # callback so the orchestrator can run its dedup skip path
                    # inline (avoids a separate post-listing iteration).
                    if on_candidate is not None:
                        for rank, raw in enumerate(rows_for_detail):
                            try:
                                on_candidate(raw, "", None, current_listing_pg)
                            except Exception as exc:
                                logger.warning(
                                    f"  [pg{current_listing_pg}:{rank}] on_candidate raised: {exc}",
                                    step="detail_collect",
                                )

                pages.append({
                    "page": current_listing_pg,
                    "rows": rows,
                    "detail_urls": detail_urls,
                })
                items_seen += len(rows)

                # Stream listing progress so the dashboard's projects_found
                # climbs page-by-page rather than jumping at the end.
                if on_progress is not None:
                    try:
                        on_progress(items_seen)
                    except Exception:
                        pass

                if (max_items and items_seen >= max_items) or (max_pages and current_listing_pg >= max_pages):
                    break

                # Targeted run: stop once every requested project has been found.
                if target_regs and found_targets is not None and target_regs <= found_targets:
                    logger.info(
                        "All targeted projects found — stopping listing walk",
                        step="detail_collect",
                    )
                    break

                # ── Navigate to the next listing page ─────────────────────────
                # The pager exposes the next page as an anchor whose href is a
                # javascript:__doPostBack(...) call.  The same href targets both
                # the direct numeric link and the "..." overflow link (which
                # jumps to the first page of the next block), so one selector
                # covers every case.  A physical Selenium click on the pager row
                # (anchored to the page bottom) is unreliable — Chrome reports
                # "element click intercepted" — so trigger the anchor's postback
                # via a JS click, mirroring the per-project popup-click path.
                next_pg = current_listing_pg + 1
                next_href = (
                    f"javascript:__doPostBack('{_GRID_TARGET}','Page${next_pg}')"
                )
                clicked = listing_page.eval_on_selector_all(
                    f'a[href="{next_href}"]',
                    "els => { if (els[0]) { els[0].click(); return true; } return false; }",
                )
                if clicked:
                    listing_page.wait_for_load_state("domcontentloaded", timeout=15_000)
                    # The postback reloads the GridView in place; confirm the
                    # pager's active page (rendered as a <span>) advanced before
                    # parsing so we never read a stale page.
                    try:
                        listing_page.wait_for_function(
                            "() => { var s = document.querySelector("
                            f"\"table#{_GRID_ID} tr.pagingDiv span\");"
                            f" return s && s.innerText.trim() === '{next_pg}'; }}",
                            timeout=15_000,
                        )
                    except SeleniumTimeout:
                        logger.warning(
                            f"Selenium: pager did not confirm page {next_pg} — proceeding",
                            step="detail_collect",
                        )
                    current_listing_pg = next_pg
                else:
                    logger.info(
                        f"Selenium: no link to page {next_pg} — all pages collected",
                        step="detail_collect",
                    )
                    break

        listing_page.close()
    except Exception as e:
        logger.error(f"Selenium detail-url collection failed: {e}", step="detail_collect")
    return pages


def _collect_detail_urls(logger: CrawlerLogger, max_items: int | None = None) -> list[str | None]:
    """Backward-compatible wrapper that flattens Selenium listing page payloads."""
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
        "project_type":              _f(proj_kv, "project type"),  # FIELD: project_type <- proj_kv label "project type"
        "status_of_the_project":     _f(proj_kv, "project status"),  # FIELD: status_of_the_project <- proj_kv label "project status"
        # FIELD: project_description <- proj_kv label "project description"
        "project_description":       _f(proj_kv, "project description"),
        # FIELD: estimated_commencement_date <- proj_kv label "project start date"
        "estimated_commencement_date": _f(proj_kv, "project start date"),
        "estimated_finish_date":     _f(proj_kv, "project end date"),  # FIELD: estimated_finish_date <- proj_kv label "project end date"
        "land_area":                 land_area_val,  # FIELD: land_area <- _safe_float(land_area_raw)
        "construction_area":         const_area_val,  # FIELD: construction_area <- _safe_float(const_area_raw)
        "land_area_details":         land_area_details,  # FIELD: land_area_details <- land_area_details local dict
        # FIELD: total_floor_area_under_residential <- covered_area_val when const_area_val is empty
        "total_floor_area_under_residential": covered_area_val if covered_area_val and not const_area_val else None,
        # location
        "project_location_raw": {k: v for k, v in loc_raw.items() if v},  # FIELD: project_location_raw <- filtered loc_raw dict
        "project_city":          _f(loc_kv, "city/town"),  # FIELD: project_city <- loc_kv label "city/town"
        "project_pin_code":      pin_code or None,  # FIELD: project_pin_code <- pin_code var (loc_kv or contact_kv)
        # promoter / contact
        "promoter_contact_details": contact or None,  # FIELD: promoter_contact_details <- contact local dict
        "promoter_address_raw":     addr or None,  # FIELD: promoter_address_raw <- addr local dict
        # promoter entity metadata
        "promoters_details": prom_details or None,  # FIELD: promoters_details <- prom_details local dict
        # structured
        "bank_details":          bank or None,  # FIELD: bank_details <- bank local dict
        "members_details":       members or None,  # FIELD: members_details <- members from GV_Member rows
        "professional_information": vendors or None,  # FIELD: professional_information <- vendors from GV_Vendor rows
        "building_details":      buildings or None,  # FIELD: building_details <- buildings from GV_Building rows
        # documents
        "uploaded_documents": docs or None,  # FIELD: uploaded_documents <- docs list (GV_Doc + reg cert)
        # project cost — values must be strings per schema
        "project_cost_detail": {  # FIELD: project_cost_detail <- filtered dict comp over _safe_float values
            k: str(v) for k, v in {
                # FIELD: project_cost_detail.estimated_construction_cost <- proj_kv "estimated cost of development (in lakh)"
                "estimated_construction_cost": _safe_float(_f(proj_kv, "estimated cost of development (in lakh)")),
                # FIELD: project_cost_detail.cost_of_land <- proj_kv "estimated cost of land (in lakh)"
                "cost_of_land":               _safe_float(_f(proj_kv, "estimated cost of land (in lakh)")),
            }.items() if v is not None
        },
        # data JSONB — only Bihar-allowed keys: link, type, govt_type, land_area_unit, construction_area_unit
        "data": {  # FIELD: data <- govt_type + area unit literals
            "govt_type":              "state",  # FIELD: data.govt_type <- literal "state"
            # FIELD: data.land_area_unit <- literal "Total Area of Land (Sq mt)" when land_area_val
            "land_area_unit":         "Total Area of Land (Sq mt)" if land_area_val else None,
            # FIELD: data.construction_area_unit <- literal "Total Builtup Area (Sq. Mtr.)" when const_area_val
            "construction_area_unit": "Total Builtup Area (Sq. Mtr.)" if const_area_val else None,
        },
    }
    # Inject registration certificate link+type into data if available
    reg_cert_url = next(
        (d["link"] for d in docs if "/Registration_Certificate/" in d.get("link", "")),
        None,
    )
    if reg_cert_url:
        out["data"]["link"] = reg_cert_url  # FIELD: data.link <- reg_cert_url (Registration_Certificate PDF)
        out["data"]["type"] = "Registration Certificate"  # FIELD: data.type <- literal "Registration Certificate"
    # Strip None-valued keys from nested dicts
    # FIELD: project_cost_detail <- filtered re-assignment dropping None values
    out["project_cost_detail"] = {k: v for k, v in out["project_cost_detail"].items() if v is not None}
    return out


# ── Quarterly Progress Report snapshot ────────────────────────────────────────
# Bihar QPRs are not on the Filanprint detail page; they live on a separate
# QRCODE.aspx?id=<rid>-<N> page whose GridView1 lists every (financial year,
# quarter) with a "View Document" link to QPR/QPR-Show.aspx (a full Form-7 HTML
# report).  Mirroring the Karnataka crawler, we capture the LATEST quarter as a
# self-contained HTML snapshot (images inlined as base64) and emit it as one
# uploaded_documents entry that _process_documents uploads to S3.
_QPR_DOC_TYPE   = "Quarterly Progress Report"
_QPR_QUARTER_RE = re.compile(r"Q\s*(\d)", re.I)
_QPR_FY_RE      = re.compile(r"(\d{4})\s*-\s*\d{4}")
_QPR_IMG_MIME   = {"jpg": "jpeg", "jpeg": "jpeg", "png": "png", "gif": "gif", "webp": "webp"}


def _qpr_resolve_pj(detail_url: str, detail_html: str) -> str | None:
    """Derive the QRCODE/QPR ``pj`` id for a project.

    The Filanprint detail URL's ``id`` query param appears in three forms, each
    mapping to the ``pj`` id that QRCODE.aspx is keyed on:

      * ``RERAP..._NNN`` (newer instance ids) — already the ``pj`` id verbatim.
      * ``RERAP...-N`` (versioned) — used as-is.
      * ``RERAP...`` (bare, older) — N is recovered from the All_Document file
        paths, which embed the bare id twice followed by ``-N`` (e.g.
        ``...105734RERAP03162019105734-1Project...``), giving ``<base>-N``.
    """
    try:
        rid = (parse_qs(urlparse(detail_url).query).get("id") or [""])[0].strip()
    except Exception:
        rid = ""
    if not rid:
        return None
    # Newer instance ids (RERAP..._NNN) are themselves the QRCODE/QPR pj id.
    if re.search(r"_\d+$", rid):
        return rid
    # Otherwise the id is the hyphen-versioned form; strip any -N suffix so the
    # pattern matches the base id and we don't double the suffix (-N-N).
    base = re.sub(r"-\d+$", "", rid)
    m = re.search(re.escape(base) + r"-(\d+)", detail_html)
    if m:
        return f"{base}-{m.group(1)}"
    # Fall back to the id verbatim when it already carries a -N suffix.
    if re.search(r"-\d+$", rid):
        return rid
    return None


def _qpr_pick_latest(qr_html: str) -> dict | None:
    """Pick the most recent QPR-Show link from a QRCODE.aspx GridView1 table."""
    soup = BeautifulSoup(qr_html, "lxml")
    gv = soup.find("table", id="GridView1")
    if not gv:
        return None
    best: dict | None = None
    best_key: tuple[int, int] = (-1, -1)
    for row in gv.find_all("tr")[1:]:
        a = row.find("a", href=True)
        if not a or "QPR-Show.aspx" not in a["href"]:
            continue
        cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
        fy       = cells[1] if len(cells) > 1 else ""
        quarter  = cells[2] if len(cells) > 2 else ""
        uploaded = cells[3] if len(cells) > 3 else ""
        fy_m, q_m = _QPR_FY_RE.search(fy), _QPR_QUARTER_RE.search(quarter)
        key = (int(fy_m.group(1)) if fy_m else 0, int(q_m.group(1)) if q_m else 0)
        if key > best_key:
            best_key = key
            best = {"href": a["href"], "fy": fy, "quarter": quarter, "uploaded": uploaded}
    return best


def _qpr_inline_images(soup: BeautifulSoup, logger: CrawlerLogger) -> tuple[int, int]:
    """Replace every QPR-Show <img> src with a base64 data URI (in place)."""
    ok = fail = 0
    for img in soup.find_all("img"):
        src = (img.get("src") or "").strip()
        if not src or src.startswith("data:"):
            continue
        # QPR-Show references images as ../QPRimages/<name>; resolve to the host root.
        clean = src.lstrip("./")
        full = clean if clean.startswith("http") else f"https://{DOMAIN}/{clean}"
        try:
            resp = download_response(full, logger=logger, timeout=30.0, verify=False)
            if not resp or not resp.content:
                fail += 1
                continue
            ext = full.rsplit(".", 1)[-1].split("?")[0].lower()
            mime = _QPR_IMG_MIME.get(ext, "jpeg")
            img["src"] = f"data:image/{mime};base64," + base64.b64encode(resp.content).decode("ascii")
            ok += 1
        except Exception as exc:
            logger.warning(f"QPR image inline failed: {exc}", url=full, step="documents")
            fail += 1
    return ok, fail


def _qpr_extract_certificates(
    soup: BeautifulSoup, page_url: str, dated_on: str | None, logger: CrawlerLogger
) -> list[dict]:
    """Extract certificate PDFs (Architect/Engineer/CA) linked in a QPR-Show report.

    These live under ../QPRimages/ and are frequently absent from the Filanprint
    detail page, so we capture them as standalone documents.  The label is taken
    from the surrounding table row (e.g. "Architect Certificate (.pdf)").
    """
    docs: list[dict] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        low = href.lower()
        if "qprimages" not in low or not low.split("?")[0].endswith(".pdf"):
            continue
        full = urljoin(page_url, href)
        if full in seen:
            continue
        seen.add(full)
        label = ""
        row = a.find_parent("tr")
        if row:
            label = row.get_text(" ", strip=True)
            link_txt = a.get_text(" ", strip=True)
            if link_txt:
                label = label.replace(link_txt, " ")
            label = re.sub(r"\(\s*\.?\s*pdf\s*\)", " ", label, flags=re.I)
            label = " ".join(label.split())
        if not label:
            label = "QPR Certificate"
        doc: dict = {"link": full, "type": label}
        if dated_on:
            doc["dated_on"] = dated_on
        docs.append(doc)
    if docs:
        logger.info(f"QPR certificates found: {len(docs)}", step="documents")
    return docs


def _build_qpr_snapshot(detail_url: str, detail_html: str, logger: CrawlerLogger) -> list[dict]:
    """Build documents for the latest QPR: a self-contained HTML snapshot plus any
    certificate PDFs (Architect/Engineer/CA) linked within the report.

    The snapshot dict carries ``_inline_bytes`` / ``_inline_filename`` so
    _process_documents uploads the pre-built HTML straight to S3; the certificate
    entries are plain ``link``/``type`` docs downloaded normally.  Returns an
    empty list when no QPR is available.
    """
    pj = _qpr_resolve_pj(detail_url, detail_html)
    if not pj:
        return []

    qr_resp = safe_get(f"{QRCODE_URL}?id={pj}", retries=2, logger=logger, verify=False)
    if not qr_resp or not qr_resp.text:
        return []
    latest = _qpr_pick_latest(qr_resp.text)
    if not latest:
        return []

    href = latest["href"].lstrip("/")
    qpr_url = href if href.startswith("http") else f"https://{DOMAIN}/{href}"
    show_resp = safe_get(qpr_url, retries=2, logger=logger, verify=False)
    if not show_resp or not show_resp.text:
        return []

    soup = BeautifulSoup(show_resp.text, "lxml")

    dated_on: str | None = None
    try:
        dated_on = datetime.strptime(latest["uploaded"], "%d %b %Y").strftime("%Y-%m-%d")
    except (ValueError, KeyError):
        pass

    # Certificate PDFs linked within the report (often missing from Filanprint).
    certs = _qpr_extract_certificates(soup, qpr_url, dated_on, logger)

    # Strip scripts and ASP.NET hidden inputs so the snapshot is static & offline.
    for tag in soup.find_all("script"):
        tag.decompose()
    for inp in soup.find_all("input", attrs={"type": "hidden"}):
        inp.decompose()
    ok, fail = _qpr_inline_images(soup, logger)

    data = str(soup).encode("utf-8")
    label = " ".join(p for p in (latest.get("fy", ""), latest.get("quarter", "")) if p).strip()
    logger.info(
        f"QPR snapshot built: {label} (images ok={ok} failed={fail}, size={len(data)/1024:.0f} KB)",
        step="documents",
    )
    doc: dict = {
        "link": qpr_url,
        "type": _QPR_DOC_TYPE,
        "_inline_bytes": data,
        "_inline_filename": "quarterly_progress_report.html",
    }
    if dated_on:
        doc["dated_on"] = dated_on
    return [doc, *certs]


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

    # ── Step 2: Discover Filanprint detail URL via Selenium ─────────────────
    # The sentinel project is always on page 1 of the listing (data_rows).
    # Find its row index first so we only collect that many popup URLs instead
    # of crawling all pages (which would take minutes / hours).
    logger.info(f"Sentinel: discovering Filanprint URL via Selenium", step="sentinel")
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
        logger.warning(f"Sentinel: Selenium discovery failed — {exc}", step="sentinel")

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

def _public_doc(doc: dict) -> dict:
    """Drop internal ``_inline_*`` keys so they never reach the project record."""
    return {k: v for k, v in doc.items() if not k.startswith("_inline_")}


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
            enriched.append(_public_doc(doc))
            continue

        url = selected.get("link") or selected.get("url")
        doc_type = selected.get("type", "document")
        inline_bytes = selected.get("_inline_bytes")
        if not url and inline_bytes is None:
            enriched.append(_public_doc(selected))
            continue

        reused, existing_s3_key = existing_uploaded_document_entry(
            project_key, {**_public_doc(selected), "link": url}
        )
        if reused:
            logger.info(f"Document reused: {doc_type!r}", s3_key=existing_s3_key, step="documents")
            logger.log_document(doc_type, url, "reused", s3_key=existing_s3_key)
            enriched.append(reused)
            continue

        filename = build_document_filename(selected)

        try:
            if inline_bytes is not None:
                # Pre-built snapshot (e.g. the QPR HTML) — skip the network
                # download and force the inline content's extension.
                data = inline_bytes
                inline_name = selected.get("_inline_filename") or "snapshot.html"
                ext = inline_name[inline_name.rfind("."):] if "." in inline_name else ".html"
                base = filename.rsplit(".", 1)[0] if "." in filename else filename
                filename = f"{base}{ext}"
            else:
                resp = download_response(url, logger=logger, timeout=60.0, verify=False)
                if not resp or len(resp.content) < 100:
                    enriched.append(_public_doc(selected))
                    logger.warning(f"Document download failed or too small: {url}", step="documents")
                    continue
                data = resp.content

            md5 = compute_md5(data)
            s3_key = upload_document(
                project_key, filename, data, dry_run=settings.DRY_RUN_S3
            )
            if s3_key is None:
                enriched.append(_public_doc(selected))
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
            enriched.append({**_public_doc(selected), "link": url, "s3_link": s3_url, "updated": True})
            upload_count += 1
            logger.info(f"Document uploaded: {doc_type!r}", s3_key=s3_key, step="documents")
            logger.log_document(doc_type, url, "uploaded", s3_key=s3_key, file_size_bytes=len(data))

        except Exception as exc:
            enriched.append(_public_doc(selected))
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
            "project_name":            cells[0].get_text(separator=" ", strip=True),  # FIELD: project_name <- listing row cells[0] text
            "project_registration_no": reg_no,  # FIELD: project_registration_no <- listing row cells[1] text (reg_no var)
            "promoter_name":           cells[2].get_text(separator=" ", strip=True),  # FIELD: promoter_name <- listing row cells[2] text
            # FIELD: project_location_raw <- {"address": listing row cells[3] text}
            "project_location_raw":    {"address": cells[3].get_text(separator=" ", strip=True)},
            "submitted_date":          cells[4].get_text(strip=True),  # FIELD: submitted_date <- listing row cells[4] text
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

# ── Details phase (per-candidate worker) ──────────────────────────────────────

def _process_bihar_inline(
    raw: dict,
    detail_url: str,
    detail_html: str | None,
    current_page: int,
    config: dict,
    run_id: int,
    site_id: str,
    mode: str,
    machine_name: str,
    machine_ip: str,
    logger: CrawlerLogger,
) -> tuple[dict, dict | None]:
    """Inline phase: parse detail HTML, merge with listing row, upsert project.

    Designed to run interleaved with the listing walker (via on_candidate).
    Skips QPR + document downloads — those would issue further Selenium GETs
    which would navigate the listing tab away.  Returns:
        (deltas, pending_doc_work | None)
    where pending_doc_work carries the data the deferred documents phase
    needs (key, detail_url, detail_html, uploaded_documents) so QPR snapshot
    + PDF download/upload can run in a second pass after listing completes.
    """
    deltas = {
        "projects_skipped": 0, "projects_new": 0, "projects_updated": 0,
        "error_count": 0,
    }
    reg_no = raw.get("project_registration_no", "").strip()
    if not reg_no:
        deltas["error_count"] += 1
        return deltas, None
    # Bihar key recipe matches prod: project_name + reg_no + promoter_name, concatenated
    # raw (no separator, no case/whitespace changes), then siphash24. Falling back to
    # reg_no alone here is what created the historical duplicate rows — refuse the row.
    project_name_raw  = raw.get("project_name", "") or ""
    promoter_name_raw = raw.get("promoter_name", "") or ""
    if not project_name_raw or not promoter_name_raw:
        deltas["error_count"] += 1
        return deltas, None
    key = generate_project_key(project_name_raw + reg_no + promoter_name_raw)
    logger.set_project(
        key=key, reg_no=reg_no,
        url=detail_url or LISTING_URL, page=current_page,
    )
    try:
        if mode == "daily_light" and get_project_by_key(key):
            deltas["projects_skipped"] += 1
            return deltas, None

        detail_extra: dict = {}
        effective_html: str | None = detail_html
        if detail_html and "Invalid Project ID" not in detail_html:
            detail_extra = _parse_detail_page(detail_html)
            logger.info(f"Detail parsed (inline) for {reg_no!r}", step="detail")
        elif detail_html:
            logger.warning(
                f"Popup HTML reported 'Invalid Project ID' for {reg_no!r}",
                step="detail",
            )
            effective_html = None
        elif detail_url:
            # Fallback: no pre-captured popup HTML (e.g. compat wrapper /
            # sentinel re-entry).  Issue a Selenium GET — safe here because
            # the listing walker is not active.
            detail_resp = safe_get(detail_url, retries=2, logger=logger, verify=False)
            if detail_resp and "Invalid Project ID" not in detail_resp.text:
                effective_html = detail_resp.text
                detail_extra = _parse_detail_page(effective_html)
                logger.info(f"Detail parsed for {reg_no!r}", step="detail")
            elif detail_resp:
                logger.warning(
                    f"Detail page returned 'Invalid Project ID' for {reg_no!r}",
                    step="detail",
                )
                effective_html = None
            else:
                logger.warning(f"Detail fetch failed for {reg_no!r}", step="detail")
                effective_html = None
        else:
            logger.warning(
                f"No detail URL for listing page {current_page} ({reg_no!r})",
                step="detail",
            )

        try:
            source_url = detail_url or LISTING_URL
            merged: dict = {
                **detail_extra,
                "key":                     key,  # FIELD: key <- Bihar-specific project_name + reg_no + promoter_name hash
                "project_name":            raw["project_name"],  # FIELD: project_name <- raw["project_name"] from listing row
                "project_registration_no": reg_no,  # FIELD: project_registration_no <- reg_no var from listing row
                "promoter_name":           raw["promoter_name"],  # FIELD: promoter_name <- raw["promoter_name"] from listing row
                "submitted_date":          raw["submitted_date"],  # FIELD: submitted_date <- raw["submitted_date"] from listing row
                "project_location_raw": {  # FIELD: project_location_raw <- merge of raw + detail_extra + state
                    **raw.get("project_location_raw", {}),
                    **detail_extra.get("project_location_raw", {}),
                    "state": config.get("state", "bihar").title(),  # FIELD: project_location_raw.state <- config["state"] titled
                },
                "project_state": config.get("state", "bihar").title(),  # FIELD: project_state <- config["state"] titled
                "domain": DOMAIN,  # FIELD: domain <- DOMAIN constant
                "url":    source_url,  # FIELD: url <- Filanprint detail URL or listing URL fallback
                "state":  config.get("state", "Bihar"),  # FIELD: state <- config["state"]
                "is_live": True,  # FIELD: is_live <- literal True
                "data": merge_data_sections(  # FIELD: data <- merge_data_sections(detail_extra.data, listing_address)
                    detail_extra.get("data"),
                    {
                        "listing_address": raw.get("project_location_raw", {}).get("address", ""),
                        "source_url": source_url,
                    },
                ),
            }
            merged = {k: v for k, v in merged.items() if v is not None}

            normalized = normalize_project_payload(
                merged, config,
                machine_name=machine_name, machine_ip=machine_ip,
            )
            record  = ProjectRecord(**normalized)
            db_dict = record.to_db_dict()
            status  = upsert_project(db_dict)

            if status == "new":
                deltas["projects_new"] += 1
                logger.info(f"New project: {reg_no}", step="upsert")
            else:
                deltas["projects_updated"] += 1
                logger.info(f"Updated: {reg_no}", step="upsert")

            pending = {
                "key": key,
                "reg_no": reg_no,
                "detail_url": detail_url or "",
                "detail_html": effective_html or "",
                "uploaded_documents": list(detail_extra.get("uploaded_documents") or []),
                "current_page": current_page,
            }
            return deltas, pending

        except ValidationError as exc:
            deltas["error_count"] += 1
            logger.error(f"Validation error for {reg_no}: {exc}", step="validate")
            insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(exc),
                               project_key=key, url=detail_url or LISTING_URL)
            return deltas, None
        except Exception as exc:
            deltas["error_count"] += 1
            logger.error(f"Unexpected error for {reg_no}: {exc}", step="upsert")
            insert_crawl_error(run_id, site_id, "CRAWLER_EXCEPTION", str(exc),
                               project_key=key, url=detail_url or LISTING_URL)
            return deltas, None
    finally:
        logger.clear_project()


def _process_bihar_documents(
    pending: dict,
    config: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
) -> dict:
    """Deferred phase: build QPR snapshot, download PDFs, upload to S3, update record.

    Runs after the listing walk completes so its Selenium GETs (QRCODE.aspx,
    QPR-Show, document URLs) don't disturb the listing tab.
    """
    deltas = {"documents_uploaded": 0, "error_count": 0}
    key          = pending["key"]
    reg_no       = pending["reg_no"]
    detail_url   = pending["detail_url"]
    detail_html  = pending["detail_html"]
    uploaded_docs = list(pending.get("uploaded_documents") or [])
    current_page = pending.get("current_page", 0)

    logger.set_project(
        key=key, reg_no=reg_no,
        url=detail_url or LISTING_URL, page=current_page,
    )
    try:
        if detail_url and detail_html and not (settings.SKIP_DOCUMENTS or mode == "daily_light"):
            try:
                qpr_docs = _build_qpr_snapshot(detail_url, detail_html, logger)
                uploaded_docs.extend(qpr_docs)  # FIELD: uploaded_documents[] <- inline QPR snapshot + QPR certificate PDFs (latest quarter)
            except Exception as exc:
                logger.warning(f"QPR snapshot failed for {reg_no!r}: {exc}", step="documents")

        if uploaded_docs and (settings.SKIP_DOCUMENTS or mode == "daily_light"):
            logger.info(
                f"Skipping {len(uploaded_docs)} documents (light/skip-documents mode)",
                step="documents",
            )
        elif uploaded_docs:
            try:
                enriched_docs, doc_count = _process_documents(
                    key, uploaded_docs, run_id, site_id, logger,
                )
                deltas["documents_uploaded"] += doc_count
                if doc_count:
                    doc_urls = [
                        {"link": d["s3_link"], "type": d.get("type")}
                        for d in enriched_docs if d.get("s3_link")
                    ]
                    upsert_project({
                        "key": key,  # FIELD: key <- generate_project_key(reg_no)
                        "uploaded_documents": enriched_docs,  # FIELD: uploaded_documents <- enriched_docs from _process_documents
                        "document_urls": doc_urls,  # FIELD: document_urls <- doc_urls built from enriched s3_link entries
                    })
            except Exception as exc:
                deltas["error_count"] += 1
                logger.error(f"Document processing failed for {reg_no}: {exc}", step="documents")
                insert_crawl_error(run_id, site_id, "CRAWLER_EXCEPTION", str(exc),
                                   project_key=key, url=detail_url or LISTING_URL)
    finally:
        logger.clear_project()
    return deltas


def _process_bihar_candidate(
    raw: dict,
    detail_url: str,
    current_page: int,
    config: dict,
    run_id: int,
    site_id: str,
    mode: str,
    machine_name: str,
    machine_ip: str,
    logger: CrawlerLogger,
) -> dict:
    """Backwards-compatible wrapper — runs inline + documents sequentially.

    Preserves the original return shape (single deltas dict including
    documents_uploaded) for callers that don't use the interleaved path
    (sentinel re-entry, tests, dry_run_compare).
    """
    deltas, pending = _process_bihar_inline(
        raw, detail_url, None, current_page,
        config, run_id, site_id, mode,
        machine_name, machine_ip, logger,
    )
    deltas.setdefault("documents_uploaded", 0)
    if pending:
        doc_deltas = _process_bihar_documents(pending, config, run_id, site_id, logger)
        for k, v in doc_deltas.items():
            deltas[k] = deltas.get(k, 0) + v
    return deltas


# ── Main entry point ──────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    """Public entry point — ensures the Selenium driver is shut down after the run."""
    try:
        return _run(config, run_id, mode)
    finally:
        _quit_driver()


def _run(config: dict, run_id: int, mode: str) -> dict:
    logger = CrawlerLogger(config["id"], run_id)
    counters = dict(projects_found=0, projects_new=0, projects_updated=0,
                    projects_skipped=0, documents_uploaded=0, error_count=0)
    machine_name, machine_ip = get_machine_context()
    site_id       = config["id"]
    item_limit    = settings.CRAWL_ITEM_LIMIT or 0  # 0 = unlimited
    t_run = time.monotonic()

    # ── Targeted run handling ────────────────────────────────────────────────
    # --target-reg-no restricts the run to one or more specific projects
    # (comma-separated, case-insensitive). The reg-no is present on every listing
    # row, so each page is filtered down to the requested project(s) and the page
    # walk stops as soon as all targets are found. The sentinel check is skipped
    # for targeted runs (mirrors maharashtra_rera).
    target_regs = get_target_reg_nos()
    found_targets: set[str] = set()

    # ── Sentinel health check ────────────────────────────────────────────────
    if target_regs or mode == "daily_light":
        logger.info("Sentinel skipped (targeted run via --target-reg-no)", step="sentinel")
        counters["sentinel_passed"] = True
    else:
        t0 = time.monotonic()
        if not _sentinel_check(config, run_id, logger):
            logger.error("Sentinel failed — aborting crawl", step="sentinel")
            counters["sentinel_passed"] = False
            counters["error_count"] += 1
            return counters
        counters["sentinel_passed"] = True
        logger.timing("sentinel", time.monotonic() - t0)

    max_pages    = settings.MAX_PAGES
    delay_range  = config.get("rate_limit_delay", (2, 4))

    # ── Step 1: Walk listing pages and process each project inline ──────────
    # The popup HTML for each project is captured during the listing walk and
    # handed to _process_bihar_inline via on_candidate, so projects_new /
    # projects_updated climb in real time on the dashboard rather than only
    # after the listing phase completes.  QPR snapshots + S3 document uploads
    # are deferred to Phase B (below) because they would issue Selenium GETs
    # that navigate the listing tab away.
    #
    # daily_light only needs project_registration_no for the DB dedup check,
    # never the detail URL — skip the per-project popup clicks entirely.
    capture_detail_urls = mode != "daily_light"
    # Honour item_limit uniformly across modes — projects_found reflects only
    # the rows actually walked rather than the full Bihar catalog.
    listing_max_items = item_limit or None
    t0 = time.monotonic()
    logger.info(
        f"Walking listing + processing inline via Selenium"
        f" (item_limit={item_limit or 'unlimited'}, max_pages={max_pages or 'unlimited'},"
        f" capture_detail={capture_detail_urls})",
        step="detail_collect",
    )

    pending_doc_work: list[dict] = []

    def _on_listing_progress(found_so_far: int) -> None:
        # Push the running projects_found to crawl_runs for live dashboard view.
        counters["projects_found"] = found_so_far
        update_crawl_run_progress(run_id, counters)

    def _on_candidate(raw: dict, detail_url: str, detail_html: str | None,
                      current_page: int) -> None:
        # Inline parse + upsert per candidate as the listing walker captures
        # each popup.  Stash QPR/document work for a deferred second pass.
        try:
            deltas, pending = _process_bihar_inline(
                raw, detail_url, detail_html, current_page,
                config, run_id, site_id, mode,
                machine_name, machine_ip, logger,
            )
            for k, v in deltas.items():
                counters[k] = counters.get(k, 0) + v
            if pending:
                pending_doc_work.append(pending)
        except Exception as exc:
            counters["error_count"] += 1
            logger.exception("Inline candidate processing failed", exc, step="project_loop")
        update_crawl_run_progress(run_id, counters)

    listing_pages = _collect_listing_pages(
        logger,
        max_items=listing_max_items,
        max_pages=max_pages or None,
        capture_detail_urls=capture_detail_urls,
        on_progress=_on_listing_progress,
        on_candidate=_on_candidate,
        target_regs=target_regs,
        found_targets=found_targets,
    )
    if target_regs:
        for missing in sorted(target_regs - found_targets):
            logger.warning(f"Target reg_no={missing!r} not found in listing", step="listing")
        logger.info(
            f"Targeted run — {len(found_targets)} of {len(target_regs)} requested "
            f"project(s) matched", step="listing",
        )
    logger.info(
        f"Walked {sum(len(p['rows']) for p in listing_pages)} listing rows across"
        f" {len(listing_pages)} page(s);"
        f" {sum(1 for p in listing_pages for u in p['detail_urls'] if u)} detail URLs captured;"
        f" {len(pending_doc_work)} pending documents",
        step="detail_collect",
    )
    if not listing_pages:
        logger.error("Failed to collect any listing pages", step="listing")
        insert_crawl_error(run_id, config["id"], "HTTP_ERROR", "listing pagination failed", LISTING_URL)
        counters["error_count"] += 1
        return counters

    logger.warning(
        f"Step timing [search+inline]: {time.monotonic()-t0:.2f}s"
        f"  pages={len(listing_pages)}"
        f"  rows={sum(len(p['rows']) for p in listing_pages)}",
        step="timing",
    )

    # Final authoritative projects_found from the walked listing.
    counters["projects_found"] = sum(len(p["rows"]) for p in listing_pages)
    for payload in listing_pages:
        logger.info(
            f"Page {payload['page']}: {len(payload['rows'])} projects",
            step="listing",
        )
    update_crawl_run_progress(run_id, counters)

    if not pending_doc_work:
        reset_checkpoint(config["id"], mode)
        logger.info(f"Bihar RERA complete (no documents to process): {counters}", step="done")
        logger.timing("total_run", time.monotonic() - t_run)
        return counters

    # ── Phase B: deferred QPR snapshots + document downloads ────────────────
    # SeleniumSession driver is not thread-safe — force serial processing.
    n_workers = 1
    logger.info(
        f"Phase B: deferred documents ({len(pending_doc_work)} projects, "
        f"{n_workers} workers)",
        step="detail_fetch",
    )
    tB = time.monotonic()

    def _doc_worker(_idx: int, pending: dict) -> dict:
        return _process_bihar_documents(pending, config, run_id, site_id, logger)

    def _on_doc_result(_idx: int, deltas: dict | None, exc: Exception | None) -> None:
        # Fold each completed document task's deltas into the running counters
        # and push them to crawl_runs so documents_uploaded climbs live.
        if exc is not None:
            counters["error_count"] += 1
            logger.exception("Doc worker raised", exc, step="documents")
        else:
            for k, v in (deltas or {}).items():
                counters[k] = counters.get(k, 0) + v
        update_crawl_run_progress(run_id, counters)

    process_details(pending_doc_work, _doc_worker, n_workers=n_workers,
                    on_result=_on_doc_result)
    logger.timing("details", time.monotonic() - tB,
                  items=len(pending_doc_work), workers=n_workers)

    reset_checkpoint(config["id"], mode)
    logger.info(f"Bihar RERA complete: {counters}", step="done")
    logger.timing("total_run", time.monotonic() - t_run)
    return counters
