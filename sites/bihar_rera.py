"""
Bihar RERA Crawler — rera.bihar.gov.in/RegisteredPP.aspx
Type: httpx listing + Playwright detail (popup capture)

Strategy:
- The listing page is a server-side ASP.NET GridView (~10 total projects; small state).
- Each project name is a link that triggers __doPostBack('...', 'PrintIndicator$N'),
  which opens a popup window at Filanprint.aspx?id=RERAP...
- Direct httpx postbacks fail (ViewState MAC cluster mismatch); Playwright is used to
  click each link and capture the popup URL via context.expect_page().
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

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from pydantic import ValidationError

from core.checkpoint import reset_checkpoint
from core.config import settings
from core.crawler_base import generate_project_key, random_delay, safe_get, safe_post
from core.db import get_project_by_key, upsert_project, insert_crawl_error
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import (
    get_machine_context,
    merge_data_sections,
    normalize_project_payload,
)

LISTING_URL  = "https://rera.bihar.gov.in/RegisteredPP.aspx"
FILANPRINT   = "https://rera.bihar.gov.in/Filanprint.aspx"
STATE_CODE   = "BR"
DOMAIN       = "rera.bihar.gov.in"
# ASP.NET GridView control ID
_GRID_ID     = "ContentPlaceHolder1_GV_Building"
_GRID_TARGET = "ctl00$ContentPlaceHolder1$GV_Building"


# ── Playwright: collect Filanprint popup URLs ──────────────────────────────────

def _collect_detail_urls(logger: CrawlerLogger, max_items: int | None = None) -> list[str | None]:
    """
    Use Playwright to click every project link on the Bihar listing page.
    Each click opens a popup window at Filanprint.aspx?id=RERAP...

    Strategy: reload the listing page before each click to avoid execution-context
    destruction that happens when the previous click navigates the main page.

    Returns an ordered list of Filanprint URLs aligned with listing row order.
    None means the popup was not captured for that row.

    max_items: if set, stop after collecting this many URLs (for CRAWL_ITEM_LIMIT).
    """
    links_sel = f"table#{_GRID_ID} tr td:first-child a"
    detail_urls: list[str | None] = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context()

            # ── First pass: count data-row links ─────────────────────────────
            page = ctx.new_page()
            page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=30_000)
            link_texts: list[str] = page.eval_on_selector_all(
                links_sel, "els => els.map(e => e.innerText.trim())"
            )
            # Filter out pager links (digits, "...", navigation words)
            project_indices = [
                i for i, t in enumerate(link_texts)
                if t and not t.isdigit() and t not in ("...", "Next", "Prev", "Previous", "First", "Last")
            ]
            if max_items:
                project_indices = project_indices[:max_items]
            page.close()
            logger.info(
                f"Playwright: {len(project_indices)} project row links to collect"
                + (f" (capped at {max_items})" if max_items else ""),
                step="detail_collect",
            )

            # ── Second pass: one fresh page per link ──────────────────────────
            for rank, idx in enumerate(project_indices):
                name = link_texts[idx]
                try:
                    page = ctx.new_page()
                    page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=30_000)
                    with ctx.expect_page(timeout=15_000) as popup_info:
                        page.eval_on_selector_all(links_sel, f"els => els[{idx}].click()")
                    popup = popup_info.value
                    popup.wait_for_load_state("domcontentloaded", timeout=15_000)
                    url = popup.url
                    popup.close()
                    page.close()
                    if "Filanprint.aspx" in url:
                        detail_urls.append(url)
                        logger.info(f"  [{rank}] {name!r} → {url}", step="detail_collect")
                    else:
                        detail_urls.append(None)
                        logger.warning(f"  [{rank}] {name!r}: unexpected URL {url}", step="detail_collect")
                except Exception as e:
                    try:
                        page.close()
                    except Exception:
                        pass
                    detail_urls.append(None)
                    logger.warning(f"  [{rank}] {name!r}: popup failed — {e}", step="detail_collect")

            browser.close()
    except Exception as e:
        logger.error(f"Playwright detail-url collection failed: {e}", step="detail_collect")
    return detail_urls


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

    # ── Org members (Table 4: header + data rows) ─────────────────────────────
    members: list[dict] = []
    if len(tables) > 4:
        rows = tables[4].find_all("tr")
        if len(rows) > 1:
            headers = [c.get_text(strip=True).lower() for c in rows[0].find_all(["td", "th"])]
            for row in rows[1:]:
                cells = [c.get_text(separator=" ", strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) >= len(headers):
                    members.append(dict(zip(headers, cells)))

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

    # ── Vendors / professionals (Table 16 header + data rows) ────────────────
    vendors: list[dict] = []
    if len(tables) > 16:
        rows = tables[16].find_all("tr")
        if len(rows) > 1:
            headers = [c.get_text(strip=True).lower() for c in rows[0].find_all(["td", "th"])]
            for row in rows[1:]:
                cells = [c.get_text(separator=" ", strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) >= 2:
                    vendors.append(dict(zip(headers, cells)))

    # ── Building details (Table 11 header + data rows) ────────────────────────
    buildings: list[dict] = []
    if len(tables) > 11:
        rows = tables[11].find_all("tr")
        if len(rows) > 1:
            headers = [c.get_text(strip=True).lower() for c in rows[0].find_all(["td", "th"])]
            for row in rows[1:]:
                cells = [c.get_text(separator=" ", strip=True) for c in row.find_all(["td", "th"])]
                if any(cells):
                    buildings.append(dict(zip(headers, cells)))

    # ── Assemble output ───────────────────────────────────────────────────────
    def _f(kv: dict, *keys: str) -> str:
        """Return first matching key value (case-insensitive)."""
        for k in keys:
            v = kv.get(k.lower(), "")
            if v:
                return v
        return ""

    loc_raw: dict = {
        "address":  _f(loc_kv, "project address"),
        "district": _f(loc_kv, "district"),
        "plot_no":  _f(loc_kv, "khesra no./plot no."),
        "anchal":   _f(loc_kv, "anchal"),
        "mauja":    _f(loc_kv, "mauja"),
        "city":     _f(loc_kv, "city/town"),
    }
    try:
        lat = float(_f(loc_kv, "latitude of end point of the plot"))
        lng = float(_f(loc_kv, "longitude of end point of the plot"))
        if lat and lng:
            loc_raw["latitude"]  = lat
            loc_raw["longitude"] = lng
    except ValueError:
        pass

    bank: dict = {}
    if any(bank_kv.values()):
        bank = {
            "bank_name":    _f(bank_kv, "bank name"),
            "branch_name":  _f(bank_kv, "branch name"),
            "account_no":   _f(bank_kv, "account no."),
            "ifsc_code":    _f(bank_kv, "ifsc code"),
            "bank_address": _f(bank_kv, "bank address"),
        }

    contact: dict = {}
    mobile = _f(prom_kv, "mobile number")
    email  = _f(prom_kv, "e-mail")
    if mobile or email:
        contact = {"mobile": mobile, "email": email}

    addr: dict = {}
    reg_addr = _f(contact_kv, "register address")
    if reg_addr:
        addr = {"address": reg_addr, "district": _f(contact_kv, "district"),
                "state": _f(contact_kv, "state"), "pincode": _f(contact_kv, "pin code")}

    def _safe_float(val: str) -> float | None:
        try:
            v = float(val.replace(",", ""))
            return v if v > 0 else None
        except (ValueError, AttributeError):
            return None

    out: dict = {
        # project info
        "project_type":              _f(proj_kv, "project type"),
        "status_of_the_project":     _f(proj_kv, "project status"),
        "project_description":       _f(proj_kv, "project description"),
        "estimated_commencement_date": _f(proj_kv, "project start date"),
        "estimated_finish_date":     _f(proj_kv, "project end date"),
        "land_area":                 _safe_float(_f(proj_kv, "total area of land (sq mt)")),
        "construction_area":         _safe_float(_f(proj_kv, "total covered area (sq mtr)")),
        "total_floor_area_under_residential": _safe_float(_f(proj_kv, "total builtup area (sq. mtr.)")),
        # location
        "project_location_raw": {k: v for k, v in loc_raw.items() if v},
        "project_city":          _f(loc_kv, "city/town"),
        # promoter / contact
        "promoter_contact_details": contact or None,
        "promoter_address_raw":     addr or None,
        # structured
        "bank_details":          bank or None,
        "members_details":       members or None,
        "professional_information": vendors or None,
        "building_details":      buildings or None,
        # project cost
        "project_cost_detail": {
            "development_cost_lakh": _safe_float(_f(proj_kv, "estimated cost of development (in lakh)")),
            "land_cost_lakh":        _safe_float(_f(proj_kv, "estimated cost of land (in lakh)")),
        },
        # detail page source
        "data": {
            "application_no":   _f(prom_kv, "your application number"),
            "promoter_type":    _f(prom_kv, "promoter type"),
            "pan_number":       _f(prom_kv, "pan number"),
            "company_reg_no":   _f(prom_kv, "company registration no /deed no."),
            "contact_person":   _f(contact_kv, "name of contact person"),
            "contact_desig":    _f(contact_kv, "designation of contact person"),
            "completion_months": _f(proj_kv, "proposed period of completion (in month)"),
        },
    }
    # Strip None-valued keys from nested dicts
    out["project_cost_detail"] = {k: v for k, v in out["project_cost_detail"].items() if v is not None}
    return out


# ── Sentinel ──────────────────────────────────────────────────────────────────

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """Verify the site is reachable and returns a project table."""
    resp = safe_get(LISTING_URL, retries=2, logger=logger)
    if not resp:
        logger.error("Sentinel: listing page unreachable", step="sentinel")
        return False
    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table", id=_GRID_ID)
    if not table or not table.find("tr"):
        logger.error("Sentinel: project table not found in response", step="sentinel")
        return False
    logger.info("Sentinel passed", step="sentinel")
    return True


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

    Uses CSS selector for direct tbody children only, so the nested <table>
    inside the ASP.NET GridView pager row is never traversed.
    """
    # Only select <tr> that are direct children of the GridView <table> (no <tbody>).
    # This prevents picking up nested pager table rows.
    rows = soup.select(f"table#{_GRID_ID} > tr")
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
    resp = safe_post(LISTING_URL, data=payload, retries=3, logger=logger)
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

    if not _sentinel_check(config, run_id, logger):
        return counters

    # ── Step 1: Collect Filanprint popup URLs via Playwright ─────────────────
    logger.info(
        f"Collecting detail page URLs via Playwright (item_limit={item_limit or 'unlimited'})",
        step="detail_collect",
    )
    detail_url_list = _collect_detail_urls(logger, max_items=item_limit or None)
    logger.info(
        f"Collected {sum(1 for u in detail_url_list if u)} detail URLs ({len(detail_url_list)} total)",
        step="detail_collect",
    )

    # ── Step 2: Fetch listing HTML via httpx ─────────────────────────────────
    logger.info("Fetching listing page 1", step="listing")
    resp = safe_get(LISTING_URL, retries=config.get("max_retries", 3), logger=logger)
    if not resp:
        logger.error("Failed to fetch listing page", step="listing")
        insert_crawl_error(run_id, config["id"], "HTTP_ERROR", "listing page unreachable", LISTING_URL)
        counters["error_count"] += 1
        return counters

    max_pages    = settings.MAX_PAGES
    delay_range  = config.get("rate_limit_delay", (2, 4))
    current_page = 1
    soup         = BeautifulSoup(resp.text, "lxml")
    stop_all     = False

    while True:
        if stop_all:
            break
        rows = _parse_page_rows(soup)
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

            key = generate_project_key(STATE_CODE, reg_no)

            # ── Step 3: Fetch & parse detail page ────────────────────────────
            # Use positional alignment: detail_url_list[row_idx] matches listing row
            detail_url: str = ""
            if row_idx < len(detail_url_list) and detail_url_list[row_idx]:
                detail_url = detail_url_list[row_idx]
            detail_extra: dict = {}
            if detail_url:
                detail_resp = safe_get(detail_url, retries=2, logger=logger)
                if detail_resp:
                    detail_extra = _parse_detail_page(detail_resp.text)
                    logger.info(f"Detail parsed for {reg_no!r}", step="detail")
                else:
                    logger.warning(f"Detail fetch failed for {reg_no!r}", step="detail")
            else:
                logger.warning(f"No detail URL for row {row_idx} ({reg_no!r})", step="detail")

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
                    },
                    "domain": DOMAIN,
                    "url":    detail_url or LISTING_URL,
                    "state":  config.get("state", "Bihar"),
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
                    logger.info(f"New project: {reg_no}", project_key=key, step="upsert")
                elif status == "updated":
                    counters["projects_updated"] += 1
                    logger.info(f"Updated: {reg_no}", project_key=key, step="upsert")
                else:
                    counters["projects_skipped"] += 1

            except ValidationError as exc:
                counters["error_count"] += 1
                logger.error(f"Validation error for {reg_no}: {exc}", project_key=key, step="validate")
                insert_crawl_error(run_id, config["id"], "VALIDATION_FAILED", str(exc),
                                   detail_url or LISTING_URL, project_key=key)
            except Exception as exc:
                counters["error_count"] += 1
                logger.error(f"Unexpected error for {reg_no}: {exc}", project_key=key, step="upsert")
                insert_crawl_error(run_id, config["id"], "CRAWLER_EXCEPTION", str(exc),
                                   detail_url or LISTING_URL, project_key=key)

            random_delay(*delay_range)

        # ── Advance to next page ──────────────────────────────────────────────
        if max_pages and current_page >= max_pages:
            logger.info(f"Reached max_pages={max_pages}, stopping", step="listing")
            break
        if not _has_next_page(soup, current_page):
            logger.info("No more pages", step="listing")
            break
        current_page += 1
        form_fields = _extract_form_fields(soup)
        soup = _fetch_page(current_page, form_fields, logger)
        if soup is None:
            logger.error(f"Failed to fetch page {current_page}", step="listing")
            insert_crawl_error(run_id, config["id"], "HTTP_ERROR", f"page {current_page} failed", LISTING_URL)
            break
        random_delay(*delay_range)

    reset_checkpoint(config["id"], mode)
    logger.info(f"Bihar RERA complete: {counters}", step="done")
    return counters
