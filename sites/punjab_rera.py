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

import time

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PWTimeout, sync_playwright
from pydantic import ValidationError

from core.checkpoint import reset_checkpoint
from core.crawler_base import generate_project_key, random_delay
from core.config import settings
from core.db import upsert_project, insert_crawl_error
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import get_machine_context, normalize_project_payload

LISTING_URL   = "https://rera.punjab.gov.in/reraindex/publicview/projectinfo"
DUMMY_CAPTCHA = "ABCDEF"
STATE_CODE    = "PB"
DOMAIN        = "rera.punjab.gov.in"

# CSS / text selectors (confirmed from live HTML)
SEL_CAPTCHA   = "#Input_RegdProject_CaptchaText"
SEL_SUBMIT    = "#ProjectPVform_submit"
SEL_TABLE     = "table#dataTableSearchProject"
SEL_ROWS      = f"{SEL_TABLE} tbody tr"
SEL_VIEW_BTN  = "a#modalOpenerButton"
SEL_MODAL     = "#myModal"
SEL_MODAL_VIS = "#myModal.show"


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


def _close_modal(page) -> None:
    try:
        page.click(f"{SEL_MODAL} .close", timeout=3_000)
        page.wait_for_selector(SEL_MODAL_VIS, state="hidden", timeout=5_000)
    except PWTimeout:
        page.evaluate("$('#myModal').modal('hide')")
        time.sleep(0.5)


def _sentinel_check(page, logger: CrawlerLogger) -> bool:
    """Submit a blank search and verify at least one result row appears."""
    try:
        page.goto(LISTING_URL, wait_until="networkidle", timeout=30_000)
        page.fill(SEL_CAPTCHA, DUMMY_CAPTCHA)
        page.click(SEL_SUBMIT)
        page.wait_for_selector(SEL_ROWS, timeout=20_000)
        logger.info("Sentinel passed", step="sentinel")
        return True
    except PWTimeout:
        logger.error("Sentinel: no project rows appeared after search", step="sentinel")
        return False


# ── Main entry point ──────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    logger = CrawlerLogger(config["id"], run_id)
    counters = dict(projects_found=0, projects_new=0, projects_updated=0,
                    projects_skipped=0, documents_uploaded=0, error_count=0)
    machine_name, machine_ip = get_machine_context()
    delay_range = config.get("rate_limit_delay", (2, 4))
    item_limit = settings.CRAWL_ITEM_LIMIT or 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx     = browser.new_context(viewport={"width": 1280, "height": 900})
        page    = ctx.new_page()

        try:
            if not _sentinel_check(page, logger):
                return counters

            # ── DataTables "Show All" ────────────────────────────────────────
            # Set DataTables length menu to -1 (All) so every row is visible in DOM
            try:
                page.select_option(f"{SEL_TABLE}_length select", "-1")
                page.wait_for_timeout(2_000)
            except Exception:
                pass  # older DataTables may not support -1; rows still in DOM

            row_handles = page.query_selector_all(SEL_ROWS)
            if item_limit:
                row_handles = row_handles[:item_limit]
                logger.info(f"Punjab: CRAWL_ITEM_LIMIT={item_limit} applied — processing {len(row_handles)} projects", step="listing")
            counters["projects_found"] = len(row_handles)
            logger.info(f"Found {len(row_handles)} project rows", step="listing")

            # ── Process each row ─────────────────────────────────────────────
            for idx, row in enumerate(row_handles):
                cells = row.query_selector_all("td")
                if len(cells) < 6:
                    continue

                # Listing columns — read by their on-screen position
                district   = cells[1].inner_text().strip()
                proj_name  = cells[2].inner_text().strip()
                prom_name  = cells[3].inner_text().strip()
                reg_no     = cells[4].inner_text().strip()
                valid_upto = cells[5].inner_text().strip()

                if not reg_no:
                    counters["error_count"] += 1
                    continue

                key = generate_project_key(reg_no)

                try:
                    # ── Open modal and extract labeled fields ────────────────
                    view_btn = row.query_selector(SEL_VIEW_BTN)
                    modal_fields: dict = {}
                    if view_btn:
                        view_btn.click()
                        try:
                            page.wait_for_selector(SEL_MODAL_VIS, timeout=10_000)
                            modal_fields = _extract_modal_fields(page)
                            _close_modal(page)
                        except PWTimeout:
                            logger.warning(f"Modal did not open for {reg_no}", project_key=key, step="detail")

                    # ── Build payload ────────────────────────────────────────
                    payload: dict = {
                        # Listing-level fields (labeled by visible column headers)
                        "district":                district,
                        "project_name":            proj_name,
                        "promoter_name":           prom_name,
                        "project_registration_no": reg_no,
                        "valid_upto":              valid_upto,
                        # Modal detail fields (keys = exact label text from page)
                        **modal_fields,
                        # Meta
                        "domain":     DOMAIN,
                        "url":        LISTING_URL,
                        "state_code": STATE_CODE,
                        "state":      config.get("state", "Punjab"),
                        "project_location_raw": {"district": district},
                    }

                    normalized = normalize_project_payload(
                        payload, config,
                        machine_name=machine_name,
                        machine_ip=machine_ip,
                    )
                    record  = ProjectRecord(**normalized)
                    db_dict = record.to_db_dict()
                    status  = upsert_project(db_dict)

                    if status == "new":
                        counters["projects_new"] += 1
                        logger.info(f"New: {reg_no}", project_key=key, registration_no=reg_no, step="upsert")
                    elif status == "updated":
                        counters["projects_updated"] += 1
                        logger.info(f"Updated: {reg_no}", project_key=key, registration_no=reg_no, step="upsert")
                    else:
                        counters["projects_skipped"] += 1

                except ValidationError as exc:
                    counters["error_count"] += 1
                    logger.error(str(exc), project_key=key, step="validate")
                    insert_crawl_error(run_id, config["id"], "VALIDATION_FAILED", str(exc), LISTING_URL, project_key=key)
                except Exception as exc:
                    counters["error_count"] += 1
                    logger.error(str(exc), project_key=key, step="upsert")
                    insert_crawl_error(run_id, config["id"], "CRAWLER_EXCEPTION", str(exc), LISTING_URL, project_key=key)

                if idx % 10 == 0:
                    random_delay(*delay_range)

        finally:
            browser.close()

    reset_checkpoint(config["id"], mode)
    logger.info(f"Punjab RERA complete: {counters}", step="done")
    return counters
