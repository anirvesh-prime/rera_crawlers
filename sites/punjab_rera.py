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
import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import httpx
from playwright.sync_api import TimeoutError as PWTimeout, sync_playwright
from pydantic import ValidationError

from core.checkpoint import reset_checkpoint
from core.captcha_solver import captcha_to_text
from core.crawler_base import generate_project_key, random_delay
from core.config import settings
from core.db import upsert_project, insert_crawl_error
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import get_machine_context, normalize_project_payload

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


def _sentinel_check(page, logger: CrawlerLogger) -> bool:
    """Submit a blank search and verify at least one result row appears."""
    page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=30_000)

    for attempt in range(1, 3):
        captcha_value = _solve_search_captcha(page, logger)
        if not captcha_value:
            logger.warning("Captcha solve failed during sentinel", step="sentinel")
            return False

        try:
            page.fill(SEL_CAPTCHA, captcha_value)
            page.click(SEL_SUBMIT)
            page.wait_for_selector(SEL_ROWS, timeout=20_000)
            logger.info("Sentinel passed", step="sentinel")
            return True
        except PWTimeout:
            logger.warning(
                f"Sentinel search attempt {attempt} returned no rows; refreshing captcha",
                step="sentinel",
            )
            try:
                page.click("a.capcha-refresh", timeout=5_000)
                page.wait_for_timeout(1_000)
            except Exception:
                page.reload(wait_until="domcontentloaded", timeout=30_000)

    logger.error("Sentinel: no project rows appeared after search", step="sentinel")
    return False


def _solve_listing_captcha(session: httpx.Client, logger: CrawlerLogger) -> tuple[str | None, BeautifulSoup | None]:
    try:
        resp = session.get(LISTING_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        captcha_img = soup.select_one(SEL_CAPTCHA_IMG)
        if not captcha_img or not captcha_img.get("src"):
            logger.warning("Captcha image not found on listing page", step="captcha")
            return None, soup
        img_url = urljoin(LISTING_URL, captcha_img["src"])
        img_resp = session.get(img_url)
        img_resp.raise_for_status()
        data_url = "data:image/png;base64," + base64.b64encode(img_resp.content).decode()
        solved = (captcha_to_text(data_url, default_captcha_source="eprocure") or "").strip()
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
            headers={"Referer": LISTING_URL, "X-Requested-With": "XMLHttpRequest"},
        )
        resp.raise_for_status()
        return _extract_modal_fields_html(resp.text)
    except Exception as exc:
        logger.warning(f"Detail fetch failed: {exc}", step="detail")
        return {}


# ── Main entry point ──────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    logger = CrawlerLogger(config["id"], run_id)
    counters = dict(projects_found=0, projects_new=0, projects_updated=0,
                    projects_skipped=0, documents_uploaded=0, error_count=0)
    machine_name, machine_ip = get_machine_context()
    delay_range = config.get("rate_limit_delay", (2, 4))
    item_limit = settings.CRAWL_ITEM_LIMIT or 0

    with httpx.Client(timeout=60.0, follow_redirects=True) as session:
        rows = _search_projects(session, logger)
        if not rows:
            return counters

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
            logger.set_project(key=key, reg_no=reg_no, url=LISTING_URL, page=idx)
            try:
                try:
                    modal_fields = _fetch_detail_fields(session, row, logger)
                    payload: dict = {
                        "district": row.get("district"),
                        "project_name": row.get("project_name"),
                        "promoter_name": row.get("promoter_name"),
                        "project_registration_no": reg_no,
                        "valid_upto": row.get("valid_upto"),
                        **modal_fields,
                        "domain": DOMAIN,
                        "url": LISTING_URL,
                        "state_code": STATE_CODE,
                        "state": config.get("state", "Punjab"),
                        "project_location_raw": {"district": row.get("district")},
                    }

                    normalized = normalize_project_payload(
                        payload, config,
                        machine_name=machine_name,
                        machine_ip=machine_ip,
                    )
                    record = ProjectRecord(**normalized)
                    db_dict = record.to_db_dict()
                    status = upsert_project(db_dict)

                    if status == "new":
                        counters["projects_new"] += 1
                    elif status == "updated":
                        counters["projects_updated"] += 1
                    else:
                        counters["projects_skipped"] += 1

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
                logger.clear_project()

            if idx % 10 == 0:
                random_delay(*delay_range)

    reset_checkpoint(config["id"], mode)
    logger.info(f"Punjab RERA complete: {counters}", step="done")
    return counters
