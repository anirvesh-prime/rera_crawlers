"""
Odisha RERA Crawler — rera.odisha.gov.in/projects/project-list
Type: Playwright (Angular SPA)

Strategy:
- Listing page: 10 cards/page, ~112 pages. Each card: project name, promoter,
  city, type, start/end dates, units, reg_no, cert PDF link, phone.
- Detail page: reached by clicking each card's 'View Details' button → Angular
  route with an encrypted project token. Three tabs are scraped:
    1. Project Overview — registration date, full location, building type,
       professionals (engineers/architects/CA/GRO), bank accounts, financial details.
    2. Promoter Details — company name, addresses, entity, directors, email, GST.
    3. Documents — all uploaded PDFs (layout plans, legal docs, financial docs).
- Per page: cards are processed sequentially (click → parse → go_back → next card).
"""
from __future__ import annotations

import copy
import re
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page, TimeoutError as PWTimeout
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import generate_project_key, random_delay, safe_get
from core.db import get_project_by_key, upsert_project, insert_crawl_error, upsert_document
from core.document_policy import select_document_for_download
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import (
    build_document_filename,
    build_document_urls,
    document_identity_url,
    get_machine_context,
    merge_data_sections,
    normalize_project_payload,
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings

LISTING_URL     = "https://rera.odisha.gov.in/projects/project-list"
BASE_URL        = "https://rera.odisha.gov.in"
STATE_CODE      = "OD"
DOMAIN          = "rera.odisha.gov.in"
DMS_BASE_URL    = "https://reraapps.odisha.gov.in/dms"
DMS_DECRYPT_URL = DMS_BASE_URL + "/fileDecryptHandlerForPdfPublic"


# ── Playwright helpers ────────────────────────────────────────────────────────

def _dismiss_modal(page: Page) -> None:
    for selector in (".swal2-confirm", ".swal2-cancel", ".swal2-close"):
        try:
            if page.locator(selector).count():
                page.locator(selector).first.click(timeout=1500)
                page.wait_for_timeout(400)
        except Exception:
            pass
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(400)
    except Exception:
        pass
    try:
        page.locator(".swal2-container").wait_for(state="hidden", timeout=1500)
    except Exception:
        pass


def _scroll_full(page: Page) -> None:
    """Scroll the page fully so Angular lazy-loaded cards / buttons are all rendered."""
    for pct in (0.2, 0.4, 0.6, 0.8, 1.0):
        page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {pct})")
        page.wait_for_timeout(600)
    page.wait_for_timeout(400)


def _wait_for_loaders(page: Page, timeout: int = 25000) -> None:
    """Scroll down to trigger lazy sections then settle."""
    try:
        page.wait_for_load_state("networkidle", timeout=timeout)
    except Exception:
        pass
    _scroll_full(page)
    page.wait_for_timeout(800)


# ── HTML parsing helpers ──────────────────────────────────────────────────────

def _parse_label_values(container: BeautifulSoup) -> dict[str, str]:
    """
    Extract label-control → value pairs.
    Values may live in <strong> (core fields) or <span> (bank/financial fields).
    Child <a> tags are stripped from <span> values so only text remains.
    """
    result: dict[str, str] = {}
    for label in container.find_all("label", class_="label-control"):
        key = label.get_text(strip=True)
        if not key:
            continue
        val_elem = label.find_next_sibling("strong") or label.find_next_sibling("span")
        if not val_elem:
            continue
        if val_elem.name == "span":
            elem_copy = copy.copy(val_elem)
            for a in elem_copy.find_all("a"):
                a.decompose()
            val = elem_copy.get_text(strip=True)
        else:
            val = val_elem.get_text(strip=True)
        if val:
            result[key] = val
    return result


def _extract_doc_links(soup: BeautifulSoup) -> list[dict]:
    """Collect all reraapps DMS document viewer links anywhere in the page.

    Label resolution priority (highest → lowest):
    1. The ``<a>`` tag text content — what the site itself displays next to
       the link (e.g. 'Coloured Layout Plan', 'Site Plan').
    2. A nearby ``label.label-control`` element found by walking up the DOM.
    3. Falls back to ``"document"`` when nothing else is found.

    The ``ngbtooltip`` attribute is always the generic 'Download document'
    string and is intentionally ignored.
    """
    seen: set[str] = set()
    docs: list[dict] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "reraapps.odisha.gov.in/dms" not in href or href in seen:
            continue
        seen.add(href)

        # 1. Text the site shows next to the link
        label = a.get_text(strip=True)

        # 2. Walk up the DOM for label.label-control
        if not label or label.lower() == "download document":
            label = ""
            el = a
            for _ in range(8):
                el = el.parent
                if el is None:
                    break
                lbl_tag = el.find("label", class_="label-control")
                if lbl_tag:
                    label = lbl_tag.get_text(strip=True)
                    break

        if not label or label.lower() == "download document":
            label = "document"

        docs.append({"label": label, "url": href})
    return docs


def _resolve_dms_viewer_url(page: Page, viewer_url: str, logger: CrawlerLogger) -> str | None:
    """Resolve a DMS PDF.js viewer URL to a direct downloadable file URL.

    The DMS viewer page uses JavaScript to POST to the decrypt endpoint (with the
    token from the URL) and receive a temporary ``filePath``.  We replicate that
    POST using Playwright's request API so that the browser session cookies are
    included automatically, making the call succeed.

    Args:
        page: Active Playwright page (must be in the same browser context that
              loaded the RERA project detail page).
        viewer_url: Full DMS viewer URL, e.g.
            ``https://reraapps.odisha.gov.in/dms/public/library/pdfjsnewds/web/
            viewer.html?fileId=195411&text=5b1c34a…``
        logger: Crawler logger for warnings.

    Returns:
        The direct URL of the decrypted PDF/file, or *None* on failure.
    """
    parsed = urlparse(viewer_url)
    params = parse_qs(parsed.query)
    file_id = (params.get("fileId") or params.get("fileid") or [None])[0]
    token   = (params.get("text") or [None])[0]
    if not file_id or not token:
        logger.warning("Cannot resolve DMS URL — missing fileId or text token", url=viewer_url)
        return None
    try:
        resp = page.request.post(
            DMS_DECRYPT_URL,
            form={"fileId": file_id, "logId": "", "token": token},
            headers={"Authorization": f"bearer {token}"},
        )
        data = resp.json()
        if data.get("status") == 200:
            file_path = (data.get("result") or {}).get("filePath", "")
            if file_path:
                logger.info("Resolved DMS viewer URL", file_id=file_id, resolved=file_path[:80])
                return file_path
        logger.warning("DMS decrypt returned non-200", status=data.get("status"), file_id=file_id)
    except Exception as exc:
        logger.warning(f"DMS viewer URL resolution failed: {exc}", url=viewer_url)
    return None


def _parse_professionals(soup: BeautifulSoup) -> list[dict]:
    """Parse the Projects Professionals cards."""
    professionals: list[dict] = []
    for header in soup.find_all(class_="card-header"):
        h = header.find("h5")
        if not (h and "professional" in h.get_text(strip=True).lower()):
            continue
        body = header.find_next_sibling(class_="card-body")
        if not body:
            break
        for card in body.find_all("div", class_="card"):
            cb = card.find(class_="card-body")
            if not cb:
                continue
            name = (cb.find("h5", class_="card-title") or cb.find("h5") or BeautifulSoup("", "lxml")).get_text(strip=True)
            role_p = cb.find("p", class_=lambda c: c and "text-body-secondary" in c)
            role = role_p.get_text(strip=True) if role_p else ""
            strongs = [s.get_text(strip=True) for s in cb.find_all("strong")]
            if name:
                professionals.append({
                    "name": name, "role": role,
                    "email": strongs[0] if len(strongs) > 0 else "",
                    "phone": strongs[1] if len(strongs) > 1 else "",
                    "registration_no": strongs[2] if len(strongs) > 2 else "",
                })
        break
    return professionals


# ── Tab parsers ───────────────────────────────────────────────────────────────

_OVERVIEW_LABEL_MAP: dict[str, str] = {
    "Project Name":         "project_name",
    "Project Type":         "project_type",
    "RERA Regd. No.":       "project_registration_no",
}

_FINANCIAL_KEYS = {
    "Estimated Project Cost",
    "Fund to be invested by promoter from own source",
    "Funds to be mobilized from allottees",
    "Funds to be mobilized through Bank finance",
    "Funds to be mobilized through Investor",
}


def _parse_overview(soup: BeautifulSoup) -> dict:
    """Parse Project Overview tab. Returns schema fields + raw JSONB + _doc_links."""
    labels = _parse_label_values(soup)
    out: dict = {}
    raw: dict = {"labels": labels}

    for lbl, field in _OVERVIEW_LABEL_MAP.items():
        val = labels.get(lbl)
        if val:
            out[field] = val

    building_fields = ["Building Type", "Planning Authority", "Authority Details"]
    bld = {k: labels[k] for k in building_fields if k in labels}
    if labels.get("Number of Units"):
        bld["number_of_units"] = labels["Number of Units"]
    if bld:
        out["building_details"] = bld

    financial = {k: labels[k] for k in _FINANCIAL_KEYS if k in labels}
    if financial:
        out["project_cost_detail"] = financial
        raw["financial_details"] = financial

    bank_accounts = _parse_bank_accounts(soup)
    if bank_accounts:
        out["bank_details"] = bank_accounts
        raw["bank_accounts"] = bank_accounts

    professionals = _parse_professionals(soup)
    if professionals:
        raw["professionals"] = professionals
        out["professional_information"] = professionals

    if labels.get("Project Location"):
        out["project_location_raw"] = {"project_location": labels["Project Location"]}

    out["data"] = raw
    out["_doc_links"] = _extract_doc_links(soup)
    return out


_PROMOTER_LABEL_MAP: dict[str, str | None] = {
    "Company Name":                  "promoter_name",
    "Registration No.":              None,
    "Entity":                        None,
    "Email Id":                      None,
    "Mobile":                        None,
    "GST No.":                       None,
    "Correspondence Office Address": None,
    "Registered Office Address":     None,
}


def _parse_promoter_tab(soup: BeautifulSoup) -> dict:
    """Parse Promoter Details tab."""
    labels = _parse_label_values(soup)
    out: dict = {}
    addr_raw: dict = {}
    contact_raw: dict = {}

    for lbl, field in _PROMOTER_LABEL_MAP.items():
        val = labels.get(lbl)
        if not val:
            continue
        if field:
            out[field] = val
        if "Address" in lbl or lbl == "Entity":
            addr_raw[lbl] = val
        else:
            contact_raw[lbl] = val

    if addr_raw:
        out["promoter_address_raw"] = addr_raw
    if contact_raw:
        out["promoter_contact_details"] = contact_raw

    # Board members — each appears as a row: name | role | email | phone
    board: list[dict] = []
    for card in soup.find_all("div", class_=lambda c: c and "col-md" in c):
        strongs = [s.get_text(strip=True) for s in card.find_all("strong")]
        texts   = [p.get_text(strip=True) for p in card.find_all("p") if p.get_text(strip=True)]
        if len(strongs) >= 2 and len(texts) >= 2:
            board.append({
                "name":  strongs[0],
                "role":  texts[0] if texts else "",
                "email": strongs[1] if len(strongs) > 1 else "",
                "phone": strongs[2] if len(strongs) > 2 else "",
            })
    if board:
        out["members_details"] = board

    out["_raw"] = labels
    return out


# ── Listing page parser ───────────────────────────────────────────────────────

def _parse_page_cards(page: Page) -> list[dict]:
    """Extract all project cards from the current Playwright page state."""
    soup = BeautifulSoup(page.content(), "lxml")
    projects: list[dict] = []

    reg_spans = soup.find_all("span", class_=lambda c: c and "fw-bold" in c and "me-2" in c)
    for span in reg_spans:
        reg_no = span.get_text(strip=True)
        if not re.match(r"[A-Z]{2,4}/\d{2}/\d{4}/\d{5}", reg_no):
            continue

        # Cert PDF link
        cert_url = None
        next_a = span.find_next("a", class_=lambda c: c and "icon-pdf" in c)
        if next_a and next_a.get("href"):
            cert_url = next_a["href"]

        # Phone from tel: link
        phone = None
        tel_a = span.find_next("a", href=re.compile(r"^tel:"))
        if tel_a:
            phone = tel_a["href"].replace("tel:", "").strip()

        # Walk up to card container
        card = span
        for _ in range(10):
            parent = card.find_parent()
            if parent is None:
                break
            if any(k in " ".join(parent.get("class", [])) for k in ("card", "col-md", "col-lg", "project")):
                card = parent
                break
            card = parent

        fields = [f.strip() for f in card.get_text(separator="|", strip=True).split("|") if f.strip()]
        project_name = promoter = city = proj_type = start_date = end_date = units = status = ""

        for i, tok in enumerate(fields):
            if tok.lower().startswith("by ") and not promoter:
                promoter = tok[3:].strip()
                if i > 0:
                    project_name = fields[i - 1]
            if tok.lower() == "address" and i + 1 < len(fields):
                city = fields[i + 1]
            if tok.lower() == "project type" and i + 1 < len(fields):
                proj_type = fields[i + 1]
            if tok.lower() == "started from" and i + 1 < len(fields):
                start_date = fields[i + 1]
            if tok.lower() == "possession by" and i + 1 < len(fields):
                end_date = fields[i + 1]
            units_match = re.match(r"^(\d+)\s+units?(?:\s+(available|sold|fully sold))?$", tok, re.I)
            if units_match:
                units = units_match.group(1)
                if units_match.group(2):
                    status = units_match.group(2)
            if tok.lower() in ("available", "sold", "fully sold"):
                status = tok

        projects.append({
            "project_registration_no":     reg_no,
            "project_name":                project_name or None,
            "promoter_name":               promoter or None,
            "listing_city":                city or None,
            "project_type":                proj_type or None,
            "estimated_commencement_date": start_date or None,
            "estimated_finish_date":       end_date or None,
            "listing_unit_count":          units or None,
            "listing_availability_status": status or None,
            "phone":                       phone or None,
            "cert_url":                    cert_url,
        })
    return projects


def _open_detail_page(page: Page, reg: str, logger: CrawlerLogger) -> bool:
    """Open a detail page from the listing view using several fallback strategies."""
    _dismiss_modal(page)
    _scroll_full(page)
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(300)

    clicked = page.evaluate(
        """(reg) => {
            const spans = Array.from(document.querySelectorAll('span'));
            const span = spans.find(s => s.textContent.trim() === reg);
            if (!span) return false;
            let el = span.parentElement;
            for (let i = 0; i < 12; i++) {
                if (!el) return false;
                const els = Array.from(el.querySelectorAll('a, button'));
                const btn = els.find(b => b.textContent.trim() === 'View Details');
                if (btn) { btn.click(); return true; }
                el = el.parentElement;
            }
            return false;
        }""",
        reg,
    )
    if clicked:
        return True

    try:
        button = page.locator("span.fw-bold.me-2", has_text=reg).locator(
            "xpath=ancestor::*[contains(@class, 'card')][1]"
        ).get_by_text("View Details", exact=True).first
        button.click(force=True, timeout=5000)
        return True
    except Exception as exc:
        logger.warning("View Details open failed", reg=reg, error=str(exc))
        return False


def _parse_bank_accounts(soup: BeautifulSoup) -> list[dict]:
    """Parse the bank account fieldsets (Master, RERA Designated, Promoter)."""
    accounts: list[dict] = []
    for fieldset in soup.find_all("fieldset"):
        legend = fieldset.find("legend")
        if not legend:
            continue
        vals = {k: v for k, v in _parse_label_values(fieldset).items() if v}
        if vals:
            accounts.append({"account_type": legend.get_text(strip=True), **vals})
    return accounts



# ── Sentinel check ────────────────────────────────────────────────────────────

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger, page: Page) -> bool:
    sentinel_reg = config.get("sentinel_registration_no", "")
    if not sentinel_reg:
        logger.warning("No sentinel configured — skipping")
        return True
    key = generate_project_key(sentinel_reg)
    if not get_project_by_key(key):
        logger.warning("Sentinel not in DB yet — skipping check")
        return True
    if sentinel_reg not in page.content():
        logger.error("Sentinel not found on listing page", reg=sentinel_reg)
        return False
    logger.info("Sentinel check passed", reg=sentinel_reg)
    return True


# ── Document handler ──────────────────────────────────────────────────────────

def _handle_document(project_key: str, doc: dict, run_id: int,
                     site_id: str, logger: CrawlerLogger) -> dict | None:
    """Download a document, upload to S3, persist to DB. Returns normalized document metadata or None."""
    url   = doc.get("url")
    label = doc.get("label", "document")
    if not url:
        return None
    filename = build_document_filename(doc)
    try:
        resp = safe_get(url, logger=logger, timeout=30.0)
        if not resp or len(resp.content) < 100:
            return None
        content = resp.content
        md5     = compute_md5(content)
        s3_key  = upload_document(project_key, filename, content, dry_run=settings.DRY_RUN_S3)
        s3_url  = get_s3_url(s3_key)
        upsert_document(project_key=project_key, document_type=label,
                        original_url=document_identity_url(doc) or url, s3_key=s3_key,
                        s3_bucket=settings.S3_BUCKET_NAME, file_name=filename,
                        md5_checksum=md5, file_size_bytes=len(content))
        page_url = doc.get("source_url") or doc.get("url")
        logger.info("Document handled", label=label, s3_key=s3_key)
        return {"type": label, "link": page_url, "s3_link": s3_url}
    except Exception as e:
        logger.error(f"Document failed: {e}", url=url)
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(e),
                           project_key=project_key, url=url)
        return None


# ── Main run() ────────────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    logger       = CrawlerLogger(config["id"], run_id)
    site_id      = config["id"]
    counts       = dict(projects_found=0, projects_new=0, projects_updated=0,
                        projects_skipped=0, documents_uploaded=0, error_count=0)
    checkpoint   = load_checkpoint(site_id, mode) or {}
    # Resume from the page AFTER the last completed one — the saved page was
    # already fully processed, so re-starting there would duplicate work.
    start_page   = checkpoint.get("last_page", 0) + 1
    done_regs: set[str] = set()
    item_limit   = settings.CRAWL_ITEM_LIMIT or 0
    items_processed = 0
    max_pages: int | None = settings.MAX_PAGES
    machine_name, machine_ip = get_machine_context()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page    = browser.new_page()
        page.goto(LISTING_URL, wait_until="networkidle", timeout=40000)
        page.wait_for_timeout(5000)
        _dismiss_modal(page)

        if not _sentinel_check(config, run_id, logger, page):
            insert_crawl_error(run_id, site_id, "SENTINEL_FAILED", "Sentinel check failed")
            browser.close()
            return counts

        page_num = 1
        while True:
            if item_limit and items_processed >= item_limit:
                logger.info(f"Item limit {item_limit} reached — stopping.", step="listing")
                break

            # Skip to start_page (resume after checkpoint)
            if page_num < start_page:
                try:
                    all_btns = page.query_selector_all(
                        "li.page-item:not(.disabled):not(.active) button.page-link")
                    found = next(
                        (b for b in all_btns
                         if (b.text_content() or "").strip() == str(page_num + 1)), None)
                    if not found:
                        break
                    found.click()
                    page.wait_for_timeout(2500)
                    page_num += 1
                    continue
                except Exception:
                    break

            logger.info(f"Odisha listing page {page_num}")
            _dismiss_modal(page)
            # Scroll full page so all Angular lazy-cards are rendered
            _scroll_full(page)
            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(400)
            cards = _parse_page_cards(page)
            counts["projects_found"] += len(cards)

            for card in cards:
                if item_limit and items_processed >= item_limit:
                    logger.info(f"Item limit {item_limit} reached — stopping.", step="listing")
                    break

                reg  = card["project_registration_no"]
                key  = generate_project_key(reg)

                if reg in done_regs:
                    counts["projects_skipped"] += 1
                    continue

                logger.set_project(key=key, reg_no=reg, page=page_num)

                if mode == "daily_light" and get_project_by_key(key):
                    logger.info("Skipping — already in DB (daily_light)", step="skip")
                    counts["projects_skipped"] += 1
                    logger.clear_project()
                    continue

                try:
                    # ── Navigate to detail page ───────────────────────────
                    logger.info("Opening detail page", step="detail_fetch")
                    if not _open_detail_page(page, reg, logger):
                        logger.warning("No View Details button found", step="detail_fetch")
                        continue
                    page.wait_for_url("**/project-details/**", timeout=15000)
                    detail_url = page.url
                    logger.set_project(key=key, reg_no=reg, url=detail_url, page=page_num)
                    _wait_for_loaders(page)

                    # ── Parse Project Overview tab ────────────────────────
                    overview  = _parse_overview(BeautifulSoup(page.content(), "lxml"))
                    doc_links = overview.pop("_doc_links", [])

                    # ── Parse Promoter Details tab ────────────────────────
                    promoter: dict = {}
                    try:
                        _dismiss_modal(page)  # dismiss any SweetAlert2 modal first
                        page.click("text=Promoter Details", timeout=8000)
                        page.wait_for_timeout(4000)
                        try:
                            page.wait_for_load_state("networkidle", timeout=8000)
                        except Exception:
                            pass
                        promoter = _parse_promoter_tab(BeautifulSoup(page.content(), "lxml"))
                    except Exception as e:
                        logger.warning(f"Promoter tab failed for {reg}: {e}")

                    # ── Parse Documents tab ───────────────────────────────
                    try:
                        _dismiss_modal(page)
                        page.click("text=Documents", timeout=8000)
                        page.wait_for_timeout(3000)
                        try:
                            page.wait_for_load_state("networkidle", timeout=10000)
                        except Exception:
                            pass
                        extra_docs = _extract_doc_links(BeautifulSoup(page.content(), "lxml"))
                        seen_urls = {d["url"] for d in doc_links}
                        doc_links += [d for d in extra_docs if d["url"] not in seen_urls]
                    except Exception as e:
                        logger.warning(f"Documents tab failed for {reg}: {e}")

                    # ── Add registration cert from listing card ───────────
                    if card.get("cert_url"):
                        cert_doc = {"label": "registration_certificate", "url": card["cert_url"]}
                        if cert_doc["url"] not in {d["url"] for d in doc_links}:
                            doc_links.insert(0, cert_doc)

                    # ── Resolve DMS viewer URLs → direct PDF URLs ─────────
                    # DMS links now point to a PDF.js/HTML viewer page.
                    # We POST to the decrypt endpoint (within the active
                    # browser session so cookies are valid) to obtain the
                    # temporary direct file URL before navigating away.
                    resolved: list[dict] = []
                    for doc in doc_links:
                        url = doc.get("url", "")
                        is_viewer = (
                            "reraapps.odisha.gov.in/dms" in url
                            and any(v in url for v in ("viewer.html", "demos-preview.html"))
                        )
                        if is_viewer:
                            direct_url = _resolve_dms_viewer_url(page, url, logger)
                            if direct_url:
                                doc = {**doc, "url": direct_url, "source_url": url}
                        resolved.append(doc)
                    doc_links = resolved

                    # ── Go back to listing page ───────────────────────────
                    page.go_back()
                    # If we're still on detail page (Promoter/Documents tabs push history), go back once more
                    if "/project-details/" in page.url:
                        page.go_back()
                    try:
                        page.wait_for_url("**/project-list**", timeout=8000)
                    except Exception:
                        pass
                    try:
                        page.wait_for_load_state("networkidle", timeout=12000)
                    except Exception:
                        pass
                    page.wait_for_timeout(1500)
                    # Re-render all cards (Angular lazy-scroll)
                    _scroll_full(page)
                    page.evaluate("window.scrollTo(0, 0)")
                    page.wait_for_timeout(400)

                    # ── Build data dict ───────────────────────────────────
                    overview_data  = {k: v for k, v in overview.items()
                                      if v is not None and not k.startswith("_") and k != "data"}
                    promoter_data  = {k: v for k, v in promoter.items()
                                      if v is not None and not k.startswith("_")}
                    card_data      = {k: v for k, v in card.items()
                                      if k not in ("cert_url", "phone") and v is not None}

                    data: dict = {
                        "key":              key,
                        "state":            config["state"],
                        "project_state":    config["state"],
                        "domain":           DOMAIN,
                        "config_id":        config["config_id"],
                        "url":              detail_url,
                        "is_live":          True,
                        "machine_name":     machine_name,
                        "crawl_machine_ip": machine_ip,
                        **card_data,
                        **overview_data,
                        **promoter_data,
                        "data": merge_data_sections(
                            # PROD-compatible metadata — must be first so raw sections don't overwrite
                            {
                                "govt_type":   "state",
                                "is_processed": False,
                            },
                            {
                                "source_url": detail_url,
                                "page_num": page_num,
                                "raw_card": {k: v for k, v in card.items() if k != "cert_url" and v},
                            },
                            overview.get("data"),
                            {"promoter_tab": promoter.get("_raw")} if promoter.get("_raw") else None,
                        ),
                    }
                    if card.get("phone"):
                        existing_contact = data.get("promoter_contact_details")
                        if isinstance(existing_contact, dict):
                            existing_contact.setdefault("listing_phone", card["phone"])
                        elif existing_contact is None:
                            data["promoter_contact_details"] = {"listing_phone": card["phone"]}

                    logger.info("Normalizing and validating", step="normalize")
                    try:
                        normalized = normalize_project_payload(data, config, machine_name=machine_name, machine_ip=machine_ip)
                        record  = ProjectRecord(**normalized)
                        db_dict = record.to_db_dict()
                    except (ValidationError, ValueError) as e:
                        logger.warning("Validation failed — using raw fallback", step="normalize", error=str(e))
                        insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(e),
                                           project_key=key, url=detail_url, raw_data=data)
                        counts["error_count"] += 1
                        db_dict = normalize_project_payload(
                            {**data, "data": merge_data_sections(data.get("data"), {"validation_fallback": True})},
                            config, machine_name=machine_name, machine_ip=machine_ip,
                        )

                    logger.info("Upserting to DB", step="db_upsert")
                    action = upsert_project(db_dict)
                    items_processed += 1
                    if action == "new":       counts["projects_new"] += 1
                    elif action == "updated": counts["projects_updated"] += 1
                    else:                     counts["projects_skipped"] += 1
                    logger.info(f"DB result: {action}", step="db_upsert")

                    logger.info(f"Downloading {len(doc_links)} documents", step="documents")
                    uploaded_documents: list[dict] = []
                    doc_name_counts: dict[str, int] = {}
                    for doc in doc_links:
                        selected_doc = select_document_for_download(config["state"], doc, doc_name_counts, domain=DOMAIN)
                        if selected_doc:
                            uploaded_doc = _handle_document(key, selected_doc, run_id, site_id, logger)
                            if uploaded_doc:
                                uploaded_documents.append(uploaded_doc)
                                counts["documents_uploaded"] += 1
                            else:
                                uploaded_documents.append({"link": doc.get("url"), "type": doc.get("label", "document")})
                        else:
                            uploaded_documents.append({"link": doc.get("url"), "type": doc.get("label", "document")})
                    if uploaded_documents:
                        upsert_project({
                            "key": db_dict["key"], "url": db_dict["url"],
                            "state": db_dict["state"], "domain": db_dict["domain"],
                            "project_registration_no": db_dict["project_registration_no"],
                            "uploaded_documents": uploaded_documents,
                            "document_urls": build_document_urls(uploaded_documents),
                        })

                    done_regs.add(reg)
                    random_delay(*config.get("rate_limit_delay", (1, 2)))

                except Exception as exc:
                    logger.exception("Project processing failed", exc, step="project_loop")
                    insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                                       project_key=key, url=detail_url)
                    counts["error_count"] += 1
                    if "/project-details/" in page.url:
                        try:
                            page.goto(LISTING_URL, wait_until="networkidle", timeout=20000)
                            page.wait_for_timeout(3000)
                            _dismiss_modal(page)
                        except Exception:
                            pass
                finally:
                    logger.clear_project()

            save_checkpoint(site_id, mode, page_num, None, run_id)

            if max_pages and page_num >= start_page + max_pages - 1:
                logger.info(f"Reached max_pages={max_pages}, stopping.")
                break

            _dismiss_modal(page)
            try:
                next_page_num = page_num + 1
                all_btns = page.query_selector_all(
                    "li.page-item:not(.disabled):not(.active) button.page-link")
                found_next = next(
                    (b for b in all_btns
                     if (b.text_content() or "").strip() == str(next_page_num)), None)
                if not found_next:
                    logger.info(f"No page {next_page_num} button — crawl complete")
                    break
                found_next.click()
                page.wait_for_timeout(3000)
                page.wait_for_load_state("networkidle", timeout=15000)
                page_num += 1
                random_delay(*config.get("rate_limit_delay", (2, 4)))
            except PWTimeout:
                logger.info("No more pages")
                break
            except Exception as e:
                logger.warning(f"Pagination error at page {page_num}: {e}")
                break

        browser.close()

    reset_checkpoint(site_id, mode)
    logger.info(f"Odisha RERA complete: {counts}")
    return counts
