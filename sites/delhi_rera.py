"""
Delhi RERA Crawler — rera.delhi.gov.in/registered_promoters_list
Type: static (Drupal 7 — httpx + BeautifulSoup)

Strategy:
- ALL project data lives inline in the listing table rows. No detail pages.
- Listing: table.views-table.cols-5, paginated via ?page=N (0-indexed).
- Each <tr> has five <td> cells:
    td.views-field-php-1                   → promoter name, address, email, phone
    td.views-field-field-project-address   → project name, location string
    td.views-field-field-rera-registrationno → reg no, valid-until date, construction
                                               status, certificate PDF link
    td.views-field-php                     → QPR history page link
- Pagination: ul.pager li.pager-next a present → more pages; stops at absence or
  no rows returned.
- Sentinel: DLRERA2023P0017 (TARC KAILASA / TARC PROJECTS LIMITED)
"""
from __future__ import annotations

import re
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup, Tag
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.config import settings
from core.crawler_base import generate_project_key, random_delay, safe_get
from core.db import upsert_project, upsert_document, insert_crawl_error
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import build_document_urls, get_machine_context, normalize_project_payload
from core.s3 import compute_md5, upload_document, get_s3_url

LISTING_URL   = "https://rera.delhi.gov.in/registered_promoters_list"
BASE_URL      = "https://rera.delhi.gov.in"
DOMAIN        = "rera.delhi.gov.in"
_SENTINEL_REG = "DLRERA2023P0017"
# Matches e.g. DLRERA2023P0017 or DLRERA2022A0001
_REG_NO_RE  = re.compile(r"DLRERA\d{4}[PA]\d{4,5}", re.IGNORECASE)
_PIN_RE     = re.compile(r"\b(\d{6})\b")


def _get_listing_response(url: str, logger: CrawlerLogger, params: dict | None = None) -> httpx.Response | None:
    """Delhi returns a parseable listing page with a broken TLS chain and HTTP 500."""
    headers = {"User-Agent": settings.user_agents[0]}
    try:
        with httpx.Client(timeout=60.0, follow_redirects=True, verify=False) as client:
            resp = client.get(url, headers=headers, params=params)
        if resp.status_code >= 400 and "views-table" not in resp.text:
            logger.warning(
                f"Listing fetch returned HTTP {resp.status_code} without usable table markup",
                url=url,
            )
            return None
        return resp
    except Exception as exc:
        logger.warning(f"Listing fetch failed: {exc}", url=url)
        return None


# ─── Row-level helpers ────────────────────────────────────────────────────────

def _strong_values(td: Tag) -> dict[str, str]:
    """Return {label: value} from a <td> containing <strong>Label:</strong>text pairs.

    Iterates each <strong> element; collects NavigableString siblings (skipping
    <br>, <a>, <img>) until the next <strong>.  <span> children are included via
    get_text so that Drupal date-display-single values are captured.
    """
    result: dict[str, str] = {}
    for strong in td.find_all("strong"):
        label = strong.get_text(strip=True).rstrip(":").strip().lower()
        if not label:
            continue
        parts: list[str] = []
        for node in strong.next_siblings:
            tag = getattr(node, "name", None)
            if tag == "strong":
                break
            if tag in ("br", "a", "img"):
                continue
            if tag is None:                             # NavigableString
                text = str(node).strip()
            else:                                       # span, b, etc.
                text = node.get_text(separator=" ", strip=True)
            if text:
                parts.append(text)
        result[label] = " ".join(parts).strip()
    return result


def _abs(href: str) -> str:
    """Convert a relative href to an absolute URL."""
    return href if href.startswith("http") else urljoin(BASE_URL, href)


def _parse_row(tr: Tag) -> dict | None:
    """Parse one <tr> from the Delhi RERA listing table into a project dict.

    Returns None if no registration number can be extracted (row is unusable).

    Table columns (by CSS class):
      views-field-php-1                    → promoter name / address / email / phone
      views-field-field-project-address    → project name / location
      views-field-field-rera-registrationno → reg no / valid-until / status / cert PDF
      views-field-php                      → QPR history link
    """
    # ── Cell selectors ────────────────────────────────────────────────────────
    promo_td = tr.select_one("td.views-field-php-1")
    proj_td  = tr.select_one("td.views-field-field-project-address")
    reg_td   = tr.select_one("td.views-field-field-rera-registrationno")
    qpr_td   = tr.select_one("td.views-field-php")

    if not reg_td:
        return None

    # ── Registration number ───────────────────────────────────────────────────
    reg_text = reg_td.get_text(separator=" ", strip=True)
    m = _REG_NO_RE.search(reg_text)
    if not m:
        return None
    reg_no = m.group(0).upper()

    # ── Promoter cell ─────────────────────────────────────────────────────────
    promo_kv: dict[str, str] = _strong_values(promo_td) if promo_td else {}
    promoter_name  = promo_kv.get("name", "").strip()
    promoter_addr  = promo_kv.get("address", "").strip()
    promoter_email = promo_kv.get("email", "").strip()
    promoter_phone = (
        promo_kv.get("phone number", "") or promo_kv.get("phone", "")
    ).strip()

    # ── Project cell ──────────────────────────────────────────────────────────
    proj_kv: dict[str, str] = _strong_values(proj_td) if proj_td else {}
    project_name = proj_kv.get("name", "").strip()
    location_str = proj_kv.get("location", "").strip()

    # ── Registration cell ─────────────────────────────────────────────────────
    reg_kv = _strong_values(reg_td)

    # Valid-until: prefer ISO content attribute on date-display-single span
    valid_until: str | None = None
    date_span = reg_td.select_one("span.date-display-single")
    if date_span:
        valid_until = date_span.get("content") or date_span.get_text(strip=True) or None

    const_status = (
        reg_kv.get("construction status", "") or reg_kv.get("construction status:", "")
    ).strip() or None

    # Certificate PDF link
    cert_url: str | None = None
    cert_a = reg_td.select_one("span.file a[href]")
    if cert_a:
        cert_url = _abs(cert_a["href"])

    # Extension certificate (text note)
    ext_cert = reg_kv.get("extension certificate", "").strip() or None

    # ── QPR cell ──────────────────────────────────────────────────────────────
    qpr_url: str | None = None
    if qpr_td:
        qpr_a = qpr_td.find("a", href=True)
        if qpr_a:
            qpr_url = _abs(qpr_a["href"])

    # ── Build sub-dicts ───────────────────────────────────────────────────────
    contact: dict = {
        k: v for k, v in {"email": promoter_email, "phone": promoter_phone}.items() if v
    }

    loc_raw: dict = {}
    if location_str:
        loc_raw["raw_address"] = location_str
        pin_m = _PIN_RE.search(location_str)
        if pin_m:
            loc_raw["pin_code"] = pin_m.group(1)

    prom_addr_raw: dict = {}
    if promoter_addr:
        prom_addr_raw["registered_address"] = promoter_addr
        if not loc_raw.get("pin_code"):
            pin_m = _PIN_RE.search(promoter_addr)
            if pin_m:
                loc_raw["pin_code"] = pin_m.group(1)

    docs: list[dict] = []
    if cert_url:
        docs.append({"link": cert_url, "type": "Registration Certificate"})
    if qpr_url:
        docs.append({"link": qpr_url, "type": "QPR History"})

    data_snap: dict = {}
    if promoter_email:
        data_snap["email"] = promoter_email
    if cert_url:
        data_snap["link"] = cert_url

    out: dict = {
        "project_registration_no":  reg_no,
        "project_name":             project_name or None,
        "promoter_name":            promoter_name or None,
        "status_of_the_project":    const_status,
        "estimated_finish_date":    valid_until,
        "project_location_raw":     loc_raw or None,
        "promoter_address_raw":     prom_addr_raw or None,
        "promoter_contact_details": contact or None,
        "uploaded_documents":       docs or None,
        "data":                     data_snap or None,
    }
    if ext_cert:
        out["extension_certificate"] = ext_cert   # stored in data jsonb via normalizer
    return {k: v for k, v in out.items() if v not in (None, "", [], {})}


def _parse_listing_page(html: str) -> list[dict]:
    """Extract all project dicts from a single listing page."""
    soup = BeautifulSoup(html, "lxml")
    results: list[dict] = []
    table = soup.select_one("div.view-content table")
    if not table:
        return results
    for tr in table.select("tbody tr"):
        row = _parse_row(tr)
        if row:
            results.append(row)
    return results


def _has_next_page(html: str) -> bool:
    """Return True if the Drupal Views pager has a 'next' link."""
    soup = BeautifulSoup(html, "lxml")
    return bool(soup.select_one(
        "ul.pager li.pager-next a, "
        "ul.pager__items li.pager__item--next a"
    ))


# ─── Sentinel ─────────────────────────────────────────────────────────────────

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """Verify the listing page is reachable and returns ≥1 parsed rows."""
    resp = _get_listing_response(LISTING_URL, logger)
    if not resp:
        logger.error("Sentinel: listing page unreachable", step="sentinel")
        insert_crawl_error(
            run_id, config["id"], "SENTINEL_FAILED",
            "Listing page unreachable", url=LISTING_URL,
        )
        return False

    rows = _parse_listing_page(resp.text)
    if not rows:
        logger.error(
            "Sentinel: no rows found — site structure may have changed",
            step="sentinel",
        )
        insert_crawl_error(
            run_id, config["id"], "SENTINEL_FAILED",
            "No rows found on listing page", url=LISTING_URL,
        )
        return False

    sentinel_reg = config.get("sentinel_registration_no", _SENTINEL_REG)
    logger.info(
        f"Sentinel passed: {len(rows)} rows on page 0"
        + (f" (expected sentinel {sentinel_reg!r})" if sentinel_reg else ""),
        step="sentinel",
    )
    return True


# ─── Document processing ──────────────────────────────────────────────────────

def _process_documents(
    project_key: str,
    documents: list[dict],
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
) -> tuple[list[dict], int]:
    """Download, MD5-check, and upload each PDF document to S3.

    Returns:
        (enriched_documents, upload_count)
        enriched_documents: same list with 's3_link' injected for uploaded docs
        upload_count: number of documents actually uploaded
    """
    enriched: list[dict] = []
    upload_count = 0

    for doc in documents:
        url      = doc.get("link", "")
        doc_type = doc.get("type", "document")
        if not url or not url.lower().endswith(".pdf"):
            enriched.append(doc)
            continue

        slug     = re.sub(r"[^a-z0-9]+", "_", doc_type.lower()).strip("_") or "document"
        filename = f"{slug}.pdf"

        try:
            resp = safe_get(url, logger=logger, timeout=60.0, verify=False)
            if not resp or len(resp.content) < 100:
                enriched.append(doc)
                logger.warning(f"Document download failed or too small: {url}", step="documents")
                continue

            data   = resp.content
            md5    = compute_md5(data)
            s3_key = upload_document(project_key, filename, data, dry_run=settings.DRY_RUN_S3)
            if s3_key is None:
                enriched.append(doc)
                logger.warning(f"S3 upload returned None: {url}", step="documents")
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
            enriched.append({**doc, "s3_link": s3_url})
            upload_count += 1
            logger.info(f"Document uploaded: {doc_type!r}", s3_key=s3_key, step="documents")

        except Exception as exc:
            enriched.append(doc)
            logger.error(f"Document processing error: {exc}", url=url, step="documents")
            insert_crawl_error(
                run_id, site_id, "S3_UPLOAD_FAILED", str(exc),
                url=url, project_key=project_key,
            )

    return enriched, upload_count


# ─── Main entry point ─────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    """
    Args:
        config  : site dict from sites_config.SITES
        run_id  : crawl_runs.id for this run
        mode    : 'daily_light' | 'weekly_deep'
    Returns:
        dict with keys: projects_found, projects_new, projects_updated,
                        projects_skipped, documents_uploaded, error_count
    """
    logger = CrawlerLogger(config["id"], run_id)
    counters = dict(
        projects_found=0, projects_new=0, projects_updated=0,
        projects_skipped=0, documents_uploaded=0, error_count=0,
    )
    machine_name, machine_ip = get_machine_context()
    item_limit   = settings.CRAWL_ITEM_LIMIT or 0
    items_done   = 0
    delay_range  = config.get("rate_limit_delay", (2, 4))
    max_pages    = settings.MAX_PAGES

    # ── Sentinel ──────────────────────────────────────────────────────────────
    if not _sentinel_check(config, run_id, logger):
        return counters

    # ── Resume from checkpoint ────────────────────────────────────────────────
    checkpoint = load_checkpoint(config["id"], mode)
    page = (checkpoint or {}).get("last_page", 0)
    if page:
        logger.info(f"Resuming from checkpoint: page {page}", step="checkpoint")

    # ── Pagination loop ───────────────────────────────────────────────────────
    while True:
        page_url = f"{LISTING_URL}?page={page}"
        logger.info(f"Fetching listing page {page}", url=page_url, step="listing")

        resp = _get_listing_response(page_url, logger)
        if not resp:
            logger.error(f"Failed to fetch page {page}", step="listing")
            insert_crawl_error(
                run_id, config["id"], "HTTP_ERROR",
                f"page {page} fetch failed", url=page_url,
            )
            counters["error_count"] += 1
            break

        html = resp.text
        rows = _parse_listing_page(html)
        if not rows:
            logger.info(f"No rows on page {page} — pagination complete", step="listing")
            break

        counters["projects_found"] += len(rows)
        logger.info(f"Page {page}: {len(rows)} rows", step="listing")

        stop_all = False
        for row in rows:
            if item_limit and items_done >= item_limit:
                logger.info(f"Item limit {item_limit} reached — stopping", step="listing")
                stop_all = True
                break

            reg_no = row.get("project_registration_no", "").strip().upper()
            if not reg_no:
                counters["error_count"] += 1
                continue

            key = generate_project_key(reg_no)
            logger.set_project(key=key, reg_no=reg_no, url=page_url, page=page)
            try:
                # ── Build, normalize, upsert ──────────────────────────────────
                try:
                    payload: dict = {
                        **row,
                        "url":    page_url,
                        "domain": DOMAIN,
                        "state":  config.get("state", "delhi"),
                    }
                    payload = {k: v for k, v in payload.items() if v not in (None, "", [], {})}

                    normalized = normalize_project_payload(
                        payload, config,
                        machine_name=machine_name,
                        machine_ip=machine_ip,
                    )
                    record  = ProjectRecord(**normalized)
                    db_dict = record.to_db_dict()
                    status  = upsert_project(db_dict)
                    items_done += 1

                    if status == "new":
                        counters["projects_new"] += 1
                        logger.info(f"New project: {reg_no}", step="upsert")
                    elif status == "updated":
                        counters["projects_updated"] += 1
                        logger.info(f"Updated: {reg_no}", step="upsert")
                    else:
                        counters["projects_skipped"] += 1

                    # ── Documents ──────────────────────────────────────────────
                    docs = row.get("uploaded_documents") or []
                    if docs and (mode == "weekly_deep" or status == "new"):
                        enriched, doc_count = _process_documents(
                            key, docs, run_id, config["id"], logger,
                        )
                        counters["documents_uploaded"] += doc_count
                        upsert_project({
                            "key":                key,
                            "url":                db_dict["url"],
                            "state":              db_dict["state"],
                            "domain":             db_dict["domain"],
                            "project_registration_no": db_dict["project_registration_no"],
                            "uploaded_documents": enriched,
                            "document_urls":      build_document_urls(enriched),
                        })

                except ValidationError as exc:
                    counters["error_count"] += 1
                    logger.error(f"Validation error for {reg_no}: {exc}", step="validate")
                    insert_crawl_error(
                        run_id, config["id"], "VALIDATION_FAILED", str(exc),
                        project_key=key, url=page_url,
                    )
                except Exception as exc:
                    counters["error_count"] += 1
                    logger.error(f"Unexpected error for {reg_no}: {exc}", step="upsert")
                    insert_crawl_error(
                        run_id, config["id"], "EXTRACTION_FAILED", str(exc),
                        project_key=key, url=page_url,
                    )
            finally:
                logger.clear_project()

            random_delay(*delay_range)

        # ── Checkpoint + advance ───────────────────────────────────────────
        save_checkpoint(config["id"], mode, page, None, run_id)

        if stop_all:
            break
        if max_pages is not None and page >= max_pages - 1:
            logger.info(f"Reached max_pages={max_pages}, stopping", step="listing")
            break
        if not _has_next_page(html):
            logger.info("No more pages in Drupal pager", step="listing")
            break

        page += 1
        random_delay(*delay_range)

    reset_checkpoint(config["id"], mode)
    logger.info(f"Delhi RERA complete: {counters}", step="done")
    return counters
