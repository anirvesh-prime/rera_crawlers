"""
Pondicherry RERA Crawler — prera.py.gov.in
Type: static (httpx with SSL verification disabled — legacy government SSL)

Strategy:
- GET /reraAppOffice/viewDefaulterProjects (~1.3 MB page, 363 project cards)
- Each card (div.no_pad_lft) contains:
    <h1>  → project name
    <p>   → address text
    <p>   → "Reg No.: PRPY..." → real registration number
    <table> → Promoter, Promoter Type, Property Type, Status
    <a href="viewProjectDetailPage?projectID=N"> → detail page link
- For each project, fetch /reraAppOffice/viewProjectDetailPage?projectID=N to get:
    Promoter, Date of Registration, Address, Project Type/Status/Description/Cost,
    Start/End dates, District, Taluk, Village, Applicant email+phone, documents
- Documents: registration certificate PDF at /reraAppOffice/getdocument?DOC_ID=N
"""
from __future__ import annotations

import re

from bs4 import BeautifulSoup
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
    document_result_entry,
    get_machine_context,
    merge_data_sections,
    normalize_project_payload,
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings

LISTING_URL  = "https://prera.py.gov.in/reraAppOffice/viewDefaulterProjects"
BASE_URL     = "https://prera.py.gov.in"
APP_BASE     = "https://prera.py.gov.in/reraAppOffice"
STATE_CODE   = "PY"
DOMAIN       = "prera.py.gov.in"


def _get(url: str, logger: CrawlerLogger):
    """Thin wrapper around safe_get with SSL verification disabled (legacy govt cert)."""
    return safe_get(url, verify=False, logger=logger, timeout=60.0)


def _parse_pondicherry_date(raw: str) -> str | None:
    """
    Convert "Wed Mar 25 16:15:36 IST 2026" → "2026-03-25" so that
    ProjectRecord's _parse_dt validator can handle it.
    Falls back to None if the string can't be parsed.
    """
    if not raw:
        return None
    import datetime
    # Strip timezone abbreviation (IST, UTC, etc.) since strptime can't handle it
    cleaned = re.sub(r"\b[A-Z]{2,5}\b", "", raw).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    for fmt in ("%a %b %d %H:%M:%S %Y", "%a %b  %d %H:%M:%S %Y"):
        try:
            dt = datetime.datetime.strptime(cleaned, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ── Listing page parsing ───────────────────────────────────────────────────────

def _parse_listing_cards(soup: BeautifulSoup) -> list[dict]:
    """
    Extract project cards from the listing page.
    Each card is a div.col-md-10.no_pad_lft containing project name (h1),
    address (p), reg number (p with 'Reg No.'), table row, and detail link.
    """
    cards = []
    for card in soup.find_all("div", class_=lambda c: c and "no_pad_lft" in c):
        # Project name from <h1> — strip the "Project: " span
        h1 = card.find("h1")
        if not h1:
            continue
        span = h1.find("span")
        project_name = h1.get_text(strip=True)
        if span:
            project_name = project_name.replace(span.get_text(strip=True), "").strip()

        # Registration number and address from <p> tags
        reg_no = None
        address = None
        for p in card.find_all("p"):
            txt = p.get_text(separator=" ", strip=True)
            m = re.search(r"Reg\s*No\.?:?\s*(\S+)", txt, re.I)
            if m:
                reg_no = m.group(1).strip()
            elif txt and "pull-right" not in (p.get("class") or []) and len(txt) > 5:
                address = re.sub(r"\s+", " ", txt).strip()

        if not reg_no:
            continue  # skip cards without a real registration number

        # Promoter info from the table row
        tds = [td.get_text(strip=True) for td in card.find_all("td")]
        promoter_name  = tds[0] if len(tds) > 0 else ""
        promoter_type  = tds[1] if len(tds) > 1 else ""
        prop_type      = tds[2] if len(tds) > 2 else ""
        listing_status = tds[3] if len(tds) > 3 else ""
        revoke_reason  = tds[4] if len(tds) > 4 else ""

        # Detail page URL
        detail_a = card.find("a", href=re.compile(r"viewProjectDetailPage"))
        detail_url = (APP_BASE + "/" + detail_a["href"]) if detail_a else None

        cards.append({
            "project_name":      project_name,
            "project_registration_no": reg_no,
            "promoter_address_text": address,
            "promoter_name":     promoter_name,
            "promoter_type":     promoter_type,
            "project_type":      prop_type,
            "listing_status":    listing_status,
            "revoke_reason":     revoke_reason,
            "detail_url":        detail_url,
        })
    return cards


# ── Detail page parsing ────────────────────────────────────────────────────────

_DETAIL_LABEL_MAP: dict[str, str] = {
    "promoter":           "promoter_name",
    "date of registration": "approved_on_date",
    "project type":       "project_type",
    "project status":     "status_of_the_project",
    "project description": "project_description",
    "project start date": "estimated_commencement_date",
    "project end date":   "estimated_finish_date",
    "district/region":    "project_city",
}

_LOCATION_DETAIL_LABELS = {
    "address",
    "project address",
    "district/region",
    "taluk",
    "village",
    "ward",
    "block",
}


def _parse_detail_page(url: str, logger: CrawlerLogger) -> dict:
    """
    Fetch and parse a Pondicherry project detail page.
    Returns a dict with mapped schema fields + raw JSONB data.
    """
    resp = _get(url, logger)
    if not resp:
        return {}
    soup = BeautifulSoup(resp.text, "lxml")
    out: dict = {}
    raw: dict = {"source_url": url}
    location_raw: dict = {}

    # Key-value rows: label in text-right <p>, value in the following sibling <p>
    for row in soup.find_all("div", class_="row"):
        cols = row.find_all("div", recursive=False)
        # Collect (label_text, value_text) pairs from adjacent col pairs
        pairs: list[tuple[str, str]] = []
        i = 0
        while i < len(cols):
            p_label = cols[i].find("p", class_="text-right")
            if p_label and i + 1 < len(cols):
                label = p_label.get_text(strip=True).replace(":", "").strip()
                value = cols[i + 1].get_text(separator=" ", strip=True)
                pairs.append((label, value))
                i += 2
            else:
                i += 1
        for label, value in pairs:
            raw[label] = value
            if label.lower().strip() in _LOCATION_DETAIL_LABELS and value:
                location_raw[label] = value
            schema_field = _DETAIL_LABEL_MAP.get(label.lower().strip())
            if schema_field and value and schema_field not in out:
                # Registration date comes as "Wed Mar 25 16:15:36 IST 2026" —
                # convert to YYYY-MM-DD so _parse_dt in the model can handle it.
                if schema_field == "approved_on_date":
                    value = _parse_pondicherry_date(value) or value
                out[schema_field] = value
            if label.lower().strip() == "project cost" and value:
                out["project_cost_detail"] = {"project_cost": value}

    if location_raw:
        out["project_location_raw"] = location_raw

    # Promoter contact details from the applicant table
    tables = soup.find_all("table")
    if tables:
        hdrs = [th.get_text(strip=True) for th in tables[0].find_all("th")]
        rows_data = []
        for tr in tables[0].find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if cells:
                rows_data.append(dict(zip(hdrs, cells)))
        if rows_data:
            out["promoter_contact_details"] = rows_data
            raw["applicant_table"] = rows_data

    # Document links: href may be absolute ("/reraAppOffice/getdocument?...")
    # or relative ("reraAppOffice/getdocument?...") — always anchor to BASE_URL.
    doc_links = []
    for a in soup.find_all("a", href=re.compile(r"getdocument")):
        label = a.get_text(strip=True) or "document"
        href = a["href"]
        if href.startswith("http"):
            full_url = href
        elif href.startswith("/"):
            full_url = f"{BASE_URL}{href}"
        else:
            full_url = f"{BASE_URL}/{href}"
        doc_links.append({"label": label, "url": full_url})

    out["_doc_links"] = doc_links
    out["data"] = raw
    return out


# ── Document handling ─────────────────────────────────────────────────────────

def _handle_document(project_key: str, doc: dict, run_id: int,
                     site_id: str, logger: CrawlerLogger) -> dict | None:
    url   = doc["url"]
    label = doc["label"]
    fname = build_document_filename(doc)
    try:
        resp = _get(url, logger)
        if not resp or len(resp.content) < 100:
            return None
        md5    = compute_md5(resp.content)
        s3_key = upload_document(project_key, fname, resp.content, dry_run=settings.DRY_RUN_S3)
        s3_url = get_s3_url(s3_key)
        upsert_document(project_key=project_key, document_type=label,
                        original_url=document_identity_url(doc) or url, s3_key=s3_key,
                        s3_bucket=settings.S3_BUCKET_NAME,
                        file_name=fname, md5_checksum=md5,
                        file_size_bytes=len(resp.content))
        logger.info("Document handled", label=label, s3_key=s3_key)
        return document_result_entry(doc, s3_url, fname)
    except Exception as e:
        logger.error(f"Doc failed for {project_key}: {e}")
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(e),
                           project_key=project_key, url=url)
        return None


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """Verify the sentinel project still appears on the live Pondicherry listing page."""
    sentinel_reg = config.get("sentinel_registration_no", "")
    if not sentinel_reg:
        logger.warning("No sentinel configured — skipping")
        return True
    key = generate_project_key(sentinel_reg)
    existing = get_project_by_key(key)
    if not existing:
        logger.warning("Sentinel not in DB yet — skipping check")
        return True
    resp = _get(LISTING_URL, logger)
    if not resp:
        logger.error("Sentinel: could not fetch Pondicherry listing page")
        return False
    if sentinel_reg not in resp.text:
        logger.error("Sentinel reg number not found on Pondicherry listing page", reg=sentinel_reg)
        return False
    logger.info("Sentinel check passed", reg=sentinel_reg)
    return True


# ── Main run() ────────────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    logger   = CrawlerLogger(config["id"], run_id)
    site_id  = config["id"]
    counts   = dict(projects_found=0, projects_new=0, projects_updated=0,
                    projects_skipped=0, documents_uploaded=0, error_count=0)
    checkpoint  = load_checkpoint(site_id, mode) or {}
    done_regs: set[str] = set(checkpoint.get("done_regs", []))
    item_limit = settings.CRAWL_ITEM_LIMIT or 0

    if not _sentinel_check(config, run_id, logger):
        insert_crawl_error(run_id, site_id, "SENTINEL_FAILED", "Sentinel check failed")
        return counts

    # Fetch listing page
    resp = _get(LISTING_URL, logger)
    if not resp:
        logger.error("Failed to load Pondicherry listing page")
        insert_crawl_error(run_id, site_id, "listing_load_failed",
                           "Could not fetch listing page", url=LISTING_URL)
        counts["error_count"] += 1
        return counts

    soup  = BeautifulSoup(resp.text, "lxml")
    cards = _parse_listing_cards(soup)
    if item_limit:
        cards = cards[:item_limit]
        logger.info(f"Pondicherry: CRAWL_ITEM_LIMIT={item_limit} applied — processing {len(cards)} projects")
    else:
        # max_pages treats every 50 projects as one "page" (single-page site)
        max_pages = settings.MAX_PAGES
        if max_pages:
            cards = cards[:max_pages * 50]
            logger.info(f"Pondicherry: limiting to first {len(cards)} projects (max_pages={max_pages})")
    counts["projects_found"] = len(cards)
    logger.info(f"Pondicherry: {len(cards)} project cards found")

    machine_name, machine_ip = get_machine_context()

    for i, card in enumerate(cards):
        reg_no = card["project_registration_no"]
        if reg_no in done_regs:
            counts["projects_skipped"] += 1
            continue

        try:
            key  = generate_project_key(reg_no)
            logger.set_project(key=key, reg_no=reg_no, url=card.get("detail_url", LISTING_URL))

            if mode == "daily_light" and get_project_by_key(key):
                logger.info("Skipping — already in DB (daily_light)", step="skip")
                counts["projects_skipped"] += 1
                logger.clear_project()
                continue

            data: dict = {
                "key":                     key,
                "state":                   config["state"],
                "project_state":           config["state"],
                "project_registration_no": reg_no,
                "project_name":            card["project_name"] or None,
                "promoter_name":           card["promoter_name"] or None,
                "project_type":            card["project_type"] or None,
                "status_of_the_project":   card["listing_status"] or None,
                "domain":                  DOMAIN,
                "config_id":               config["config_id"],
                "url":                     card["detail_url"] or LISTING_URL,
                "is_live":                 True,
                "machine_name":            machine_name,
                "crawl_machine_ip":        machine_ip,
                "promoter_address_raw": {
                    "promoter_type":   card["promoter_type"],
                    "address_text":    card["promoter_address_text"],
                    "revoke_reason":   card["revoke_reason"],
                } if any([card["promoter_type"], card["promoter_address_text"]]) else None,
            }



            doc_links: list[dict] = []
            if card["detail_url"]:
                random_delay(*config.get("rate_limit_delay", (1, 3)))
                logger.info("Fetching detail page", step="detail_fetch")
                detail = _parse_detail_page(card["detail_url"], logger)
                doc_links = detail.pop("_doc_links", [])
                for k, v in detail.items():
                    if v is not None and not k.startswith("_"):
                        data[k] = v
                data["data"] = merge_data_sections({"listing_card": card}, data.get("data"))
            else:
                data["data"] = {"listing_card": card}

            logger.info("Normalizing and validating", step="normalize")
            try:
                normalized = normalize_project_payload(data, config, machine_name=machine_name, machine_ip=machine_ip)
                record  = ProjectRecord(**normalized)
                db_dict = record.to_db_dict()
            except (ValidationError, ValueError) as e:
                logger.warning("Validation failed — using raw fallback", step="normalize", error=str(e))
                insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(e),
                                   project_key=key, url=data.get("url"), raw_data=data)
                counts["error_count"] += 1
                db_dict = normalize_project_payload(
                    {**data, "data": merge_data_sections(data.get("data"), {"validation_fallback": True})},
                    config, machine_name=machine_name, machine_ip=machine_ip,
                )

            logger.info("Upserting to DB", step="db_upsert")
            action = upsert_project(db_dict)
            if action == "new":       counts["projects_new"] += 1
            elif action == "updated": counts["projects_updated"] += 1
            else:                     counts["projects_skipped"] += 1
            logger.info(f"DB result: {action}", step="db_upsert")

            logger.info(f"Downloading {len(doc_links)} documents", step="documents")
            uploaded_documents = []
            doc_name_counts: dict[str, int] = {}
            for doc in doc_links:
                selected_doc = select_document_for_download(config["state"], doc, doc_name_counts, domain=DOMAIN)
                if selected_doc:
                    uploaded_doc = _handle_document(db_dict["key"], selected_doc, run_id, site_id, logger)
                    if uploaded_doc:
                        uploaded_documents.append(uploaded_doc)
                        counts["documents_uploaded"] += 1
                    else:
                        uploaded_documents.append({"link": doc.get("url"), "type": doc.get("label") or doc.get("type", "document")})
                else:
                    uploaded_documents.append({"link": doc.get("url"), "type": doc.get("label") or doc.get("type", "document")})
            if uploaded_documents:
                upsert_project({
                    "key": db_dict["key"], "url": db_dict["url"],
                    "state": db_dict["state"], "domain": db_dict["domain"],
                    "project_registration_no": db_dict["project_registration_no"],
                    "uploaded_documents": uploaded_documents,
                    "document_urls": build_document_urls(uploaded_documents),
                })

            done_regs.add(reg_no)
            if i % 50 == 0:
                save_checkpoint(site_id, mode, i, reg_no, run_id)

        except Exception as exc:
            logger.exception("Project processing failed", exc, step="project_loop")
            insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                               project_key=key, url=card.get("detail_url"))
            counts["error_count"] += 1
        finally:
            logger.clear_project()

    reset_checkpoint(site_id, mode)
    logger.info(f"Pondicherry RERA complete: {counts}")
    return counts
