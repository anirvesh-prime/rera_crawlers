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
import time

from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import download_response, generate_project_key, random_delay, safe_get
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
STATE_DISPLAY_NAME = "Puducherry"

# Maps normalized fragments of raw document filename to canonical type names.
_PUDUCHERRY_DOC_TYPE_MAP = [
    ("fromb",                   "Declaration (Form B)"),
    ("formb",                   "Declaration (Form B)"),
    ("declarationformb",        "Declaration (Form B)"),
    ("registrationcertificate", "Project Registration Certificate"),
]


def _normalize_puducherry_doc_label(raw_label: str) -> str:
    """Map raw Puducherry document filenames to canonical document type names."""
    normalized = "".join(c for c in (raw_label or "").lower() if c.isalnum())
    for fragment, canonical in _PUDUCHERRY_DOC_TYPE_MAP:
        if fragment in normalized:
            return canonical
    return raw_label


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
    "promoter":             "promoter_name",
    # "Date of Registration" is the submission timestamp — map to submitted_date
    # so the full HH:MM:SS is preserved instead of being truncated to midnight.
    "date of registration": "submitted_date",
    "date of application":  "submitted_date",
    "application date":     "submitted_date",
    "project type":         "project_type",
    "project status":       "status_of_the_project",
    "project description":  "project_description",
    "project start date":   "estimated_commencement_date",
    "project end date":     "estimated_finish_date",
    # district/region only goes to project_location_raw.district, not project_city
}

# Map raw label → normalized key accepted by project_location_raw whitelist
_LOCATION_LABEL_TO_KEY: dict[str, str] = {
    "address":        "raw_address",
    "project address": "raw_address",
    "district/region": "district",
    "district":       "district",
    "taluk":          "taluk",
    "village":        "village",
    "ward":           "ward",
    "block":          "block",
    "pin code":       "pin_code",
    "pincode":        "pin_code",
    "latitude":       "latitude",
    "longitude":      "longitude",
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

    # Project name: second <h1> on the page contains the project title followed by
    # "Last UpdatedOn <timestamp>".  Strip the timestamp suffix to get the clean name.
    h1_tags = soup.find_all("h1")
    if len(h1_tags) >= 2:
        raw_h1 = h1_tags[1].get_text(separator=" ", strip=True)
        # Remove "Last UpdatedOn ..." suffix (any capitalisation)
        clean_name = re.split(r"Last\s*Updated\s*On", raw_h1, flags=re.I)[0].strip()
        if clean_name:
            out["project_name"] = clean_name

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
            label_lower = label.lower().strip()
            # Normalize location label to whitelisted key and accumulate.
            # raw_address is only set from the first matching label ("Address")
            # so that a later "Project Address" field (which may omit commas)
            # does not overwrite the more complete value already captured.
            loc_key = _LOCATION_LABEL_TO_KEY.get(label_lower)
            if loc_key and value:
                if loc_key == "raw_address" and "raw_address" in location_raw:
                    pass  # keep the first (more complete) address
                else:
                    location_raw[loc_key] = value
            schema_field = _DETAIL_LABEL_MAP.get(label_lower)
            if schema_field and value and schema_field not in out:
                # Pass date strings as-is — parse_datetime in the ProjectRecord
                # validator handles "Wed Mar 25 16:15:36 IST 2026" natively and
                # preserves the full HH:MM:SS precision.
                out[schema_field] = value
            if label_lower in ("project cost", "estimated project cost", "total project cost") and value:
                out["project_cost_detail"] = {"total_project_cost": value}
            if label_lower in ("land area", "plot area", "total land area") and value:
                out["land_area"] = value

    # Always populate land_area_details so the field is present even when values are null
    out["land_area_details"] = {k: v for k, v in {
        "land_area":              str(out["land_area"]) if out.get("land_area") else None,
        "land_area_unit":         "Sq Mtr" if out.get("land_area") else None,
        "construction_area":      None,
        "construction_area_unit": "Sq Mtr",
    }.items() if v is not None}

    # Build a fuller raw_address: "<detail address>, <district>, Puducherry"
    if location_raw:
        _raw_addr = location_raw.get("raw_address", "").strip()
        _district  = location_raw.get("district", "").strip()
        if _raw_addr:
            addr_parts = [_raw_addr]
            if _district:
                addr_parts.append(_district)
            addr_parts.append(STATE_DISPLAY_NAME)
            location_raw["raw_address"] = ", ".join(addr_parts)
        location_raw["state"] = STATE_DISPLAY_NAME
        out["project_location_raw"] = location_raw

    # Promoter contact details and professional information from page tables.
    # Table 0: applicant (Name / E-mail / Mobile)
    # Table 3: Architects
    # Table 4: Structural Engineers (label headers may say "Engineer Name")
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

    # Professional information: Architects (table 3) + Engineers (table 4)
    _PROF_TABLE_ROLES = [(3, "Architects"), (4, "Structural Engineers")]
    professionals = []
    for tbl_idx, role in _PROF_TABLE_ROLES:
        if tbl_idx >= len(tables):
            continue
        tbl = tables[tbl_idx]
        hdrs = [th.get_text(strip=True) for th in tbl.find_all("th")]
        for tr in tbl.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if not any(cells):
                continue
            row_dict = dict(zip(hdrs, cells))
            entry: dict = {"role": role}
            for hdr, val in row_dict.items():
                hdr_l = hdr.lower()
                if not val:
                    continue
                if "name" in hdr_l:
                    entry["name"] = val
                elif "email" in hdr_l:
                    entry["email"] = val
                elif "address" in hdr_l:
                    entry["address"] = val
                elif "mobile" in hdr_l or "phone" in hdr_l:
                    entry["mobile"] = val
            if len(entry) > 1:  # role + at least one other field
                professionals.append(entry)
    if professionals:
        out["professional_information"] = professionals

    # Document links: href may be absolute ("/reraAppOffice/getdocument?...")
    # or relative ("reraAppOffice/getdocument?...") — always anchor to BASE_URL.
    # Labels are normalized from raw filenames to canonical document type names.
    doc_links = []
    for a in soup.find_all("a", href=re.compile(r"getdocument")):
        raw_label = a.get_text(strip=True) or "document"
        label = _normalize_puducherry_doc_label(raw_label)
        href = a["href"]
        if href.startswith("http"):
            full_url = href
        elif href.startswith("/"):
            full_url = f"{BASE_URL}{href}"
        else:
            full_url = f"{BASE_URL}/{href}"
        doc_links.append({"label": label, "url": full_url})

    out["_doc_links"] = doc_links

    # Build promoters_details from the promoter name already extracted via the
    # label map so the sentinel coverage check finds the field.
    if out.get("promoter_name") and "promoters_details" not in out:
        out["promoters_details"] = {"name": out["promoter_name"]}

    # Expose doc links as uploaded_documents so the sentinel coverage check
    # (which looks for this key, not _doc_links) sees the field as populated.
    if doc_links:
        out["uploaded_documents"] = [
            {"type": d.get("label", "document"), "link": d["url"]} for d in doc_links
        ]

    out["data"] = raw
    return out


# ── Document handling ─────────────────────────────────────────────────────────

def _handle_document(project_key: str, doc: dict, run_id: int,
                     site_id: str, logger: CrawlerLogger) -> dict | None:
    url   = doc["url"]
    label = doc["label"]
    fname = build_document_filename(doc)
    try:
        resp = download_response(url, logger=logger, timeout=60.0, verify=False)
        if not resp or len(resp.content) < 100:
            return None
        md5    = compute_md5(resp.content)
        s3_key = upload_document(project_key, fname, resp.content, dry_run=settings.DRY_RUN_S3)
        if s3_key is None:
            return None
        s3_url = get_s3_url(s3_key)
        upsert_document(project_key=project_key, document_type=label,
                        original_url=document_identity_url(doc) or url, s3_key=s3_key,
                        s3_bucket=settings.S3_BUCKET_NAME,
                        file_name=fname, md5_checksum=md5,
                        file_size_bytes=len(resp.content))
        logger.info("Document handled", label=label, s3_key=s3_key, step="documents")
        logger.log_document(label, url, "uploaded", s3_key=s3_key, file_size_bytes=len(resp.content))
        return document_result_entry(doc, s3_url, fname)
    except Exception as e:
        logger.error(f"Doc failed for {project_key}: {e}")
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(e),
                           project_key=project_key, url=url)
        return None


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Pondicherry RERA.
    Loads state_projects_sample/puducherry.json as the baseline, re-scrapes the
    sentinel project's detail page, and verifies ≥ 80% field coverage.
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
        "state_projects_sample", "puducherry.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check",
                       path=sample_path, step="sentinel")
        return True

    detail_url = baseline.get("url", "")
    if not detail_url:
        logger.warning("Sentinel: no detail URL in sample — skipping", step="sentinel")
        return True

    logger.info(f"Sentinel: scraping {sentinel_reg}", url=detail_url, step="sentinel")
    try:
        fresh = _parse_detail_page(detail_url, logger) or {}
    except Exception as exc:
        logger.error(f"Sentinel: scrape error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", url=detail_url, step="sentinel")
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "pondicherry_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
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
    t_run = time.monotonic()

    # ── Sentinel health check ────────────────────────────────────────────────
    t0 = time.monotonic()
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counts["error_count"] += 1
        return counts
    logger.warning(f"Step timing [sentinel]: {time.monotonic()-t0:.2f}s", step="timing")

    # Fetch listing page
    t0 = time.monotonic()
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
    logger.warning(f"Step timing [search]: {time.monotonic()-t0:.2f}s  rows={len(cards)}", step="timing")

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
                # project_state is NOT pre-populated: Puducherry doesn't expose
                # a separate project_state field on its pages, so it should remain
                # null rather than being defaulted to the config state key.
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
            }
            # Fields populated from the listing card that must not be overwritten
            # by the detail page (the listing is the canonical source for these).
            _LISTING_LOCKED_FIELDS = {"status_of_the_project", "project_type",
                                      "promoter_name", "project_name"}

            # Promoters details from listing card (promoter type is always available)
            if card.get("promoter_type") or card.get("promoter_name"):
                data["promoters_details"] = {k: v for k, v in {
                    "type_of_firm": card.get("promoter_type"),
                    "name":         card.get("promoter_name"),
                }.items() if v}



            doc_links: list[dict] = []
            if card["detail_url"]:
                random_delay(*config.get("rate_limit_delay", (1, 3)))
                logger.info("Fetching detail page", step="detail_fetch")
                detail = _parse_detail_page(card["detail_url"], logger)
                doc_links = detail.pop("_doc_links", [])
                for k, v in detail.items():
                    if v is not None and not k.startswith("_"):
                        # Don't let detail page values overwrite listing-card fields
                        # (e.g. listing shows "APPROVED" while detail page says "Ongoing").
                        if k in _LISTING_LOCKED_FIELDS and data.get(k) is not None:
                            continue
                        data[k] = v
                # Build data JSONB with schema-allowed keys
                _raw_addr = (data.get("project_location_raw") or {}).get("raw_address") if isinstance(data.get("project_location_raw"), dict) else None
                data["data"] = merge_data_sections(
                    {"listing_card": card},
                    data.get("data"),
                    {
                        "govt_type":     "state",
                        "is_processed":  False,
                        "promoter_type": card.get("promoter_type") or None,
                        "raw_address":   _raw_addr,
                    },
                )
            else:
                data["data"] = {"listing_card": card, "govt_type": "state", "is_processed": False}

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
    logger.warning(f"Step timing [total_run]: {time.monotonic()-t_run:.2f}s", step="timing")
    return counts
