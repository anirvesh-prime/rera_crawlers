"""
Rajasthan RERA Crawler — rera.rajasthan.gov.in
Type: JSON REST API (Angular SPA)

Strategy:
- POST to reraapi.rajasthan.gov.in/api/web/Home/GetProjects with x-api-key header
- Single call returns all registered projects (no pagination required)
- Each project has a detail page at rera.rajasthan.gov.in/ProjectDetail/{id}
"""
from __future__ import annotations

import re

from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import generate_project_key, random_delay, safe_get, safe_post
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

BASE_URL       = "https://rera.rajasthan.gov.in"
API_BASE       = "https://reraapi.rajasthan.gov.in/api/web"
APP_BASE       = "https://reraapp.rajasthan.gov.in"
STATE_CODE     = "RJ"
DOMAIN         = "rera.rajasthan.gov.in"
_API_KEY       = "MySuperSecretApiKey_123"
_API_HEADERS   = {
    "x-api-key":   _API_KEY,
    "Accept":      "application/json, text/plain, */*",
    "Origin":      BASE_URL,
    "Referer":     f"{BASE_URL}/",
}

# Listing API field → schema field
_LIST_API_TO_FIELD: dict[str, str] = {
    "ProjectName":     "project_name",
    "PromoterName":    "promoter_name",
    "DistrictName":    "project_city",
    "ProjectTypeName": "project_type",
    "REGISTRATIONNO":  "project_registration_no",
    "APPROVEDON":      "approved_on_date",
    "AppStatus":       "status_of_the_project",
    "ApplicationNo":   "acknowledgement_no",
    "RevisedDateOfComplation": "estimated_finish_date",
}

# GetProjectById detail API field → schema field
_DETAIL_API_TO_FIELD: dict[str, str] = {
    "ProjectName":          "project_name",
    "PromoterName":         "promoter_name",
    "RevisedDateOfComplation": "estimated_finish_date",
    "DateofRegistration":   "approved_on_date",
    "ProjectCategory":      "project_type",
    "RegistrationNo":       "project_registration_no",
}



def _fetch_project_detail(enc_id: str, logger: CrawlerLogger) -> dict:
    """Call GetProjectById API and return explicit fields plus raw structured payloads."""
    resp = safe_post(
        f"{API_BASE}/Home/GetProjectById",
        json_data={"ProjectId": enc_id},
        headers=_API_HEADERS, retries=2, timeout=20,
    )
    if not resp:
        return {}
    try:
        proj = resp.json().get("Data", {}).get("Project", [{}])[0]
    except Exception:
        return {}

    out: dict = {}
    for api_f, schema_f in _DETAIL_API_TO_FIELD.items():
        val = proj.get(api_f)
        if val is not None and str(val).strip() and str(val) not in ("0", "None"):
            out[schema_f] = str(val).strip()

    if proj.get("ProjectLocation"):
        out["project_location_raw"] = {"project_location": proj.get("ProjectLocation")}

    promoter_address = {
        "details_of_promoter": proj.get("DetailsofPromoter"),
        "promoter_type": proj.get("PromoterType"),
    }
    promoter_contact = {
        "mobile": proj.get("promotermobileno"),
        "email": proj.get("promoteremail"),
    }
    if any(promoter_address.values()):
        out["promoter_address_raw"] = {k: v for k, v in promoter_address.items() if v}
    if any(promoter_contact.values()):
        out["promoter_contact_details"] = {k: v for k, v in promoter_contact.items() if v}

    if proj.get("TotalBuildingCount"):
        out["building_details"] = {
            "total_buildings": proj.get("TotalBuildingCount"),
            "sanctioned_buildings": proj.get("SanctionedbuildingCount"),
            "not_sanctioned_buildings": proj.get("NotSanctionedbuildingCount"),
            "open_space_area": proj.get("AggregateAreaOpenSpace"),
        }
    if any(proj.get(key) for key in ("Rectified_PhaseArea", "AggregateAreaOpenSpace")):
        out["land_area_details"] = {
            "rectified_phase_area": proj.get("Rectified_PhaseArea"),
            "aggregate_open_space_area": proj.get("AggregateAreaOpenSpace"),
        }
    out["data"] = {"source_api": "GetProjectById", "raw": proj}
    return out


def _fetch_all_projects(logger: CrawlerLogger) -> list[dict]:
    payload = {
        "DistrictId": 0, "TeshilId": 0, "ProjectName": None,
        "PromoterName": None, "RegistrationNo": None,
        "ProjectType": 0, "ApplicationStatus": "3", "Year": 0,
    }
    resp = safe_post(f"{API_BASE}/Home/GetProjects", json_data=payload,
                     headers=_API_HEADERS, retries=3, timeout=45)
    if not resp:
        logger.error("Failed to fetch Rajasthan project list from API")
        return []
    try:
        d = resp.json()
        data = d.get("Data", [])
        logger.info(f"Rajasthan API returned {len(data)} projects")
        return data
    except Exception as e:
        logger.error(f"Failed to parse Rajasthan API response: {e}")
        return []


# Ordered list of hosts to probe when resolving relative document paths.
# Checked left-to-right; first host that returns HTTP < 400 wins.
_DOC_HOSTS = [APP_BASE, BASE_URL]


def _is_real_document(resp) -> bool:
    """
    Return True only if the response body looks like an actual document.

    Government sites routinely serve soft-404 HTML pages with HTTP 200, so
    status code alone cannot be trusted. We instead peek at the first few bytes:
      - PDF files start with the magic bytes b'%PDF'
      - For other content types we reject anything whose Content-Type is HTML
    """
    if resp is None:
        return False
    content_type = resp.headers.get("Content-Type", "").lower()
    # Read just enough bytes to check the magic signature without pulling the full file
    chunk = resp.content[:8] if resp.content else b""
    if chunk.startswith(b"%PDF"):
        return True
    if "text/html" in content_type or "text/plain" in content_type:
        return False
    # Non-HTML content type with some body — treat as a real document
    return len(chunk) > 0


def _resolve_relative_url(path: str, hosts: list[str] = _DOC_HOSTS) -> str | None:
    """
    Turn a relative path (e.g. '~/Content/uploads/Certificate/Signed_xxx.pdf')
    into an absolute URL by probing each candidate host.

    Returns the first URL whose response body looks like a real document.
    Falls back to the first candidate so we never silently discard a URL
    (e.g. during a transient outage where all probes fail).

    Status codes are deliberately NOT used as the sole signal — government
    sites commonly return HTTP 200 for soft-404 HTML error pages.
    """
    clean = path.replace("~/", "").replace("~\\", "").replace("../", "").replace("..\\", "")
    if not clean.startswith("/"):
        clean = f"/{clean}"

    fallback: str | None = None
    for host in hosts:
        url = f"{host}{clean}"
        if fallback is None:
            fallback = url
        try:
            resp = safe_get(url, timeout=10)
            if _is_real_document(resp):
                return url
        except Exception:
            pass
    return fallback


def _build_cert_url(cert_path: str) -> str | None:
    """Convert relative UploadedCertificatePath to an absolute download URL."""
    if not cert_path or cert_path == "0":
        return None
    if cert_path.startswith("http://") or cert_path.startswith("https://"):
        return cert_path
    return _resolve_relative_url(cert_path)


def _build_app_url(path: str | None) -> str | None:
    if not path or path == "0":
        return None
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return _resolve_relative_url(path)


def _fetch_project_website_detail(enc_id: str, logger: CrawlerLogger) -> dict:
    resp = safe_get(f"{APP_BASE}/HomeWebsite/ProjectDtlsWebsite/{enc_id}", logger=logger, timeout=30)
    if not resp:
        return {}
    try:
        payload = resp.json()
    except Exception:
        return {}
    if not payload.get("success"):
        return {}
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


def _iter_website_documents(node, *, docs: list[dict], seen: set[str], section: str | None = None) -> None:
    if isinstance(node, dict):
        doc_url = _build_app_url(node.get("DocumentUrl"))
        if doc_url:
            label = (
                node.get("ApplicationDocumentName")
                or node.get("DocumentName")
                or node.get("MasterType")
                or section
                or "document"
            )
            if doc_url not in seen:
                seen.add(doc_url)
                entry = {
                    "label": str(label).strip(),
                    "url": doc_url,
                    "remarks": node.get("DocumentName"),
                    "upload_date": node.get("CreatedOn"),
                    "category": node.get("MasterType") or section,
                }
                docs.append({k: v for k, v in entry.items() if v not in (None, "")})
        for key, value in node.items():
            next_section = section
            if isinstance(key, str) and key.startswith("Get") and key.endswith("List"):
                next_section = key
            _iter_website_documents(value, docs=docs, seen=seen, section=next_section)
    elif isinstance(node, list):
        for item in node:
            _iter_website_documents(item, docs=docs, seen=seen, section=section)


def _extract_project_website_documents(website_data: dict) -> list[dict]:
    docs: list[dict] = []
    seen: set[str] = set()

    project_id = website_data.get("ProjectId")
    if project_id:
        for label, doc_type in (
            ("project_details_at_registration", "O"),
            ("updated_project_details", "U"),
        ):
            url = f"{BASE_URL}/ViewProject?id={project_id}&type={doc_type}"
            if url not in seen:
                seen.add(url)
                docs.append({"label": label, "url": url})

    _iter_website_documents(website_data, docs=docs, seen=seen)
    return docs


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """Verify the sentinel project still appears in the live Rajasthan API project list."""
    sentinel_reg = config.get("sentinel_registration_no", "")
    if not sentinel_reg:
        logger.warning("No sentinel configured — skipping")
        return True
    key = generate_project_key(STATE_CODE, sentinel_reg)
    existing = get_project_by_key(key)
    if not existing:
        logger.warning("Sentinel not in DB yet — skipping check")
        return True
    projects = _fetch_all_projects(logger)
    if not projects:
        logger.error("Sentinel: could not fetch project list from API")
        return False
    reg_numbers = {str(p.get("REGISTRATIONNO", "")).strip() for p in projects}
    if sentinel_reg not in reg_numbers:
        logger.error("Sentinel reg number not found in Rajasthan API project list", reg=sentinel_reg)
        return False
    logger.info("Sentinel check passed", reg=sentinel_reg)
    return True


def _handle_document(project_key: str, doc: dict, run_id: int,
                     site_id: str, logger: CrawlerLogger) -> dict | None:
    """Download a document, upload to S3, persist to DB. Returns normalized document metadata or None."""
    url   = doc.get("url")
    label = doc.get("label", "document")
    if not url:
        return None
    filename = build_document_filename(doc)
    try:
        resp = safe_get(url, logger=logger, timeout=30)
        if not resp or len(resp.content) < 100:
            return None
        content = resp.content
        md5     = compute_md5(content)
        s3_key  = upload_document(project_key, filename, content, dry_run=settings.DRY_RUN_S3)
        s3_url  = get_s3_url(s3_key)
        upsert_document(project_key=project_key, document_type=label, original_url=document_identity_url(doc) or url,
                        s3_key=s3_key, s3_bucket=settings.S3_BUCKET_NAME,
                        file_name=filename, md5_checksum=md5, file_size_bytes=len(content))
        logger.info("Document handled", label=label, s3_key=s3_key)
        return document_result_entry(doc, s3_url, filename)
    except Exception as e:
        logger.error(f"Document failed: {e}", url=url)
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(e),
                           project_key=project_key, url=url)
        return None


def run(config: dict, run_id: int, mode: str) -> dict:
    logger = CrawlerLogger(config["id"], run_id)
    site_id = config["id"]
    counts = dict(projects_found=0, projects_new=0, projects_updated=0,
                  projects_skipped=0, documents_uploaded=0, error_count=0)

    item_limit    = settings.CRAWL_ITEM_LIMIT or 0  # 0 = unlimited
    items_processed = 0

    if not _sentinel_check(config, run_id, logger):
        insert_crawl_error(run_id, site_id, "SENTINEL_FAILED", "Sentinel check failed")
        return counts

    checkpoint = load_checkpoint(site_id, mode) or {}
    resume_after_key = checkpoint.get("last_project_key")
    resume_pending = bool(resume_after_key)
    api_projects = _fetch_all_projects(logger)
    if not api_projects:
        return counts
    # Apply item_limit before max_pages so the env variable takes precedence
    if item_limit:
        api_projects = api_projects[:item_limit]
        logger.info(f"Rajasthan: CRAWL_ITEM_LIMIT={item_limit} applied — processing {len(api_projects)} projects")
    else:
        max_pages = settings.MAX_PAGES
        if max_pages:
            api_projects = api_projects[: max_pages * 50]
            logger.info(f"Rajasthan: limiting to first {len(api_projects)} projects (max_pages={max_pages})")
    counts["projects_found"] = len(api_projects)
    machine_name, machine_ip = get_machine_context()

    for i, proj in enumerate(api_projects):
        pid    = str(proj.get("Id", ""))
        enc_id = proj.get("EncryptedProjectId", "")
        reg_no = proj.get("REGISTRATIONNO") or f"RJ-{pid}"
        key = generate_project_key(STATE_CODE, reg_no)
        if resume_pending:
            if key == resume_after_key:
                resume_pending = False
            counts["projects_skipped"] += 1
            continue

        detail_url = f"{BASE_URL}/ProjectDetail?id={enc_id}" if enc_id else f"{BASE_URL}/ProjectList?status=3"
        logger.set_project(key=key, reg_no=reg_no, url=detail_url, page=i)

        if mode == "daily_light" and get_project_by_key(key):
            logger.info("Skipping — already in DB (daily_light)", step="skip")
            counts["projects_skipped"] += 1
            logger.clear_project()
            continue

        try:
            data: dict = {
                "key": key,
                "state": config["state"], "project_state": config["state"],
                "domain": DOMAIN, "config_id": config["config_id"],
                "url": detail_url,
                "is_live": True, "machine_name": machine_name,
                "crawl_machine_ip": machine_ip,
            }
            for api_f, schema_f in _LIST_API_TO_FIELD.items():
                val = proj.get(api_f)
                if val and str(val).strip():
                    data[schema_f] = str(val).strip()

            cert_url = _build_cert_url(proj.get("UploadedCertificatePath", ""))
            doc_links = []
            if cert_url:
                doc_links.append({"label": "registration_certificate", "url": cert_url})

            if enc_id:
                logger.info("Fetching project detail API", step="detail_fetch")
                detail = _fetch_project_detail(enc_id, logger)
                data.update({k: v for k, v in detail.items() if k != "data" and v is not None and not k.startswith("_")})
                logger.info("Fetching project website detail", step="detail_fetch")
                website_detail = _fetch_project_website_detail(enc_id, logger)
                if website_detail:
                    doc_links.extend(_extract_project_website_documents(website_detail))
                    data["data"] = merge_data_sections(
                        data.get("data"), detail.get("data"),
                        {"source_api": "ProjectDtlsWebsite", "raw_website": website_detail},
                    )
                else:
                    data["data"] = merge_data_sections(data.get("data"), detail.get("data"))

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
                    {**data, "data": {"validation_fallback": True, "raw": data.get("data")}},
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
            uploaded_documents = []
            doc_name_counts: dict[str, int] = {}
            for doc in doc_links:
                selected_doc = select_document_for_download(config["state"], doc, doc_name_counts, domain=DOMAIN)
                if selected_doc:
                    uploaded_doc = _handle_document(key, selected_doc, run_id, site_id, logger)
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

            if i % 100 == 0:
                save_checkpoint(site_id, mode, i, key, run_id)
            random_delay(*config.get("rate_limit_delay", (1, 3)))

        except Exception as exc:
            logger.exception("Project processing failed", exc, step="project_loop",
                             pid=pid, enc_id=enc_id)
            insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                               project_key=key, url=detail_url)
            counts["error_count"] += 1
        finally:
            logger.clear_project()
    reset_checkpoint(site_id, mode)
    logger.info(f"Rajasthan RERA complete: {counts}")
    return counts
