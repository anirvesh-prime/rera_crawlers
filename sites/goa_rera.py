"""
Goa RERA Crawler — rera.goa.gov.in
Type: playwright (captcha on listing) + static httpx for detail pages

Strategy:
- POST /reraApp/search with solved captcha to get registered project listing
- Each result card: project name (h1), address (p), reg no (span.reg), detail link (a)
- Fetch each /reraApp/viewProjectDetailPage?projectID=N via httpx (no captcha required)
- Bootstrap grid rows (p.text-right labels) supply key-value metadata
- Tables: applicant contact, inventory, architects, engineers, documents
- Construction progress: collapsible building-details panel (col-lg-6/col-lg-3 rows)
- Fallback: if captcha solving fails and sentinel_project_url is set,
  process that URL directly (enables dry-run testing without a working solver)
"""
from __future__ import annotations

import base64
import re
import datetime

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

BASE_URL    = "https://rera.goa.gov.in"
APP_BASE    = "https://rera.goa.gov.in/reraApp"
HOME_URL    = "https://rera.goa.gov.in/reraApp/home"
SEARCH_URL  = "https://rera.goa.gov.in/reraApp/search"
DOMAIN      = "rera.goa.gov.in"
PAGE_SIZE          = 10   # rows per search-result page (approximate)
_CAPTCHA_MAX_TRIES = 5    # maximum attempts to solve the captcha before giving up


def _get(url: str, logger: CrawlerLogger, **kw):
    return safe_get(url, verify=False, logger=logger, timeout=60.0, **kw)


# ── Date parsing ───────────────────────────────────────────────────────────────

def _parse_goa_date(raw: str) -> str | None:
    """Convert 'Wed Feb 22 13:47:27 IST 2023' or 'DD-MM-YYYY' to ISO date."""
    if not raw:
        return None
    raw = raw.strip()
    # Short date form: DD-MM-YYYY
    m = re.match(r"^(\d{2})-(\d{2})-(\d{4})$", raw)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    # Long form: "Wed Feb 22 13:47:27 IST 2023"
    cleaned = re.sub(r"\b[A-Z]{2,5}\b", "", raw).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    for fmt in ("%a %b %d %H:%M:%S %Y", "%a %b  %d %H:%M:%S %Y"):
        try:
            dt = datetime.datetime.strptime(cleaned, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None


# ── Detail page parsing ────────────────────────────────────────────────────────

_LABEL_MAP: dict[str, str] = {
    "date of registration":        "submitted_date",
    "project type":                "project_type",
    "project status":              "status_of_the_project",
    "project start date":          "estimated_commencement_date",
    "project end date":            "estimated_finish_date",
}

_LOCATION_MAP: dict[str, str] = {
    "state":    "state",
    "district": "district",
    "taluk":    "taluk",
    "village":  "village",
}


def _parse_detail_page(url: str, logger: CrawlerLogger) -> dict:
    resp = _get(url, logger)
    if not resp:
        return {}
    soup = BeautifulSoup(resp.text, "lxml")
    out: dict = {}
    raw: dict = {"source_url": url}
    location_raw: dict = {}

    # ── Registration number and type ──────────────────────────────────────────
    for span in soup.find_all("span", class_="reg"):
        txt = span.get_text(separator=" ", strip=True)
        m = re.search(r"RERA Registration No\.\s*:?\s*(\S+)", txt, re.I)
        if m:
            out["project_registration_no"] = m.group(1).strip()
        m2 = re.search(r"Registration Type\s*:?\s*(.+)", txt, re.I)
        if m2:
            reg_type = m2.group(1).strip()
            out.setdefault("promoters_details", {})["type_of_firm"] = reg_type
            raw["promoter_type"] = reg_type

    # ── Project name and promoter from header ─────────────────────────────────
    detail_header = soup.find("div", class_="search_result_list_detail")
    if detail_header:
        name_div = detail_header.find("div", class_=lambda c: c and "col-md-9" in c)
        if name_div:
            h1 = name_div.find("h1")
            if h1:
                out["project_name"] = h1.get_text(strip=True)
        profile_box = detail_header.find("div", class_="profile_box")
        if profile_box:
            h1 = profile_box.find("h1")
            if h1:
                promoter = h1.get_text(separator=" ", strip=True).split("Applicant")[0].strip()
                out["promoter_name"] = promoter
                out.setdefault("promoters_details", {})["name"] = promoter

    # ── Project image from profile box ───────────────────────────────────────
    profile_box = soup.find("div", class_="profile_box")
    if profile_box:
        img = profile_box.find("img")
        if img and img.get("src"):
            src = img["src"]
            img_url = src if src.startswith("http") else (BASE_URL + "/" + src.lstrip("/"))
            out["project_images"] = [img_url]

    # ── Bootstrap grid key-value pairs ───────────────────────────────────────
    # Only process rows whose direct children have col-* classes (not wrapper rows
    # whose children are other div.row elements — those cause incorrect pairing).
    for row_div in soup.find_all("div", class_="row"):
        cols = [c for c in row_div.find_all("div", recursive=False)
                if any(cls.startswith("col-") for cls in (c.get("class") or []))]
        if not cols:
            continue
        i = 0
        while i < len(cols):
            p_label = cols[i].find("p", class_="text-right")
            if p_label and i + 1 < len(cols):
                label = p_label.get_text(strip=True).replace(":", "").strip()
                value = cols[i + 1].get_text(separator=" ", strip=True)
                if label and value:
                    raw[label] = value
                    ll = label.lower().strip()
                    # Schema fields
                    schema_f = _LABEL_MAP.get(ll)
                    if schema_f and schema_f not in out:
                        parsed = _parse_goa_date(value) if schema_f in (
                            "submitted_date", "estimated_commencement_date", "estimated_finish_date"
                        ) else value
                        out[schema_f] = parsed or value
                    # Location fields
                    loc_k = _LOCATION_MAP.get(ll)
                    if loc_k and value:
                        location_raw[loc_k] = value
                    # Land area
                    if ll == "total area of project land":
                        try:
                            out["land_area"] = float(value)
                        except ValueError:
                            pass
                    # Construction area
                    if "total covered area" in ll:
                        try:
                            out["construction_area"] = float(value)
                        except ValueError:
                            pass
                    # Project cost
                    if ll == "estimated cost of project":
                        out["project_cost_detail"] = {
                            "total_project_cost": value,
                            "estimated_project_cost": value,
                        }
                i += 2
            else:
                i += 1

    # ── Project description ───────────────────────────────────────────────────
    for h1 in soup.find_all("h1"):
        if "Project Description" in h1.get_text():
            nxt = h1.find_next("p")
            if nxt:
                out["project_description"] = nxt.get_text(strip=True)
            break

    # ── Location raw: build raw_address ──────────────────────────────────────
    if location_raw:
        project_name = out.get("project_name", "")
        village = location_raw.get("village", "")
        # Strip trailing "(ct)" census-town suffix from village for the address string
        village_clean = re.sub(r"\s*\(ct\)\s*$", "", village, flags=re.I).strip() if village else ""
        district = location_raw.get("district", "")
        taluk = location_raw.get("taluk", "")
        state = location_raw.get("state", "")
        # Build address with taluk if available; avoid duplicating district
        if village_clean:
            mid = f"{taluk} , {district}" if taluk else district
            raw_address = f"{project_name} , Village-{village_clean}, {mid} , {state}"
        else:
            raw_address = ""
        if raw_address:
            location_raw["raw_address"] = raw_address
            raw["raw_address"] = raw_address
        out["project_location_raw"] = location_raw

    # ── Tables ────────────────────────────────────────────────────────────────
    tables = soup.find_all("table")

    def _table_rows(t) -> list[dict]:
        hdrs = [th.get_text(strip=True) for th in t.find_all("th")]
        result = []
        for tr in t.find_all("tr")[1:]:
            cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all("td")]
            if cells:
                result.append(dict(zip(hdrs, cells)))
        return result

    # Table 0: applicant contact [Name, E-mail, Mobile]
    if tables:
        rows0 = _table_rows(tables[0])
        if rows0:
            contact = rows0[0]
            out["promoter_contact_details"] = {
                "email": contact.get("E-mail", ""),
                "phone": contact.get("Mobile", ""),
            }
            raw["applicant_table"] = rows0

    # Table 1: inventory [Type of Inventory, No of Inventory, ...]
    if len(tables) > 1:
        rows1 = _table_rows(tables[1])
        residential_units = 0
        for row in rows1:
            inv_type = row.get("Type of Inventory", "").lower()
            count = row.get("No of Inventory", "0")
            if inv_type in ("flats", "villas", "plots", "residential"):
                try:
                    residential_units += int(count)
                except ValueError:
                    pass
        if residential_units:
            out["number_of_residential_units"] = residential_units

    # Tables 4 + 5: architects and structural engineers
    professional_info = []
    architect_table = None
    engineer_table = None
    for t in tables:
        hdrs_raw = [th.get_text(strip=True) for th in t.find_all("th")]
        if "Architect Name" in hdrs_raw:
            architect_table = t
        elif "Engineer Name" in hdrs_raw:
            engineer_table = t
    if architect_table:
        for row in _table_rows(architect_table):
            professional_info.append({
                "name":    row.get("Architect Name", ""),
                "role":    "Architects",
                "email":   row.get("Email", ""),
                "address": row.get("Address", ""),
            })
    if engineer_table:
        for row in _table_rows(engineer_table):
            professional_info.append({
                "name":    row.get("Engineer Name", ""),
                "role":    "Structural Engineers",
                "email":   row.get("Email", ""),
                "address": row.get("Address", ""),
            })
    if professional_info:
        out["professional_information"] = professional_info

    # ── Document links ────────────────────────────────────────────────────────
    doc_links = []
    seen_hrefs: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "download" not in href.lower() and "DOC_ID" not in href:
            continue
        if href in seen_hrefs:
            continue
        seen_hrefs.add(href)
        label = re.sub(r"\s+", " ", a.get_text(separator=" ", strip=True))
        full_url = href if href.startswith("http") else (BASE_URL + href if href.startswith("/") else f"{BASE_URL}/{href}")
        doc_links.append({"label": label, "url": full_url, "type": label})

    # ── Construction progress (quarterly building details panel) ──────────────
    progress: list[dict] = []
    building_panel = None
    for panel in soup.find_all("div", class_="panel-collapse"):
        if "Building Details" in panel.get_text():
            building_panel = panel
            break
    if building_panel:
        for row_div in building_panel.find_all("div", class_="row"):
            label_div = row_div.find("div", class_=lambda c: c and "col-lg-6" in c)
            value_div = row_div.find("div", class_=lambda c: c and "col-lg-3" in c)
            if label_div and value_div:
                title = label_div.get_text(strip=True)
                pct   = value_div.get_text(strip=True)
                if title and pct:
                    progress.append({"title": title, "progress_percentage": pct})
    if progress:
        out["status_update"] = {"construction_progress": progress}

    # ── land_area_details ─────────────────────────────────────────────────────
    out["land_area_details"] = {k: v for k, v in {
        "land_area":              str(out["land_area"]) if out.get("land_area") is not None else None,
        "land_area_unit":         "sq Mtr" if out.get("land_area") is not None else None,
        "construction_area":      str(out["construction_area"]) if out.get("construction_area") is not None else None,
        "construction_area_unit": "Sq Mtr" if out.get("construction_area") is not None else None,
    }.items() if v is not None}

    raw["land_area_unit"] = "sq Mtr"
    out["data"] = raw
    out["_doc_links"] = doc_links
    return out


# ── Listing page parsing ───────────────────────────────────────────────────────

def _parse_listing_cards(soup: BeautifulSoup) -> list[dict]:
    """Parse project cards from the Goa RERA search result page."""
    cards = []
    # Goa uses same Java MVC — cards are in div.no_pad_lft (same as Pondicherry)
    for card in soup.find_all("div", class_=lambda c: c and "no_pad_lft" in c):
        h1 = card.find("h1")
        if not h1:
            continue
        project_name = h1.get_text(strip=True)
        # Remove "Project:" prefix if present
        project_name = re.sub(r"^Project\s*:\s*", "", project_name, flags=re.I).strip()

        reg_no = None
        for span in card.find_all("span", class_="reg"):
            m = re.search(r"RERA Registration No\.\s*:?\s*(\S+)", span.get_text(), re.I)
            if m:
                reg_no = m.group(1).strip()
                break
        if not reg_no:
            # Fallback: look in any text
            txt = card.get_text()
            m = re.search(r"PRGO\w+", txt, re.I)
            if m:
                reg_no = m.group(0)
        if not reg_no:
            continue

        promoter_name = None
        promoter_type = None
        tds = [td.get_text(strip=True) for td in card.find_all("td")]
        if tds:
            promoter_name = tds[0] if len(tds) > 0 else None
            promoter_type = tds[1] if len(tds) > 1 else None

        detail_a = card.find("a", href=re.compile(r"viewProjectDetailPage"))
        detail_url = None
        if detail_a:
            href = detail_a["href"]
            detail_url = href if href.startswith("http") else f"{APP_BASE}/{href}"

        cards.append({
            "project_name":            project_name,
            "project_registration_no": reg_no,
            "promoter_name":           promoter_name,
            "promoter_type":           promoter_type,
            "detail_url":              detail_url,
        })
    return cards


# ── Captcha + listing via Playwright ──────────────────────────────────────────

def _fetch_project_listing(config: dict, run_id: int, logger: CrawlerLogger) -> list[dict]:
    """
    Use Playwright to solve the captcha and submit the Goa RERA search form.
    Returns a list of project card dicts.
    On captcha failure returns an empty list (caller handles fallback).
    """
    from core.captcha_solver import captcha_to_text
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("Playwright not installed")
        return []

    all_cards: list[dict] = []
    start_from = 0
    _server_rejections = 0  # count server-side captcha rejections to avoid infinite loop

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        while True:
            # ── Captcha retry loop ────────────────────────────────────────────
            solved = None
            for captcha_attempt in range(1, _CAPTCHA_MAX_TRIES + 1):
                try:
                    page.goto(HOME_URL, timeout=45000)
                    page.wait_for_load_state("networkidle", timeout=30000)
                except Exception as e:
                    logger.error(f"Failed to load home page: {e}")
                    break

                # Capture captcha image and resize to 90×28 for the solver.
                # Strategy:
                #   1. element.screenshot() — captures the rendered element pixels
                #      directly via Playwright; completely bypasses CORS/canvas-taint
                #      issues that made the old JS canvas approach return blank PNGs.
                #   2. Feed the screenshot bytes back as a data: URL and draw it on a
                #      90×28 canvas (data: URLs are never cross-origin, so no taint).
                #   3. The resulting PNG stays within the solver's ~1750-byte limit.
                _CAPTCHA_SELECTORS = [
                    'img[src*="captcha"]',
                    'img[src*="Captcha"]',
                    'img[src*="CAPTCHA"]',
                    'img[id*="captcha" i]',
                    'img[name*="captcha" i]',
                    'img[alt*="captcha" i]',
                ]
                captcha_data = None
                for _sel in _CAPTCHA_SELECTORS:
                    try:
                        captcha_el = page.wait_for_selector(_sel, state="visible", timeout=8000)
                    except Exception:
                        captcha_el = page.query_selector(_sel)
                    if not captcha_el:
                        continue
                    try:
                        png_bytes = captcha_el.screenshot()
                    except Exception as _e:
                        logger.warning(f"element.screenshot() failed for {_sel!r}: {_e}")
                        continue
                    if not png_bytes:
                        continue
                    # DEBUG: save raw screenshot to disk for visual inspection
                    _dbg_path = f"/tmp/goa_captcha_raw_{captcha_attempt}.png"
                    try:
                        with open(_dbg_path, "wb") as _f:
                            _f.write(png_bytes)
                        logger.info(f"DEBUG: raw captcha screenshot saved to {_dbg_path} ({len(png_bytes)} bytes)")
                    except Exception:
                        pass
                    full_data_url = "data:image/png;base64," + base64.b64encode(png_bytes).decode()
                    # Resize to 90×28 inside the browser (data: URLs have no CORS restrictions)
                    captcha_data = page.evaluate("""(dataUrl) => {
                        return new Promise((resolve) => {
                            const img = new Image();
                            img.onload = () => {
                                const c = document.createElement('canvas');
                                c.width = 90; c.height = 28;
                                const cx = c.getContext('2d');
                                cx.filter = 'grayscale(1) contrast(2)';
                                cx.drawImage(img, 0, 0, c.width, c.height);
                                const url = c.toDataURL('image/png');
                                resolve((url && url !== 'data:,') ? url : null);
                            };
                            img.onerror = () => resolve(null);
                            img.src = dataUrl;
                        });
                    }""", full_data_url)
                    if captcha_data:
                        logger.info(f"DEBUG: resized data URL length={len(captcha_data)}")
                        break
                if not captcha_data or len(captcha_data) < 100:
                    logger.warning(f"Captcha element screenshot failed (attempt {captcha_attempt}/{_CAPTCHA_MAX_TRIES})")
                    continue

                try:
                    candidate = captcha_to_text(captcha_data, default_captcha_source="eprocure")
                except Exception as e:
                    logger.warning(f"Captcha solver exception (attempt {captcha_attempt}/{_CAPTCHA_MAX_TRIES}): {e}")
                    continue

                if candidate and len(candidate) >= 4:
                    solved = candidate
                    logger.info(f"Captcha solved on attempt {captcha_attempt}: {solved!r}")
                    break
                logger.warning(f"Captcha bad result {candidate!r} (attempt {captcha_attempt}/{_CAPTCHA_MAX_TRIES})")

            if not solved:
                logger.error(f"Captcha solve failed after {_CAPTCHA_MAX_TRIES} attempts — stopping")
                break

            # Set startFrom for pagination and submit form
            try:
                page.evaluate(f"document.querySelector('[name=startFrom]').value = '{start_from}';")
                page.fill('[name=captcha]', solved)
                page.evaluate("document.querySelector('[name=btn1]') && document.getElementById('searchForm') ? document.getElementById('searchForm').submit() : document.searchForm.submit()")
                page.wait_for_load_state("networkidle", timeout=30000)
                page.wait_for_timeout(1500)  # allow any secondary navigation to settle
            except Exception as e:
                logger.error(f"Form submission failed: {e}")
                break

            # page.content() can fail if the page is mid-navigation; retry briefly
            html_content = None
            for _retry in range(3):
                try:
                    html_content = page.content()
                    break
                except Exception:
                    page.wait_for_timeout(1000)
            if not html_content:
                logger.warning(f"Could not get page content at startFrom={start_from}; skipping page")
                break
            soup = BeautifulSoup(html_content, "lxml")
            # Detect captcha rejection: server redirects back to home page (has captcha form again)
            if soup.find("input", {"name": "captcha"}) and not soup.find("div", class_=lambda c: c and "no_pad_lft" in c):
                _server_rejections += 1
                logger.warning(f"Captcha rejected by server (rejection #{_server_rejections}) — retrying")
                if _server_rejections >= _CAPTCHA_MAX_TRIES:
                    logger.error("Too many captcha rejections — stopping")
                    break
                continue  # restart outer while True loop to re-attempt captcha
            cards = _parse_listing_cards(soup)
            if not cards:
                logger.info(f"No cards at startFrom={start_from} — listing complete")
                break
            all_cards.extend(cards)
            logger.info(f"Fetched {len(cards)} cards at startFrom={start_from}")

            # Check for more pages
            next_links = soup.find_all("a", string=re.compile(r"Next|>>", re.I))
            if not next_links:
                break
            start_from += PAGE_SIZE

            max_pages = settings.MAX_PAGES
            if max_pages and (start_from // PAGE_SIZE) >= max_pages:
                logger.info(f"Reached MAX_PAGES={max_pages}")
                break

        browser.close()
    return all_cards


# ── Document handling ──────────────────────────────────────────────────────────

def _handle_document(project_key: str, doc: dict, run_id: int,
                     site_id: str, logger: CrawlerLogger) -> dict | None:
    url   = doc["url"]
    label = doc.get("label") or doc.get("type", "document")
    fname = build_document_filename(doc)
    try:
        resp = _get(url, logger)
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
        logger.info("Document uploaded", label=label, s3_key=s3_key, step="documents")
        logger.log_document(label, url, "uploaded", s3_key=s3_key, file_size_bytes=len(resp.content))
        return document_result_entry(doc, s3_url, fname)
    except Exception as e:
        logger.error(f"Doc failed for {project_key}: {e}")
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(e),
                           project_key=project_key, url=url)
        return None


# ── Main run() ─────────────────────────────────────────────────────────────────

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Goa RERA.
    Loads state_projects_sample/goa.json as the baseline, re-scrapes the
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
        "state_projects_sample", "goa.json",
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
            run_id, config.get("id", "goa_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


def run(config: dict, run_id: int, mode: str) -> dict:
    logger  = CrawlerLogger(config["id"], run_id)
    site_id = config["id"]
    counts  = dict(projects_found=0, projects_new=0, projects_updated=0,
                   projects_skipped=0, documents_uploaded=0, error_count=0)
    checkpoint  = load_checkpoint(site_id, mode) or {}
    done_regs: set[str] = set(checkpoint.get("done_regs", []))
    item_limit  = settings.CRAWL_ITEM_LIMIT or 0

    # ── Sentinel health check ────────────────────────────────────────────────
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counts["error_count"] += 1
        return counts

    # ── Get project listing ───────────────────────────────────────────────────
    cards = _fetch_project_listing(config, run_id, logger)

    if not cards:
        logger.error("No project listing obtained")
        insert_crawl_error(run_id, site_id, "LISTING_FAILED",
                           "Captcha solve failed and no project listing was obtained")
        counts["error_count"] += 1
        return counts

    if item_limit:
        cards = cards[:item_limit]
    counts["projects_found"] = len(cards)
    logger.info(f"Goa RERA: {len(cards)} projects to process")

    machine_name, machine_ip = get_machine_context()

    for i, card in enumerate(cards):
        reg_no = card["project_registration_no"]
        if reg_no in done_regs:
            counts["projects_skipped"] += 1
            continue

        key = generate_project_key(reg_no)
        try:
            logger.set_project(key=key, reg_no=reg_no, url=card.get("detail_url", HOME_URL))

            if mode == "daily_light" and get_project_by_key(key):
                counts["projects_skipped"] += 1
                logger.clear_project()
                continue

            data: dict = {
                "key":                     key,
                "state":                   config["state"],
                "project_state":           config["state"],
                "project_registration_no": reg_no,
                "project_name":            card.get("project_name") or None,
                "promoter_name":           card.get("promoter_name") or None,
                "domain":                  DOMAIN,
                "config_id":               config["config_id"],
                "url":                     card.get("detail_url") or HOME_URL,
                "is_live":                 True,
                "machine_name":            machine_name,
                "crawl_machine_ip":        machine_ip,
            }

            if card.get("promoter_type") or card.get("promoter_name"):
                data["promoters_details"] = {k: v for k, v in {
                    "type_of_firm": card.get("promoter_type"),
                    "name":         card.get("promoter_name"),
                }.items() if v}

            doc_links: list[dict] = []
            if card.get("detail_url"):
                random_delay(*config.get("rate_limit_delay", (1, 3)))
                detail = _parse_detail_page(card["detail_url"], logger)
                doc_links = detail.pop("_doc_links", [])
                for k, v in detail.items():
                    if v is not None and not k.startswith("_"):
                        data[k] = v

                # Use properly-cased state name from location_raw (e.g. "Goa" not "goa")
                loc = data.get("project_location_raw") or {}
                if isinstance(loc, dict) and loc.get("state"):
                    data["project_state"] = loc["state"]

                # Build raw_address for data JSONB
                loc = data.get("project_location_raw") or {}
                _raw_addr = loc.get("raw_address") if isinstance(loc, dict) else None
                promoter_type = (data.get("promoters_details") or {}).get("type_of_firm")
                data["data"] = merge_data_sections(
                    data.get("data"),
                    {
                        "govt_type":      "state",
                        "raw_address":    _raw_addr,
                        "promoter_type":  promoter_type,
                        "land_area_unit": "sq Mtr",
                    },
                )
                # Use submitted_date as approved_on_date if missing
                if not data.get("approved_on_date") and data.get("submitted_date"):
                    pass  # submitted_date is the registration date for Goa
            else:
                data["data"] = {"govt_type": "state"}

            logger.info("Normalizing", step="normalize")
            try:
                normalized = normalize_project_payload(data, config,
                                                       machine_name=machine_name,
                                                       machine_ip=machine_ip)
                record  = ProjectRecord(**normalized)
                db_dict = record.to_db_dict()
            except (ValidationError, ValueError) as e:
                logger.warning("Validation failed — raw fallback", error=str(e))
                insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(e),
                                   project_key=key, url=data.get("url"), raw_data=data)
                counts["error_count"] += 1
                db_dict = normalize_project_payload(
                    {**data, "data": merge_data_sections(data.get("data"),
                                                         {"validation_fallback": True})},
                    config, machine_name=machine_name, machine_ip=machine_ip,
                )

            action = upsert_project(db_dict)
            if action == "new":       counts["projects_new"] += 1
            elif action == "updated": counts["projects_updated"] += 1
            else:                     counts["projects_skipped"] += 1
            logger.info(f"DB result: {action}", step="db_upsert")

            # Documents
            uploaded_documents = []
            doc_name_counts: dict[str, int] = {}
            for doc in doc_links:
                selected = select_document_for_download(config["state"], doc, doc_name_counts, domain=DOMAIN)
                if selected:
                    uploaded = _handle_document(db_dict["key"], selected, run_id, site_id, logger)
                    if uploaded:
                        uploaded_documents.append(uploaded)
                        counts["documents_uploaded"] += 1
                    else:
                        uploaded_documents.append({"link": doc.get("url"), "type": doc.get("label") or doc.get("type", "document")})
                else:
                    uploaded_documents.append({"link": doc.get("url"), "type": doc.get("label") or doc.get("type", "document")})

            if uploaded_documents:
                upsert_project({
                    "key":                     db_dict["key"],
                    "url":                     db_dict["url"],
                    "state":                   db_dict["state"],
                    "domain":                  db_dict["domain"],
                    "project_registration_no": db_dict["project_registration_no"],
                    "uploaded_documents":      uploaded_documents,
                    "document_urls":           build_document_urls(uploaded_documents),
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
    logger.info(f"Goa RERA complete: {counts}")
    return counts
