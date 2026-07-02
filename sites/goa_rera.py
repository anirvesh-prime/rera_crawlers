"""
Goa RERA Crawler — rera.goa.gov.in
Type: selenium (captcha on listing) + selenium-backed fetch for detail pages

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

import re
import time
import datetime

from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import (
    SeleniumSession,
    generate_project_key,
    get_target_reg_nos,
    log_daily_light_listing_progress,
    page_adapter,
    random_delay,
)
from core.db import get_project_by_key, upsert_project, insert_crawl_error, upsert_document, update_crawl_run_progress
from core.document_policy import select_document_for_download
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import (
    build_document_filename,
    build_document_urls,
    document_identity_url,
    document_result_entry,
    existing_uploaded_document_entry,
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
_CAPTCHA_MAX_TRIES    = 3   # solver attempts per captcha round (inner loop)
_MAX_SERVER_REJECTS   = 5   # max server-side rejections before giving up entirely
_CAPTCHA_READY_TIMEOUT_MS = 8_000
_CAPTCHA_FETCH_TIMEOUT_MS = 5_000
_CAPTCHA_SOLVER_TIMEOUT_S = 30
_CAPTCHA_SELECTORS = [
    '#captcha_id',
    'img[name="imgCaptcha"]',
    'img[src*="captcha"]',
    'img[src*="Captcha"]',
    'img[src*="CAPTCHA"]',
    'img[id*="captcha" i]',
    'img[name*="captcha" i]',
    'img[alt*="captcha" i]',
]


# ── Selenium session (shared driver via core.crawler_base.SeleniumSession) ────

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


def _get(url: str, logger: CrawlerLogger, **kw):
    # Drop httpx-only kwargs that SeleniumSession.get accepts-and-ignores anyway,
    # but pass through retries / delay / page_load_timeout if a caller sets them.
    kw.pop("verify", None)
    kw.pop("timeout", None)
    kw.pop("client", None)
    return _session().get(url, logger=logger, **kw)


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
            out["project_registration_no"] = m.group(1).strip()  # FIELD: project_registration_no <- span.reg "RERA Registration No"
        m2 = re.search(r"Registration Type\s*:?\s*(.+)", txt, re.I)
        if m2:
            reg_type = m2.group(1).strip()
            out.setdefault("promoters_details", {})["type_of_firm"] = reg_type  # FIELD: promoters_details.type_of_firm <- span.reg "Registration Type"
            raw["promoter_type"] = reg_type

    # ── Project name and promoter from header ─────────────────────────────────
    detail_header = soup.find("div", class_="search_result_list_detail")
    if detail_header:
        name_div = detail_header.find("div", class_=lambda c: c and "col-md-9" in c)
        if name_div:
            h1 = name_div.find("h1")
            if h1:
                out["project_name"] = h1.get_text(strip=True)  # FIELD: project_name <- detail header col-md-9 <h1>
        profile_box = detail_header.find("div", class_="profile_box")
        if profile_box:
            h1 = profile_box.find("h1")
            if h1:
                promoter = h1.get_text(separator=" ", strip=True).split("Applicant")[0].strip()
                out["promoter_name"] = promoter  # FIELD: promoter_name <- profile_box <h1> split on "Applicant"
                out.setdefault("promoters_details", {})["name"] = promoter  # FIELD: promoters_details.name <- profile_box <h1> split on "Applicant"

    # ── Project image from profile box ───────────────────────────────────────
    profile_box = soup.find("div", class_="profile_box")
    if profile_box:
        img = profile_box.find("img")
        if img and img.get("src"):
            src = img["src"]
            img_url = src if src.startswith("http") else (BASE_URL + "/" + src.lstrip("/"))
            out["project_images"] = [img_url]  # FIELD: project_images <- profile_box <img> src

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
                            out["land_area"] = float(value)  # FIELD: land_area <- raw label "total area of project land"
                        except ValueError:
                            pass
                    # Construction area
                    if "total covered area" in ll:
                        try:
                            out["construction_area"] = float(value)  # FIELD: construction_area <- raw label "total covered area"
                        except ValueError:
                            pass
                    # Project cost
                    if ll == "estimated cost of project":
                        out["project_cost_detail"] = {  # FIELD: project_cost_detail <- raw label "estimated cost of project"
                            "total_project_cost": value,  # FIELD: project_cost_detail.total_project_cost <- "estimated cost of project" value
                            "estimated_project_cost": value,  # FIELD: project_cost_detail.estimated_project_cost <- "estimated cost of project" value
                        }
                i += 2
            else:
                i += 1

    # ── Project description ───────────────────────────────────────────────────
    for h1 in soup.find_all("h1"):
        if "Project Description" in h1.get_text():
            nxt = h1.find_next("p")
            if nxt:
                out["project_description"] = nxt.get_text(strip=True)  # FIELD: project_description <- <p> after <h1> "Project Description"
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
        out["project_location_raw"] = location_raw  # FIELD: project_location_raw <- location_raw dict (state/district/taluk/village)

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
            out["promoter_contact_details"] = {  # FIELD: promoter_contact_details <- tables[0] first row (applicant contact)
                "email": contact.get("E-mail", ""),  # FIELD: promoter_contact_details.email <- tables[0] "E-mail" column
                "phone": contact.get("Mobile", ""),  # FIELD: promoter_contact_details.phone <- tables[0] "Mobile" column
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
            out["number_of_residential_units"] = residential_units  # FIELD: number_of_residential_units <- sum of tables[1] "No of Inventory"

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
        out["professional_information"] = professional_info  # FIELD: professional_information <- architect_table + engineer_table rows

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
        out["status_update"] = {"construction_progress": progress}  # FIELD: status_update <- progress list from Building Details panel

    # ── land_area_details ─────────────────────────────────────────────────────
    out["land_area_details"] = {k: v for k, v in {  # FIELD: land_area_details <- composed from out.land_area / construction_area
        "land_area":              str(out["land_area"]) if out.get("land_area") is not None else None,  # FIELD: land_area_details.land_area <- str(out["land_area"])
        "land_area_unit":         "sq Mtr" if out.get("land_area") is not None else None,  # FIELD: land_area_details.land_area_unit <- literal "sq Mtr"
        "construction_area":      str(out["construction_area"]) if out.get("construction_area") is not None else None,  # FIELD: land_area_details.construction_area <- str(out["construction_area"])
        "construction_area_unit": "Sq Mtr" if out.get("construction_area") is not None else None,  # FIELD: land_area_details.construction_area_unit <- literal "Sq Mtr"
    }.items() if v is not None}

    raw["land_area_unit"] = "sq Mtr"
    out["data"] = raw  # FIELD: data <- raw dict (source_url + label/value pairs)
    out["_doc_links"] = doc_links  # FIELD: _doc_links <- doc_links list (download/DOC_ID anchors)
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


def _parse_pagination_offsets(soup: BeautifulSoup) -> list[int]:
    """Return startFrom offsets exposed by Goa's javascript:pagging(N) links."""
    offsets: set[int] = set()
    for anchor in soup.find_all("a", href=True):
        match = re.search(r"pagging\((\d+)\)", anchor.get("href") or "", re.I)
        if match:
            offsets.add(int(match.group(1)))
    return sorted(offsets)


# ── Captcha + listing via Selenium ────────────────────────────────────────────

def _wait_for_captcha_selector(page, logger: CrawlerLogger) -> str | None:
    """Return the first visible captcha image selector, or None."""
    ready_script = """
    (selectors) => {
        const visible = (el) => {
            if (!el) return false;
            const rect = el.getBoundingClientRect();
            const style = window.getComputedStyle(el);
            return rect.width > 0 && rect.height > 0 &&
                style.visibility !== 'hidden' &&
                style.display !== 'none';
        };
        return (selectors || []).some((selector) => {
            try {
                return visible(document.querySelector(selector));
            } catch (_) {
                return false;
            }
        });
    }
    """
    try:
        page.wait_for_function(
            ready_script,
            arg=_CAPTCHA_SELECTORS,
            timeout=_CAPTCHA_READY_TIMEOUT_MS,
        )
    except Exception as exc:
        logger.warning(f"Captcha image not ready within {_CAPTCHA_READY_TIMEOUT_MS}ms: {exc}")
        return None

    try:
        return page.evaluate(
            """
            (selectors) => {
                const visible = (el) => {
                    if (!el) return false;
                    const rect = el.getBoundingClientRect();
                    const style = window.getComputedStyle(el);
                    return rect.width > 0 && rect.height > 0 &&
                        style.visibility !== 'hidden' &&
                        style.display !== 'none';
                };
                for (const selector of selectors || []) {
                    try {
                        if (visible(document.querySelector(selector))) return selector;
                    } catch (_) {}
                }
                return null;
            }
            """,
            _CAPTCHA_SELECTORS,
        )
    except Exception as exc:
        logger.warning(f"Captcha selector detection failed: {exc}")
        return None


def _captcha_data_url_from_page(page, selector: str, logger: CrawlerLogger) -> str | None:
    """Fetch the captcha image through the browser session and return a data URL."""
    try:
        data_url = page.evaluate(
            """async (selector, timeoutMs) => {
                const img = document.querySelector(selector);
                if (!img) return null;
                const src = img.currentSrc || img.getAttribute('src');
                if (!src) return null;
                const url = new URL(src, document.baseURI).href;
                const ctrl = new AbortController();
                const timer = setTimeout(() => ctrl.abort(), timeoutMs);
                try {
                    const resp = await fetch(url, {
                        credentials: 'include',
                        cache: 'no-store',
                        signal: ctrl.signal
                    });
                    if (!resp.ok) return null;
                    const blob = await resp.blob();
                    return await new Promise((resolve) => {
                        const reader = new FileReader();
                        reader.onload = () => resolve(reader.result || null);
                        reader.onerror = () => resolve(null);
                        reader.readAsDataURL(blob);
                    });
                } catch (_) {
                    return null;
                } finally {
                    clearTimeout(timer);
                }
            }""",
            selector,
            _CAPTCHA_FETCH_TIMEOUT_MS,
        )
    except Exception as exc:
        logger.warning(f"Captcha fetch failed for {selector!r}: {exc}")
        return None
    if isinstance(data_url, str) and data_url.startswith("data:image/"):
        return data_url
    return None


def _fetch_project_listing(
    config: dict,
    run_id: int,
    logger: CrawlerLogger,
    *,
    item_limit: int = 0,
) -> list[dict]:
    """
    Use Selenium to solve the captcha and submit the Goa RERA search form.
    Returns a list of project card dicts.
    On captcha failure returns an empty list (caller handles fallback).
    """
    from core.captcha_solver import captcha_to_text

    all_cards: list[dict] = []
    seen_reg_nos: set[str] = set()
    start_from = 0
    pages_fetched = 0
    _server_rejections = 0  # count server-side captcha rejections to avoid infinite loop

    page = page_adapter(_session())
    logger.info("Starting Goa listing captcha search", step="timing")

    while True:
        max_pages = settings.MAX_PAGES
        if max_pages and pages_fetched >= max_pages:
            logger.info(f"Reached MAX_PAGES={max_pages}")
            break

        logger.info(
            f"Goa listing page startFrom={start_from}: solving captcha",
            step="timing",
            start_from=start_from,
            collected=len(all_cards),
        )
        # ── Captcha retry loop ────────────────────────────────────────────
        solved = None
        for captcha_attempt in range(1, _CAPTCHA_MAX_TRIES + 1):
            try:
                page.goto(HOME_URL, timeout=25_000, wait_until="domcontentloaded")
                page.wait_for_load_state("domcontentloaded", timeout=10_000)
            except Exception as e:
                logger.error(f"Failed to load home page: {e}")
                break

            captcha_data = None
            captcha_selector = _wait_for_captcha_selector(page, logger)
            if captcha_selector:
                captcha_data = _captcha_data_url_from_page(page, captcha_selector, logger)
            if not captcha_data or len(captcha_data) < 100:
                logger.warning(f"Captcha element screenshot failed (attempt {captcha_attempt}/{_CAPTCHA_MAX_TRIES})")
                continue

            try:
                logger.info(
                    f"Captcha solver request attempt {captcha_attempt}/{_CAPTCHA_MAX_TRIES}",
                    step="timing",
                    start_from=start_from,
                    captcha_attempt=captcha_attempt,
                )
                candidate = captcha_to_text(
                    captcha_data,
                    default_captcha_source="model_captcha",
                    time_out=_CAPTCHA_SOLVER_TIMEOUT_S,
                )
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

        # Set the real Goa search controls and submit the captcha-gated form.
        try:
            page.evaluate(
                """(value) => {
                    const regType = document.querySelector('#Regtype, [name=Regtype]');
                    if (regType) {
                        regType.value = 'Project';
                        regType.dispatchEvent(new Event('change', { bubbles: true }));
                    }

                    let input = document.querySelector('[name=startFrom]');
                    if (!input) {
                        input = document.createElement('input');
                        input.type = 'hidden';
                        input.name = 'startFrom';
                        const form = document.getElementById('searchForm') || document.searchForm;
                        if (form) form.appendChild(input);
                    }
                    if (input) input.value = String(value);

                    const pagination = document.querySelector('[name=isPagination]');
                    if (pagination) pagination.value = value > 0 ? 'true' : '';

                    return Boolean(input);
                }""",
                start_from,
            )
            page.fill('[name=captcha]', solved)
            submitted = page.evaluate(
                """() => {
                    const form = document.getElementById('searchForm') || document.searchForm;
                    if (!form) return false;
                    form.submit();
                    return true;
                }"""
            )
            if not submitted:
                raise RuntimeError("search form not found")
            page.wait_for_load_state("domcontentloaded", timeout=15_000)
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
                page.wait_for_timeout(250)
        if not html_content:
            logger.warning(f"Could not get page content at startFrom={start_from}; skipping page")
            break
        soup = BeautifulSoup(html_content, "lxml")
        # Detect captcha rejection: server redirects back to home page (has captcha form again)
        if soup.find("input", {"name": "captcha"}) and not soup.find("div", class_=lambda c: c and "no_pad_lft" in c):
            _server_rejections += 1
            logger.warning(f"Captcha rejected by server (rejection #{_server_rejections}/{_MAX_SERVER_REJECTS}) — retrying")
            if _server_rejections >= _MAX_SERVER_REJECTS:
                logger.error("Too many captcha rejections — stopping")
                break
            continue  # restart outer while True loop to re-attempt captcha
        cards = _parse_listing_cards(soup)
        if not cards:
            logger.info(f"No cards at startFrom={start_from} — listing complete")
            break
        pages_fetched += 1
        new_cards = []
        for card in cards:
            reg_no = (card.get("project_registration_no") or "").strip().upper()
            if not reg_no or reg_no in seen_reg_nos:
                continue
            seen_reg_nos.add(reg_no)
            new_cards.append(card)
        all_cards.extend(new_cards)
        logger.info(
            f"Fetched {len(cards)} cards ({len(new_cards)} new) at startFrom={start_from}"
        )
        if item_limit and len(all_cards) >= item_limit:
            break
        if not new_cards:
            logger.info(
                f"No unseen cards at startFrom={start_from} — listing complete"
            )
            break
        pagination_offsets = _parse_pagination_offsets(soup)
        next_offsets = [offset for offset in pagination_offsets if offset > start_from]
        if not next_offsets:
            logger.info(f"No pagination offset after startFrom={start_from} — listing complete")
            break
        start_from = next_offsets[0]

    return all_cards


# ── Document handling ──────────────────────────────────────────────────────────

def _handle_document(project_key: str, doc: dict, run_id: int,
                     site_id: str, logger: CrawlerLogger) -> dict | None:
    url   = doc["url"]
    label = doc.get("label") or doc.get("type", "document")
    reused, existing_s3_key = existing_uploaded_document_entry(project_key, {**doc, "url": url, "type": label})
    if reused:
        logger.info("Document reused", label=label, s3_key=existing_s3_key, step="documents")
        logger.log_document(label, url, "reused", s3_key=existing_s3_key)
        return reused
    fname = build_document_filename(doc)
    try:
        resp = _session().download(url, logger=logger)
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
    """Public entry point — ensures the Selenium driver is shut down after the run."""
    try:
        return _run(config, run_id, mode)
    finally:
        _quit_driver()


def _run(config: dict, run_id: int, mode: str) -> dict:
    logger  = CrawlerLogger(config["id"], run_id)
    site_id = config["id"]
    counts  = dict(projects_found=0, projects_new=0, projects_updated=0,
                   projects_skipped=0, documents_uploaded=0, error_count=0)
    checkpoint  = load_checkpoint(site_id, mode) or {}
    done_regs: set[str] = set(checkpoint.get("done_regs", []))
    item_limit  = settings.CRAWL_ITEM_LIMIT or 0
    t_run = time.monotonic()

    # ── Targeted run handling ──────────────────────────────────────────────────
    # --target-reg-no restricts the run to one or more specific projects
    # (comma-separated, case-insensitive). The listing is filtered down to the
    # requested registration number(s) below and the sentinel health check is
    # skipped (mirrors karnataka_rera / uttarakhand_rera).
    target_regs = get_target_reg_nos()

    # ── Sentinel health check ────────────────────────────────────────────────
    if target_regs or mode == "daily_light":
        logger.info("Sentinel skipped (targeted run via --target-reg-no)", step="sentinel")
        counts["sentinel_passed"] = True
    else:
        t0 = time.monotonic()
        if not _sentinel_check(config, run_id, logger):
            logger.error("Sentinel failed — aborting crawl", step="sentinel")
            counts["sentinel_passed"] = False
            counts["error_count"] += 1
            return counts
        counts["sentinel_passed"] = True
        logger.timing("sentinel", time.monotonic() - t0)

    # ── Get project listing ───────────────────────────────────────────────────
    t0 = time.monotonic()
    cards = _fetch_project_listing(config, run_id, logger, item_limit=item_limit)

    if not cards:
        logger.error("No project listing obtained")
        insert_crawl_error(run_id, site_id, "LISTING_FAILED",
                           "Captcha solve failed and no project listing was obtained")
        counts["error_count"] += 1
        return counts

    # ── Targeted filtering ─────────────────────────────────────────────────────
    # Restrict the listing to the requested registration number(s).
    if target_regs:
        matched_regs: set[str] = set()
        cards = [
            c for c in cards
            if (c.get("project_registration_no") or "").strip().upper() in target_regs
        ]
        matched_regs.update(
            (c.get("project_registration_no") or "").strip().upper() for c in cards
        )
        for missing in sorted(target_regs - matched_regs):
            logger.warning(f"Target reg_no={missing!r} not found in listing", step="listing")
        logger.info(
            f"Targeted run — {len(matched_regs)} of {len(target_regs)} requested "
            f"project(s) matched", step="listing",
        )

    if item_limit:
        cards = cards[:item_limit]
    counts["projects_found"] = len(cards)
    update_crawl_run_progress(run_id, counts)
    logger.info(f"Goa RERA: {len(cards)} projects to process")
    logger.timing("search", time.monotonic() - t0, rows=len(cards))

    machine_name, machine_ip = get_machine_context()
    checked_listing_rows = 0
    existing_listing_rows = 0
    candidate_listing_rows = 0

    for i, card in enumerate(cards):
        reg_no = card["project_registration_no"]
        if reg_no in done_regs:
            counts["projects_skipped"] += 1
            continue

        key = generate_project_key(reg_no)
        try:
            logger.set_project(key=key, reg_no=reg_no, url=card.get("detail_url", HOME_URL))

            if mode == "daily_light":
                checked_listing_rows += 1
                existing = get_project_by_key(key)
                if existing:
                    existing_listing_rows += 1
                    counts["projects_skipped"] += 1
                    log_daily_light_listing_progress(
                        site_id,
                        "Goa",
                        checked_rows=checked_listing_rows,
                        existing_rows=existing_listing_rows,
                        candidate_rows=candidate_listing_rows,
                        reg_no=reg_no,
                        project_key=key,
                        existing_match_key=key,
                        raw_reg_no=reg_no,
                    )
                    logger.clear_project()
                    continue
                candidate_listing_rows += 1
                log_daily_light_listing_progress(
                    site_id,
                    "Goa",
                    checked_rows=checked_listing_rows,
                    existing_rows=existing_listing_rows,
                    candidate_rows=candidate_listing_rows,
                    reg_no=reg_no,
                    project_key=key,
                    raw_reg_no=reg_no,
                )
                if settings.LIGHT_SKIP_NEW_ADDITIONS and not target_regs:
                    counts["projects_new"] += 1
                    logger.info(
                        "Skipping new candidate before detail fetch (--skip-new)",
                        step="skip",
                    )
                    logger.clear_project()
                    continue

            data: dict = {
                "key":                     key,  # FIELD: key <- generate_project_key(reg_no)
                "state":                   config["state"],  # FIELD: state <- config["state"]
                "project_state":           config["state"],  # FIELD: project_state <- config["state"]
                "project_registration_no": reg_no,  # FIELD: project_registration_no <- listing card reg_no
                "project_name":            card.get("project_name") or None,  # FIELD: project_name <- listing card project_name
                "promoter_name":           card.get("promoter_name") or None,  # FIELD: promoter_name <- listing card promoter_name
                "domain":                  DOMAIN,  # FIELD: domain <- module DOMAIN constant
                "config_id":               config["config_id"],  # FIELD: config_id <- config["config_id"]
                "url":                     card.get("detail_url") or HOME_URL,  # FIELD: url <- card detail_url or HOME_URL
                "is_live":                 True,  # FIELD: is_live <- literal True
                "machine_name":            machine_name,  # FIELD: machine_name <- get_machine_context()
                "crawl_machine_ip":        machine_ip,  # FIELD: crawl_machine_ip <- get_machine_context()
            }

            if card.get("promoter_type") or card.get("promoter_name"):
                data["promoters_details"] = {k: v for k, v in {  # FIELD: promoters_details <- listing card promoter_type/promoter_name
                    "type_of_firm": card.get("promoter_type"),  # FIELD: promoters_details.type_of_firm <- card promoter_type
                    "name":         card.get("promoter_name"),  # FIELD: promoters_details.name <- card promoter_name
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
                    data["project_state"] = loc["state"]  # FIELD: project_state <- project_location_raw["state"] (properly-cased)

                # Build raw_address for data JSONB
                loc = data.get("project_location_raw") or {}
                _raw_addr = loc.get("raw_address") if isinstance(loc, dict) else None
                promoter_type = (data.get("promoters_details") or {}).get("type_of_firm")
                data["data"] = merge_data_sections(  # FIELD: data <- merge_data_sections of existing data + extras
                    data.get("data"),
                    {
                        "govt_type":      "state",  # FIELD: data.govt_type <- literal "state"
                        "raw_address":    _raw_addr,  # FIELD: data.raw_address <- project_location_raw.raw_address
                        "promoter_type":  promoter_type,  # FIELD: data.promoter_type <- promoters_details.type_of_firm
                        "land_area_unit": "sq Mtr",  # FIELD: data.land_area_unit <- literal "sq Mtr"
                    },
                )
                # Use submitted_date as approved_on_date if missing
                if not data.get("approved_on_date") and data.get("submitted_date"):
                    pass  # submitted_date is the registration date for Goa
            else:
                data["data"] = {"govt_type": "state"}  # FIELD: data <- {"govt_type": "state"} when no detail_url

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
            if action == "new": counts["projects_new"] += 1
            else:               counts["projects_updated"] += 1
            logger.info(f"DB result: {action}", step="db_upsert")

            # Documents
            uploaded_documents = []
            if doc_links and (settings.SKIP_DOCUMENTS or mode == "daily_light"):
                logger.info(
                    f"Skipping {len(doc_links)} documents (light/skip-documents mode)",
                    step="documents",
                )
                uploaded_documents = [
                    {"link": doc.get("url"), "type": doc.get("label") or doc.get("type", "document")}
                    for doc in doc_links
                ]
            else:
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
                    "key":                     db_dict["key"],  # FIELD: key <- db_dict["key"]
                    "url":                     db_dict["url"],  # FIELD: url <- db_dict["url"]
                    "state":                   db_dict["state"],  # FIELD: state <- db_dict["state"]
                    "domain":                  db_dict["domain"],  # FIELD: domain <- db_dict["domain"]
                    "project_registration_no": db_dict["project_registration_no"],  # FIELD: project_registration_no <- db_dict["project_registration_no"]
                    "uploaded_documents":      uploaded_documents,  # FIELD: uploaded_documents <- list built from _handle_document results
                    "document_urls":           build_document_urls(uploaded_documents),  # FIELD: document_urls <- build_document_urls(uploaded_documents)
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
            update_crawl_run_progress(run_id, counts)

    reset_checkpoint(site_id, mode)
    logger.info(f"Goa RERA complete: {counts}")
    logger.timing("total_run", time.monotonic() - t_run)
    return counts
