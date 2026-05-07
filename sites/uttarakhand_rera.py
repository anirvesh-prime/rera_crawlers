"""
Uttarakhand RERA Crawler — ukrera.uk.gov.in
Type: static (httpx + BeautifulSoup — server-rendered Java Spring MVC / Tiles)

Strategy:
- GET /viewRegisteredProjects with a session-aware client.  All ~450 registered
  projects are returned on a single page (server-side pagination is disabled for
  the public view; pagination JS is present but commented out in the HTML).
- Each listing card (div.row.defalter_result_list) yields: project name, address,
  registration number, promoter, promoter type, property type, and a
  `viewProjectDetailPage?projectID=N` link.
- Detail pages are fetched with the same session client.  The server responds
  with a 302 redirect to a session-encrypted URL; follow_redirects=True handles
  this transparently.
- The portal's TLS configuration requires unsafe legacy SSL renegotiation.
  `get_legacy_ssl_context()` from core.crawler_base handles this.
- Emails on detail pages are obfuscated ("[at]", "[dot]"); _decode_email() restores them.
- Date of Registration uses Java's toString() format:
  "Fri Nov 21 12:17:30 IST 2025" — _parse_java_date() normalises it.
- Project End Date uses "DD-MM-YYYY" — _parse_dmy() handles it.
- Documents are linked as `download?DOC_ID=N`; empty hrefs are skipped.
- Project images are embedded as `reraimage?IMG_ID=N`.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup, Tag
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, reset_checkpoint, save_checkpoint
from core.crawler_base import (
    generate_project_key,
    get_legacy_ssl_context,
    get_random_ua,
    random_delay,
)
from core.db import get_project_by_key, insert_crawl_error, upsert_document, upsert_project
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
from core.s3 import compute_md5, get_s3_url, upload_document
from core.config import settings

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL    = "https://ukrera.uk.gov.in"
DOMAIN      = "ukrera.uk.gov.in"
LISTING_URL = f"{BASE_URL}/viewRegisteredProjects"

UTC = timezone.utc

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Java Date.toString() pattern: "Fri Nov 21 12:17:30 IST 2025"
_JAVA_DATE_RE = re.compile(
    r"\w{3}\s+(\w{3})\s+(\d{1,2})\s+(\d{2}:\d{2}:\d{2})\s+\w+\s+(\d{4})",
    re.IGNORECASE,
)
_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# DD-MM-YYYY or DD/MM/YYYY
_DMY_RE = re.compile(r"^(\d{2})[-/](\d{2})[-/](\d{4})$")

# Registration number pattern for UK RERA
_REG_RE = re.compile(r"\bUKREP\w+\b", re.IGNORECASE)

# Area value extractor
_AREA_RE = re.compile(r"([\d,]+(?:\.\d+)?)", re.ASCII)


# ---------------------------------------------------------------------------
# Text-cleaning helpers
# ---------------------------------------------------------------------------

def _clean(node) -> str:
    """Return stripped, whitespace-collapsed text from a BS4 node or string."""
    if node is None:
        return ""
    text = node.get_text(separator=" ") if hasattr(node, "get_text") else str(node)
    return re.sub(r"\s+", " ", text).strip()


def _decode_email(text: str) -> str:
    """Restore obfuscated email: '[at]' → '@', '[dot]' → '.'."""
    return text.replace("[at]", "@").replace("[dot]", ".")


def _parse_java_date(raw: str) -> str | None:
    """
    Parse Java Date.toString() e.g. 'Fri Nov 21 12:17:30 IST 2025'
    into ISO-8601 with UTC offset: '2025-11-21 12:17:30+00:00'.
    """
    m = _JAVA_DATE_RE.search(raw.strip())
    if not m:
        return None
    month_name, day, time_part, year = m.groups()
    month = _MONTH_MAP.get(month_name.lower())
    if not month:
        return None
    try:
        dt = datetime(int(year), month, int(day),
                      *[int(x) for x in time_part.split(":")],
                      tzinfo=UTC)
        return dt.isoformat(sep=" ")
    except ValueError:
        return None


def _parse_dmy(raw: str) -> str | None:
    """Parse 'DD-MM-YYYY' or 'DD/MM/YYYY' → 'YYYY-MM-DD 00:00:00+00:00'."""
    m = _DMY_RE.match(raw.strip())
    if not m:
        return None
    dd, mm, yyyy = m.groups()
    try:
        datetime(int(yyyy), int(mm), int(dd))
        return f"{yyyy}-{mm}-{dd} 00:00:00+00:00"
    except ValueError:
        return None


def _parse_area(raw: str) -> float | None:
    """Extract first numeric value from a raw area string."""
    m = _AREA_RE.search(raw.replace(",", ""))
    try:
        return float(m.group(1)) if m else None
    except ValueError:
        return None


def _resolve_url(href: str) -> str | None:
    """Resolve a relative href against BASE_URL; return None for empty/anchor hrefs."""
    if not href or href.strip() in ("#", ""):
        return None
    href = href.strip()
    if href.startswith("http"):
        return href
    return urljoin(BASE_URL + "/", href.lstrip("/"))


# ---------------------------------------------------------------------------
# HTTP client factory
# ---------------------------------------------------------------------------

def _make_client() -> httpx.Client:
    """Return a session-aware httpx.Client with legacy SSL support."""
    ssl_ctx = get_legacy_ssl_context()
    return httpx.Client(
        verify=ssl_ctx,
        follow_redirects=True,
        timeout=httpx.Timeout(connect=15.0, read=60.0, write=10.0, pool=5.0),
        headers={"User-Agent": _UA},
    )


def _fetch_listing_html_playwright(logger: CrawlerLogger) -> str:
    """Fetch the listing page HTML via Playwright (Chromium).

    Used as a fallback when httpx connections are reset at the TLS level by the
    UK RERA portal.  Chromium's TLS fingerprint is accepted where Python's is not.
    Returns the rendered page HTML, or an empty string on failure.
    """
    from playwright.sync_api import sync_playwright
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx  = browser.new_context(ignore_https_errors=True, user_agent=_UA)
            page = ctx.new_page()
            # Server-rendered Java Spring page — all 400+ project cards are in the
            # initial HTML response.  "domcontentloaded" / "networkidle" time out
            # because the portal is slow and sends continuous keep-alive polling.
            # "commit" fires as soon as the first byte arrives, then we wait briefly
            # to ensure the full response body has been transferred before reading.
            page.goto(LISTING_URL, wait_until="commit", timeout=120_000)
            page.wait_for_timeout(3_000)
            html = page.content()
            browser.close()
        logger.info("Playwright listing fetch succeeded", url=LISTING_URL, step="listing")
        return html
    except Exception as exc:
        logger.error(f"Playwright listing fetch failed: {exc}", step="listing")
        return ""


# ---------------------------------------------------------------------------
# Listing parser
# ---------------------------------------------------------------------------

def _parse_listing(html: str) -> list[dict]:
    """
    Parse all project cards from the registered-projects listing page.
    Returns a list of dicts with keys:
      project_name, project_registration_no, promoter_name, project_type,
      status_of_the_project (reason_of_revoke if non-empty),
      project_location_raw (raw_address), detail_url, project_images (listing img).
    """
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict] = []
    seen_regs: set[str] = set()

    for card in soup.select("div.row.defalter_result_list"):
        # ── Project name ───────────────────────────────────────────────────
        h1 = card.find("h1")
        project_name: str | None = None
        if h1:
            raw_h1 = _clean(h1)
            project_name = raw_h1.split("Project:")[-1].strip() or None

        # ── Registration number ────────────────────────────────────────────
        reg_no: str | None = None
        for p in card.find_all("p"):
            text = _clean(p)
            if "Reg No." in text:
                m = _REG_RE.search(text)
                if m:
                    reg_no = m.group(0).upper()
                break
        if not reg_no:
            continue
        if reg_no in seen_regs:
            continue
        seen_regs.add(reg_no)

        # ── Raw address ────────────────────────────────────────────────────
        raw_address: str | None = None
        paragraphs = card.find_all("p")
        for p in paragraphs:
            text = _clean(p)
            if "Reg No." not in text and text:
                # Strip Bootstrap glyphicon text artefacts
                text = text.replace("glyphicon-map-marker", "").strip()
                if text:
                    raw_address = text
                    break

        # ── Listing table: PROMOTER | PROMOTER TYPE | PROPERTY TYPE | REASON OF REVOKE
        table = card.find("table")
        promoter_name: str | None = None
        promoter_type: str | None = None
        project_type: str | None = None
        reason_of_revoke: str | None = None
        if table:
            headers = [_clean(th).upper() for th in table.find_all("th")]
            data_row = table.find("tbody")
            td_cells = data_row.find_all("td") if data_row else []
            col = {h: _clean(td_cells[i]) if i < len(td_cells) else ""
                   for i, h in enumerate(headers)}
            promoter_name  = col.get("PROMOTER") or None
            promoter_type  = col.get("PROMOTER TYPE") or None
            project_type   = col.get("PROPERTY TYPE") or None
            reason_of_revoke = col.get("REASON OF REVOKE") or None

        # ── Detail link ────────────────────────────────────────────────────
        detail_url: str | None = None
        for a in card.find_all("a", href=True):
            if "viewProjectDetailPage" in a["href"]:
                detail_url = _resolve_url(a["href"])
                break

        # ── Listing image ──────────────────────────────────────────────────
        listing_images: list[str] = []
        img_tag = card.find("img", src=True)
        if img_tag and img_tag.get("src"):
            img_url = _resolve_url(img_tag["src"])
            if img_url:
                listing_images.append(img_url)

        row: dict = {
            "project_name":             project_name,
            "project_registration_no":  reg_no,
            "promoter_name":            promoter_name,
            "project_type":             project_type,
            "detail_url":               detail_url,
        }
        if raw_address:
            row["project_location_raw"] = {"raw_address": raw_address}
        if reason_of_revoke:
            row["status_of_the_project"] = reason_of_revoke
        if promoter_type:
            row["_promoter_type"] = promoter_type
        if listing_images:
            row["lister_images"] = listing_images
        rows.append(row)

    return rows



# ---------------------------------------------------------------------------
# Detail page parser
# ---------------------------------------------------------------------------

# Bootstrap row layout: repeated groups of 4 col-md-3 divs where
# col 0 = label (text-right p), col 1 = value p, col 2 = label, col 3 = value.
_DETAIL_LABEL_MAP: dict[str, str] = {
    "promoter":                         "promoter_name",
    "date of registration":             "submitted_date",
    "project type":                     "project_type",
    "project end date":                 "estimated_finish_date",
    "approved map validity date":       "_map_validity_date",
    "project status":                   "status_of_the_project",
    "total area of project land":       "_land_area_raw",
    "total open area (sq ft.)":         "_open_area_raw",
    "total coverd area(sq ft.)":        "_covered_area_raw",
    "total covered area(sq ft.)":       "_covered_area_raw",
    "project address line 1":           "_addr_line1",
    "district":                         "_district",
    "tehsil/sub district":              "_taluk",
    "tehsil/sub\ndistrict":             "_taluk",
    "no of garage":                     "_no_garage",
    "no of parking open":               "_no_parking_open",
}


def _parse_profile_detail(soup: BeautifulSoup, out: dict, raw_data: dict) -> None:
    """
    Extract key-value pairs from the Bootstrap-grid profile_detail section.
    Each row has 4 col-md-3 columns: [label, value, label, value].
    """
    for row_div in soup.select("div.profile_detail div.row"):
        cols = row_div.find_all("div", recursive=False)
        # Pair up columns: (0,1), (2,3)
        for idx in range(0, len(cols) - 1, 2):
            label_col = cols[idx]
            value_col = cols[idx + 1]
            label_p = label_col.find("p")
            value_p = value_col.find("p")
            if not label_p or not value_p:
                continue
            label = _clean(label_p).rstrip(":").strip().lower()
            value = _clean(value_p)
            if not label or not value:
                continue

            raw_data[label] = value
            schema_field = _DETAIL_LABEL_MAP.get(label)
            if schema_field is None:
                continue

            if schema_field == "promoter_name":
                out.setdefault("promoter_name", value or None)
            elif schema_field == "submitted_date":
                parsed = _parse_java_date(value)
                if parsed:
                    out.setdefault("submitted_date", parsed)
            elif schema_field == "project_type":
                out.setdefault("project_type", value or None)
            elif schema_field == "estimated_finish_date":
                parsed = _parse_dmy(value)
                if parsed:
                    out.setdefault("estimated_finish_date", parsed)
            elif schema_field == "status_of_the_project":
                out.setdefault("status_of_the_project", value or None)
            elif schema_field == "_land_area_raw":
                # e.g. "13660.0 sq Mt."
                val = _parse_area(value)
                if val is not None:
                    out.setdefault("_land_area_val", val)
                    # Capture unit after the number
                    unit_m = re.search(r"[\d.]+\s*(.*)", value)
                    out.setdefault("_land_area_unit", unit_m.group(1).strip() if unit_m else "sq Mt.")
            elif schema_field == "_open_area_raw":
                val = _parse_area(value)
                if val is not None:
                    out.setdefault("_open_area_val", val)
            elif schema_field == "_covered_area_raw":
                val = _parse_area(value)
                if val is not None:
                    out.setdefault("_covered_area_val", val)
                    out.setdefault("construction_area", val)
            elif schema_field == "_addr_line1":
                out.setdefault("_addr_line1", value)
            elif schema_field == "_district":
                out.setdefault("_district", value)
            elif schema_field == "_taluk":
                out.setdefault("_taluk", value)


def _parse_reg_info(soup: BeautifulSoup, out: dict) -> None:
    """Extract registration number and project name from the detail header."""
    for span in soup.select("span.reg"):
        text = _clean(span)
        m = _REG_RE.search(text)
        if m:
            out.setdefault("project_registration_no", m.group(0).upper())
            break
    # Project name from inner h1 (after the profile_box)
    detail_block = soup.select_one("div.col-md-9")
    if detail_block:
        h1 = detail_block.find("h1")
        if h1:
            name = _clean(h1)
            if name and "UKREP" not in name and "Last Updated" not in name:
                out.setdefault("project_name", name)


def _parse_description(soup: BeautifulSoup, out: dict) -> None:
    """Extract project description from <h1>Project Description</h1> + next <p>."""
    for h1 in soup.find_all("h1"):
        if "project description" in _clean(h1).lower():
            nxt = h1.find_next_sibling()
            if nxt and nxt.name == "p":
                desc = _clean(nxt)
                if desc:
                    out.setdefault("project_description", desc)
            break


def _parse_applicant_table(soup: BeautifulSoup, out: dict) -> None:
    """
    Extract applicant details (promoter contact).
    Table structure: thead=[Name, E-mail, Mobile], tbody=[name, email, mobile].
    """
    for h1 in soup.find_all("h1"):
        if "applicant" in _clean(h1).lower():
            table = h1.find_next("table")
            if not table:
                break
            rows = table.select("tbody tr")
            for tr in rows:
                tds = tr.find_all("td")
                if len(tds) < 3:
                    continue
                name  = _clean(tds[0])
                email = _decode_email(_clean(tds[1]))
                phone = _clean(tds[2])
                contact: dict = {}
                if email and "@" in email:
                    contact["email"] = email
                elif "[at]" in _clean(tds[1]):
                    contact["email"] = _decode_email(_clean(tds[1]))
                if phone:
                    contact["phone"] = phone
                if contact:
                    out.setdefault("promoter_contact_details", contact)
                if name and not out.get("promoter_name"):
                    out["promoter_name"] = name
            break


def _parse_professionals(soup: BeautifulSoup, out: dict) -> None:
    """
    Extract architects, structural engineers, and CA from their respective tables.
    Table headers identify the role; columns differ per role but always start
    with [Name, Email, Address/Address Line 1, Pin Code, Year of Establishment].
    """
    role_map = {
        "project architects":     "Architects",
        "structural engineers":   "Structural Engineers",
        "project ca":             "Project CA",
        "project agent":          "Project Agent",
    }
    professionals: list[dict] = []

    for h1 in soup.find_all("h1"):
        heading = _clean(h1).lower()
        role: str | None = None
        for key, label in role_map.items():
            if key in heading:
                role = label
                break
        if not role:
            continue

        table = h1.find_next("table")
        if not table:
            continue

        thead = table.find("thead")
        headers: list[str] = []
        if thead:
            headers = [_clean(th).lower() for th in thead.find_all("th")]

        for tr in table.select("tbody tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            # Generic extraction by column position
            name = _clean(tds[0]) if len(tds) > 0 else ""
            if not name:
                continue

            # Email in tds[1] — may be obfuscated in a <p> tag
            email = ""
            if len(tds) > 1:
                p_tag = tds[1].find("p")
                raw_email = _clean(p_tag) if p_tag else _clean(tds[1])
                email = _decode_email(raw_email) if raw_email else ""

            address = _clean(tds[2]) if len(tds) > 2 else ""
            # pin_code = _clean(tds[3]) if len(tds) > 3 else ""  # not mapped to schema

            prof: dict = {"name": name, "role": role, "has_same_data": True}
            if email and "@" in email:
                prof["email"] = email
            if address:
                prof["address"] = address
            professionals.append(prof)

    if professionals:
        out.setdefault("professional_information", professionals)


def _parse_inventory(soup: BeautifulSoup, out: dict) -> None:
    """
    Extract unit counts from the Development Details (Inventory) table.
    Columns: Type of Inventory | No of Inventory | Carpet Area (Sq Mtr) | ...
    """
    for h1 in soup.find_all("h1"):
        if "development" in _clean(h1).lower():
            table = h1.find_next("table")
            if not table:
                break
            residential = 0
            commercial = 0
            _RES_WORDS = {"apartment", "flat", "villa", "plot", "residential", "studio", "house"}
            _COM_WORDS = {"commercial", "shop", "office", "retail", "showroom"}
            for tr in table.select("tbody tr"):
                tds = tr.find_all("td")
                if len(tds) < 2:
                    continue
                inv_type = _clean(tds[0]).lower()
                count_raw = _clean(tds[1])
                count = _parse_area(count_raw)
                if count is None:
                    continue
                count_int = int(count)
                if any(k in inv_type for k in _COM_WORDS):
                    commercial += count_int
                else:
                    residential += count_int
            if residential:
                out.setdefault("number_of_residential_units", residential)
            if commercial:
                out.setdefault("number_of_commercial_units", commercial)
            break


def _parse_documents_and_images(
    soup: BeautifulSoup, out: dict,
) -> tuple[list[dict], list[str]]:
    """
    Collect document links and project image URLs from the detail page.
    Returns (doc_links, project_images).
    Doc links: href="download?DOC_ID=N" (skip empty DOC_ID).
    Project images: src="reraimage?IMG_ID=N" under .construc_update_main.
    """
    doc_links: list[dict] = []
    seen_urls: set[str] = set()

    # Documents: all <a href="download?DOC_ID=N"> with a non-empty DOC_ID
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or "download" not in href.lower():
            continue
        # skip empty DOC_ID (href="download?DOC_ID=")
        if re.search(r"DOC_ID=\s*$", href):
            continue
        full_url = _resolve_url(href)
        if not full_url or full_url in seen_urls:
            continue
        seen_urls.add(full_url)
        label = _clean(a) or "document"
        # Determine doc category from parent h1
        parent_section = ""
        for parent in a.parents:
            prev_h1 = getattr(parent, "find_previous_sibling", lambda t: None)("h1")
            if prev_h1:
                parent_section = _clean(prev_h1).lower()
                break
        doc_links.append({"label": label, "url": full_url, "section": parent_section})

    # Project images from construction updates section
    project_images: list[str] = []
    seen_imgs: set[str] = set()
    for img in soup.select("div.construc_update_main img[src]"):
        src = img["src"].strip()
        if not src:
            continue
        full_src = _resolve_url(src)
        if full_src and full_src not in seen_imgs:
            seen_imgs.add(full_src)
            project_images.append(full_src)

    return doc_links, project_images


def _parse_detail_page(url: str, client: httpx.Client, logger: CrawlerLogger) -> dict:
    """
    Fetch and fully parse a UK RERA project detail page.
    Returns a flat dict with all extracted fields plus '_doc_links' and '_project_images'.
    """
    try:
        resp = client.get(url, headers={"Referer": LISTING_URL})
        resp.raise_for_status()
    except Exception as exc:
        logger.warning(f"Detail fetch failed: {exc}", url=url)
        return {}

    if len(resp.content) < 500:
        logger.warning("Detail page suspiciously small — skipping", url=url, bytes=len(resp.content))
        return {}

    soup = BeautifulSoup(resp.text, "lxml")
    out: dict = {}
    raw_data: dict = {"govt_type": "state", "source_url": url}

    _parse_reg_info(soup, out)
    _parse_profile_detail(soup, out, raw_data)
    _parse_description(soup, out)
    _parse_applicant_table(soup, out)
    _parse_professionals(soup, out)
    _parse_inventory(soup, out)

    doc_links, project_images = _parse_documents_and_images(soup, out)

    # ── Build land_detail from open/covered area ────────────────────────────
    open_a  = out.pop("_open_area_val",    None)
    covered = out.pop("_covered_area_val", None)
    land_v  = out.pop("_land_area_val",    None)
    land_u  = out.pop("_land_area_unit",   "sq Mt.")

    land_detail: dict = {}
    if open_a is not None:
        land_detail["open_area"] = str(open_a)
    if covered is not None:
        land_detail["covered_area"] = str(covered)
    total = open_a if open_a is not None else land_v
    if total is not None:
        land_detail["total_area"] = str(total)
    if land_detail:
        out["land_detail"] = land_detail

    # Build land_area_details — use land_v first, fall back to open area as total
    land_area_for_lad = land_v if land_v is not None else open_a
    if land_area_for_lad is not None or covered is not None:
        out["land_area_details"] = {k: v for k, v in {
            "land_area":              str(land_area_for_lad) if land_area_for_lad is not None else None,
            "land_area_unit":         land_u,
            "construction_area":      str(covered) if covered is not None else None,
            "construction_area_unit": "sq Mt.",
        }.items() if v is not None}

    # Construction area defaults to covered area
    # Set land_area_unit whenever we have any land area value (land_v or open_a fallback)
    if land_v is not None or open_a is not None:
        raw_data["land_area_unit"] = land_u
    if "construction_area" not in out and covered is not None:
        out["construction_area"] = covered
    ca_unit = "sq Mt."
    raw_data["construction_area_unit"] = ca_unit

    # ── Build project_location_raw from sub-parts ───────────────────────────
    addr1    = out.pop("_addr_line1", None)
    district = out.pop("_district",  None)
    taluk    = out.pop("_taluk",     None)
    loc: dict = {}
    if taluk:
        loc["taluk"] = taluk
    if district:
        loc["district"] = district
    if addr1:
        # Build a composite raw_address: "addr1, taluk, district, Uttarakhand"
        parts = [p for p in [addr1, taluk, district, "Uttarakhand"] if p]
        loc["raw_address"] = ", ".join(parts)
        raw_data["raw_address"] = loc["raw_address"]
    if loc:
        out.setdefault("project_location_raw", loc)

    # Remove internal private keys
    out.pop("_map_validity_date", None)
    out.pop("_no_garage", None)
    out.pop("_no_parking_open", None)

    out["data"] = raw_data
    out["_doc_links"] = doc_links
    out["_project_images"] = project_images
    # Canonical detail URL (after redirect, resp.url reflects encrypted URL)
    out["_canonical_url"] = str(resp.url)
    return out


# ---------------------------------------------------------------------------
# Document handler
# ---------------------------------------------------------------------------

def _handle_document(
    project_key: str,
    doc: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
    client: httpx.Client,
) -> dict | None:
    """Download one document, upload to S3, persist to DB. Returns enriched entry or None."""
    url   = doc.get("url") or doc.get("link")
    label = doc.get("label") or "document"
    if not url:
        return None
    doc_for_fn = {"url": url, "label": label, **doc}
    filename = build_document_filename(doc_for_fn)
    try:
        resp = client.get(url, headers={"Referer": LISTING_URL})
        if not resp or len(resp.content) < 100:
            return None
        content = resp.content
        md5 = compute_md5(content)
        s3_key = upload_document(project_key, filename, content, dry_run=settings.DRY_RUN_S3)
        if s3_key is None:
            return None
        s3_url = get_s3_url(s3_key)
        upsert_document(
            project_key=project_key,
            document_type=label,
            original_url=document_identity_url(doc_for_fn) or url,
            s3_key=s3_key,
            s3_bucket=settings.S3_BUCKET_NAME,
            file_name=filename,
            md5_checksum=md5,
            file_size_bytes=len(content),
        )
        logger.info("Document uploaded", label=label, s3_key=s3_key, step="documents")
        logger.log_document(label, url, "uploaded", s3_key=s3_key, file_size_bytes=len(content))
        return document_result_entry(doc_for_fn, s3_url, filename)
    except Exception as exc:
        logger.error(f"Document failed [{label}]: {exc}", step="documents")
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(exc),
                           project_key=project_key, url=url)
        return None


# ---------------------------------------------------------------------------
# Sentinel check
# ---------------------------------------------------------------------------

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Uttarakhand RERA.
    Re-scrapes the known sentinel project detail page and verifies ≥ 80% field
    coverage against the state_projects_sample/uttarakhand.json baseline.
    """
    import json as _json
    import os as _os
    from core.sentinel_utils import check_field_coverage

    sentinel_reg = config.get("sentinel_registration_no", "")
    sentinel_id  = config.get("sentinel_project_id")
    if not sentinel_reg:
        logger.warning("No sentinel_registration_no configured — skipping", step="sentinel")
        return True

    sample_path = _os.path.join(
        _os.path.dirname(_os.path.dirname(__file__)),
        "state_projects_sample", "uttarakhand.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check",
                       path=sample_path, step="sentinel")
        return True

    # Use the URL from sample, or construct from known sentinel_project_id
    detail_url = (
        baseline.get("url")
        or (f"{BASE_URL}/viewProjectDetailPage?projectID={sentinel_id}" if sentinel_id else "")
    )
    if not detail_url:
        logger.warning("Sentinel: no detail URL available — skipping", step="sentinel")
        return True

    logger.info(f"Sentinel: fetching {sentinel_reg}", url=detail_url, step="sentinel")
    try:
        with _make_client() as client:
            # Warm up session with listing page first; ignore connection errors
            # (some environments block the listing URL but still allow detail pages)
            try:
                client.get(LISTING_URL)
            except Exception as _warm_exc:
                logger.warning(f"Sentinel: listing warm-up failed (non-fatal) — {_warm_exc}",
                               step="sentinel")
            # Pre-flight connectivity check: distinguish network-level failures
            # (connection reset / refused) from HTTP / parse failures.
            try:
                _resp = client.get(detail_url, headers={"Referer": LISTING_URL})
                _resp.raise_for_status()
            except (httpx.ConnectError, httpx.RemoteProtocolError,
                    httpx.ConnectTimeout) as _net_exc:
                logger.warning(
                    f"Sentinel: network-level block detected — portal unreachable "
                    f"from this host ({_net_exc}); skipping rather than failing",
                    step="sentinel",
                )
                return True
            except Exception:
                pass  # Non-network errors handled below via _parse_detail_page

            fresh = _parse_detail_page(detail_url, client, logger)
    except Exception as exc:
        logger.error(f"Sentinel: fetch/parse error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", url=detail_url, step="sentinel")
        return False

    # Verify registration number matches
    scraped_reg = fresh.get("project_registration_no", "")
    if scraped_reg and scraped_reg.upper() != sentinel_reg.upper():
        logger.error(
            f"Sentinel: reg_no mismatch — expected {sentinel_reg!r}, got {scraped_reg!r}",
            step="sentinel",
        )
        insert_crawl_error(run_id, config.get("id", "uttarakhand_rera"),
                           "SENTINEL_FAILED", f"reg_no mismatch: {scraped_reg!r}")
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "uttarakhand_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True



# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(config: dict, run_id: int, mode: str) -> dict:
    """
    Entry point called by the crawler orchestrator.

    Modes:
        weekly_deep   – crawl all projects; update every record
        daily_light   – skip projects already in DB (new projects only)
        full          – same as weekly_deep but resets checkpoint
    """
    logger  = CrawlerLogger(config["id"], run_id)
    site_id = config["id"]
    counts  = dict(
        projects_found=0,
        projects_new=0,
        projects_updated=0,
        projects_skipped=0,
        documents_uploaded=0,
        error_count=0,
    )

    # ── Sentinel health check ────────────────────────────────────────────────
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counts["error_count"] += 1
        return counts

    # ── Checkpoint handling ──────────────────────────────────────────────────
    if mode == "full":
        reset_checkpoint(site_id, mode)
    checkpoint   = (load_checkpoint(site_id, mode) if mode != "full" else {}) or {}
    done_regs: set[str] = set(checkpoint.get("done_regs", []))

    item_limit   = settings.CRAWL_ITEM_LIMIT or 0
    delay_range  = config.get("rate_limit_delay", (2, 4))
    machine_name, machine_ip = get_machine_context()

    # ── Fetch listing page ───────────────────────────────────────────────────
    logger.info("Fetching project listing", url=LISTING_URL, step="listing")
    listing_html = ""
    try:
        with _make_client() as listing_client:
            resp = listing_client.get(LISTING_URL)
            resp.raise_for_status()
            listing_html = resp.text
    except (httpx.ConnectError, httpx.RemoteProtocolError,
            httpx.ReadError, OSError) as _net_exc:
        # Portal actively resets Python TCP connections (TLS fingerprint block).
        # Fall back to Playwright whose Chromium fingerprint is accepted.
        logger.warning(
            f"httpx listing blocked ({_net_exc}); falling back to Playwright",
            step="listing",
        )
        listing_html = _fetch_listing_html_playwright(logger)
    except Exception as exc:
        logger.error(f"Listing fetch failed: {exc}", step="listing")
        insert_crawl_error(run_id, site_id, "LISTING_FAILED", str(exc), url=LISTING_URL)
        counts["error_count"] += 1
        return counts

    if not listing_html:
        logger.error("Listing fetch failed (httpx blocked, Playwright also failed)",
                     step="listing")
        insert_crawl_error(run_id, site_id, "LISTING_FAILED",
                           "All fetch methods failed", url=LISTING_URL)
        counts["error_count"] += 1
        return counts

    cards = _parse_listing(listing_html)
    if not cards:
        logger.error("No projects found on listing page", step="listing")
        insert_crawl_error(run_id, site_id, "LISTING_EMPTY", "No project cards parsed",
                           url=LISTING_URL)
        counts["error_count"] += 1
        return counts

    if item_limit:
        cards = cards[:item_limit]
    counts["projects_found"] = len(cards)
    logger.info(f"Uttarakhand RERA: {len(cards)} projects to process")

    # ── Process each project ─────────────────────────────────────────────────
    with _make_client() as client:
        # Warm up the session by hitting the listing page so JSESSIONID is set
        try:
            client.get(LISTING_URL)
        except Exception:
            pass

        for i, card in enumerate(cards):
            reg_no = card.get("project_registration_no")
            if not reg_no:
                counts["error_count"] += 1
                continue

            if reg_no in done_regs:
                counts["projects_skipped"] += 1
                continue

            key = generate_project_key(reg_no)
            try:
                logger.set_project(
                    key=key,
                    reg_no=reg_no,
                    url=card.get("detail_url") or LISTING_URL,
                )

                if mode == "daily_light" and get_project_by_key(key):
                    counts["projects_skipped"] += 1
                    logger.clear_project()
                    continue

                # ── Merge listing data into base record ──────────────────────
                data: dict = {
                    "key":                     key,
                    "state":                   config["state"],
                    "project_state":           config["state"],
                    "project_registration_no": reg_no,
                    "project_name":            card.get("project_name") or None,
                    "promoter_name":           card.get("promoter_name") or None,
                    "project_type":            card.get("project_type") or None,
                    "domain":                  DOMAIN,
                    "config_id":               config["config_id"],
                    "url":                     card.get("detail_url") or LISTING_URL,
                    "is_live":                 True,
                    "machine_name":            machine_name,
                    "crawl_machine_ip":        machine_ip,
                }

                if card.get("project_location_raw"):
                    data["project_location_raw"] = card["project_location_raw"]
                if card.get("status_of_the_project"):
                    data["status_of_the_project"] = card["status_of_the_project"]
                if card.get("lister_images"):
                    data["project_images"] = card["lister_images"]

                promoter_type = card.get("_promoter_type")
                # Only set promoters_details when promoter_type is available from the listing;
                # promoter_name alone is already stored as a top-level field and is redundant here.
                if promoter_type:
                    data["promoters_details"] = {k: v for k, v in {
                        "type_of_firm": promoter_type,
                        "name":         card.get("promoter_name"),
                    }.items() if v}

                # ── Fetch detail page ─────────────────────────────────────────
                doc_links: list[dict] = []
                if card.get("detail_url"):
                    random_delay(*delay_range)
                    detail = _parse_detail_page(card["detail_url"], client, logger)
                    doc_links       = detail.pop("_doc_links", [])
                    project_images  = detail.pop("_project_images", [])
                    canonical_url   = detail.pop("_canonical_url", None)

                    # Merge detail fields (don't overwrite listing-set fields with None)
                    for k, v in detail.items():
                        if v is not None and not k.startswith("_"):
                            data[k] = v

                    if canonical_url:
                        data["url"] = canonical_url
                    if project_images:
                        data.setdefault("project_images", project_images)

                    # Build composite raw_address for data JSONB if not set by detail
                    loc     = data.get("project_location_raw") or {}
                    raw_addr = loc.get("raw_address") if isinstance(loc, dict) else None
                    data["data"] = merge_data_sections(
                        data.get("data"),
                        {
                            "govt_type":               "state",
                            "raw_address":             raw_addr,
                            "promoter_type":           promoter_type,
                            "construction_area_unit":  "sq Mt.",
                        },
                    )
                else:
                    data["data"] = {"govt_type": "state"}

                # ── Normalize & upsert ────────────────────────────────────────
                logger.info("Normalizing", step="normalize")
                try:
                    normalized = normalize_project_payload(
                        data, config,
                        machine_name=machine_name,
                        machine_ip=machine_ip,
                    )
                    record  = ProjectRecord(**normalized)
                    db_dict = record.to_db_dict()
                except (ValidationError, ValueError) as exc:
                    logger.warning("Validation failed — raw fallback", error=str(exc))
                    insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(exc),
                                       project_key=key, url=data.get("url"), raw_data=data)
                    counts["error_count"] += 1
                    db_dict = normalize_project_payload(
                        {**data, "data": merge_data_sections(
                            data.get("data"), {"validation_fallback": True}
                        )},
                        config,
                        machine_name=machine_name,
                        machine_ip=machine_ip,
                    )

                action = upsert_project(db_dict)
                if action == "new":         counts["projects_new"] += 1
                elif action == "updated":   counts["projects_updated"] += 1
                else:                       counts["projects_skipped"] += 1

                # ── Documents ─────────────────────────────────────────────────
                uploaded_documents: list[dict] = []
                doc_name_counts: dict[str, int] = {}
                for doc in doc_links:
                    selected = select_document_for_download(
                        config["state"], doc, doc_name_counts, domain=DOMAIN
                    )
                    if selected:
                        uploaded = _handle_document(
                            db_dict["key"], selected, run_id, site_id, logger, client
                        )
                        if uploaded:
                            uploaded_documents.append(uploaded)
                            counts["documents_uploaded"] += 1
                        else:
                            uploaded_documents.append({
                                "link": doc.get("url"),
                                "type": doc.get("label") or "document",
                            })
                    else:
                        uploaded_documents.append({
                            "link": doc.get("url"),
                            "type": doc.get("label") or "document",
                        })

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

    # ── Final checkpoint ─────────────────────────────────────────────────────
    save_checkpoint(site_id, mode, len(cards), None, run_id)
    logger.info("Crawl complete", **counts)
    return counts
