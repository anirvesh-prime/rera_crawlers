"""
Maharashtra RERA Crawler — maharera.maharashtra.gov.in/projects-search-result
Type: static listing (httpx + BeautifulSoup) + SPA detail (Playwright HTML scraping)

Strategy:
- Bootstrap-card listing page, server-rendered HTML. Pagination via ?page=N.
- ~47,000+ projects across ~4,776 pages of 10 cards each.
- Pagination quirk: ?page=0 and ?page=1 both return the first 10 records,
  so we start with the clean URL (page 0) then use ?page=N+1 for N>=1.
- Each card (div.shadow.rounded) has 7 col-xl-4 cells:
    [0] header: p.p-0=reg_no, h4.title4=name, p.darkBlue.bold=promoter,
                ul.listingList li[0]=location
    [1] State,  [2] Pincode,  [3] Certificate link,  [4] District,
    [5] Last Modified,  [6] Extension Certificate
- Total pages parsed from div.pagination ("of N" text).
- Detail pages: the detail site (maharerait.maharashtra.gov.in) is an Angular SPA
  gated by a canvas CAPTCHA. Strategy:
    1. Use Playwright to load the detail page, solve the CAPTCHA via
       core.captcha_solver against the rendered canvas, and fall back to canvas
       text interception if OCR fails.
    2. Once CAPTCHA is accepted, scrape all rendered Angular tab HTML directly.
    3. Each project gets its own Playwright session — no token management needed.
- CRAWL_ITEM_LIMIT env variable caps total projects processed.
- SCRAPE_DETAILS env variable (default True) enables detail fetching.
- Checkpointing: saves last completed page_no so runs can resume.
"""
from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.captcha_solver import solve_captcha_from_page, wait_for_captcha_canvas
from core.config import settings
from core.crawler_base import generate_project_key, random_delay, safe_get
from core.db import upsert_project, insert_crawl_error
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import get_machine_context, merge_data_sections, normalize_project_payload

LISTING_URL  = "https://maharera.maharashtra.gov.in/projects-search-result"
DETAIL_BASE  = "https://maharerait.maharashtra.gov.in"
STATE_CODE   = "MH"
DOMAIN       = "maharera.maharashtra.gov.in"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_total_pages(soup: BeautifulSoup) -> int:
    """Parse total page count from the Bootstrap pagination div."""
    pag = soup.select_one("div.pagination")
    if pag:
        m = re.search(r"of\s+(\d+)", pag.get_text())
        if m:
            return int(m.group(1))
    return 9999  # fallback: iterate until no cards returned


def _url_for_page(page_no: int) -> str:
    """
    Return the listing URL for the given 0-indexed page_no.

    ?page=0 and ?page=1 both return the first 10 records (server quirk),
    so we skip page=1 by using page_no+1 for all pages after the first.
      page_no=0 -> LISTING_URL      (first page, records 1-10)
      page_no=1 -> ?page=2          (second page, records 11-20)
      page_no=N -> ?page=N+1
    """
    if page_no == 0:
        return LISTING_URL
    return f"{LISTING_URL}?page={page_no + 1}"


def _cell_value(cell: BeautifulSoup) -> str:
    """Return text of a label+value col-xl-4 cell with the label stripped."""
    for lbl in cell.select(".greyColor"):
        lbl.decompose()
    return cell.get_text(strip=True)


def _parse_location(header_cell: BeautifulSoup) -> str:
    """Extract location name from ul.listingList (first <li>, icon stripped)."""
    ul = header_cell.select_one("ul.listingList")
    if not ul:
        return ""
    first_li = ul.select_one("li")
    if not first_li:
        return ""
    for em in first_li.find_all("em"):
        em.decompose()
    return first_li.get_text(strip=True)


def _parse_coords(header_cell: BeautifulSoup) -> tuple[Optional[float], Optional[float]]:
    """Extract (latitude, longitude) from the Google Maps search link in the header cell."""
    maps_link = header_cell.select_one("a[href*='maps.google'], a[href*='google.com/maps/search']")
    if not maps_link:
        return None, None
    m = re.search(r"query=([-\d.]+),([-\d.]+)", maps_link.get("href", ""))
    if not m:
        return None, None
    try:
        return float(m.group(1)), float(m.group(2))
    except ValueError:
        return None, None


def _parse_cards(soup: BeautifulSoup) -> list[dict]:
    """Parse all Bootstrap project cards from a listing page."""
    cards = soup.select("div.shadow.rounded")
    projects = []
    for card in cards:
        cells = card.select(".col-xl-4")
        if len(cells) < 7:
            continue

        # ── Header cell (index 0) ─────────────────────────────────────────────
        hdr        = cells[0]
        reg_el     = hdr.select_one("p.p-0")
        name_el    = hdr.select_one("h4.title4")
        prom_el    = hdr.select_one("p.darkBlue")
        reg_no     = reg_el.get_text(strip=True).lstrip("#").strip() if reg_el else ""
        proj_name  = name_el.get_text(strip=True) if name_el else ""
        prom_name  = prom_el.get_text(strip=True) if prom_el else ""
        location   = _parse_location(hdr)
        lat, lng   = _parse_coords(hdr)

        # ── Detail cells (indices 1-6) ────────────────────────────────────────
        # Order: State, Pincode, Certificate, District, Last Modified, Ext Cert
        state    = _cell_value(cells[1])
        pincode  = _cell_value(cells[2])
        district = _cell_value(cells[4])
        last_mod = _cell_value(cells[5])
        ext_cert = _cell_value(cells[6])

        # Certificate: modal trigger has data-qstr = internal project ID
        cert_btn  = cells[3].select_one("a[data-qstr]")
        cert_id   = cert_btn["data-qstr"] if cert_btn else None
        has_cert  = cert_id is not None

        # View Details link in col-xl-2 action column
        detail_link = card.select_one("a.click-projectmodal.viewLink, a[href*='/public/project/view/']")
        detail_url  = detail_link["href"] if detail_link else None

        if not reg_no:
            continue

        loc_raw: dict = {
            "location": location,
            "district": district,
            "state":    state,
            "pincode":  pincode,
        }
        if lat is not None:
            loc_raw["latitude"]  = lat
            loc_raw["longitude"] = lng

        # Extra metadata stored in data{} so it passes through the normalizer
        extra_data: dict = {}
        if cert_id:
            extra_data["certificate_id"] = cert_id
        if detail_url:
            extra_data["view_details_url"] = detail_url
        if ext_cert and ext_cert not in ("N/A", ""):
            extra_data["extension_certificate"] = ext_cert

        projects.append({
            "project_registration_no": reg_no,
            "project_name":            proj_name,
            "promoter_name":           prom_name,
            "project_location_raw":    loc_raw,
            "last_modified":           last_mod,
            "certificate_available":   has_cert,
            # Exposed at top level so run() can use cert_id for detail API calls
            "certificate_id":          cert_id,
            # view_details_url doubles as the canonical project URL
            "view_details_url":        detail_url,
            "data":                    extra_data or None,
        })
    return projects


# ── Maharashtra detail API helpers ───────────────────────────────────────────

_CAPTCHA_INTERCEPT_SCRIPT = """
(function() {
    var origFillText = CanvasRenderingContext2D.prototype.fillText;
    window.__captchaTexts = [];
    CanvasRenderingContext2D.prototype.fillText = function(text, x, y, maxWidth) {
        if (typeof text === 'string' && text.length > 0 && text.trim().length > 0) {
            window.__captchaTexts.push(text);
        }
        return origFillText.apply(this, arguments);
    };
})();
"""


_MAX_CAPTCHA_ATTEMPTS = 3


def _scrape_mh_detail_page(cert_id: str, logger: CrawlerLogger) -> dict:
    """
    Open maharerait detail page via Playwright, solve CAPTCHA,
    then scrape the rendered Angular HTML tabs.
    Returns a flat dict of schema-mapped fields.
    Returns {} on failure.
    """
    url = f"{DETAIL_BASE}/public/project/view/{cert_id}"
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                )
            )
            context.add_init_script(_CAPTCHA_INTERCEPT_SCRIPT)
            page = context.new_page()
            page.goto(url, timeout=45_000)

            for attempt in range(1, _MAX_CAPTCHA_ATTEMPTS + 1):
                logger.info(f"Captcha attempt {attempt}/{_MAX_CAPTCHA_ATTEMPTS}", step="captcha")

                canvas_ready = wait_for_captcha_canvas(
                    page, "canvas", timeout_ms=20_000, logger=logger
                )
                if not canvas_ready:
                    logger.warning("Canvas not ready — refreshing page", step="captcha")
                    page.reload(timeout=45_000)
                    continue

                captcha_value = solve_captcha_from_page(
                    page, logger=logger, selectors=["canvas"], captcha_source="eprocure",
                )
                if captcha_value:
                    logger.info(f"Captcha solved via OCR: {captcha_value!r}", step="captcha")
                else:
                    captcha_texts = page.evaluate("() => window.__captchaTexts || []")
                    captcha_value = "".join(captcha_texts).strip()
                    if captcha_value:
                        logger.info(
                            f"Captcha recovered via canvas interception: {captcha_value!r}",
                            step="captcha",
                        )

                if not captcha_value:
                    logger.warning(
                        f"Captcha solve failed on attempt {attempt} — refreshing", step="captcha"
                    )
                    page.reload(timeout=45_000)
                    continue

                page.fill("input[name='captcha']", captcha_value)
                page.click("button.next")
                # Wait for Angular to start rendering — label.form-label is the
                # earliest reliable indicator that the project detail page has loaded.
                try:
                    page.wait_for_selector("label.form-label, .col-md-4 .f-s-15", timeout=10_000)
                    logger.info("CAPTCHA accepted — Angular content loaded", step="captcha")
                    break
                except Exception:
                    pass

                logger.warning(
                    f"No Angular content after submit on attempt {attempt} — refreshing",
                    step="captcha",
                )
                page.reload(timeout=45_000)

            out = _extract_mh_html_fields(page, cert_id, logger)
            browser.close()
    except Exception as exc:
        logger.error(f"Playwright detail scrape failed: {exc}", step="detail")
        return {}

    return out


_MH_OVERVIEW_LABEL_MAP: dict[str, str] = {
    "registration no":                       "project_registration_no",
    "registration number":                   "project_registration_no",
    "project type":                          "project_type",
    "proposed completion date (original)":   "estimated_finish_date",
    "proposed completion date":              "estimated_finish_date",
    "completion date":                       "estimated_finish_date",
    "registration date":                     "approved_on_date",
    "date of registration":                  "approved_on_date",
    "approved on":                           "approved_on_date",
    "start date":                            "estimated_commencement_date",
    "acknowledgement no":                    "acknowledgement_no",
    "acknowledgement number":                "acknowledgement_no",
    "submitted date":                        "submitted_date",
    "application date":                      "submitted_date",
}

_MH_STATUS_VALUES = {"active", "revoked", "lapsed", "expired", "extended", "cancelled", "rejected"}


def _find_section_container(soup: BeautifulSoup, heading_text: str) -> Optional[BeautifulSoup]:
    """Return the closest div container that holds the section identified by heading_text."""
    heading = soup.find(
        ["h5", "div", "b", "h4"],
        string=lambda t: t and heading_text.lower() in t.strip().lower(),
    )
    if not heading:
        return None
    # Walk up to the nearest white-box / wh / card-body container
    for ancestor in heading.parents:
        if ancestor.name != "div":
            continue
        classes = ancestor.get("class") or []
        if any(c in classes for c in ("white-box", "card-body", "wh")):
            return ancestor
    return heading.find_parent("div")


def _extract_col4_pairs(container: BeautifulSoup) -> dict[str, str]:
    """Extract label.form-label.col-4 → sibling .col-8 value pairs from a section container."""
    result: dict[str, str] = {}
    for lbl in container.select("label.form-label.col-4"):
        parent_row = lbl.find_parent("div", class_="row")
        val_el = parent_row.select_one(".col-8 .f-w-700, .col-8 .text-font") if parent_row else None
        if val_el:
            key = lbl.get_text(strip=True).lower().rstrip(":")
            value = val_el.get_text(strip=True)
            if value and value != "-":
                result[key] = value
    return result


def _parse_mh_label_value_pairs(soup: BeautifulSoup, out: dict, label_map: dict) -> None:
    """Map label text → schema field using the two layout patterns on the Angular page."""
    # Pattern 1: .col-md-4 overview grid — label in .f-s-15, value in .f-w-700
    for el in soup.select(".col-md-4"):
        lbl_el = el.select_one(".f-s-15")
        val_el = el.select_one(".f-w-700")
        if lbl_el and val_el:
            label = lbl_el.get_text(strip=True).lower().rstrip(":")
            value = val_el.get_text(strip=True)
            if label in label_map and value:
                out[label_map[label]] = value

    # Pattern 2: label.form-label (any) with nearest .f-w-700 or .text-font value
    for lbl_el in soup.select("label.form-label"):
        parent = lbl_el.find_parent()
        val_el = parent.select_one(".f-w-700, .col-12.text-font") if parent else None
        if val_el:
            label = lbl_el.get_text(strip=True).lower().rstrip(":")
            value = val_el.get_text(strip=True)
            if label in label_map and value and value != "-":
                out.setdefault(label_map[label], value)


def _parse_mh_overview_tab(soup: BeautifulSoup, out: dict) -> None:
    """Extract project overview fields (type, dates, status) from the full-page HTML."""
    _parse_mh_label_value_pairs(soup, out, _MH_OVERVIEW_LABEL_MAP)

    # Registration Number + Date of Registration from alternating label.bg-blue.f-w-700 elements
    bg_blue = soup.select("label.bg-blue.f-w-700")
    for i in range(0, len(bg_blue) - 1, 2):
        key = bg_blue[i].get_text(strip=True).lower()
        val = bg_blue[i + 1].get_text(strip=True)
        if not val:
            continue
        if "registration number" in key or "registration no" in key:
            out.setdefault("project_registration_no", val)
        elif "date of registration" in key or "registration date" in key:
            out.setdefault("approved_on_date", val)

    # Project status from the status badge span
    for span in soup.select("span"):
        txt = span.get_text(strip=True)
        if txt.lower() in _MH_STATUS_VALUES:
            out.setdefault("status_of_the_project", txt)
            break


def _parse_mh_promoter_tab(soup: BeautifulSoup, out: dict) -> None:
    """Extract project address, promoter details, and promoter communication address."""
    # ── Project Address Details ──────────────────────────────────────────────
    proj_addr_container = _find_section_container(soup, "Project Address Details")
    if proj_addr_container:
        pairs = _extract_col4_pairs(proj_addr_container)
        loc = dict(out.get("project_location_raw") or {})
        _PROJ_ADDR_MAP = {
            "locality": "locality", "state/ut": "state", "district": "district",
            "taluka": "taluk", "village": "village", "pin code": "pin_code",
        }
        for lbl, field in _PROJ_ADDR_MAP.items():
            if lbl in pairs:
                loc[field] = pairs[lbl]
        try:
            if "longitude :" in pairs:
                loc["longitude"] = float(pairs["longitude :"])
            if "latitude :" in pairs:
                loc["latitude"] = float(pairs["latitude :"])
        except (ValueError, TypeError):
            pass
        if loc:
            out["project_location_raw"] = loc
        if pairs.get("district"):
            out.setdefault("project_city", pairs["district"])
        if pairs.get("pin code"):
            out.setdefault("project_pin_code", pairs["pin code"])

    # ── Promoter Details ─────────────────────────────────────────────────────
    promo_container = _find_section_container(soup, "Promoter Details")
    if promo_container:
        pairs = _extract_col4_pairs(promo_container)
        # "Name of Limited Liability Partnership" / "Name of Company" etc.
        for key, val in pairs.items():
            if key.startswith("name of") and val:
                out.setdefault("promoter_name", val)
                out["promoters_details"] = {"name": val}
                break

    # ── Promoter Official Communication Address ───────────────────────────────
    promo_addr_container = _find_section_container(soup, "Promoter Official Communication Address")
    if promo_addr_container:
        pairs = _extract_col4_pairs(promo_addr_container)
        _PROMO_ADDR_MAP = {
            "unit number": "house_no_building_name",
            "building name": "building_name",
            "landmark": "locality",
            "state/ut": "state",
            "district": "district",
            "taluka": "taluk",
            "village": "village",
            "pin code": "pin_code",
        }
        addr: dict = {}
        for lbl, field in _PROMO_ADDR_MAP.items():
            if lbl in pairs:
                addr[field] = pairs[lbl]
        if addr:
            out["promoter_address_raw"] = addr
        # data extras: state_promo, district_promo, pin_code_promo
        data_extras: dict = {}
        if pairs.get("state/ut"):
            data_extras["state_promo"] = pairs["state/ut"]
        if pairs.get("district"):
            data_extras["district_promo"] = pairs["district"]
        if pairs.get("pin code"):
            try:
                data_extras["pin_code_promo"] = int(pairs["pin code"])
            except (ValueError, TypeError):
                data_extras["pin_code_promo"] = pairs["pin code"]
        if data_extras:
            existing_data = dict(out.get("data") or {})
            existing_data.update({k: v for k, v in data_extras.items() if k not in existing_data})
            out["data"] = existing_data

    # ── Promoter contact details (phone / email anywhere on page) ─────────────
    contact: dict = {}
    for lbl_el in soup.select("label.form-label.col-4"):
        parent_row = lbl_el.find_parent("div", class_="row")
        val_el = parent_row.select_one(".col-8 .f-w-700") if parent_row else None
        if not val_el:
            continue
        lbl_text = lbl_el.get_text(strip=True).lower()
        value = val_el.get_text(strip=True)
        if ("phone" in lbl_text or "mobile" in lbl_text) and value and value != "-":
            contact["phone"] = value
        elif "email" in lbl_text and value and value != "-":
            contact["email"] = value
    if contact:
        out.setdefault("promoter_contact_details", contact)


def _parse_mh_building_tab(soup: BeautifulSoup, out: dict) -> None:
    """Extract land area and building unit details."""
    # Land area from white-box label.form-label (not col-4) containers
    _LAND_LABELS = {
        "total land area of approved layout (sq. mts.)",
        "land area for project applied for this registration (sq. mts)",
        "total land area",
        "land area",
    }
    for lbl_el in soup.select("label.form-label:not(.col-4)"):
        parent = lbl_el.find_parent()
        val_el = parent.select_one(".f-w-700") if parent else None
        if val_el:
            label = lbl_el.get_text(strip=True).lower()
            value = val_el.get_text(strip=True)
            if label in _LAND_LABELS and value:
                try:
                    area = float(value.replace(",", ""))
                    out.setdefault("land_area", area)
                    out.setdefault("land_area_details", {"land_area": area, "land_area_unit": "sq mt"})
                    existing_data = dict(out.get("data") or {})
                    existing_data.setdefault("land_area_unit", "sq mt")
                    out["data"] = existing_data
                except (ValueError, TypeError):
                    pass
                break

    # Building unit details from wing/unit summary table
    for table in soup.select("table"):
        headers = [th.get_text(strip=True).lower() for th in table.select("thead th")]
        if not any("wing" in h or "identification" in h for h in headers):
            continue
        col_map: dict[str, int] = {}
        for i, h in enumerate(headers):
            if "identification of building" in h or "wing" in h:
                col_map.setdefault("flat_type", i)
            elif "residential apartments" in h or "total no. of residential" in h:
                col_map["no_of_units"] = i
            elif "floor" in h and "sanctioned" in h:
                col_map["floor_no"] = i
        if col_map:
            units: list[dict] = []
            for tr in table.select("tbody tr"):
                cells = tr.select("td")
                entry: dict = {}
                for field, idx in col_map.items():
                    if idx < len(cells):
                        val = cells[idx].get_text(strip=True)
                        if val and val not in ("Total", ""):
                            entry[field] = val
                if entry and "flat_type" in entry:
                    units.append(entry)
            if units and not out.get("building_details"):
                out["building_details"] = units
        break


def _parse_mh_bank_details(soup: BeautifulSoup, out: dict) -> None:
    """Extract bank account details from the Bank Details section."""
    bank_container = _find_section_container(soup, "Bank Details")
    if not bank_container:
        return
    bank: dict = {}
    for lbl_el in bank_container.select("label.form-label:not(.col-4)"):
        parent = lbl_el.find_parent()
        val_el = parent.select_one(".f-w-700, .col-12.text-font") if parent else None
        if not val_el:
            continue
        label = lbl_el.get_text(strip=True).lower().rstrip(":")
        value = val_el.get_text(strip=True)
        if not value or value == "-":
            continue
        if "bank name" in label:
            bank["bank_name"] = value
        elif "ifsc" in label:
            bank["IFSC"] = value
        elif "bank address" in label or "address" in label:
            bank.setdefault("address", value)
        elif "branch" in label:
            bank["branch"] = value
    if bank:
        out["bank_details"] = bank


def _parse_mh_partner_tables(soup: BeautifulSoup, out: dict) -> None:
    """
    Extract co-promoter / designated-partner details, authorised signatories,
    and project professional information from rendered tables.
    """
    _SKIP_ROWS = {"no records found", "no record found", "total"}

    for table in soup.select("table"):
        headers = [th.get_text(strip=True).lower() for th in table.select("thead th")]
        rows = table.select("tbody tr")

        # Designated Partners / Co-promoters: [#, Name, Designation, View]
        if "name" in headers and "designation" in headers:
            name_idx = headers.index("name")
            role_idx = headers.index("designation")
            partners: list[dict] = []
            for row in rows:
                cells = [td.get_text(strip=True) for td in row.select("td")]
                if len(cells) <= max(name_idx, role_idx):
                    continue
                name = cells[name_idx]
                role = cells[role_idx]
                if name and name.lower() not in _SKIP_ROWS:
                    partners.append({"name": name, "role": role or "Partner"})
            if partners:
                # Check whether it looks like authorised signatories or co-promoters
                # by inspecting the nearest preceding heading
                heading_el = table.find_previous(["h5", "b", "h4", "h3"])
                heading_txt = heading_el.get_text(strip=True).lower() if heading_el else ""
                if "authorised signatory" in heading_txt or "signatory" in heading_txt:
                    out.setdefault("authorised_signatory_details", partners)
                else:
                    out.setdefault("co_promoter_details", partners)

        # Project Professionals: [#, Professional Name, Certificate No., Professional Type]
        elif any("professional name" in h for h in headers):
            name_idx = next((i for i, h in enumerate(headers) if "professional name" in h), None)
            type_idx = next((i for i, h in enumerate(headers) if "professional type" in h), None)
            profs: list[dict] = []
            for row in rows:
                cells = [td.get_text(strip=True) for td in row.select("td")]
                if name_idx is None or name_idx >= len(cells):
                    continue
                name = cells[name_idx]
                if not name or name.lower() in _SKIP_ROWS:
                    continue
                entry: dict = {"name": name, "role": "Professional"}
                if type_idx is not None and type_idx < len(cells) and cells[type_idx]:
                    entry["role"] = cells[type_idx]
                profs.append(entry)
            if profs:
                out.setdefault("professional_information", profs)


def _parse_mh_documents_tab(soup: BeautifulSoup, out: dict) -> None:
    """Collect document links rendered in the Angular page."""
    docs: list[dict] = []
    seen: set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href or href in seen or href.startswith("javascript"):
            continue
        if any(kw in href for kw in ("/download", "/dms", ".pdf", "documentId", "/doc/")):
            abs_url = href if href.startswith("http") else f"{DETAIL_BASE}{href}"
            label = a.get_text(strip=True) or a.get("title", "Document") or "Document"
            seen.add(href)
            docs.append({"label": label, "url": abs_url})
    if docs:
        out["uploaded_documents"] = docs
        out["document_urls"] = [{"type": d["label"], "link": d["url"]} for d in docs]


def _extract_mh_html_fields(page, cert_id: str, logger: CrawlerLogger) -> dict:
    """
    Scrape all detail fields from the rendered Angular page on maharerait.
    The site renders all sections in a single HTML page (no tab-click navigation
    needed). We wait for network idle so Angular finishes its API calls.
    """
    out: dict = {}

    # Wait for Angular to finish rendering / API calls
    try:
        page.wait_for_load_state("networkidle", timeout=20_000)
    except Exception:
        logger.warning("networkidle timeout — scraping page as-is", step="detail")

    # Confirm expected Angular content is present
    try:
        page.wait_for_selector("label.form-label, .col-md-4 .f-s-15", timeout=10_000)
    except Exception:
        logger.warning("Angular form labels not found on detail page", step="detail")

    soup = BeautifulSoup(page.content(), "lxml")

    _parse_mh_overview_tab(soup, out)      # Registration, status, project type, dates
    _parse_mh_promoter_tab(soup, out)      # Project address, promoter details & address
    _parse_mh_building_tab(soup, out)      # Land area, building units
    _parse_mh_bank_details(soup, out)      # Bank account details
    _parse_mh_partner_tables(soup, out)    # Designated partners, signatories, professionals
    _parse_mh_documents_tab(soup, out)     # Document library links

    existing_data = dict(out.get("data") or {})
    existing_data["project_id"] = str(cert_id)
    out["data"] = existing_data

    return {k: v for k, v in out.items() if v is not None}


# ── Sentinel ──────────────────────────────────────────────────────────────────

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Maharashtra RERA.
    Loads state_projects_sample/maharashtra.json as the baseline, re-scrapes
    the sentinel project's detail page via Playwright, and verifies ≥ 80% field coverage.
    """
    import json as _json
    import os as _os
    from urllib.parse import urlparse as _urlparse
    from core.sentinel_utils import check_field_coverage

    sentinel_reg = config.get("sentinel_registration_no", "")
    if not sentinel_reg:
        logger.warning("No sentinel_registration_no configured — skipping", step="sentinel")
        return True

    sample_path = _os.path.join(
        _os.path.dirname(_os.path.dirname(__file__)),
        "state_projects_sample", "maharashtra.json",
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

    # Extract cert_id from URL path (last segment)
    path_parts = _urlparse(detail_url).path.rstrip("/").split("/")
    cert_id = path_parts[-1] if path_parts else ""
    if not cert_id:
        logger.warning("Sentinel: could not extract cert_id from URL — skipping",
                       url=detail_url, step="sentinel")
        return True

    logger.info(f"Sentinel: scraping {sentinel_reg}", cert_id=cert_id, step="sentinel")
    try:
        fresh = _scrape_mh_detail_page(cert_id, logger) or {}
    except Exception as exc:
        logger.error(f"Sentinel: scrape error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", cert_id=cert_id, step="sentinel")
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        from core.db import insert_crawl_error as _ice
        _ice(
            run_id, config.get("id", "maharashtra_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


# ── Main entry point ──────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    logger = CrawlerLogger(config["id"], run_id)
    counters = dict(projects_found=0, projects_new=0, projects_updated=0,
                    projects_skipped=0, documents_uploaded=0, error_count=0)
    machine_name, machine_ip = get_machine_context()
    delay_range   = config.get("rate_limit_delay", (2, 4))
    item_limit    = settings.CRAWL_ITEM_LIMIT or 0   # 0 = unlimited
    scrape_detail = settings.SCRAPE_DETAILS

    # ── Sentinel health check ────────────────────────────────────────────────
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counters["error_count"] += 1
        return counters

    # ── Determine total pages ────────────────────────────────────────────────
    resp0 = safe_get(LISTING_URL, retries=3, logger=logger)
    if not resp0:
        logger.error("Could not fetch first listing page", step="listing")
        return counters

    soup0       = BeautifulSoup(resp0.text, "lxml")
    total_pages = _get_total_pages(soup0)

    checkpoint = load_checkpoint(config["id"], mode)
    start_page = (checkpoint["last_page"] + 1) if checkpoint else 0

    max_pages = settings.MAX_PAGES
    end_page  = min(total_pages, start_page + max_pages) if max_pages else total_pages
    logger.info(
        f"Total pages: {total_pages} | crawling {start_page}–{end_page - 1} "
        f"| item_limit={item_limit or 'unlimited'} | scrape_detail={scrape_detail}",
        step="listing",
    )

    items_processed = 0
    stop_all = False

    # ── Page loop ────────────────────────────────────────────────────────────
    for page_no in range(start_page, end_page):
        if stop_all:
            break

        url = _url_for_page(page_no)
        logger.info(f"Page {page_no + 1}/{total_pages}", step="listing")

        if page_no == 0:
            soup = soup0
        else:
            resp = safe_get(url, retries=config.get("max_retries", 3), logger=logger)
            if not resp:
                insert_crawl_error(run_id, config["id"], "HTTP_ERROR", f"page {page_no} fetch failed", url)
                counters["error_count"] += 1
                random_delay(*delay_range)
                continue
            soup = BeautifulSoup(resp.text, "lxml")

        cards = _parse_cards(soup)
        if not cards:
            logger.warning(f"No cards on page {page_no} — stopping", step="listing")
            break

        counters["projects_found"] += len(cards)

        for raw in cards:
            if item_limit and items_processed >= item_limit:
                logger.info(f"Item limit {item_limit} reached — stopping", step="listing")
                stop_all = True
                break

            reg_no = raw.get("project_registration_no", "").strip()
            if not reg_no:
                counters["error_count"] += 1
                continue

            key         = generate_project_key(reg_no)
            detail_url  = raw.pop("view_details_url", None)
            cert_id     = raw.pop("certificate_id", None)
            project_url = detail_url or url
            logger.set_project(key=key, reg_no=reg_no, url=project_url, page=page_no)
            try:
                # ── Detail page enrichment via Playwright HTML scrape ────────
                detail_fields: dict = {}
                if scrape_detail and cert_id:
                    detail_fields = _scrape_mh_detail_page(cert_id, logger)
                    if not detail_fields:
                        logger.warning(
                            f"Detail scrape returned empty for {cert_id}", step="detail"
                        )
                    else:
                        logger.info(
                            f"Detail scraped for {reg_no} — "
                            f"status={detail_fields.get('status_of_the_project')!r}",
                            step="detail",
                        )
                    random_delay(1, 2)

                try:
                    payload: dict = {
                        **raw,
                        **detail_fields,
                        "domain": DOMAIN,
                        "url":    project_url,
                        "state":  config.get("state", "Maharashtra"),
                    }
                    # Merge data JSONB sections (listing data + detail API raw data)
                    if "data" in detail_fields and "data" in raw:
                        payload["data"] = merge_data_sections(raw.get("data", {}), detail_fields.get("data", {}))

                    # Derive project_city from listing district when the API doesn't provide it
                    if not payload.get("project_city"):
                        loc = payload.get("project_location_raw") or {}
                        if isinstance(loc, dict) and loc.get("district"):
                            payload["project_city"] = loc["district"]

                    normalized = normalize_project_payload(payload, config, machine_name=machine_name, machine_ip=machine_ip)
                    record  = ProjectRecord(**normalized)
                    db_dict = record.to_db_dict()
                    status  = upsert_project(db_dict)

                    items_processed += 1
                    if status == "new":
                        counters["projects_new"] += 1
                        logger.info(f"New: {reg_no}", step="upsert")
                    elif status == "updated":
                        counters["projects_updated"] += 1
                        logger.info(f"Updated: {reg_no}", step="upsert")
                    else:
                        counters["projects_skipped"] += 1

                except ValidationError as exc:
                    counters["error_count"] += 1
                    logger.error(str(exc), step="validate")
                    insert_crawl_error(run_id, config["id"], "VALIDATION_FAILED", str(exc), url, project_key=key)
                except Exception as exc:
                    counters["error_count"] += 1
                    logger.error(str(exc), step="upsert")
                    insert_crawl_error(run_id, config["id"], "CRAWLER_EXCEPTION", str(exc), url, project_key=key)
            finally:
                logger.clear_project()

        save_checkpoint(config["id"], mode, page_no, None, run_id)
        random_delay(*delay_range)

    reset_checkpoint(config["id"], mode)
    logger.info(f"Maharashtra RERA complete: {counters}", step="done")
    return counters
