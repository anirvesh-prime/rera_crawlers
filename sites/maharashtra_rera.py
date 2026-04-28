"""
Maharashtra RERA Crawler — maharera.maharashtra.gov.in/projects-search-result
Type: static listing (httpx + BeautifulSoup) + SPA detail (Playwright + httpx)

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
    1. Use Playwright to load the first detail page, solve the CAPTCHA via
       core.captcha_solver against the rendered canvas, and fall back to canvas
       text interception if OCR fails. Capture the JWT Bearer token from
       authenticatePublic.
    2. Re-use that token for all subsequent projects via plain httpx API calls.
    3. Token is valid ~100 minutes; re-acquire if a call returns 401.
- CRAWL_ITEM_LIMIT env variable caps total projects processed.
- SCRAPE_DETAILS env variable (default True) enables detail fetching.
- Checkpointing: saves last completed page_no so runs can resume.
"""
from __future__ import annotations

import re
import urllib.parse
from typing import Optional

import httpx
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
API_BASE     = f"{DETAIL_BASE}/api/maha-rera-public-view-project-registration-service/public/projectregistartion"
DMS_BASE     = f"{DETAIL_BASE}/api/maha-rera-dms-service/batch-job"
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


def _acquire_mh_token(cert_id: str, logger: CrawlerLogger) -> str | None:
    """
    Load maharerait detail page via Playwright, solve the CAPTCHA, submit it,
    then return the JWT Bearer token from authenticatePublic.
    The token is valid for ~100 minutes and can be reused for all project API calls.
    Retries up to _MAX_CAPTCHA_ATTEMPTS times (refreshing the page each attempt).
    """
    url = f"{DETAIL_BASE}/public/project/view/{cert_id}"
    token: str | None = None

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

            def _on_resp(resp):
                nonlocal token
                if "authenticatePublic" in resp.url and token is None:
                    try:
                        obj = resp.json().get("responseObject") or {}
                        if isinstance(obj, dict):
                            token = obj.get("accessToken")
                    except Exception:
                        pass

            page = context.new_page()
            page.on("response", _on_resp)
            page.goto(url, timeout=45_000)

            for attempt in range(1, _MAX_CAPTCHA_ATTEMPTS + 1):
                logger.info(f"Captcha attempt {attempt}/{_MAX_CAPTCHA_ATTEMPTS}", step="captcha")

                # Wait until the canvas has non-blank pixel data before reading it
                canvas_ready = wait_for_captcha_canvas(
                    page, "canvas", timeout_ms=20_000, logger=logger
                )
                if not canvas_ready:
                    logger.warning("Canvas not ready — refreshing page", step="captcha")
                    page.reload(timeout=45_000)
                    continue

                captcha_value = solve_captcha_from_page(
                    page,
                    logger=logger,
                    selectors=["canvas"],
                    captcha_source="eprocure",
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
                page.wait_for_timeout(4_000)

                if token:
                    break  # Token received via response listener — done

                logger.warning(
                    f"No token after submit on attempt {attempt} — refreshing", step="captcha"
                )
                page.reload(timeout=45_000)

            browser.close()
    except Exception as exc:
        logger.error(f"Playwright CAPTCHA token acquisition failed: {exc}", step="captcha")

    if token:
        logger.info("JWT token acquired for Maharashtra detail API", step="captcha")
    else:
        logger.warning("JWT token NOT obtained — detail scraping will be skipped", step="captcha")
    return token


def _api_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Referer": DETAIL_BASE + "/",
        "Origin": DETAIL_BASE,
    }


def _post_api(
    endpoint: str,
    project_id: str | int,
    token: str,
    timeout: float = 15.0,
    extra: dict | None = None,
) -> dict | None:
    """POST to a maharerait project API endpoint and return responseObject or None.

    ``extra`` keys are merged into the JSON body alongside ``projectId``.
    """
    body: dict = {"projectId": int(project_id)}
    if extra:
        body.update(extra)
    try:
        resp = httpx.post(
            f"{API_BASE}/{endpoint}",
            json=body,
            headers=_api_headers(token),
            timeout=timeout,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "1":
                return data.get("responseObject")
        elif resp.status_code == 401:
            return {"_expired": True}
    except Exception:
        pass
    return None


def _fetch_mh_project_detail(cert_id: str, token: str, logger: CrawlerLogger) -> dict:
    """
    Call key maharerait project APIs for a single project.
    Returns a flat dict of schema-mapped fields + raw data for the JSONB store.
    Returns {} if the token is expired (caller should re-acquire).
    """
    pid = int(cert_id)

    # ── Status ───────────────────────────────────────────────────────────────
    status_obj = _post_api("getProjectCurrentStatus", pid, token)
    if isinstance(status_obj, dict) and status_obj.get("_expired"):
        logger.warning("JWT token expired — need to re-acquire", step="detail")
        return {"_token_expired": True}

    status_name = ""
    if status_obj and isinstance(status_obj, dict):
        status_name = (status_obj.get("coreStatus") or {}).get("statusName", "")

    # ── General details ────────────────────────────────────────────────────
    gen_obj = _post_api("getProjectGeneralDetailsByProjectId", pid, token) or {}
    if isinstance(gen_obj, dict) and gen_obj.get("_expired"):
        return {"_token_expired": True}

    project_type  = gen_obj.get("projectTypeName", "")
    finish_date   = gen_obj.get("projectProposeComplitionDate", "")        # revised
    orig_finish   = gen_obj.get("originalProjectProposeCompletionDate", "")
    start_date    = gen_obj.get("projectStartDate", "")
    reg_date      = gen_obj.get("reraRegistrationDate", "")
    ack_no        = gen_obj.get("acknowledgementNumber", "")
    reg_no_api    = gen_obj.get("projectRegistartionNo", "")

    # ── Land address ──────────────────────────────────────────────────────
    land_obj = _post_api("getProjectLandAddressDetails", pid, token) or {}
    pincode    = land_obj.get("pinCode", "")
    address    = land_obj.get("addressLine", "")
    boundaries = {k: land_obj.get(k, "")
                  for k in ("boundariesEast", "boundariesWest", "boundariesNorth", "boundariesSouth")
                  if land_obj.get(k)}
    total_land = land_obj.get("totalAreaSqmts")

    # ── Geo-tagging (lat/lng) ─────────────────────────────────────────────
    geo_obj = _post_api("getProjectLegalGeoTaggingDetailByProjectId", pid, token) or {}
    if isinstance(geo_obj, list) and geo_obj:
        geo_obj = geo_obj[0] or {}
    latitude  = geo_obj.get("latitude") if isinstance(geo_obj, dict) else None
    longitude = geo_obj.get("longitude") if isinstance(geo_obj, dict) else None

    # ── Promoter ──────────────────────────────────────────────────────────
    promo_obj = _post_api("getProjectAndAssociatedPromoterDetails", pid, token) or {}
    promo_details = (promo_obj.get("promoterDetails") or {}) if isinstance(promo_obj, dict) else {}
    promoter_name = promo_details.get("promoterName", "")
    promoter_addr: dict = {}
    if promo_details:
        promoter_addr = {
            "building": promo_details.get("buildingName", ""),
            "district": promo_details.get("districtName", ""),
            "state":    promo_details.get("stateName", ""),
            "pincode":  promo_details.get("pincode", ""),
        }

    # ── Bank details (API returns a list) ────────────────────────────────
    bank_raw = _post_api("getProjectPromoterBankDetails", pid, token) or []
    if isinstance(bank_raw, dict):      # defensive: normalise to list
        bank_raw = [bank_raw]
    bank_obj = bank_raw[0] if isinstance(bank_raw, list) and bank_raw else {}
    bank: dict = {}
    if isinstance(bank_obj, dict) and bank_obj.get("bankFullName"):
        bank = {
            "bank_name": bank_obj.get("bankFullName", ""),
            "branch":    bank_obj.get("branchFullName") or bank_obj.get("branchName", ""),
            "IFSC":      bank_obj.get("ifsccode", ""),
            "address":   bank_obj.get("bankAddress", ""),
        }

    # ── userProfileId (needed for personnel call) ─────────────────────────
    user_profile_id = promo_details.get("userProfileId") if promo_details else None

    # ── Authorized Signatory / Single Point of Contact ───────────────────
    spoc_list = _post_api("getPromoterSpocDetails", pid, token) or []
    auth_signatory: list = []
    promoter_contact: dict = {}
    if isinstance(spoc_list, list):
        for spoc in spoc_list:
            if not isinstance(spoc, dict):
                continue
            name = " ".join(filter(None, [
                spoc.get("firstName"), spoc.get("middleName"), spoc.get("lastName"),
            ])).strip()
            role = spoc.get("designation", "")
            if name:
                auth_signatory.append({"name": name, "role": role})
            # SPOC has unencrypted phone + email — use for promoter_contact_details
            if not promoter_contact:
                ph = spoc.get("mobileNumber", "")
                em = spoc.get("emailId", "")
                if ph or em:
                    promoter_contact = {k: v for k, v in {"phone": ph, "email": em}.items() if v}

    # ── Designated Partners → co_promoter_details ────────────────────────
    co_promos: list = []
    if user_profile_id:
        personnel = _post_api(
            "fetchPromoterPersonnelContactAddressDetails", pid, token,
            extra={"userProfileId": user_profile_id},
        ) or []
        if isinstance(personnel, list):
            for p in personnel:
                if not isinstance(p, dict):
                    continue
                name = " ".join(filter(None, [
                    p.get("firstname"), p.get("middleName"), p.get("lastName"),
                ])).strip()
                role = p.get("userProfilePersonnelDesignationId", "Partner")
                if name:
                    co_promos.append({"name": name, "role": role})

    # ── Promoter general details (GSTIN, firm type) ──────────────────────
    gen_promo = _post_api("fetchPromoterGeneralDetails", pid, token) or {}
    promoters_info: dict = {}
    if isinstance(gen_promo, dict) and not gen_promo.get("_expired"):
        promoters_info = {k: v for k, v in {
            "name":         gen_promo.get("organizationName") or promoter_name or None,
            "GSTIN":        gen_promo.get("gstinNumber") or None,
            "pan_no":       None,
            "type_of_firm": gen_promo.get("organizationType") or None,
        }.items() if v is not None}

    # ── Better promoter address (has taluk / village names) ──────────────
    addr_obj = _post_api("getPromoterAddressDetails", pid, token) or {}
    if isinstance(addr_obj, dict) and not addr_obj.get("_expired") and addr_obj.get("districtName"):
        promoter_addr = {k: v for k, v in {
            "state":                  addr_obj.get("stateName"),
            "taluk":                  addr_obj.get("talukaName"),
            "village":                addr_obj.get("villageName"),
            "district":               addr_obj.get("districtName"),
            "locality":               addr_obj.get("locality") or addr_obj.get("landmark") or None,
            "pin_code":               addr_obj.get("pinCode"),
            "building_name":          addr_obj.get("buildingName") if addr_obj.get("buildingName") != "-" else None,
            "house_no_building_name": addr_obj.get("unitNumber") if addr_obj.get("unitNumber") != "-" else None,
        }.items() if v}

    # ── Construction progress ─────────────────────────────────────────────
    act_obj = _post_api("getBuildingWingsActivityDetails", pid, token) or {}
    construction_progress: list = []
    if isinstance(act_obj, dict) and not act_obj.get("_expired"):
        for bldg in (act_obj.get("projectActivityDetails") or []):
            bname = bldg.get("buildingNameNumber", "")
            for act in (bldg.get("activities") or []):
                if act.get("isAvailable") != "1":
                    continue
                title = act.get("projectActivityParticularTypeName") or act.get("activityParticularOtherName", "")
                pct   = act.get("completionPercentage") or 0
                if title:
                    pct_str = str(int(pct)) if pct == int(pct) else str(pct)
                    construction_progress.append({
                        "title":               title,
                        "building_name":       bname,
                        "progress_percentage": pct_str,
                    })

    # ── Uploaded documents ────────────────────────────────────────────────
    docs_raw = _post_api("getUploadedDocuments", pid, token) or []
    uploaded_docs: list = []
    doc_urls: list = []
    if isinstance(docs_raw, list):
        for doc in docs_raw:
            if not isinstance(doc, dict):
                continue
            fname = doc.get("documentFileName") or ""
            dms_ref = doc.get("documentDmsRefNo") or ""
            dtype = doc.get("documentDescription") or doc.get("documentDetails") or "Document"
            if fname and dms_ref:
                # Construct a reference URL encoding the DMS ref and file name
                link = (
                    f"{DMS_BASE}/downloadDocumentForPublicView"
                    f"?documentId={urllib.parse.quote(dms_ref)}"
                    f"&fileName={urllib.parse.quote(fname)}"
                )
                uploaded_docs.append({"type": dtype, "name": fname, "link": link})
                doc_urls.append({"type": dtype, "link": link})
            elif fname:
                uploaded_docs.append({"type": dtype, "name": fname})

    # ── Land header (construction area, land area details) ───────────────
    land_hdr = _post_api("getProjectLandHeaderDetails", pid, token) or {}
    construction_area_val: float | None = None
    land_area_hdr: float | None = None
    land_area_details_val: dict = {}
    if isinstance(land_hdr, dict) and not land_hdr.get("_expired"):
        try:
            construction_area_val = float(land_hdr.get("projectProposedNotSanctionedBuildUpArea") or 0) or None
        except (TypeError, ValueError):
            pass
        try:
            land_area_hdr = float(land_hdr.get("landAreaSqmts") or 0) or None
        except (TypeError, ValueError):
            pass
        if land_area_hdr or construction_area_val:
            land_area_details_val = {k: v for k, v in {
                "land_area":               land_area_hdr,
                "land_area_unit":          "sq mt" if land_area_hdr else None,
                "construction_area":       construction_area_val,
                "construction_area_unit":  "sq mt" if construction_area_val else None,
            }.items() if v is not None}

    # ── Means of finance → project_cost_detail ───────────────────────────
    finance_obj = _post_api("getProjectMeansOfFinance", pid, token) or {}
    project_cost: dict = {}
    if isinstance(finance_obj, dict) and not finance_obj.get("_expired"):
        est = finance_obj.get("estimated") or {}
        if isinstance(est, dict):
            total = est.get("totalEstimatedCostTableA") or est.get("totalFundsForProject")
            if total is not None:
                try:
                    project_cost = {"estimated_construction_cost": float(total)}
                except (TypeError, ValueError):
                    pass

    # ── Sold/Unsold inventory → building_details ─────────────────────────
    inv_obj = _post_api("getProjectSoldUnsoldInventory", pid, token) or {}
    building_units: list = []
    if isinstance(inv_obj, dict) and not inv_obj.get("_expired"):
        for inv_key, is_sold in [
            ("projectBuildingSoldInventory", True),
            ("projectBuildingUnSoldInventory", False),
        ]:
            for bldg in (inv_obj.get(inv_key) or []):
                if not isinstance(bldg, dict):
                    continue
                bname = bldg.get("buildingName") or ""
                wname = bldg.get("buildingWingName") or ""
                block = f"{bname} {wname}".strip() if wname and wname.upper() != "NA" else bname
                for unit in (bldg.get("floorsUnitDetails") or []):
                    if not isinstance(unit, dict):
                        continue
                    agreement = unit.get("unitConsiderationAsPerAgreement") or 0.0
                    reckoner  = unit.get("unitConsiderationAsPerReckonerRate") or 0.0
                    entry: dict = {
                        "floor_no":       unit.get("floorName"),
                        "flat_name":      unit.get("unitNameNumber"),
                        "block_name":     block or None,
                        "amount_paid":    unit.get("receivedAmount") if is_sold else 0.0,
                        "carpet_area":    unit.get("unitCarpetAreaSqmts"),
                        "max_flat_value": agreement if is_sold else 0.0,
                        "min_flat_value": reckoner if not is_sold else 0.0,
                    }
                    building_units.append({k: v for k, v in entry.items() if v is not None})

    # ── Professional information ──────────────────────────────────────────
    prof_list = _post_api("getProjectProfessional", pid, token) or []
    professionals: list = []
    if isinstance(prof_list, list):
        for p in prof_list:
            if not isinstance(p, dict):
                continue
            name = " ".join(filter(None, [
                p.get("firstName"), p.get("middleName"), p.get("lastName"),
            ])).strip() or (p.get("entityCompanyName") or "")
            role = p.get("executiveOfficerDesignation") or "Professional"
            addr_parts = [p.get("locality"), p.get("districtName")]
            entry_p: dict = {
                "name":    name,
                "role":    role,
                "email":   p.get("emailId"),
                "phone":   p.get("primaryContactNo"),
                "pan_no":  None,
                "address": " ".join(filter(None, addr_parts)) or None,
            }
            professionals.append({k: v for k, v in entry_p.items() if v is not None})

    # ── Assemble location raw ─────────────────────────────────────────────
    loc_raw: dict = {}
    if address:
        loc_raw["address"] = address
    if pincode:
        loc_raw["pincode"] = pincode
    if boundaries:
        loc_raw.update(boundaries)
    if latitude is not None:
        loc_raw["latitude"]  = latitude
        loc_raw["longitude"] = longitude

    # ── Submitted date & city from general / promoter ────────────────────
    submitted_date = gen_obj.get("projectApplicationDate", "")
    project_city   = promo_details.get("districtName", "")

    out: dict = {
        "status_of_the_project":       status_name or None,
        "project_type":                project_type or None,
        "estimated_finish_date":       finish_date or None,
        "estimated_commencement_date": start_date or None,
        "approved_on_date":            reg_date or None,
        "acknowledgement_no":          ack_no or None,
    }
    if reg_no_api:
        out["project_registration_no"] = reg_no_api
    if loc_raw:
        out["project_location_raw"] = loc_raw
    if pincode:
        out["project_pin_code"] = pincode
    if promoter_name:
        out["promoter_name"] = promoter_name
    if promoter_addr:
        out["promoter_address_raw"] = promoter_addr
    if bank:
        out["bank_details"] = bank
    if submitted_date:
        out["submitted_date"] = submitted_date
    if project_city:
        out["project_city"] = project_city
    # Use land header area as more authoritative source; fall back to address area
    if land_area_hdr is not None:
        out["land_area"] = land_area_hdr
    elif total_land is not None:
        try:
            out["land_area"] = float(total_land)
        except (TypeError, ValueError):
            pass
    if construction_area_val is not None:
        out["construction_area"] = construction_area_val
    if land_area_details_val:
        out["land_area_details"] = land_area_details_val
    if project_cost:
        out["project_cost_detail"] = project_cost
    if building_units:
        out["building_details"] = building_units
    if professionals:
        out["professional_information"] = professionals
    if auth_signatory:
        out["authorised_signatory_details"] = auth_signatory
    if promoter_contact:
        out["promoter_contact_details"] = promoter_contact
    if co_promos:
        out["co_promoter_details"] = co_promos
    if promoters_info:
        out["promoters_details"] = promoters_info
    if construction_progress:
        out["construction_progress"] = construction_progress
    if uploaded_docs:
        out["uploaded_documents"] = uploaded_docs
    if doc_urls:
        out["document_urls"] = doc_urls

    # data JSONB — only Maharashtra-allowed keys (see _STATE_JSON_FIELD_ALLOWED_KEYS)
    out["data"] = {k: v for k, v in {
        "project_id":               str(pid),
        "agent_type":               gen_promo.get("organizationType") if isinstance(gen_promo, dict) else None,
        "land_area_unit":           "sq mt" if (land_area_hdr or total_land) else None,
        "construction_area_unit":   "sq mt" if construction_area_val else None,
        "estimated_construction_cost": str(project_cost.get("estimated_construction_cost")) if project_cost else None,
        "state_promo":              promo_details.get("stateName") if promo_details else None,
        "district_promo":           promo_details.get("districtName") if promo_details else None,
        "pin_code_promo":           promo_details.get("pincode") if promo_details else None,
    }.items() if v is not None}
    return {k: v for k, v in out.items() if v is not None}


# ── Sentinel ──────────────────────────────────────────────────────────────────

def _sentinel_check(logger: CrawlerLogger) -> bool:
    resp = safe_get(LISTING_URL, retries=2, logger=logger)
    if not resp:
        logger.error("Sentinel: listing page unreachable", step="sentinel")
        return False
    soup = BeautifulSoup(resp.text, "lxml")
    if not soup.select("div.shadow.rounded"):
        logger.error("Sentinel: no project cards found on listing page", step="sentinel")
        return False
    logger.info("Sentinel passed", step="sentinel")
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

    if not _sentinel_check(logger):
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

    # ── Auth token for detail API (acquired lazily on first detail request) ──
    mh_token: str | None = None

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
                # ── Detail page enrichment ───────────────────────────────────
                detail_fields: dict = {}
                if scrape_detail and cert_id:
                    # Acquire token lazily (once per run)
                    if mh_token is None:
                        mh_token = _acquire_mh_token(cert_id, logger)

                    if mh_token:
                        detail_fields = _fetch_mh_project_detail(cert_id, mh_token, logger)
                        if detail_fields.get("_token_expired"):
                            # Re-acquire token and retry once
                            mh_token = _acquire_mh_token(cert_id, logger)
                            detail_fields = _fetch_mh_project_detail(cert_id, mh_token, logger) if mh_token else {}

                        detail_fields.pop("_token_expired", None)
                        logger.info(
                            f"Detail fetched for {reg_no} — status={detail_fields.get('status_of_the_project')!r}",
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
