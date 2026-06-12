"""
Gujarat RERA Crawler — gujrera.gujarat.gov.in
Type: Selenium (Angular SPA — bulk enumeration API + detail page scraping)

Strategy:
- Enumeration: a single call to the dashboard's bulk project-list endpoint
  /dashboard/get-district-wise-projectlist/0/0/all/all/all returns every
  registered project with its projectRegId and regNo.  This is the only
  internal API used; it exists because the public listing page's search
  form cannot be driven via automation, and the registration number is
  required to compute the project key before the detail page is fetched
  (so daily_light can skip already-known projects without a per-project
  page.goto).
- Detail scrape: for each stub, navigate to /#/project-preview?id={b64}
  via Selenium, wait for the Angular SPA to render, and parse the HTML
  with BeautifulSoup using CSS selectors.  The registration number from
  the enumeration stub is the single source of truth — it is NOT
  re-extracted from the detail page.
- Documents: collect document tokens by triggering each View File button
  on the rendered detail page (Angular click handlers fire
  /vdms/getDocMetadata/{uid}) and download the resulting files.
"""
from __future__ import annotations

import base64
import re
import time
from datetime import timezone, timedelta

from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import (
    SeleniumSession,
    SeleniumTimeout,
    generate_project_key,
    get_target_reg_nos,
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
    get_machine_context,
    merge_data_sections,
    normalize_project_payload,
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings

BASE_URL       = "https://gujrera.gujarat.gov.in"
VDMS_BASE      = f"{BASE_URL}/vdms/download"
VDMS_VIEW_DOC  = f"{BASE_URL}/vdms/view-doc"
DOMAIN         = "gujrera.gujarat.gov.in"
STATE          = "Gujarat"
# Warmup URL used to bootstrap the Angular SPA before issuing in-browser
# fetch() calls against the dashboard enumeration endpoint.
WARMUP_URL     = f"{BASE_URL}/#/home-p/registered-project"
# Bulk enumeration: returns every registered project with projectRegId + regNo.
BULK_LIST_PATH = "/dashboard/get-district-wise-projectlist/0/0/all/all/all"


# ── SeleniumSession wiring ────────────────────────────────────────────────────

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


def safe_get(url, *, logger=None, timeout=None, **_ignored):
    """Backwards-compatible shim — dispatches through the SeleniumSession."""
    plt = float(timeout) if isinstance(timeout, (int, float)) and timeout else None
    return _session().get(url, logger=logger, page_load_timeout=plt)


def download_response(url, *, logger=None, **_ignored):
    """Backwards-compatible shim — dispatches through the SeleniumSession."""
    return _session().download(url, logger=logger)


# HTML label (lowercase, colon-stripped) → schema field name.
# Fields prefixed with "_" are internal and assembled into compound fields below.
# Note: project_registration_no is intentionally NOT extracted from the detail
# page.  It is the single source of truth coming from the bulk enumeration API
# (see _fetch_listing_stubs), so any detail-page label variants are ignored to
# avoid inconsistency.
_LABEL_TO_FIELD: dict[str, str] = {
    "application no":               "acknowledgement_no",
    "application number":           "acknowledgement_no",
    "acknowledgement no":           "acknowledgement_no",
    "acknowledgement number":       "acknowledgement_no",
    "project name":                 "project_name",
    "name of project":              "project_name",
    "project type":                 "project_type",
    "type of project":              "project_type",
    "about property":               "project_description",
    "project status":               "status_of_the_project",
    "status":                       "status_of_the_project",
    "promoter name":                "promoter_name",
    "name of promoter":             "promoter_name",
    "mobile no":                    "_promoter_mobile",
    "mobile":                       "_promoter_mobile",
    "promoter mobile":              "_promoter_mobile",
    "email":                        "_promoter_email",
    "email id":                     "_promoter_email",
    "promoter email":               "_promoter_email",
    "promoter type":                "_promoter_type",
    "district":                     "project_city",
    "district name":                "project_city",
    "sub district":                 "_sub_district",
    "sub-district":                 "_sub_district",
    "taluka":                       "_taluka",
    "taluk":                        "_taluka",
    "village":                      "_village",
    "moje":                         "_village",
    "pin code":                     "project_pin_code",
    "pincode":                      "project_pin_code",
    "project address":              "_project_address",
    "address":                      "_project_address",
    "plot no":                      "_plot_no",
    "final plot no":                "_plot_no",
    "tp no":                        "_tp_no",
    "start date":                           "actual_commencement_date",
    "commencement date":                    "actual_commencement_date",
    "actual commencement date":             "actual_commencement_date",
    "project start date":                   "actual_commencement_date",
    "estimated start date":                 "estimated_commencement_date",
    "estimated commencement date":          "estimated_commencement_date",
    "proposed start date":                  "estimated_commencement_date",
    "completion date":                      "actual_finish_date",
    "actual completion date":               "actual_finish_date",
    "project end date":                     "actual_finish_date",
    "proposed completion date":             "estimated_finish_date",
    "estimated completion date":            "estimated_finish_date",
    "estimated end date":                   "estimated_finish_date",
    "proposed end date":                    "estimated_finish_date",
    "submission date":                      "submitted_date",
    "approved date":                        "approved_on_date",
    "approval date":                        "approved_on_date",
    "date of approval":                     "approved_on_date",
    # Gujarat RERA uses two specific labels for area:
    #   "Total Open Area"  → project land area (open ground footprint)
    #   "Total Covered Area" → building/construction footprint
    # Other labels (Project Land Area, Carpet Area, etc.) are intentionally ignored.
    "total open area":                      "land_area",
    "total covered area":                   "construction_area",
    "project estimated cost (rs.)":         "_total_project_cost",
    "office address":                       "_promoter_address",
    "total residential units":              "number_of_residential_units",
    "no of residential units":              "number_of_residential_units",
    "residential units":                    "number_of_residential_units",
    "total commercial units":               "number_of_commercial_units",
    "no of commercial units":               "number_of_commercial_units",
    "total project cost":                   "_total_project_cost",
    "estimated cost":                       "_estimated_cost",
    "cost of land":                         "_cost_of_land",
    "project description":                  "project_description",
    "description":                          "project_description",
    "total floor area under residential":   "total_floor_area_under_residential",
    "residential floor area":               "total_floor_area_under_residential",
    "total floor area under commercial":    "total_floor_area_under_commercial_or_other_uses",
    "commercial floor area":                "total_floor_area_under_commercial_or_other_uses",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _normalize_date(val) -> str | None:
    """Normalize date strings found in HTML to canonical ISO format."""
    if val is None:
        return None
    v = str(val).strip()
    if v in ("", "null", "None", "0"):
        return None
    m = re.match(r"^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})", v)
    if m:
        return f"{m.group(1)} {m.group(2)}+00:00"
    m = re.match(r"^(\d{2})-(\d{2})-(\d{4})$", v)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)} 00:00:00+00:00"
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", v)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)} 00:00:00+00:00"
    m = re.match(r"^(\d{4}-\d{2}-\d{2})$", v)
    if m:
        return f"{v} 00:00:00+00:00"
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", v):
        return v if "+" in v else v + "+00:00"
    return v


def _clean(text) -> str:
    """Strip and collapse whitespace."""
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()



def _extract_label_values(soup: BeautifulSoup) -> dict[str, str]:
    """Generic label→value extractor for Angular-rendered RERA detail pages.

    Primary pattern (gujrera.gujarat.gov.in #/project-preview):
      <td><strong>Label:-</strong> Value text</td>
    Also handles multiple <strong> tags per cell:
      <td><strong>L1:-</strong> V1 <br/> <strong>L2:-</strong> V2</td>

    Falls back to plain <td>Label</td><td>Value</td> for standard tables.
    """
    result: dict[str, str] = {}

    # Pattern 1 (primary): <td><strong>Label:-</strong> Value</td>
    # Iterates every <strong> in every <td>; collects sibling text until next <strong>.
    # This pattern runs FIRST so its results take priority over fallback patterns.
    for td in soup.find_all("td"):
        strongs = td.find_all("strong")
        if not strongs:
            continue
        for strong in strongs:
            raw_label = strong.get_text(strip=True)
            # Strip trailing ":-" or ":" from the label text
            label = re.sub(r"\s*:-?\s*$", "", raw_label).strip()
            if not label or len(label) > 120:
                continue
            # Collect text from siblings after this <strong> until the next <strong>
            value_parts: list[str] = []
            node = strong.next_sibling
            while node is not None:
                if getattr(node, "name", None) == "strong":
                    break
                if getattr(node, "name", None) == "br":
                    node = node.next_sibling
                    continue
                text = node.get_text(separator=" ") if hasattr(node, "get_text") else str(node)
                value_parts.append(text)
                node = node.next_sibling
            value = _clean(" ".join(value_parts)).strip(", ").strip()
            if value:
                result.setdefault(label, value)   # Pattern 1 wins; don't overwrite

    # Pattern 2 (fallback): <th>Label</th><td>Value</td> pairs inside a <tr>
    for tr in soup.find_all("tr"):
        ths = tr.find_all("th")
        tds = tr.find_all("td")
        if len(ths) == 1 and len(tds) == 1:
            key = _clean(ths[0].get_text(separator=" "))
            val = _clean(tds[0].get_text(separator=" "))
            if key and val and len(key) < 120:
                result.setdefault(key, val)

    # Pattern 3 (fallback): plain <td>Label</td><td>Value</td> rows without <strong>
    # Normalize label by stripping trailing ":-" to avoid duplicates with Pattern 1.
    for tr in soup.find_all("tr"):
        cells = [c for c in tr.find_all("td") if not c.find("strong")]
        i = 0
        while i < len(cells) - 1:
            label = re.sub(r"\s*:-?\s*$", "",
                           _clean(cells[i].get_text(separator=" "))).strip()
            value = _clean(cells[i + 1].get_text(separator=" "))
            if label and value and len(label) < 120:
                result.setdefault(label, value)
            i += 2

    # Pattern 4 (fallback): <label>Key</label> followed by a sibling element
    for label_tag in soup.find_all("label"):
        key = _clean(label_tag.get_text())
        if not key or len(key) > 120:
            continue
        sib = label_tag.find_next_sibling(["span", "strong", "div", "p"])
        if sib:
            val = _clean(sib.get_text(separator=" "))
            if val:
                result.setdefault(key, val)

    # Pattern 5 (fallback): Bootstrap col-* divs where a <strong>/<b> is the label
    # Non-destructive: collect sibling text after the <strong> instead of extracting it.
    for div in soup.find_all("div", class_=re.compile(r"\bcol-")):
        strong = div.find(["strong", "b"])
        if not strong:
            continue
        # Strip trailing ":-" to keep key consistent with Pattern 1
        key = re.sub(r"\s*:-?\s*$", "", _clean(strong.get_text())).strip()
        if not key or len(key) > 120:
            continue
        # Collect text from siblings AFTER the strong tag (non-destructive)
        val_parts: list[str] = []
        for node in strong.next_siblings:
            text = node.get_text(separator=" ") if hasattr(node, "get_text") else str(node)
            val_parts.append(text)
        val = _clean(" ".join(val_parts))
        if val:
            result.setdefault(key, val)

    return result


def _extract_html_fields(lv: dict[str, str], proj_id: int) -> dict:
    """Map a label-value dict (from the rendered HTML) to project schema fields."""
    out: dict = {}
    # Normalize keys: lowercase, strip trailing ":-" and trailing periods
    # (first-wins to match Pattern 1 priority)
    norm: dict[str, str] = {}
    for k, v in lv.items():
        nk = re.sub(r"\s*:-?\s*$", "", k.lower()).strip().rstrip(".")
        norm.setdefault(nk, v)

    for label_key, schema_field in _LABEL_TO_FIELD.items():
        val = norm.get(label_key, "")
        if not val or val.lower() in ("n/a", "na", "-", "null", "none"):
            continue
        if schema_field.startswith("_"):
            out[schema_field] = val          # assembled into compound fields below
        elif schema_field.endswith("_date"):
            d = _normalize_date(val)
            if d:
                out[schema_field] = d
        elif schema_field in (
            "land_area", "construction_area",
            "total_floor_area_under_residential",
            "total_floor_area_under_commercial_or_other_uses",
        ):
            try:
                out[schema_field] = float(re.sub(r"[^\d.]", "", val.replace(",", "")))
            except (ValueError, TypeError):
                pass
        elif schema_field in ("number_of_residential_units", "number_of_commercial_units"):
            try:
                out[schema_field] = int(val.replace(",", ""))
            except (ValueError, TypeError):
                pass
        else:
            out[schema_field] = val

    # Promoter contact — extract first valid phone/email from potentially multi-value strings
    def _first_match(text: str, pattern: str) -> str:
        m = re.search(pattern, text or "")
        return m.group(0).strip() if m else ""

    raw_phone = norm.get("promoter mobile") or norm.get("mobile no") or norm.get("mobile", "")
    raw_email = norm.get("promoter email") or norm.get("email id") or norm.get("email", "")
    # Extract first phone number and email from potentially concatenated multi-partner strings
    phone = _first_match(raw_phone, r"[\d]{10,}")
    email = _first_match(raw_email, r"[\w.+-]+@[\w.-]+\.[a-zA-Z]{2,}")
    contact: dict = {}
    if phone:
        contact["phone"] = phone                          # FIELD: promoter_contact_details.phone <- first 10+ digits from promoter mobile label
    if email:
        contact["email"] = email                          # FIELD: promoter_contact_details.email <- first email match from promoter email label
    if contact:
        out["promoter_contact_details"] = contact         # FIELD: promoter_contact_details <- assembled contact dict

    # project_location_raw
    district = norm.get("district") or norm.get("district name", "")
    sub_dist = norm.get("sub district") or norm.get("sub-district", "")
    taluka   = norm.get("taluka") or norm.get("taluk", "")
    village  = norm.get("village") or norm.get("moje", "")
    pin      = norm.get("pin code") or norm.get("pincode", "")
    address  = norm.get("project address") or norm.get("address", "")
    # Fallback: extract 6-digit pin from office/project address when not directly available
    if not pin:
        _addr_for_pin = norm.get("office address", "") or address
        _pin_m = re.search(r"\b(\d{6})\b", _addr_for_pin)
        if _pin_m:
            pin = _pin_m.group(1)
    plot_no  = norm.get("plot no") or norm.get("final plot no", "")
    tp_no    = norm.get("tp no", "")
    loc: dict = {}
    if plot_no:
        loc["house_no_building_name"] = plot_no           # FIELD: project_location_raw.house_no_building_name <- norm "plot no"/"final plot no"
    if tp_no:
        loc["tp_no"] = tp_no                              # FIELD: project_location_raw.tp_no <- norm "tp no"
    if village:
        loc["village"] = village                          # FIELD: project_location_raw.village <- norm "village"/"moje"
    if sub_dist:
        loc["taluk"] = sub_dist                           # FIELD: project_location_raw.taluk <- norm "sub district"/"sub-district"
    elif taluka:
        loc["taluk"] = taluka                             # FIELD: project_location_raw.taluk <- norm "taluka"/"taluk" (fallback)
    if district:
        loc["district"] = district                        # FIELD: project_location_raw.district <- norm "district"/"district name"
        out["project_city"] = district                    # FIELD: project_city <- norm "district"/"district name"
    if pin:
        loc["pin_code"] = pin                             # FIELD: project_location_raw.pin_code <- norm "pin code"/"pincode" or 6-digit from address
    loc["state"] = STATE                                  # FIELD: project_location_raw.state <- STATE constant ("Gujarat")
    # raw_address — base address string plus appended location components for full address.
    # Taluka takes priority over sub-district; state is uppercased to match reference format.
    if address:
        _taluk_part = sub_dist or taluka
        suffix_parts = [p for p in [_taluk_part, district, STATE.upper(), pin] if p]
        # FIELD: project_location_raw.raw_address <- address + taluk/district/state/pin suffix
        loc["raw_address"] = (
            address + ", " + ", ".join(suffix_parts) if suffix_parts else address
        )
    if loc:
        out["project_location_raw"] = loc                 # FIELD: project_location_raw <- assembled loc dict

    # project_cost_detail
    cost: dict = {}
    raw_cost = (
        norm.get("project estimated cost (rs.)")
        or norm.get("total project cost")
        or norm.get("project cost")
    )
    if raw_cost:
        # Strip commas and convert to float-compatible string
        cost_num = raw_cost.replace(",", "").split()[0]
        try:
            cost["total_project_cost"] = f"{float(cost_num):.2f}"                          # FIELD: project_cost_detail.total_project_cost <- norm cost label, float-formatted
            cost.setdefault("estimated_project_cost", cost["total_project_cost"])          # FIELD: project_cost_detail.estimated_project_cost <- mirrors total_project_cost
        except (ValueError, TypeError):
            cost["total_project_cost"] = raw_cost                                          # FIELD: project_cost_detail.total_project_cost <- raw cost string (float parse failed)
    if norm.get("estimated cost"):
        cost.setdefault("estimated_project_cost", norm["estimated cost"])                  # FIELD: project_cost_detail.estimated_project_cost <- norm "estimated cost"
    if norm.get("cost of land"):
        cost["cost_of_land"] = norm["cost of land"]                                        # FIELD: project_cost_detail.cost_of_land <- norm "cost of land"
    if cost:
        out["project_cost_detail"] = cost                                                  # FIELD: project_cost_detail <- assembled cost dict

    # promoters_details
    promo_type = norm.get("promoter type", "")
    if promo_type:
        # FIELD: promoters_details <- dict literal with type_of_firm
        # FIELD: promoters_details.type_of_firm <- norm "promoter type"
        out["promoters_details"] = {"type_of_firm": promo_type}

    # promoter_address_raw — from "Office Address" label in the promoter section
    office_addr = norm.get("office address", "")
    if office_addr:
        # FIELD: promoter_address_raw <- dict literal with raw_address
        # FIELD: promoter_address_raw.raw_address <- norm "office address"
        out["promoter_address_raw"] = {"raw_address": office_addr}

    # bank_details — from "Linked Bank Details" section
    bank: dict = {}
    bank_name = norm.get("bank name") or norm.get("bank")
    acct_no = (
        norm.get("a/c number") or norm.get("account no") or norm.get("account number")
        or norm.get("ac number") or norm.get("account name")
    )
    ifsc = norm.get("ifsc code") or norm.get("ifsc")
    branch = norm.get("branch name") or norm.get("branch")
    acct_type = norm.get("account type") or norm.get("type of account")
    if bank_name:
        bank["bank_name"] = bank_name                     # FIELD: bank_details.bank_name <- norm "bank name"/"bank"
    if acct_no:
        bank["account_no"] = acct_no                      # FIELD: bank_details.account_no <- norm "a/c number"/"account no"/variants
    if ifsc:
        bank["IFSC"] = ifsc                               # FIELD: bank_details.IFSC <- norm "ifsc code"/"ifsc"
    if branch:
        bank["branch"] = branch                           # FIELD: bank_details.branch <- norm "branch name"/"branch"
    if acct_type:
        bank["account_type"] = acct_type                  # FIELD: bank_details.account_type <- norm "account type"/"type of account"
    if bank:
        out["bank_details"] = bank                        # FIELD: bank_details <- assembled bank dict

    # land_area_details — derive from extracted area values + units from raw value strings
    # Unit is embedded in value strings like "1817.74 Sq Mtrs"; extract it with regex.
    def _split_num_unit(raw: str) -> tuple[float | None, str]:
        """Split '3654.26 Sq Mtrs' → (3654.26, 'Sq Mtrs')."""
        m = re.match(r"^([\d,]+\.?\d*)\s+(.+)$", raw.strip())
        if m:
            try:
                return float(m.group(1).replace(",", "")), m.group(2).strip()
            except ValueError:
                pass
        return None, ""

    land_area_val        = out.get("land_area")
    construction_area_val = out.get("construction_area")
    if land_area_val or construction_area_val:
        lad: dict = {}
        # Units come from the same raw value strings the numbers were extracted from
        _, land_unit         = _split_num_unit(norm.get("total open area", ""))
        _, construction_unit = _split_num_unit(norm.get("total covered area", ""))
        if land_area_val:
            # FIELD: land_area_details.land_area <- out["land_area"] as str (int when whole)
            lad["land_area"] = (
                str(int(land_area_val)) if land_area_val == int(land_area_val)
                else str(land_area_val)
            )
            lad["land_area_unit"] = land_unit or "Sq Mtrs"           # FIELD: land_area_details.land_area_unit <- unit from "total open area" raw
        if construction_area_val:
            lad["construction_area"] = construction_area_val         # FIELD: land_area_details.construction_area <- out["construction_area"]
            lad["construction_area_unit"] = construction_unit or "Sq Mtrs"  # FIELD: land_area_details.construction_area_unit <- unit from "total covered area" raw
        if lad:
            out["land_area_details"] = lad                           # FIELD: land_area_details <- assembled lad dict

    # Remove internal keys
    for k in [k for k in list(out) if k.startswith("_")]:
        del out[k]

    out["project_state"] = STATE                                     # FIELD: project_state <- STATE constant ("Gujarat")
    return out


def _extract_doc_links(soup: BeautifulSoup, seen: set[str] | None = None) -> list[dict]:
    """Collect document download links from the rendered Gujarat detail page HTML."""
    docs: list[dict] = []
    if seen is None:
        seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href in ("#", "javascript:void(0)"):
            continue
        if href.startswith("/"):
            href = f"{BASE_URL}{href}"
        elif not href.startswith("http"):
            continue
        href_lower = href.lower()
        if not any(x in href_lower for x in ("/vdms/view-doc", "/vdms/download", "download", "upload")):
            continue
        # Skip static/navigational PDFs (annual reports, presentations, news articles)
        if "/staticpage/" in href_lower or "/resources/staticpage" in href_lower:
            continue
        if href in seen:
            continue
        seen.add(href)
        label = _clean(a.get_text(separator=" ")) or a.get("title", "")
        if not label or label.lower() in ("download", "view", "click here"):
            parent = a.find_parent(["td", "div", "li"])
            label = (_clean(parent.get_text(separator=" "))[:80] if parent else "") or "document"
        docs.append({"label": label, "url": href})
    return docs


def _parse_flat_table(soup: BeautifulSoup) -> list[dict] | None:
    """Parse the Flat Details table on the Gujarat RERA project detail page.

    The table shows per-block aggregate rows with columns like:
    Flat Type | Block | Total Area | Booked Units ... | Available Units ...

    Returns a list of building_details entries (one per block/flat-type row).
    """
    # Column header detection signals (all checked as substrings, lowercase)
    _FLAT_HEADER_SIGNALS  = {"flat type", "type of flat", "unit type", "usage"}
    _BLOCK_HEADER_SIGNALS = {"block name", "block", "tower", "wing"}
    _AREA_HEADER_SIGNALS  = {"carpet area", "total area", "area (sq", "area(sq"}
    _OPEN_HEADER_SIGNALS  = {"balcony", "open area", "terrace", "veranda"}
    _UNIT_NO_SIGNALS      = {"flat/ bungalow", "office no", "plot no", "unit no",
                             "flat no", "bungalow no"}
    _UNITS_HEADER_SIGNALS = {"total units", "no of units", "no. of units", "booked", "inventory"}

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        # Detect header row (prefer <th> cells, fall back to first <td> row)
        header_cells = rows[0].find_all("th") or rows[0].find_all("td")
        if not header_cells:
            continue
        headers = [_clean(c.get_text(separator=" ")).lower() for c in header_cells]
        hset = set(headers)

        has_flat  = any(any(s in h for s in _FLAT_HEADER_SIGNALS)  for h in hset)
        has_block = any(any(s in h for s in _BLOCK_HEADER_SIGNALS) for h in hset)
        has_area  = any(any(s in h for s in _AREA_HEADER_SIGNALS)  for h in hset)

        if not (has_flat or (has_block and has_area)):
            continue  # not a Flat Details / unit inventory table

        results: list[dict] = []
        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells:
                continue
            vals = [_clean(c.get_text(separator=" ")) for c in cells]
            entry: dict = {}
            for header, val in zip(headers, vals):
                if not val or val in ("-", "N/A", "NA"):
                    continue
                if any(s in header for s in _FLAT_HEADER_SIGNALS):
                    entry["flat_type"] = val
                elif any(s in header for s in _BLOCK_HEADER_SIGNALS):
                    entry["block_name"] = val
                elif any(s in header for s in _UNIT_NO_SIGNALS):
                    entry["_unit_no"] = val          # temp — used to build flat_name
                elif any(s in header for s in _AREA_HEADER_SIGNALS):
                    entry.setdefault("carpet_area", val)
                elif any(s in header for s in _OPEN_HEADER_SIGNALS):
                    entry.setdefault("open_area", val)
                elif any(s in header for s in _UNITS_HEADER_SIGNALS):
                    entry.setdefault("no_of_units", val)

            # flat_name = just the unit number (block info is already in block_name)
            unit_no = entry.pop("_unit_no", None)
            if unit_no:
                entry["flat_name"] = unit_no

            if entry.get("block_name") or entry.get("flat_name"):
                results.append(entry)

        if results:
            return results

    return None


def _parse_overview_card(soup: BeautifulSoup) -> dict:
    """Parse the project overview card that uses the pattern:
        <p>Label (Unit) <br/><strong>Value</strong></p>
    Returns a dict of extracted fields.
    """
    out: dict = {}
    for p_tag in soup.find_all("p"):
        strong = p_tag.find("strong")
        if not strong:
            continue
        # The label is the text content of <p> BEFORE the <br/> or the <strong>
        label_parts = []
        for node in p_tag.children:
            if getattr(node, "name", None) in ("br", "strong"):
                break
            text = node.get_text() if hasattr(node, "get_text") else str(node)
            label_parts.append(text)
        label = _clean(" ".join(label_parts)).lower()
        value = _clean(strong.get_text())
        if not label or not value:
            continue
        # Remove parenthetical unit suffixes from the label, e.g. "(Sq Mtrs)"
        label = re.sub(r"\s*\([^)]*\)\s*$", "", label).strip()

        if "project status" in label or label == "status":
            out.setdefault("status_of_the_project", value)   # FIELD: status_of_the_project <- overview card <p> with "project status"/"status" label
    return out


def _parse_avbox_person(avbox) -> dict:
    """Extract Name, Email Id, Mobile (and optionally Reg No., photo) from an avBox div.

    Preserves a single leading space on name/email/phone values (matching API-source format).
    """
    person: dict = {}
    img = avbox.find("img", src=True)
    if img and img.get("src", ""):
        src = img["src"]
        # Make relative URLs absolute
        if src.startswith("assets/"):
            src = f"{BASE_URL}/{src}"
        if src:
            person["photo"] = src
    for p_tag in avbox.find_all("p"):
        strong = p_tag.find("strong")
        if not strong:
            continue
        key = _clean(strong.get_text()).lower().rstrip(":")
        # Preserve leading space to match original API-sourced values (rstrip only)
        raw_val = p_tag.get_text().replace(strong.get_text(), "").rstrip()
        val = raw_val if raw_val.strip() else ""
        if not val:
            continue
        if key == "name":
            person["name"] = val
        elif key in ("email id", "email"):
            person["email"] = val
        elif key in ("mobile", "contact"):
            person["phone"] = val
        elif key in ("reg no.", "reg no", "registration no"):
            person["registration_no"] = val
    return person


def _parse_promoter_card(soup: BeautifulSoup) -> dict:
    """Parse the Promoter Details card (div.promoDetails) for contact, address, promoters_details."""
    out: dict = {}
    promo_div = soup.find("div", class_="promoDetails")
    if not promo_div:
        return out
    contact: dict = {}
    addr_parts = []
    promo_name = ""
    promo_type = ""
    for p_tag in promo_div.find_all("p"):
        strong = p_tag.find("strong")
        if not strong:
            continue
        key = _clean(strong.get_text()).lower().rstrip(":").rstrip()
        span = p_tag.find("span")
        val = _clean(span.get_text()) if span else _clean(
            p_tag.get_text().replace(strong.get_text(), ""))
        if not val:
            continue
        if key == "contact":
            contact["phone"] = val                         # FIELD: promoter_contact_details.phone <- promoter card <p> "Contact" value
        elif key == "email id":
            contact["email"] = val                         # FIELD: promoter_contact_details.email <- promoter card <p> "Email Id" value
        elif key == "address":
            addr_parts.append(val)
        elif key == "promoter type":
            promo_type = val
        elif key == "promoter name":
            promo_name = val
    if contact:
        out["promoter_contact_details"] = contact          # FIELD: promoter_contact_details <- assembled contact dict from promoter card
    if addr_parts:
        # FIELD: promoter_address_raw <- dict literal with raw_address
        # FIELD: promoter_address_raw.raw_address <- joined promoter card "Address" <p> values
        out["promoter_address_raw"] = {"raw_address": " ".join(addr_parts)}
    # Build promoters_details from card: include name and photo if available
    promo_details: dict = {}
    if promo_name:
        promo_details["name"] = promo_name                 # FIELD: promoters_details.name <- promoter card <p> "Promoter Name" value
    # Photo is in a sibling div (col-sm-6 col-md-6 user)
    user_div = promo_div.find_parent("div")
    if user_div:
        img = user_div.find("img", src=True)
        if img and img.get("src", ""):
            src = img["src"]
            if src.startswith("assets/"):
                src = f"{BASE_URL}/{src}"
            promo_details["photo"] = src                   # FIELD: promoters_details.photo <- <img src> in promoter card's parent div
    if promo_type:
        promo_details["type_of_firm"] = promo_type         # FIELD: promoters_details.type_of_firm <- promoter card <p> "Promoter Type" value
    if promo_details:
        out["promoters_details"] = promo_details           # FIELD: promoters_details <- assembled promo_details dict from promoter card
    return out


def _parse_partners(soup: BeautifulSoup) -> dict:
    """Parse co_promoter_details and authorised_signatory_details from assoVenderBox."""
    co_promoters: list[dict] = []
    signatories: list[dict] = []

    for asso_box in soup.find_all("div", class_="assoVenderBox"):
        # Each col within the box has an h2 title (Partners / Signatory Details)
        for col in asso_box.find_all("div", class_=re.compile(r"\bcol-")):
            h2 = col.find("h2")
            if not h2:
                continue
            section_title = _clean(h2.get_text()).lower()
            people = [_parse_avbox_person(ab) for ab in col.find_all("div", class_="avBox")]
            people = [p for p in people if p.get("name")]
            if "partner" in section_title:
                # co-promoters: strip photo field (matches sample format)
                co_promoters.extend(
                    {k: v for k, v in p.items() if k != "photo"} for p in people
                )
            elif "signatory" in section_title:
                # signatories keep photo but reorder: name, email, phone, photo
                for p in people:
                    ordered: dict = {}
                    if "name" in p:  ordered["name"] = p["name"]
                    if "email" in p: ordered["email"] = p["email"]
                    if "phone" in p: ordered["phone"] = p["phone"]
                    if "photo" in p: ordered["photo"] = p["photo"]
                    signatories.append(ordered)

    out: dict = {}
    if co_promoters:
        out["co_promoter_details"] = co_promoters          # FIELD: co_promoter_details <- avBox persons under "Partners" h2 (photo stripped)
    if signatories:
        out["authorised_signatory_details"] = signatories  # FIELD: authorised_signatory_details <- avBox persons under "Signatory" h2 (reordered)
    return out


def _parse_professionals(soup: BeautifulSoup) -> dict:
    """Parse Project Professionals (Architects, Engineers, etc.) from assoVenderBox."""
    professionals: list[dict] = []
    for asso_box in soup.find_all("div", class_="assoVenderBox"):
        h2 = asso_box.find("h2")
        if not h2 or "professional" not in h2.get_text().lower():
            continue
        # Each avCol contains an avTitle (type) and avBox entries (people)
        for av_col in asso_box.find_all("div", class_="avCol"):
            title_tag = av_col.find(["h3", "h4"], class_="avTitle")
            if not title_tag:
                continue
            prof_type = _clean(title_tag.get_text())
            found_any = False
            for avbox in av_col.find_all("div", class_="avBox"):
                if avbox.find("b"):  # "Data Not Available" marker
                    continue
                person = _parse_avbox_person(avbox)
                if person.get("name"):
                    found_any = True
                    # Order matches sample: name, type, email, phone, registration_no
                    ordered: dict = {}
                    ordered["name"] = person.get("name", "")
                    ordered["type"] = prof_type
                    if "email" in person: ordered["email"] = person["email"]
                    if "phone" in person: ordered["phone"] = person["phone"]
                    if "registration_no" in person: ordered["registration_no"] = person["registration_no"]
                    professionals.append(ordered)
            if not found_any:
                # Record type-only entry for sections with no data (matches sample format)
                professionals.append({"type": prof_type})
    if professionals:
        # FIELD: professional_information <- avBox persons under "Professional" assoVenderBox h2
        return {"professional_information": professionals}
    return {}


def _parse_facilities(soup: BeautifulSoup) -> dict:
    """Parse the Common Amenities section for provided_faciltiy list."""
    facilities: list[dict] = []
    ca_box = soup.find("div", class_="caBox")
    if not ca_box:
        return {}
    for ca_col in ca_box.find_all("div", class_="caCol"):
        img_div = ca_col.find("div", class_=re.compile(r"\bimg\b"))
        text_div = ca_col.find("div", class_="text")
        if not img_div or not text_div:
            continue
        # get_text() without separator preserves double-space from <br/> between words,
        # which matches the sample format (e.g. "Disposal of  sewage water").
        name = text_div.get_text().strip()
        if not name:
            continue
        # "img-disabled" CSS class = Not Available; absence = Available
        classes = img_div.get("class", [])
        status = "Not Available" if "img-disabled" in classes else "Available"
        # Field order matches sample: status first, then facility
        facilities.append({"status": status, "facility": name})
    if facilities:
        # FIELD: provided_faciltiy <- caCol entries under Common Amenities caBox div
        return {"provided_faciltiy": facilities}
    return {}


def _fetch_document_tokens(page) -> list[dict]:
    """Extract document links from the rendered Gujarat RERA project detail page.

    The Angular SPA renders ``<app-file-view>`` components for every document slot.
    Each component's "View File" button (``a.dwnldBtn``) fires a click handler that
    calls ``/vdms/getDocMetadata/{uid}`` before opening the document.  We intercept
    those requests to extract the UID, then build the public URL
    ``/vdms/view-doc/{uid}``.

    Strategy (pure page interaction — no separate REST API calls):
      1. Click every expansion button to unhide all document tab sections.
      2. For each ``a.dwnldBtn`` in DOM order:
           - Resolve the document label by walking up the DOM to the nearest
             ``<label>``, ``<h6>``, or ``.text`` container.
           - JS-dispatch a click event on the button.
           - Wait up to 600 ms for a ``/vdms/getDocMetadata/`` request; if one
             arrives the path suffix is the UID.  No request → document not uploaded.
      3. Skip slots where the UID is absent, "0", or already seen.
    """
    docs: list[dict] = []
    seen_uids: set[str] = set()

    try:
        # Step 1: expand all collapsible document sections
        page.evaluate(
            "() => { document.querySelectorAll('button').forEach(b => b.click()); }"
        )
        page.wait_for_timeout(2000)

        # Step 2: collect (button_index, label) pairs from the DOM
        btn_labels: list[tuple[int, str]] = []
        raw = page.evaluate(
            """() => {
                const results = [];
                const allBtns = Array.from(document.querySelectorAll('a.dwnldBtn'));
                allBtns.forEach((btn, idx) => {
                    let label = '';
                    let cur = btn.parentElement;
                    for (let d = 0; d < 8; d++) {
                        if (!cur) break;
                        const lbl = cur.querySelector(':scope > label');
                        if (lbl) { label = lbl.innerText.trim(); break; }
                        const h6 = cur.querySelector(':scope > h6');
                        if (h6) { label = h6.innerText.trim(); break; }
                        const txt = cur.querySelector(':scope > .text');
                        if (txt) { label = txt.innerText.trim(); break; }
                        const p = cur.querySelector(':scope > p');
                        if (p) {
                            const t = p.innerText.trim();
                            if (t.length > 2 && t.length < 120) { label = t; break; }
                        }
                        cur = cur.parentElement;
                    }
                    results.push({idx, label: label.replace(/\\s+/g, ' ').trim()});
                });
                return results;
            }"""
        )
        btn_labels = [(entry["idx"], entry["label"]) for entry in raw if entry["label"]]

        # Step 3: click each labelled button and capture the UID from the request
        for btn_idx, label in btn_labels:
            last_uid: list[str] = []

            def _on_req(req: object, _u: list = last_uid) -> None:
                url = req.url  # type: ignore[attr-defined]
                if "/vdms/getDocMetadata/" in url:
                    _u.append(url.split("/vdms/getDocMetadata/")[-1])

            page.on("request", _on_req)
            try:
                page.evaluate(
                    f"""() => {{
                        const btns = document.querySelectorAll('a.dwnldBtn');
                        const btn = btns[{btn_idx}];
                        if (btn) btn.dispatchEvent(
                            new MouseEvent('click', {{bubbles: true, cancelable: true}}));
                    }}"""
                )
                page.wait_for_timeout(600)
            finally:
                page.remove_listener("request", _on_req)

            uid = last_uid[0] if last_uid else None
            if not uid or uid == "0" or uid in seen_uids:
                continue
            seen_uids.add(uid)
            # FIELD: uploaded_documents[].label <- DOM-walked label near a.dwnldBtn
            # FIELD: uploaded_documents[].url <- VDMS_VIEW_DOC + captured uid
            docs.append({"label": label, "url": f"{VDMS_VIEW_DOC}/{uid}"})

    except Exception:
        pass

    return docs


def _fetch_listing_stubs(page, logger: CrawlerLogger) -> list[dict]:
    """Enumerate every registered project via the bulk dashboard endpoint.

    Issues a single in-browser fetch() against
    /dashboard/get-district-wise-projectlist/0/0/all/all/all — the dashboard
    "Grand Total" link uses this same endpoint to render the full registered-
    project table.  Each row carries projectRegId (the numeric identifier
    used to compute the base64-encoded detail-page URL) and regNo (the full
    PR/GJ/... registration number), so the project key can be derived before
    any detail page is loaded.

    Returns a list of dicts sorted ascending by proj_id with fields:
      proj_id, project_registration_no, project_name, project_type,
      promoter_name, project_city, approved_on_date, estimated_finish_date.
    """
    bulk_url = f"{BASE_URL}{BULK_LIST_PATH}"

    nav_attempts = 3
    for attempt in range(1, nav_attempts + 1):
        try:
            page.goto(WARMUP_URL, timeout=60_000, wait_until="networkidle")
            page.wait_for_timeout(2_500)
            break
        except Exception as nav_err:
            msg = str(nav_err).lower()
            if attempt < nav_attempts and ("net::" in msg or "timeout" in msg):
                logger.warning(
                    f"Warmup navigation failed (attempt {attempt}/{nav_attempts},"
                    f" retrying in 15 s): {nav_err}",
                    url=WARMUP_URL,
                )
                import time as _time
                _time.sleep(15)
            else:
                raise

    try:
        result = page.evaluate(
            """async (url) => {
                const resp = await fetch(url);
                if (!resp.ok) throw new Error('HTTP ' + resp.status);
                return await resp.json();
            }""",
            bulk_url,
        )
    except Exception as e:
        logger.error(f"Bulk enumeration fetch failed: {e}", url=bulk_url)
        return []

    api_status = result.get("status") or result.get("sataus")
    if api_status not in (200, "200"):
        logger.warning(f"Bulk enumeration returned unexpected status: {api_status}")

    rows = result.get("data") or []
    if not isinstance(rows, list):
        logger.error(f"Bulk enumeration payload not a list: {type(rows).__name__}")
        return []

    stubs_by_id: dict[int, dict] = {}
    for item in rows:
        proj_id_raw = item.get("projectRegId")
        if proj_id_raw is None:
            continue
        try:
            proj_id = int(proj_id_raw)
        except (TypeError, ValueError):
            continue
        if proj_id in stubs_by_id:
            continue
        reg_no = _clean(item.get("regNo") or "") or None
        stubs_by_id[proj_id] = {
            "proj_id":                  proj_id,                                            # FIELD: proj_id <- bulk API item["projectRegId"]
            "project_registration_no":  reg_no,                                             # FIELD: project_registration_no <- bulk API item["regNo"]
            "project_name":             _clean(item.get("projectName") or "") or None,     # FIELD: project_name <- bulk API item["projectName"]
            "project_type":             _clean(item.get("projectType") or "") or None,     # FIELD: project_type <- bulk API item["projectType"]
            "promoter_name":            _clean(item.get("promoterName") or "") or None,    # FIELD: promoter_name <- bulk API item["promoterName"]
            "project_city":             _clean(item.get("districtName") or "") or None,    # FIELD: project_city <- bulk API item["districtName"]
            "approved_on_date":         _normalize_date(item.get("approvedOn") or ""),     # FIELD: approved_on_date <- bulk API item["approvedOn"]
            "estimated_finish_date":    _normalize_date(item.get("endDate") or ""),        # FIELD: estimated_finish_date <- bulk API item["endDate"]
        }

    logger.info(f"Bulk enumeration: {len(stubs_by_id)} unique projects discovered")
    return sorted(stubs_by_id.values(), key=lambda x: x["proj_id"])

















def _browser_fetch_bytes(page: object, url: str) -> bytes | None:
    """Download *url* using the Selenium browser context.

    The VDMS server (``vdms/view-doc/{uid}``) performs TLS fingerprinting and
    resets connections from Python's httpx SSL stack.  Chromium's TLS handshake
    is accepted, so we use ``page.evaluate(fetch(...))`` to download and base64-
    encode the content inside the browser, then decode it in Python.
    """
    import base64 as _b64

    js = f"""async () => {{
        try {{
            const r = await fetch('{url}');
            if (!r.ok) return null;
            const buf = await r.arrayBuffer();
            const bytes = new Uint8Array(buf);
            // Chunk to avoid spread-operator stack overflow on large files
            let b64 = '';
            const chunk = 8192;
            for (let i = 0; i < bytes.length; i += chunk) {{
                b64 += String.fromCharCode(...bytes.subarray(i, i + chunk));
            }}
            return btoa(b64);
        }} catch (e) {{
            return null;
        }}
    }}"""
    try:
        b64_str = page.evaluate(js)  # type: ignore[attr-defined]
        if not b64_str:
            return None
        return _b64.b64decode(b64_str)
    except Exception:
        return None


def _handle_document(
    project_key: str, doc: dict, run_id: int,
    site_id: str, logger: CrawlerLogger,
    client=None,
    page: object | None = None,
) -> dict | None:
    """Download *doc* and upload it to S3.

    Tries browser-based download first (required because the VDMS endpoint
    rejects plain httpx connections via TLS fingerprinting), then falls back
    to a direct HTTP GET via *client*.
    """
    url   = doc.get("url")
    label = doc.get("label", "document")
    if not url:
        return None
    filename = build_document_filename(doc)
    try:
        content: bytes | None = None

        # Primary: download inside the Selenium browser (VDMS requires Chromium TLS)
        if page is not None:
            content = _browser_fetch_bytes(page, url)
            if content and (len(content) < 100 or content[:5] in (b"<html", b"<!DOC")):
                content = None

        # Fallback: plain httpx GET (works when VDMS allows non-browser access)
        if content is None:
            resp = download_response(url, retries=2, timeout=20, client=client)
            content = resp.content if resp else None

        if not content or len(content) < 100:
            return None
        if content[:5] in (b"<html", b"<!DOC"):
            return None

        md5    = compute_md5(content)
        s3_key = upload_document(project_key, filename, content, dry_run=settings.DRY_RUN_S3)
        if s3_key is None:
            return None
        s3_url = get_s3_url(s3_key)
        upsert_document(
            project_key=project_key, document_type=label,
            original_url=document_identity_url(doc) or url,
            s3_key=s3_key, s3_bucket=settings.S3_BUCKET_NAME,
            file_name=filename, md5_checksum=md5, file_size_bytes=len(content),
        )
        logger.info("Document handled", label=label, s3_key=s3_key, step="documents")
        logger.log_document(label, url, "uploaded", s3_key=s3_key, file_size_bytes=len(content))
        return document_result_entry(doc, s3_url, filename)
    except Exception as e:
        logger.error(f"Document failed: {e}", url=url)
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(e),
                           project_key=project_key, url=url)
        return None


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Gujarat RERA.
    Full-flow check: navigates to the sentinel project's detail page via Selenium,
    runs ALL parsers that run() uses (_extract_html_fields, _parse_overview_card,
    _parse_promoter_card, _parse_partners, _parse_professionals, _parse_facilities,
    _parse_flat_table, _extract_doc_links), merges results, and verifies ≥ 80%
    field coverage against the full baseline.
    """
    import json as _json
    import os as _os
    from core.sentinel_utils import check_field_coverage

    sentinel_reg = config.get("sentinel_registration_no", "")
    sentinel_proj_id = config.get("sentinel_project_id")
    if not sentinel_reg and not sentinel_proj_id:
        logger.warning("No sentinel configured — skipping", step="sentinel")
        return True

    sample_path = _os.path.join(
        _os.path.dirname(_os.path.dirname(__file__)),
        "state_projects_sample", "gujarat.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check",
                       path=sample_path, step="sentinel")
        return True

    proj_id = sentinel_proj_id or int(baseline.get("sentinel_project_id", 0))
    if not proj_id:
        logger.warning("Sentinel: no sentinel_project_id available — skipping", step="sentinel")
        return True

    encoded_id = base64.b64encode(str(proj_id).encode()).decode()
    detail_url = f"{BASE_URL}/#/project-preview?id={encoded_id}"

    logger.info(f"Sentinel: navigating to detail page for proj_id={proj_id}",
                url=detail_url, step="sentinel")
    try:
        page = page_adapter(_session())
        page.goto(detail_url, timeout=60_000, wait_until="networkidle")
        page.wait_for_timeout(5_000)
        html  = page.content()
        # Fetch document tokens while the page context is still alive
        _sentinel_doc_links = _fetch_document_tokens(page)

        soup = BeautifulSoup(html, "lxml")
        lv   = _extract_label_values(soup)
        if not lv:
            logger.error("Sentinel: no label-value pairs found — site structure may have changed",
                         url=detail_url, step="sentinel")
            return False

        # Call ALL parsers that run() uses (same merge logic as run())
        fresh = _extract_html_fields(lv, proj_id)
        for extra in (
            _parse_overview_card(soup),
            _parse_promoter_card(soup),
            _parse_partners(soup),
            _parse_professionals(soup),
            _parse_facilities(soup),
        ):
            for k, v in extra.items():
                fresh[k] = v  # card-section data wins over lv-derived fields

        # building_details + number_of_residential_units from flat table
        flat_entries = _parse_flat_table(soup)
        if flat_entries:
            fresh["building_details"] = flat_entries
            if not fresh.get("number_of_residential_units"):
                res_count = sum(
                    1 for e in flat_entries
                    if e.get("flat_type", "").lower() not in ("commercial", "office", "shop")
                )
                if res_count:
                    fresh["number_of_residential_units"] = res_count

        # uploaded_documents (doc links fetched from APIs; sentinel records metadata only)
        if _sentinel_doc_links:
            fresh["uploaded_documents"] = _sentinel_doc_links

    except Exception as exc:
        exc_str = str(exc)
        # Selenium timeout errors contain "Timeout" in the message; treat them
        # as transient network issues and skip the sentinel rather than aborting.
        if "timeout" in exc_str.lower() or "net::" in exc_str.lower():
            logger.warning(
                f"Sentinel: Selenium error (likely transient) — {exc}; "
                "skipping coverage check this run",
                step="sentinel",
            )
            return True
        logger.error(f"Sentinel: error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", url=detail_url, step="sentinel")
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "gujarat_rera"),
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


def _run(config: dict, run_id: int, mode: str) -> dict:  # noqa: C901
    """Main entry point — listing-page stub collection + Selenium detail scraping."""
    logger   = CrawlerLogger(config["id"], run_id)
    site_id  = config["id"]
    counts   = dict(projects_found=0, projects_new=0, projects_updated=0,
                    projects_skipped=0, documents_uploaded=0, error_count=0)
    item_limit   = settings.CRAWL_ITEM_LIMIT or 0
    machine_name, machine_ip = get_machine_context()
    t_run = time.monotonic()

    # Document downloads now share the module-level SeleniumSession; the
    # original ``session`` httpx client was used purely for legacy-TLS
    # compatibility, which Chrome handles natively via Selenium.
    session = None

    # ── Targeted run handling ──────────────────────────────────────────────────
    # --target-reg-no restricts the run to one or more specific projects
    # (comma-separated, case-insensitive). The bulk enumeration stub list is
    # filtered down to the requested registration number(s) below and the sentinel
    # health check is skipped (mirrors karnataka_rera / uttarakhand_rera).
    target_regs = get_target_reg_nos()

    # ── Sentinel health check ────────────────────────────────────────────────
    if target_regs:
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

    checkpoint     = load_checkpoint(site_id, mode) or {}
    resume_proj_id = int(checkpoint.get("last_page", 0))
    logger.info(
        "Starting Gujarat RERA crawl (listing stub collection + detail scrape mode)",
        resume_proj_id=resume_proj_id or "start",
        item_limit=item_limit or None,
    )

    items_processed = 0

    if True:
        page = page_adapter(_session())

        # ── Phase 1: bulk enumeration ─────────────────────────────────────────
        # Single call to the dashboard's grand-total endpoint returns every
        # registered project with projectRegId (detail-page ID) and regNo
        # (full registration number).  This is the single source of truth for
        # the registration number; the detail page is never consulted for it.
        t0 = time.monotonic()
        all_stubs = _fetch_listing_stubs(page, logger)
        if not all_stubs:
            logger.error("No project stubs returned from enumeration — aborting")
            return counts

        # ── Targeted filtering ─────────────────────────────────────────────────
        # Restrict the enumeration to the requested registration number(s).
        if target_regs:
            matched_regs: set[str] = set()
            all_stubs = [
                s for s in all_stubs
                if (s.get("project_registration_no") or "").strip().upper() in target_regs
            ]
            matched_regs.update(
                (s.get("project_registration_no") or "").strip().upper() for s in all_stubs
            )
            for missing in sorted(target_regs - matched_regs):
                logger.warning(f"Target reg_no={missing!r} not found in listing", step="listing")
            logger.info(
                f"Targeted run — {len(matched_regs)} of {len(target_regs)} requested "
                f"project(s) matched", step="listing",
            )

        logger.info(f"Total projects to process: {len(all_stubs)}")
        logger.timing("search", time.monotonic() - t0, rows=len(all_stubs))
        counts["projects_found"] = len(all_stubs)
        update_crawl_run_progress(run_id, counts)

        # ── Phase 2: scrape each detail page ──────────────────────────────────
        for stub in all_stubs:
            proj_id  = stub["proj_id"]

            if item_limit and items_processed >= item_limit:
                logger.info(f"Item limit {item_limit} reached — stopping")
                break
            if proj_id <= resume_proj_id:
                continue

            # Single-source reg_no: comes from the bulk enumeration stub.  If it
            # is missing, the project cannot be keyed and is skipped silently
            # (no counter touched) — there is intentionally no fallback path.
            reg_no = (stub.get("project_registration_no") or "").strip()
            if not reg_no:
                logger.warning(
                    f"No registration number in enumeration stub for proj_id={proj_id} — skipping",
                    step="reg_no",
                )
                continue

            # Count every project toward the limit BEFORE skip checks so daily_light
            # (which skips every already-DB project) still honors CRAWL_ITEM_LIMIT.
            items_processed += 1

            # Use the project-preview URL (base64-encoded ID) which renders full HTML
            encoded_id = base64.b64encode(str(proj_id).encode()).decode()
            detail_url = f"{BASE_URL}/#/project-preview?id={encoded_id}"
            key = generate_project_key(reg_no)
            logger.set_project(key=key, reg_no=reg_no, url=detail_url, page=proj_id)

            # daily_light skip happens BEFORE page.goto: the reg_no is already
            # known from the bulk stub, so there is no need to load the SPA
            # detail page just to discover it.
            if mode == "daily_light" and get_project_by_key(key):
                logger.info("Skipping — already in DB (daily_light)", step="skip")
                counts["projects_skipped"] += 1
                logger.clear_project()
                continue

            logger.info(f"Scraping detail page for project ID {proj_id}", url=detail_url)

            try:
                page.goto(detail_url, timeout=30_000, wait_until="networkidle")
                page.wait_for_timeout(5_000)
                html = page.content()
            except Exception as e:
                logger.warning(f"Detail page load failed for proj_id={proj_id}: {e}")
                counts["error_count"] += 1
                continue

            soup = BeautifulSoup(html, "lxml")
            lv   = _extract_label_values(soup)
            if not lv:
                logger.warning(f"No label-value pairs found for proj_id={proj_id} — skipping")
                continue

            try:
                data = _extract_html_fields(lv, proj_id)
                # reg_no is single-sourced from the enumeration stub above; it
                # is NOT re-extracted from the detail page.  Stamp it onto data
                # so downstream normalization sees a consistent value.
                data["project_registration_no"] = reg_no   # FIELD: project_registration_no <- bulk enumeration stub reg_no (single source of truth)

                # Enrich with fields from additional page sections (higher priority — overrides lv)
                overview   = _parse_overview_card(soup)
                promoter_c = _parse_promoter_card(soup)
                partners   = _parse_partners(soup)
                profs      = _parse_professionals(soup)
                facils     = _parse_facilities(soup)
                for extra in (overview, promoter_c, partners, profs, facils):
                    for k, v in extra.items():
                        data[k] = v   # card-section data wins over lv-derived fields

                # Rebuild land_area_details in sample field order: land_area first, then construction
                land_area = data.get("land_area")
                construction_area = data.get("construction_area")
                if land_area or construction_area:
                    lad_old: dict = data.get("land_area_details") or {}
                    lad: dict = {}
                    if land_area:
                        # FIELD: land_area_details.land_area <- data["land_area"] as str (int when whole)
                        lad["land_area"] = (
                            str(int(land_area)) if land_area == int(land_area)
                            else str(land_area)
                        )
                        lad["land_area_unit"] = lad_old.get("land_area_unit", "Sq Mtrs")               # FIELD: land_area_details.land_area_unit <- prior land_area_details unit (fallback "Sq Mtrs")
                    if construction_area:
                        lad["construction_area"] = construction_area                                   # FIELD: land_area_details.construction_area <- data["construction_area"]
                        lad["construction_area_unit"] = lad_old.get("construction_area_unit", "Sq Mtrs")  # FIELD: land_area_details.construction_area_unit <- prior unit (fallback "Sq Mtrs")
                    if lad:
                        data["land_area_details"] = lad                                                # FIELD: land_area_details <- rebuilt lad dict (sample field order)

                # building_details — parse the Flat Details table for per-block entries
                flat_entries = _parse_flat_table(soup)
                if flat_entries:
                    data["building_details"] = flat_entries                          # FIELD: building_details <- _parse_flat_table(soup) per-block entries

                    # Derive unit counts from the per-unit inventory if not already set
                    if not data.get("number_of_residential_units"):
                        res_count = sum(
                            1 for e in flat_entries
                            if e.get("flat_type", "").lower() not in ("commercial", "office", "shop")
                        )
                        if res_count:
                            data["number_of_residential_units"] = res_count          # FIELD: number_of_residential_units <- count of non-commercial flat_entries
                    if not data.get("number_of_commercial_units"):
                        com_count = sum(
                            1 for e in flat_entries
                            if e.get("flat_type", "").lower() in ("commercial", "office", "shop")
                        )
                        if com_count:
                            data["number_of_commercial_units"] = com_count           # FIELD: number_of_commercial_units <- count of commercial/office/shop flat_entries


                data.update({
                    # FIELD: key <- generate_project_key(reg_no)
                    # FIELD: state <- config["state"]
                    "key": key, "state": config["state"],
                    # FIELD: project_state <- STATE constant ("Gujarat")
                    # FIELD: domain <- DOMAIN constant ("gujrera.gujarat.gov.in")
                    "project_state": STATE, "domain": DOMAIN,
                    # FIELD: config_id <- config["config_id"]
                    # FIELD: url <- base64-encoded project-preview detail URL
                    "config_id": config["config_id"], "url": detail_url,
                    # FIELD: is_live <- literal True
                    # FIELD: machine_name <- get_machine_context() hostname
                    "is_live": True, "machine_name": machine_name,
                    "crawl_machine_ip": machine_ip,                                  # FIELD: crawl_machine_ip <- get_machine_context() IP
                    "project_registration_no": reg_no,                               # FIELD: project_registration_no <- bulk enumeration stub reg_no
                })
                # Build extra fields for the data sub-dict so downstream consumers
                # can read them without parsing nested schema structures.
                _loc = data.get("project_location_raw") or {}
                _lad = data.get("land_area_details") or {}
                _act_start = (data.get("actual_commencement_date") or "").replace("+00:00", "").strip()
                _extra_data: dict = {
                    # FIELD: data.govt_type <- literal "state"
                    # FIELD: data.is_processed <- literal False
                    "govt_type": "state", "is_processed": False,
                    # FIELD: data.proj_reg_id <- enumeration stub proj_id
                    # FIELD: data.project_id <- base64-encoded proj_id
                    "proj_reg_id": proj_id, "project_id": encoded_id,
                    "detail_url": detail_url,                                        # FIELD: data.detail_url <- base64-encoded project-preview detail URL
                }
                _n_units = (
                    (data.get("number_of_residential_units") or 0)
                    + (data.get("number_of_commercial_units") or 0)
                )
                if _n_units:
                    _extra_data["no_of_units"] = str(_n_units)                       # FIELD: data.no_of_units <- residential + commercial unit counts (str)
                if _loc.get("raw_address"):
                    _extra_data["raw_address"] = _loc["raw_address"]                 # FIELD: data.raw_address <- project_location_raw.raw_address
                if data.get("project_type"):
                    _extra_data["type_of_units"] = data["project_type"]              # FIELD: data.type_of_units <- data["project_type"]
                # Use extracted units or fall back to known Gujarat RERA defaults
                _extra_data["land_area_units"] = _lad.get("land_area_unit") or "Sq Mtrs"          # FIELD: data.land_area_units <- land_area_details.land_area_unit (fallback "Sq Mtrs")
                if _act_start:
                    _extra_data["actual_start_date"] = _act_start                    # FIELD: data.actual_start_date <- actual_commencement_date without +00:00 suffix
                _extra_data["construction_units"] = _lad.get("construction_area_unit") or "in Sq. Mts."  # FIELD: data.construction_units <- land_area_details.construction_area_unit (fallback "in Sq. Mts.")
                # FIELD: data <- merged _extra_data + {source, label_values}
                # FIELD: data.source <- literal "html_scrape"
                # FIELD: data.label_values <- _extract_label_values(soup) raw lv dict
                data["data"] = merge_data_sections(
                    _extra_data,
                    {"source": "html_scrape", "label_values": lv},
                )

                logger.info("Normalizing", step="normalize")
                try:
                    normalized = normalize_project_payload(
                        data, config, machine_name=machine_name, machine_ip=machine_ip)
                    record  = ProjectRecord(**normalized)
                    db_dict = record.to_db_dict()
                except (ValidationError, ValueError) as e:
                    logger.warning("Validation failed — raw fallback", error=str(e))
                    insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(e),
                                       project_key=key, url=detail_url, raw_data=data)
                    counts["error_count"] += 1
                    db_dict = normalize_project_payload(
                        {**data, "data": {"validation_fallback": True, "raw": data.get("data")}},
                        config, machine_name=machine_name, machine_ip=machine_ip,
                    )

                action = upsert_project(db_dict)
                if action == "new": counts["projects_new"] += 1
                else:               counts["projects_updated"] += 1
                logger.info(f"DB: {action}", step="db_upsert")

                # Extract document links by clicking each View File button on the
                # rendered page (Angular click handlers fire /vdms/getDocMetadata/{uid})
                doc_links = _fetch_document_tokens(page)
                if doc_links:
                    logger.info(f"Processing {len(doc_links)} documents", step="documents")
                    uploaded_docs: list[dict] = []
                    doc_name_counts: dict[str, int] = {}
                    for doc in doc_links:
                        selected = select_document_for_download(
                            config["state"], doc, doc_name_counts, domain=DOMAIN)
                        if selected:
                            result = _handle_document(key, selected, run_id, site_id, logger, session, page=page)
                            uploaded_docs.append(result or {"link": doc.get("url"), "type": doc.get("label", "document")})
                            if result:
                                counts["documents_uploaded"] += 1
                        else:
                            uploaded_docs.append({"link": doc.get("url"), "type": doc.get("label", "document")})
                    if uploaded_docs:
                        upsert_project({
                            # FIELD: key <- db_dict["key"]
                            # FIELD: url <- db_dict["url"]
                            "key": db_dict["key"], "url": db_dict["url"],
                            # FIELD: state <- db_dict["state"]
                            # FIELD: domain <- db_dict["domain"]
                            "state": db_dict["state"], "domain": db_dict["domain"],
                            "project_registration_no": db_dict["project_registration_no"],   # FIELD: project_registration_no <- db_dict["project_registration_no"]
                            "uploaded_documents": uploaded_docs,                             # FIELD: uploaded_documents <- per-doc upload result entries
                            "document_urls": build_document_urls(uploaded_docs),             # FIELD: document_urls <- build_document_urls(uploaded_docs)
                        })

                save_checkpoint(site_id, mode, proj_id, key, run_id)
                random_delay(*config.get("rate_limit_delay", (1, 2)))

            except Exception as exc:
                logger.exception("Project processing failed", exc, step="project_loop", proj_id=proj_id)
                insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                                   project_key=key, url=detail_url)
                counts["error_count"] += 1
            finally:
                logger.clear_project()
                update_crawl_run_progress(run_id, counts)

    reset_checkpoint(site_id, mode)
    logger.info(f"Gujarat RERA complete: {counts}")
    logger.timing("total_run", time.monotonic() - t_run)
    return counts
