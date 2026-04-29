"""
Kerala RERA Crawler — rera.kerala.gov.in
Type: static/api hybrid

Strategy:
- Paginate explore-projects (80 pages × 20 cards) to collect /projects/{id} URLs
- For each project: fetch /projects/{id} HTML to extract all fields + document links
- Documents: /signed-certificate/{id}, QPR link, Complete Project Details link
"""
from __future__ import annotations

import os
import re
import tempfile
from datetime import timezone
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import PlaywrightSession, generate_project_key, random_delay, safe_get
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
    parse_datetime,
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings

BASE_URL = "https://rera.kerala.gov.in"
EXPLORE_URL = f"{BASE_URL}/explore-projects"
STATE_CODE = "KL"
DOMAIN = "rera.kerala.gov.in"
LEGACY_DOMAIN = "reraonline.kerala.gov.in"
LEGACY_CONFIG_ID = 14521
_LEGACY_SKIP_DOC_LABELS = {"complete_project_details", "quarterly_progress_report"}
_PLAYWRIGHT_PAGE_TIMEOUT_MS = 60_000
_PLAYWRIGHT_DOWNLOAD_START_TIMEOUT_MS = 120_000


# ── Listing pagination via explore-projects ───────────────────────────────────

def _get_explore_page(page_num: int, logger: CrawlerLogger) -> BeautifulSoup | None:
    resp = safe_get(EXPLORE_URL, params={"page": page_num}, logger=logger)
    if not resp:
        return None
    return BeautifulSoup(resp.text, "lxml")


def _get_total_pages(soup: BeautifulSoup) -> int:
    try:
        nav = soup.select("nav a")
        numbers = [int(a.get_text(strip=True)) for a in nav if a.get_text(strip=True).isdigit()]
        return max(numbers) if numbers else 1
    except Exception:
        return 1


def _parse_explore_cards(soup: BeautifulSoup) -> list[dict]:
    """Extract project ID, name, and cert number from explore-projects cards."""
    results = []
    for a in soup.find_all("a", href=True):
        m = re.match(r"/projects/(\d+)", a["href"])
        if not m:
            continue
        project_id = m.group(1)
        detail_url = f"{BASE_URL}/projects/{project_id}"
        card_text = a.get_text(separator="|", strip=True)

        # Project name from img alt attribute inside the card
        img = a.find("img", alt=True)
        project_name = img["alt"].strip() if img and img.get("alt") else None
        if not project_name:
            h1 = a.find("h1")
            project_name = h1.get_text(strip=True) if h1 else None

        # Cert number from card text
        cert_match = re.search(r"K-RERA/PRJ/[A-Z]+/\d+/\d+", card_text)
        results.append({
            "project_id": project_id,
            "detail_url": detail_url,
            "project_name": project_name,
            "cert_no_from_card": cert_match.group(0) if cert_match else None,
        })
    # Deduplicate by project_id
    seen: set[str] = set()
    unique = []
    for r in results:
        if r["project_id"] not in seen:
            seen.add(r["project_id"])
            unique.append(r)
    return unique


# ── PrintPreview parser (reraonline.kerala.gov.in) ────────────────────────────
# Philosophy: extract EVERYTHING first, map to schema second, store raw always.

# Comprehensive label → schema field mapping.
# Every known label variation maps to its schema column.
_LABEL_TO_FIELD: dict[str, str] = {
    # Core identity
    "certificate no":                                                    "project_registration_no",
    "project registration no":                                           "project_registration_no",
    "project name":                                                      "project_name",
    "promoter name":                                                     "promoter_name",
    "name of the organization":                                          "promoter_name",
    "project type":                                                      "project_type",
    "project status":                                                    "status_of_the_project",
    "work status":                                                       "status_of_the_project",
    # Dates
    "proposed date of completion":                                       "estimated_finish_date",
    "proposed date of commencement (for new projects)":                  "estimated_commencement_date",
    "proposed date of commencement":                                     "estimated_commencement_date",
    "last modified by promoter":                                         "last_modified",
    "date of registration":                                              "approved_on_date",
    "date of submission":                                                "submitted_date",
    # Units
    "total building units":                                              "number_of_residential_units",
    "number of residential units (as per sanctioned plan)":              "number_of_residential_units",
    "number of commercial units (as per sanctioned plan)":               "number_of_commercial_units",
    "total building unit sold":                                          "_units_sold",
    # Areas
    "total land area (for the entire project) (in sqmts)":              "land_area",
    "project land area":                                                 "land_area",
    "total floor area of the project proposed for registration (as mentioned in the building permit) (in sqmts)": "construction_area",
    "total floor area under residential use (as mentioned in the building permit) (in sqmts)": "total_floor_area_under_residential",
    "total floor area under commercial or other uses (as mentioned in the building permit) (in sqmts)": "total_floor_area_under_commercial_or_other_uses",
    "total building count (as per sanctioned plan)":                     "_building_count",
    # Location
    "pin code":                                                          "project_pin_code",
    "district":                                                          "project_city",
    "state":                                                             "project_state",
    # Promoter
    "do you have any past experience ?":                                 "_has_past_experience",
    "gst number":                                                        "_promoter_gst",
    "type of organization":                                              "_promoter_org_type",
}


def _label_value_from_el(lbl) -> tuple[str, str]:
    """Extract (key, value) from a single <label> element.

    Three structural patterns seen on Kerala RERA:
    1. <label>Key :<span>Value</span></label>     → span child carries the value
    2. <label>Key :</label>Value (text node)       → value is a sibling/parent text node
    3. <label>Key :Value</label>                   → value is inline in the label text (no span)
    """
    child_span = lbl.find("span")
    if child_span:
        # Pattern 1: value in child span
        key = lbl.get_text(strip=True).replace(child_span.get_text(strip=True), "").rstrip(": ").strip()
        val = child_span.get_text(strip=True)
        return key, val

    raw = lbl.get_text(strip=True)
    parent = lbl.parent
    parent_text = parent.get_text(separator=" ", strip=True) if parent else ""

    # Pattern 3: value is embedded inline — "Key :Value"
    # Detect by checking whether anything follows the first colon inside the label
    if ":" in raw:
        first_colon = raw.index(":")
        possible_key = raw[:first_colon].strip()
        inline_val   = raw[first_colon + 1:].strip()
        sibling_val = parent_text[len(raw):].lstrip(": ").strip() if parent_text.startswith(raw) else ""
        if sibling_val:
            return raw.strip(), sibling_val
        if inline_val and possible_key:
            return possible_key, inline_val

    # Pattern 2: value lives outside the label as a text node of its parent
    key = raw.rstrip(": ").strip()
    val = parent_text[len(raw):].lstrip(": ").strip()
    return key, val


# Panels whose labels are table-cell values, not key→value pairs — skip them
_SKIP_LABEL_PANELS = {
    "Building Details",
    "Common Areas and Facilities",
    "Member Information",   # values live in the table, labels are column headers
}


def _extract_all_labels(soup: BeautifulSoup) -> dict[str, str]:
    """
    Exhaustively extract every label→value pair on the page.
    Skips panels whose labels are table cell values (not real key→value pairs).
    First non-empty value wins for duplicate keys — callers that need
    ordered/grouped extraction should use _extract_panel_labels_ordered().
    """
    result: dict[str, str] = {}
    for panel in soup.find_all("div", class_=lambda c: c and "panel-default" in c):
        h2 = panel.find("h2", class_=lambda c: c and "panel-title" in c)
        section = h2.get_text(strip=True) if h2 else ""
        if section in _SKIP_LABEL_PANELS:
            continue
        for lbl in panel.find_all("label"):
            key, val = _label_value_from_el(lbl)
            if key and len(key) <= 200:
                if key not in result or (not result[key] and val):
                    result[key] = val
    return result


def _extract_panel_labels_ordered(panel) -> list[tuple[str, str]]:
    """
    Return ALL (key, value) pairs from a panel in document order,
    preserving duplicates. Used for sections like Promoter's Information
    where the same address field names repeat for registered vs communication address.
    """
    pairs = []
    for lbl in panel.find_all("label"):
        key, val = _label_value_from_el(lbl)
        if key and len(key) <= 200:
            pairs.append((key, val))
    return pairs


def _extract_all_tables(soup: BeautifulSoup) -> list[dict]:
    """
    Extract every <table> on the page as a list of row dicts.
    Returns list of {section, headers, rows} dicts.
    """
    tables_out = []
    for panel in soup.find_all("div", class_=lambda c: c and "panel-default" in c):
        h = panel.find(["h2", "h3"], class_=lambda c: c and "panel-title" in c)
        section = h.get_text(strip=True) if h else ""
        for tbl in panel.find_all("table"):
            rows = tbl.find_all("tr")
            if not rows:
                continue
            headers = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
            data_rows = []
            for row in rows[1:]:
                cells = row.find_all("td")
                if not cells:
                    continue
                # Skip rows that are pure section headers (colspan=max)
                if len(cells) == 1 and cells[0].get("colspan"):
                    continue
                row_dict = {}
                for i, cell in enumerate(cells):
                    col_name = headers[i] if i < len(headers) else f"col_{i}"
                    # Capture links inside cells — skip JavaScript pseudo-links (e.g. href="javascript:();")
                    # that are JS modal triggers and carry no real URL.
                    links = [
                        a["href"] for a in cell.find_all("a", href=True)
                        if not a["href"].lower().startswith("javascript")
                    ]
                    btn_ids = [re.sub(r"^btnShow_", "", btn["id"])
                               for btn in cell.find_all("button", id=re.compile(r"^btnShow_"))]
                    row_dict[col_name] = cell.get_text(strip=True)
                    if links:
                        row_dict[f"{col_name}__links"] = links
                    if btn_ids:
                        row_dict[f"{col_name}__file_ids"] = btn_ids
                data_rows.append(row_dict)
            if data_rows:
                tables_out.append({"section": section, "headers": headers, "rows": data_rows})
    return tables_out


def _extract_doc_buttons(soup: BeautifulSoup, base_url: str) -> list[dict]:
    """
    Extract all document buttons (btnShow_*, btnDownloadFile_*) from the page.
    Constructs the direct document URL from the button ID.
    The JS function uses: /Preview/GetDocument?ID={file_id}
    """
    docs = []
    seen_ids: set[str] = set()
    base_domain = re.match(r"https?://[^/]+", base_url)
    domain = base_domain.group(0) if base_domain else "https://reraonline.kerala.gov.in"

    for panel in soup.find_all("div", class_=lambda c: c and "panel-default" in c):
        h = panel.find(["h2", "h3"], class_=lambda c: c and "panel-title" in c)
        section = h.get_text(strip=True) if h else ""

        tbl = panel.find("table")
        if not tbl:
            continue
        rows = tbl.find_all("tr")
        headers = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])] if rows else []

        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells:
                continue
            doc_name = cells[0].get_text(strip=True) if cells else ""
            if not doc_name or doc_name == section:
                continue
            # Find the paired view/download buttons. The download button is the
            # most reliable way to retrieve the file because it executes the
            # same browser-side flow as a real user.
            show_btn = row.find(["button", "a", "input"], id=re.compile(r"^btnShow_"))
            if not show_btn:
                continue
            file_id = show_btn["id"].replace("btnShow_", "")
            download_btn = row.find(["button", "a", "input"], id=re.compile(r"^btnDownloadFile_"))
            if file_id in seen_ids:
                continue
            seen_ids.add(file_id)
            # Get uploaded date if present
            date_val = cells[3].get_text(strip=True) if len(cells) > 3 else None
            remarks  = cells[2].get_text(strip=True) if len(cells) > 2 else None
            doc_url  = f"{domain}/Preview/GetDocument?ID={file_id}"
            docs.append({
                "label":       doc_name,
                "url":         doc_url,
                "file_id":     file_id,
                "show_button_id": show_btn.get("id"),
                "download_button_id": download_btn.get("id") if download_btn else None,
                "print_preview_url": base_url,
                "identity_url": doc_url,
                "section":     section,
                "remarks":     remarks,
                "upload_date": date_val,
            })
    return docs


def _parse_print_preview(url: str, logger: CrawlerLogger) -> dict:
    """
    Exhaustively parse the PrintPreview page.
    Step 1 — Extract ALL labels and ALL tables from every panel, no filtering.
    Step 2 — Map to schema fields via _LABEL_TO_FIELD lookup table.
    Step 3 — Store full raw extraction in 'data' JSONB as safety net.
    Nothing on the page is ever silently dropped.
    """
    soup = None
    all_labels: dict[str, str] = {}
    all_tables: list[dict] = []
    all_doc_btns: list[dict] = []
    for attempt in range(1, 4):
        resp = safe_get(url, logger=logger, timeout=45.0)
        if not resp or resp.status_code != 200:
            logger.warning("PrintPreview fetch failed", url=url, attempt=attempt)
            continue
        soup = BeautifulSoup(resp.text, "lxml")
        all_labels = _extract_all_labels(soup)
        all_tables = _extract_all_tables(soup)
        all_doc_btns = _extract_doc_buttons(soup, url)
        if len(all_labels) >= 10 or all_tables:
            break
        logger.warning(
            "PrintPreview payload looked incomplete; retrying",
            url=url,
            attempt=attempt,
            label_count=len(all_labels),
            table_count=len(all_tables),
        )

    if soup is None:
        return {}

    out: dict = {}

    # ── Step 1b: Ordered per-panel extraction for sections with duplicate keys ──
    # The Promoter's Information panel repeats the same address field names for
    # registered address and communication address. A flat dict would lose one.
    # We extract them in order and split at the first repeated key.
    _ADDRESS_FIELDS = {
        "House Number/ Building Name", "Street Name", "Locality", "Landmark",
        "State/ UT", "District", "Taluk", "Panchayat/ Municipality/ Corporation", "Pin Code",
    }
    for panel in soup.find_all("div", class_=lambda c: c and "panel-default" in c):
        # Support h2 or h3 panel titles (new site versions may use h3)
        heading = panel.find(["h2", "h3"], class_=lambda c: c and "panel-title" in c)
        section = heading.get_text(strip=True) if heading else ""
        if "promoter" not in section.lower():
            continue
        ordered = _extract_panel_labels_ordered(panel)
        if not ordered:
            continue

        org_info: dict[str, str] = {}
        registered: dict[str, str] = {}
        communication: dict[str, str] = {}
        in_registered = False
        in_communication = False

        for key, val in ordered:
            if key in _ADDRESS_FIELDS:
                if not in_registered:
                    in_registered = True
                elif key in registered:
                    # Second occurrence of an address field — we're in the communication block
                    in_communication = True
                    in_registered = False
                if in_communication:
                    communication[key] = val
                else:
                    registered[key] = val
            else:
                org_info[key] = val

        promoter_addr: dict = {}
        if org_info:
            promoter_addr["org_info"] = org_info
            # Surface the most important org fields directly
            if "Name of the Organization" in org_info:
                all_labels["Name of the Organization"] = org_info["Name of the Organization"]
            if "Organization Type" in org_info:
                all_labels["Organization Type"] = org_info["Organization Type"]

            # Extract promoter contact details (phone / email) from org_info
            _PHONE_LABELS = {
                "mobile number", "mobile no", "mobile no.", "mobile",
                "phone", "phone no", "phone number", "telephone", "contact number",
                "mob no", "mob no.",
            }
            _EMAIL_LABELS = {
                "email", "e-mail", "email id", "email address", "email id.",
            }
            contact_phone = None
            contact_email = None
            for k, v in org_info.items():
                kl = k.lower().strip()
                if kl in _PHONE_LABELS and v and not contact_phone:
                    contact_phone = v
                elif kl in _EMAIL_LABELS and v and not contact_email:
                    contact_email = v
            if contact_phone or contact_email:
                contact_entry = {k: v for k, v in
                                 {"phone": contact_phone, "email": contact_email}.items() if v}
                if not out.get("promoter_contact_details"):
                    out["promoter_contact_details"] = [contact_entry]

        if registered:
            promoter_addr["registered_address"] = registered
        if communication:
            promoter_addr["communication_address"] = communication
        if promoter_addr:
            out["promoter_address_raw"] = promoter_addr

    # ── Step 2: Map labels → schema fields via lookup ─────────────────────────
    for raw_key, raw_val in all_labels.items():
        schema_field = _LABEL_TO_FIELD.get(raw_key.lower().strip())
        if schema_field and raw_val:
            if not out.get(schema_field):        # first non-empty value wins
                out[schema_field] = raw_val

    # Coerce numeric area fields
    for f in ("land_area", "construction_area",
              "total_floor_area_under_residential",
              "total_floor_area_under_commercial_or_other_uses"):
        if out.get(f):
            out[f] = _extract_number(str(out[f]))

    # ── Step 3: Derived JSONB fields from all_labels ──────────────────────────
    # Map Kerala PrintPreview label names → normalizer-compatible (lowercase) keys
    _LOCATION_KEY_MAP = {
        "Survey/ Resurvey Number(s)": "survey_resurvey_number",
        "Patta No:/ Thandapper Details": "plot_no",
        "State": "state",
        "District": "district",
        "Taluk": "taluk",
        "Village": "village",
        "Street": "raw_address",
        "Locality": "locality",
        "Pin Code": "pin_code",
        "Boundaries East": "boundaries_east",
        "Boundaries West": "boundaries_west",
        "Boundaries North": "boundaries_north",
        "Boundaries South": "boundaries_south",
    }
    _LAND_KEYS = {k for k in all_labels
                  if any(w in k for w in ("Land Area", "Floor Area", "Units", "Building Count"))}

    loc = {_LOCATION_KEY_MAP[k]: v for k, v in all_labels.items()
           if k in _LOCATION_KEY_MAP and v}
    if loc:
        out["project_location_raw"] = loc
        out["project_city"]    = loc.get("district", out.get("project_city"))
        out["project_pin_code"] = loc.get("pin_code", out.get("project_pin_code"))

    land = {k: v for k, v in all_labels.items() if k in _LAND_KEYS and v}
    if land:
        out["land_detail"] = land

    # promoter_address_raw is set in Step 1b above via ordered per-panel extraction.
    # Fallback: if step 1b found no usable registered/communication address
    # (e.g. panel says "No Records Found", site uses h3, or markup changed),
    # build a proxy address from the project location labels.
    _existing_paddr = out.get("promoter_address_raw")
    _has_reg_addr = (
        isinstance(_existing_paddr, dict)
        and bool(_existing_paddr.get("registered_address") or _existing_paddr.get("communication_address"))
    )
    if not _has_reg_addr:
        _fb_addr = {k: v for k, v in {
            "State/ UT": all_labels.get("State/ UT") or all_labels.get("State"),
            "Taluk":     all_labels.get("Taluk"),
            "District":  all_labels.get("District"),
            "Locality":  all_labels.get("Locality"),
            "Pin Code":  all_labels.get("Pin Code"),
            "House Number/ Building Name": all_labels.get("House Number/ Building Name"),
        }.items() if v not in (None, "", "NA")}
        if _fb_addr:
            out["promoter_address_raw"] = {"registered_address": _fb_addr}

    # Fallback: if step 1b found no contact details, scan all_labels for phone/email.
    if "promoter_contact_details" not in out:
        _phone_keys = {"mobile number", "mobile no", "mobile no.", "mobile", "phone",
                       "phone no", "phone number", "telephone", "contact number", "mob no",
                       "primary contact no", "contact no", "contact no."}
        _email_keys = {"email", "e-mail", "email id", "email address", "email id."}
        _fb_phone = next((v for k, v in all_labels.items()
                          if k.lower().strip() in _phone_keys and v), None)
        _fb_email = next((v for k, v in all_labels.items()
                          if k.lower().strip() in _email_keys and v), None)
        if _fb_phone or _fb_email:
            out["promoter_contact_details"] = [
                {k: v for k, v in {"phone": _fb_phone, "email": _fb_email}.items() if v}
            ]

    financier = all_labels.get("Name of the Financier (If any)", "")
    if financier and financier.strip():
        out["bank_details"] = {
            "financier_name":    financier,
            "financier_address": all_labels.get("Address of the Financier", ""),
        }

    # ── Step 4: Map tables → schema JSONB fields ──────────────────────────────
    building_details: dict = {}

    for tbl in all_tables:
        sec   = tbl["section"].lower()
        rows  = tbl["rows"]
        hdrs  = " ".join(tbl["headers"]).lower()
        if not rows:
            continue

        if "member" in sec:
            out["members_details"] = rows
            # Match both UK ("authorised") and US ("authorized") spelling
            auth = next(
                (r for r in rows
                 if any(w in r.get("Designation", "").lower()
                        for w in ("authoris", "authoriz"))),
                None,
            )
            if auth:
                out["authorised_signatory_details"] = auth

        elif "land owner" in sec:
            out["co_promoter_details"] = rows

        elif "past experience" in sec:
            out["development_agreement_detail"] = rows
            out["past_experience_of_promoter"]  = len(rows)

        elif "professional" in sec:
            # Map Kerala PrintPreview column names → normalizer-compatible keys
            _PROF_COL_MAP = {
                "professional name": "name",
                "name": "name",
                "professional type": "role",
                "type of professional": "type",
                "rera certificate no.": "registration_no",
                "rera certificate no": "registration_no",
                "registration number": "registration_no",
                "registration no.": "registration_no",
                "name of the firm": "name",
                "address of the firm": "address",
                "key projects completed": "key_real_estate_projects",
                "pan no": "pan_no",
                "pan no.": "pan_no",
                "email": "email",
                "mobile": "mobile",
            }
            mapped_rows = []
            for r in rows:
                mapped = {_PROF_COL_MAP.get(k.lower().strip(), k): v
                          for k, v in r.items() if v}
                if mapped:
                    mapped_rows.append(mapped)
            out["professional_information"] = mapped_rows or rows

        elif "litigation" in sec:
            out["complaints_litigation_details"] = {"rows": rows}

        elif "common area" in sec or "facilit" in sec:
            facility_dict: dict = out.get("provided_faciltiy") or {}
            name_col = tbl["headers"][0] if tbl["headers"] else "col_0"
            val_col  = tbl["headers"][1] if len(tbl["headers"]) > 1 else "col_1"
            pct_col  = next((h for h in tbl["headers"]
                             if "percent" in h.lower() or "progress" in h.lower()), "col_2")
            details_col = next((h for h in tbl["headers"] if "detail" in h.lower() or "remark" in h.lower()), None)
            for r in rows:
                fname = r.get(name_col, "").rstrip(": ").strip()
                fval  = r.get(val_col, "")
                fpct  = r.get(pct_col, "")
                fdetails = r.get(details_col, "") if details_col else ""
                if fname:
                    entry = {"proposed": fval, "completion_pct": fpct}
                    if fdetails and fdetails != "NA":
                        entry["details"] = fdetails
                    facility_dict[fname] = entry
            if facility_dict:
                out["provided_faciltiy"] = facility_dict

        elif "bank" in sec or "separate bank" in sec:
            out["bank_details"] = {r.get("col_0", ""): r.get("col_1", "") for r in rows}

        elif "building" in sec or "permit" in sec:
            if "task" in hdrs or "activity" in hdrs or "percentage of work" in hdrs:
                out["construction_progress"] = rows
            elif "unit type" in hdrs or "carpet" in hdrs or "super built" in hdrs:
                building_details["unit_types"] = rows
            elif "parking" in hdrs:
                building_details["parking_details"] = rows
            else:
                building_details.setdefault("structure", []).extend(rows)

        elif "uploaded document" in sec or "supporting document" in sec:
            # Row-level metadata; actual URLs come from doc buttons
            out.setdefault("uploaded_documents", rows)

    if building_details:
        out["building_details"] = building_details

    # ── Step 5: Document buttons → downloadable URL list ─────────────────────
    if all_doc_btns:
        out["_print_preview_docs"] = all_doc_btns
        out["uploaded_documents"] = [
            {"label": d["label"], "url": d["url"],
             "file_id": d.get("file_id"),
             "show_button_id": d.get("show_button_id"),
             "download_button_id": d.get("download_button_id"),
             "print_preview_url": d.get("print_preview_url"),
             "identity_url": d.get("identity_url"),
             "section": d["section"], "upload_date": d.get("upload_date"),
             "remarks": d.get("remarks")}
            for d in all_doc_btns
        ]

    # ── Step 6: Raw safety net — everything stored in data JSONB ─────────────
    out["data"] = {
        "all_labels":       all_labels,
        "all_tables":       [{"section": t["section"], "headers": t["headers"],
                              "row_count": len(t["rows"]),
                              "first_row": t["rows"][0] if t["rows"] else {}}
                             for t in all_tables],
        "doc_button_count": len(all_doc_btns),
        "source_url":       url,
    }

    return {k: v for k, v in out.items() if v is not None and v != "" and v != {} and v != []}


def _extract_number(text: str) -> float | None:
    """Extract first numeric value from a string like '2233.00 Sqmts'."""
    if not text:
        return None
    m = re.search(r"[\d,]+\.?\d*", text.replace(",", ""))
    return float(m.group(0)) if m else None


def _legacy_view_file_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    file_id = next(
        (
            values[0]
            for key in ("ID", "id", "DOC_ID", "doc_id")
            for values in [query.get(key)]
            if values and values[0]
        ),
        None,
    )
    if file_id and "Preview/GetDocument" in url:
        return f"{BASE_URL}/view-file/{file_id}"
    return url


def _legacy_doc_identity(doc: dict[str, Any]) -> str | None:
    link = _legacy_view_file_url(document_identity_url(doc) or doc.get("link") or doc.get("url"))
    if link:
        return link
    label = doc.get("label") or doc.get("type")
    return str(label).strip().lower() if label else None


def _compact_kerala_members(rows: Any) -> list[dict[str, Any]] | None:
    if not isinstance(rows, list):
        return None
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = row.get("Member Name") or row.get("name")
        position = row.get("Designation") or row.get("position")
        if name or position:
            entry = {}
            if name:
                entry["name"] = str(name).strip()
            if position:
                entry["position"] = str(position).strip()
            out.append(entry)
    return out or None


def _compact_kerala_co_promoters(rows: Any) -> list[dict[str, Any]] | None:
    if not isinstance(rows, list):
        return None
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        entry = {
            "name": row.get("Land owner Name") or row.get("name"),
            "office_no": row.get("Office Number (With area code)") or row.get("office_no"),
            "owner_type": row.get("Type of Land Owner") or row.get("owner_type"),
            "agreement_type": row.get("Type of Agreement/ Arrangement") or row.get("agreement_type"),
        }
        entry = {k: str(v).strip() for k, v in entry.items() if v not in (None, "", "NA")}
        if entry:
            out.append(entry)
    return out or None


def _compact_kerala_professionals(rows: Any) -> list[dict[str, Any]] | None:
    if not isinstance(rows, list):
        return None
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        entry = {
            "name": row.get("Professional Name") or row.get("name"),
            "role": row.get("Professional Type") or row.get("role"),
            "address": row.get("Address of the firm") or row.get("address"),
            "registration_no": row.get("Registration Number") or row.get("registration_no"),
            "key_real_estate_projects": row.get("Key projects completed") or row.get("key_real_estate_projects"),
        }
        entry = {k: str(v).strip() for k, v in entry.items() if v not in (None, "", "NA")}
        if entry:
            out.append(entry)
    return out or None


def _legacy_utc_timestamp(value: Any) -> str | None:
    parsed = parse_datetime(value)
    if not parsed:
        return None
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")


def _legacy_promoter_address(record: dict[str, Any], labels: dict[str, Any]) -> dict[str, Any] | None:
    promoter_addr = record.get("promoter_address_raw")
    if isinstance(promoter_addr, dict):
        communication = promoter_addr.get("communication_address")
        registered = promoter_addr.get("registered_address")
        if isinstance(communication, dict) and communication:
            return communication
        if isinstance(registered, dict) and registered:
            return registered

    fallback = {
        "State/ UT": labels.get("State/ UT") or labels.get("State"),
        "Taluk": labels.get("Taluk"),
        "District": labels.get("District"),
        "Locality": labels.get("Locality"),
        "Pin Code": labels.get("Pin Code"),
        "House Number/ Building Name": labels.get("House Number/ Building Name"),
    }
    fallback = {k: v for k, v in fallback.items() if v not in (None, "", "NA")}
    return fallback or None


def _build_kerala_legacy_facility_progress(facilities: Any) -> list[dict[str, Any]] | None:
    if not isinstance(facilities, dict):
        return None
    preferred_order = [
        "Water conservation, Rain water harvesting",
        "Gymnasium",
        "Open parking",
        "Water supply",
        "Electrical meter room, Sub-station, Receiving station",
        "Internal Roads & Footpaths",
        "Security",
        "Solar systems",
        "Swimming pool",
        "Fire protection and Fire safety requirements",
        "Party Hall",
        "Security cameras",
        "Sewerage (Chamber, Lines, Septic tank, STP)",
        "Storm water drains",
        "Street lighting",
        "Visitors Parking",
    ]
    out = []
    for name in preferred_order + [k for k in facilities.keys() if k not in preferred_order]:
        raw = facilities.get(name)
        if not isinstance(raw, dict):
            continue
        pct = raw.get("completion_pct")
        if pct in (None, ""):
            continue
        out.append({"title": str(name).strip(), "progress_percentage": f" {str(pct).strip()}"})
    if out:
        out.append({"title": "total_completion_percentage", "progress_percentage": " 0"})
    return out or None


def _build_kerala_legacy_status_update(
    record: dict[str, Any],
    building: dict[str, Any] | None,
    tasks: list[dict[str, Any]] | None,
    facilities: dict[str, Any] | None,
) -> list[dict[str, Any]] | None:
    building = building or {}
    tasks = tasks or []
    facilities = facilities or {}
    project_name = record.get("project_name")

    # Prefer building_details.unit_types; fall back to the unit-type table in
    # data.all_tables (present when building_details was not extracted directly).
    unit_type_rows: list[dict] = list(building.get("unit_types", [])) if isinstance(building, dict) else []
    if not unit_type_rows:
        for table in record.get("data", {}).get("all_tables", []) if isinstance(record.get("data"), dict) else []:
            if not isinstance(table, dict):
                continue
            if table.get("section") != "Building Details":
                continue
            first_row = table.get("first_row", {})
            if isinstance(first_row, dict) and "Apartment/Villa Type" in first_row:
                # Reconstruct row list from first_row (all_tables only stores the first row)
                unit_type_rows = [first_row]
                # Also include additional rows if stored
                for extra in table.get("rows", []):
                    if isinstance(extra, dict) and "Apartment/Villa Type" in extra:
                        unit_type_rows.append(extra)
                break

    booking_details = []
    for row in unit_type_rows:
        if not isinstance(row, dict):
            continue
        flat_type = row.get("Apartment/Villa Type")
        area = row.get("Carpet Area")
        total_available = row.get("Proposed number of apartments")
        booked = row.get("Number of apartments Booked /Sold /Allotted")
        if flat_type or area or total_available or booked:
            booking_details.append(
                {
                    "block": project_name,
                    "flat_type": flat_type,
                    "area": area,
                    "total_available": total_available,
                    "booked_flats": booked,
                }
            )

    building_detail = []
    for table in record.get("data", {}).get("all_tables", []) if isinstance(record.get("data"), dict) else []:
        if not isinstance(table, dict):
            continue
        if table.get("section") != "Building Details":
            continue
        first_row = table.get("first_row")
        if not isinstance(first_row, dict) or "Building Name" not in first_row:
            continue
        building_detail.append(
            {
                "flat_name": first_row.get("Building Name") or project_name,
                "completion_date": first_row.get("Proposed Date of Completion (As committed to allottees)"),
                "no_basement": first_row.get("Number of Basements"),
                "no_podium": first_row.get("Number of Podiums"),
                "no_super_struct": first_row.get("Number of Slab of Super Structure"),
                "no_stilt": first_row.get("Number of Stilts"),
            }
        )
        break

    progress = []
    for row in tasks:
        if not isinstance(row, dict):
            continue
        title = row.get("Tasks / Activity")
        pct = row.get("Percentage of Work")
        if not title:
            continue
        progress.append(
            {
                "block": project_name,
                "title": str(title).strip(),
                "progress_percentage": str(pct).strip() if pct not in (None, "") else "0",
            }
        )
        if "Common Areas work for each building" in str(title):
            break
    if progress:
        progress.extend([{"block": project_name}, {"block": project_name}])
    for name, raw in facilities.items():
        if not isinstance(raw, dict):
            continue
        pct = raw.get("completion_pct")
        if pct in (None, ""):
            continue
        entry = {"title": f"{str(name).strip()} :", "progress_percentage": str(pct).strip()}
        details = raw.get("details")
        if details not in (None, "", "NA"):
            entry["remarks"] = str(details).strip()
        progress.append(entry)

    if not booking_details and not building_detail and not progress:
        return None

    status_update: dict[str, Any] = {"updated": True}
    if booking_details:
        status_update["booking_details"] = booking_details
    if building_detail:
        status_update["building_detail"] = building_detail
    if progress:
        status_update["construction_progress"] = progress
    return [status_update]


def apply_kerala_legacy_shape(record: dict[str, Any]) -> dict[str, Any]:
    out = dict(record)
    labels = out.get("data", {}).get("all_labels", {}) if isinstance(out.get("data"), dict) else {}
    original_building = out.get("building_details") if isinstance(out.get("building_details"), dict) else {}
    original_task_progress = out.get("construction_progress") if isinstance(out.get("construction_progress"), list) else []
    original_facilities = out.get("provided_faciltiy") if isinstance(out.get("provided_faciltiy"), dict) else {}
    chosen_addr = _legacy_promoter_address(out, labels) or {}

    out["state"] = "kerala"
    out["project_state"] = str(labels.get("State") or out.get("project_state") or "KERALA").upper()
    out["domain"] = LEGACY_DOMAIN
    out["config_id"] = LEGACY_CONFIG_ID

    preview_source_url = out.get("data", {}).get("source_url") if isinstance(out.get("data"), dict) else None
    if preview_source_url:
        out["url"] = preview_source_url

    if out.get("estimated_commencement_date") and not out.get("actual_commencement_date"):
        out["actual_commencement_date"] = out["estimated_commencement_date"]
    if out.get("estimated_finish_date") and not out.get("actual_finish_date"):
        out["actual_finish_date"] = out["estimated_finish_date"]
    out["actual_commencement_date"] = _legacy_utc_timestamp(out.get("actual_commencement_date"))
    out["actual_finish_date"] = _legacy_utc_timestamp(out.get("actual_finish_date"))
    out["last_modified"] = _legacy_utc_timestamp(out.get("last_modified"))
    out["estimated_commencement_date"] = None
    out["estimated_finish_date"] = None

    raw_location = out.get("project_location_raw") if isinstance(out.get("project_location_raw"), dict) else {}
    # project_location_raw keys are now stored in normalized (lowercase/underscore) form
    # from _parse_print_preview. Support both old capitalized keys and new normalized keys.
    legacy_location = {
        "state": (labels.get("State") or labels.get("State/ UT")
                  or raw_location.get("state") or raw_location.get("State")),
        "taluk": labels.get("Taluk") or raw_location.get("taluk") or raw_location.get("Taluk"),
        "plot_no": (
            labels.get("Patta No:/ Thandapper Details")
            or labels.get("Patta No")
            or raw_location.get("plot_no")
            or raw_location.get("Patta No:/ Thandapper Details")
        ),
        "village": labels.get("Village") or raw_location.get("village") or raw_location.get("Village"),
        "district": labels.get("District") or raw_location.get("district") or raw_location.get("District"),
        "pin_code": labels.get("Pin Code") or raw_location.get("pin_code") or raw_location.get("Pin Code"),
        "survey_resurvey_number": (
            labels.get("Survey/ Resurvey Number(s)")
            or raw_location.get("survey_resurvey_number")
            or raw_location.get("Survey/ Resurvey Number(s)")
        ),
    }
    legacy_location = {k: v for k, v in legacy_location.items() if v not in (None, "", "NA", "/ Thandapper Details")}
    if legacy_location:
        out["project_location_raw"] = [legacy_location]

    legacy_promoter_address = {
        "state": chosen_addr.get("State/ UT"),
        "taluk": chosen_addr.get("Taluk"),
        "district": chosen_addr.get("District"),
        "locality": chosen_addr.get("Locality"),
        "pin_code": chosen_addr.get("Pin Code"),
        "house_no_building_name": chosen_addr.get("House Number/ Building Name"),
    }
    legacy_promoter_address = {k: v for k, v in legacy_promoter_address.items() if v not in (None, "", "NA")}
    if legacy_promoter_address:
        out["promoter_address_raw"] = [legacy_promoter_address]

    if out.get("land_area") is not None or out.get("construction_area") is not None:
        out["land_area_details"] = {
            "land_area": str(out.get("land_area")) if out.get("land_area") is not None else None,
            "land_area_unit": "Sqmts",
            "construction_area": str(out.get("construction_area")) if out.get("construction_area") is not None else None,
            "construction_area_unit": "in Sqmts",
        }

    compact_members = _compact_kerala_members(out.get("members_details"))
    if compact_members:
        out["members_details"] = compact_members
    compact_promoters = _compact_kerala_co_promoters(out.get("co_promoter_details"))
    if compact_promoters:
        out["co_promoter_details"] = compact_promoters
    compact_professionals = _compact_kerala_professionals(out.get("professional_information"))
    if compact_professionals:
        out["professional_information"] = compact_professionals

    legacy_progress = _build_kerala_legacy_facility_progress(original_facilities)
    if legacy_progress:
        out["construction_progress"] = legacy_progress

    legacy_status_update = _build_kerala_legacy_status_update(
        out,
        original_building,
        original_task_progress,
        original_facilities,
    )
    if legacy_status_update:
        out["status_update"] = legacy_status_update

    out["project_city"] = None
    out["authorised_signatory_details"] = None
    out["provided_faciltiy"] = None
    out["building_details"] = None
    out["land_detail"] = None

    # Ensure PROD-required fields are present in data, preserving any extra DEV fields
    existing_data = out.get("data") if isinstance(out.get("data"), dict) else {}
    out["data"] = {
        "govt_type": "state",
        "is_processed": False,
        "land_area_unit": "Sqmts",
        "construction_area_unit": "in Sqmts",
        **existing_data,
    }
    return out


def build_kerala_legacy_uploaded_documents(
    preview_docs: list[dict] | None,
    doc_links: list[dict] | None,
    uploaded_docs: list[dict] | None,
) -> list[dict]:
    raw_entries: list[dict] = []
    by_identity: dict[str, dict] = {}

    preview_docs = preview_docs or []
    doc_links = doc_links or []
    uploaded_docs = uploaded_docs or []

    for doc in [*doc_links, *preview_docs]:
        if not isinstance(doc, dict):
            continue
        raw_label = str(doc.get("label") or doc.get("type") or "").strip()
        if not raw_label or raw_label in _LEGACY_SKIP_DOC_LABELS:
            continue
        legacy_label = (
            "Rera Registration Certificate"
            if raw_label == "registration_certificate" or raw_label.startswith("Registration Certificate")
            else raw_label
        )
        legacy_link = _legacy_view_file_url(doc.get("url") or doc.get("link"))
        if not legacy_link:
            continue
        identity = _legacy_doc_identity(doc)
        if not identity or identity in by_identity:
            continue
        entry = {"link": legacy_link, "type": legacy_label}
        dated_on = doc.get("upload_date")
        if dated_on not in (None, "", "NA"):
            utc_dated_on = _legacy_utc_timestamp(dated_on)
            entry["dated_on"] = utc_dated_on if utc_dated_on else dated_on
        raw_entries.append(entry)
        by_identity[identity] = entry

    final_entries = list(raw_entries)

    for doc in uploaded_docs:
        if not isinstance(doc, dict):
            continue
        identity = _legacy_doc_identity(doc)
        legacy_link = _legacy_view_file_url(doc.get("link"))
        legacy_type = str(doc.get("type") or "").strip()
        if legacy_type.startswith("Registration Certificate"):
            legacy_type = "Rera Registration Certificate"
        if identity and identity in by_identity:
            if doc.get("s3_link"):
                by_identity[identity]["s3_link"] = doc["s3_link"]
            if by_identity[identity].get("type") == legacy_type:
                continue
        entry = {"type": legacy_type, "s3_link": doc.get("s3_link")}
        if legacy_link:
            entry["link"] = legacy_link
        if identity and identity in by_identity and by_identity[identity].get("dated_on"):
            entry["dated_on"] = by_identity[identity]["dated_on"]
        entry = {k: v for k, v in entry.items() if v not in (None, "", "NA")}
        if entry:
            final_entries.append(entry)

    return final_entries


# ── Detail page scraping ──────────────────────────────────────────────────────

def _scrape_detail_page(url: str, logger: CrawlerLogger) -> dict:
    resp = safe_get(url, logger=logger)
    if not resp:
        return {}
    soup = BeautifulSoup(resp.text, "lxml")
    page_text = soup.get_text(separator="\n", strip=True)
    extracted: dict = {"url": url, "data": {"raw_html_length": len(resp.text)}}

    # Project name from og:title meta tag
    og_title = soup.find("meta", {"property": "og:title"})
    if og_title and og_title.get("content"):
        extracted["project_name"] = og_title["content"].strip()

    # Registration / certificate number
    cert_match = re.search(r"K-RERA/PRJ/[A-Z]+/\d+/\d+", page_text)
    if cert_match:
        extracted["project_registration_no"] = cert_match.group(0)

    # Status
    status_match = re.search(r"(In\s*Progress|Completed|Lapsed|Revoked|De-?registered)", page_text, re.I)
    if status_match:
        extracted["status_of_the_project"] = status_match.group(0).strip()

    # Units: "71 Residential Units  0 Commercial Units"
    res_match = re.search(r"(\d+)\s+Residential\s+Units?", page_text, re.I)
    com_match = re.search(r"(\d+)\s+Commercial\s+Units?", page_text, re.I)
    if res_match:
        extracted["number_of_residential_units"] = res_match.group(1)
    if com_match:
        extracted["number_of_commercial_units"] = com_match.group(1)

    # Proposed completion: "Proposed Completion On\n20, Feb 2031"
    comp_match = re.search(r"Proposed\s+Completion\s+On\s+([\d,\w\s]+)", page_text, re.I)
    if comp_match:
        extracted["estimated_finish_date"] = comp_match.group(1).strip().replace(",", "")

    # Last modified: "Information As Of: 14/03/2026" on the main detail page
    info_match = re.search(r"Information\s+As\s+Of\s*:?\s*(\d{1,2}/\d{1,2}/\d{4})", page_text, re.I)
    if info_match:
        extracted["last_modified"] = info_match.group(1)

    # Available / total units: "71 / 71"
    avail_match = re.search(r"Available\s+Units?\s*[:\|]?\s*(\d+)\s*/\s*(\d+)", page_text, re.I)
    if avail_match:
        extracted["_available_units"] = avail_match.group(1)
        extracted["_total_units"] = avail_match.group(2)

    # Floor areas
    res_area = re.search(r"Total\s+Floor\s+Area\s+Under\s+Residential\s+Use\s*[:\|]?\s*([\d.]+)", page_text, re.I)
    other_area = re.search(r"Total\s+Floor\s+Area\s+Under\s+Other\s+Use\s*[:\|]?\s*([\d.]+)", page_text, re.I)
    if res_area:
        extracted["total_floor_area_under_residential"] = res_area.group(1)
    if other_area:
        extracted["total_floor_area_under_commercial_or_other_uses"] = other_area.group(1)

    # Document links + parse PrintPreview inline
    doc_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        label = a.get_text(strip=True) or "document"
        full_url = urljoin(BASE_URL, href) if href.startswith("/") else href

        if re.match(r"/signed-certificate/\d+", href):
            doc_links.append({"label": "registration_certificate", "url": full_url})
        elif "PrintPreview" in href:
            doc_links.append({
                "label": "complete_project_details",
                "url": full_url,
                "identity_url": full_url.split("?", 1)[0],
            })
            # Parse the PrintPreview HTML to extract all rich fields
            preview_data = _parse_print_preview(full_url, logger)
            extracted.update({k: v for k, v in preview_data.items() if v is not None})
        elif "ProjectStatusPublic" in href or "QPR" in label.upper() or "Quarterly" in label:
            doc_links.append({
                "label": "quarterly_progress_report",
                "url": full_url,
                "identity_url": full_url.split("?", 1)[0],
            })
        elif ".pdf" in href.lower():
            doc_links.append({"label": label, "url": full_url})

    extracted["_doc_links"] = doc_links
    return extracted


# ── Document download + S3 upload ─────────────────────────────────────────────

def _download_print_preview_document(
    browser_session: PlaywrightSession,
    page_cache: dict[str, Any],
    doc: dict[str, Any],
    logger: CrawlerLogger,
) -> bytes | None:
    print_preview_url = doc.get("print_preview_url")
    download_button_id = doc.get("download_button_id")
    if not print_preview_url or not download_button_id:
        return None

    selector = f"#{download_button_id}"
    try:
        page = page_cache.get(print_preview_url)
        if page is None:
            page = browser_session.new_page()
            page.goto(print_preview_url, wait_until="domcontentloaded", timeout=_PLAYWRIGHT_PAGE_TIMEOUT_MS)
            page_cache[print_preview_url] = page
        # Short timeout — if the button isn't immediately available the page
        # likely requires a live session cookie the headless browser doesn't
        # have; we fall back to direct GET rather than blocking for 15 s.
        page.wait_for_selector(selector, timeout=3_000)
        with page.expect_download(timeout=_PLAYWRIGHT_DOWNLOAD_START_TIMEOUT_MS) as download_info:
            page.locator(selector).click()
        download = download_info.value
        suggested_name = download.suggested_filename or "document.pdf"
        suffix = os.path.splitext(suggested_name)[1] or ".pdf"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            temp_path = temp_file.name
        try:
            # save_as() waits for the download to complete; do not fall back
            # early while the browser is still transferring bytes.
            download.save_as(temp_path)
            with open(temp_path, "rb") as file_obj:
                data = file_obj.read()
            return data if len(data) >= 100 else None
        finally:
            try:
                os.unlink(temp_path)
            except OSError:
                pass
    except Exception as exc:
        # Expected on headless servers without a live RERA session cookie;
        # _handle_document will fall back to direct GET automatically.
        logger.debug("Playwright download skipped; using direct GET", error=str(exc), url=print_preview_url)
        return None


def _handle_document(
    project_key: str,
    doc: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
    *,
    browser_session: PlaywrightSession | None = None,
    page_cache: dict[str, Any] | None = None,
) -> dict | None:
    url = doc.get("url")
    if not url:
        return None
    label = doc.get("label", "document")
    filename = build_document_filename(doc)
    try:
        data = None
        if browser_session is not None and page_cache is not None:
            data = _download_print_preview_document(browser_session, page_cache, doc, logger)
        if data is None:
            resp = safe_get(url, logger=logger, timeout=60.0)
            if not resp or len(resp.content) < 100:
                return None
            data = resp.content
        md5 = compute_md5(data)
        s3_key = upload_document(project_key, filename, data, dry_run=settings.DRY_RUN_S3)
        if s3_key is None:
            return None
        s3_url = get_s3_url(s3_key)
        upsert_document(project_key=project_key, document_type=label, original_url=document_identity_url(doc) or url,
                        s3_key=s3_key, s3_bucket=settings.S3_BUCKET_NAME,
                        file_name=filename, md5_checksum=md5, file_size_bytes=len(data))
        logger.info("Document handled", label=label, s3_key=s3_key)
        return document_result_entry(doc, s3_url, filename)
    except Exception as e:
        logger.error(f"Document failed: {e}", url=url)
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(e), project_key=project_key, url=url)
        return None


def _document_queue(*doc_groups: list[dict] | None) -> list[dict]:
    merged: list[dict] = []
    seen: set[str] = set()
    for group in doc_groups:
        if not group:
            continue
        for doc in group:
            if not isinstance(doc, dict):
                continue
            file_id = str(doc.get("file_id")).strip() if doc.get("file_id") not in (None, "") else None
            url = str(doc.get("url")).strip() if doc.get("url") not in (None, "") else None
            marker = document_identity_url(doc) or file_id or url
            if not marker or marker in seen:
                continue
            seen.add(marker)
            merged.append(doc)
    return merged


# ── Sentinel check ────────────────────────────────────────────────────────────

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    sentinel_reg = config.get("sentinel_registration_no", "")
    if not sentinel_reg:
        logger.warning("No sentinel configured — skipping")
        return True
    key = generate_project_key(sentinel_reg)
    existing = get_project_by_key(key)
    if not existing:
        logger.warning("Sentinel not in DB yet — skipping check")
        return True
    soup = _get_explore_page(1, logger)
    if not soup:
        logger.error("Sentinel: could not fetch explore-projects page 1")
        return False
    page_text = soup.get_text()
    if sentinel_reg not in page_text:
        logger.error("Sentinel reg number not found on explore-projects page 1", reg=sentinel_reg)
        return False
    logger.info("Sentinel check passed", reg=sentinel_reg)
    return True


# ── Main run() ────────────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    site_id = config["id"]
    logger = CrawlerLogger(site_id, run_id)
    counts = {"projects_found": 0, "projects_new": 0, "projects_updated": 0,
              "projects_skipped": 0, "documents_uploaded": 0, "error_count": 0}

    item_limit = settings.CRAWL_ITEM_LIMIT or 0  # 0 = unlimited
    items_processed = 0

    if not _sentinel_check(config, run_id, logger):
        insert_crawl_error(run_id, site_id, "SENTINEL_FAILED", "Sentinel check failed")
        return counts

    checkpoint = load_checkpoint(site_id, mode)
    # Resume from the page AFTER the last completed one — the saved page was
    # already fully processed, so re-starting there would duplicate work.
    start_page = (checkpoint["last_page"] + 1) if checkpoint else 1
    if checkpoint:
        logger.info(f"Resuming from checkpoint page {start_page}")

    delay_min, delay_max = config.get("rate_limit_delay", (2, 4))

    # Get total pages from page 1
    soup = _get_explore_page(1, logger)
    if not soup:
        insert_crawl_error(run_id, site_id, "HTTP_ERROR", "Could not fetch explore-projects page 1")
        counts["error_count"] += 1
        return counts

    total_pages = _get_total_pages(soup)
    max_pages = settings.MAX_PAGES  # None = unlimited
    effective_end = (min(total_pages, start_page + max_pages - 1)
                     if max_pages else total_pages)
    logger.info(
        f"Total listing pages: {total_pages} | crawling up to page {effective_end} "
        f"| item_limit={item_limit or 'unlimited'}",
    )

    machine_name, machine_ip = get_machine_context()
    stop_all = False
    print_preview_pages: dict[str, Any] = {}

    with PlaywrightSession(headless=True) as browser_session:
        for page_num in range(start_page, effective_end + 1):
            if stop_all:
                break
            logger.info(f"Listing page {page_num}/{effective_end}")

            page_soup = soup if page_num == 1 else None
            if page_num > 1:
                random_delay(delay_min, delay_max)
                page_soup = _get_explore_page(page_num, logger)
                if not page_soup:
                    logger.error(f"Failed page {page_num}")
                    counts["error_count"] += 1
                    save_checkpoint(site_id, mode, page_num, None, run_id)
                    continue

            cards = _parse_explore_cards(page_soup)
            logger.info(f"  {len(cards)} project cards on page {page_num}")

            for card in cards:
                if item_limit and items_processed >= item_limit:
                    logger.info(f"Item limit {item_limit} reached — stopping", step="listing")
                    stop_all = True
                    break

                cert_no = card.get("cert_no_from_card")
                if not cert_no:
                    continue

                counts["projects_found"] += 1
                key = generate_project_key(cert_no)
                logger.set_project(key=key, reg_no=cert_no, url=card["detail_url"], page=page_num)

                if mode == "daily_light" and get_project_by_key(key):
                    logger.info("Skipping — already in DB (daily_light)", step="skip")
                    counts["projects_skipped"] += 1
                    logger.clear_project()
                    continue

                try:
                    random_delay(delay_min, delay_max)
                    logger.info("Fetching detail page", step="detail_fetch")
                    detail_data = _scrape_detail_page(card["detail_url"], logger)
                    if not detail_data:
                        logger.error("Detail page returned no data", step="detail_fetch")
                        counts["error_count"] += 1
                        continue

                    doc_links = detail_data.pop("_doc_links", [])
                    preview_docs = detail_data.get("uploaded_documents")
                    detail_data.pop("_available_units", None)
                    detail_data.pop("_total_units", None)

                    if not detail_data.get("project_name") and card.get("project_name"):
                        detail_data["project_name"] = card["project_name"]

                    final_reg = detail_data.get("project_registration_no") or cert_no
                    detail_data["project_registration_no"] = final_reg
                    detail_data["key"] = generate_project_key(final_reg)
                    detail_data["state"] = config["state"]
                    detail_data["project_state"] = config["state"]
                    detail_data["domain"] = DOMAIN
                    detail_data["config_id"] = config["config_id"]
                    detail_data["crawl_machine_ip"] = machine_ip
                    detail_data["machine_name"] = machine_name
                    detail_data["is_live"] = False
                    detail_data["data"] = merge_data_sections(
                        {"listing_card": card, "detail_url": card["detail_url"]},
                        detail_data.get("data"),
                    )

                    logger.info("Normalizing and validating", step="normalize")
                    try:
                        normalized = normalize_project_payload(detail_data, config, machine_name=machine_name, machine_ip=machine_ip)
                        record  = ProjectRecord(**normalized)
                        db_dict = apply_kerala_legacy_shape(record.to_db_dict())
                    except (ValidationError, ValueError) as ve:
                        logger.warning("Validation failed — using raw fallback", step="normalize",
                                       error=str(ve))
                        insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(ve),
                                           project_key=key, url=card["detail_url"], raw_data=detail_data)
                        counts["error_count"] += 1
                        db_dict = apply_kerala_legacy_shape(
                            normalize_project_payload(
                                {**detail_data, "data": merge_data_sections(detail_data.get("data"), {"validation_fallback": True})},
                                config, machine_name=machine_name, machine_ip=machine_ip,
                            )
                        )

                    logger.info("Upserting to DB", step="db_upsert")
                    action = upsert_project(db_dict)
                    items_processed += 1
                    if action == "new":       counts["projects_new"] += 1
                    elif action == "updated": counts["projects_updated"] += 1
                    else:                     counts["projects_skipped"] += 1
                    logger.info(f"DB result: {action}", step="db_upsert")

                    queued_docs = _document_queue(doc_links, preview_docs)
                    logger.info(f"Downloading {len(queued_docs)} documents", step="documents")
                    uploaded_doc_results: list[dict] = []
                    doc_name_counts: dict[str, int] = {}
                    for doc in queued_docs:
                        selected_doc = select_document_for_download(config["state"], doc, doc_name_counts, domain=DOMAIN)
                        if selected_doc:
                            uploaded_doc = _handle_document(
                                db_dict["key"],
                                selected_doc,
                                run_id,
                                site_id,
                                logger,
                                browser_session=browser_session,
                                page_cache=print_preview_pages,
                            )
                            if uploaded_doc:
                                uploaded_doc_results.append(uploaded_doc)
                                counts["documents_uploaded"] += 1
                    uploaded_documents = build_kerala_legacy_uploaded_documents(preview_docs, doc_links, uploaded_doc_results)

                    if uploaded_documents:
                        upsert_project({
                            "key": db_dict["key"],
                            "url": db_dict["url"],
                            "state": db_dict["state"],
                            "domain": db_dict["domain"],
                            "project_registration_no": db_dict["project_registration_no"],
                            "uploaded_documents": uploaded_documents,
                            "document_urls": build_document_urls(uploaded_documents),
                        })

                except Exception as exc:
                    logger.exception("Project processing failed", exc, step="project_loop")
                    insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                                       project_key=key, url=card["detail_url"])
                    counts["error_count"] += 1
                finally:
                    logger.clear_project()

            save_checkpoint(site_id, mode, page_num, None, run_id)

    reset_checkpoint(site_id, mode)
    logger.info("Kerala RERA crawl finished", **counts)
    return counts
