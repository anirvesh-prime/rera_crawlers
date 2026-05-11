"""
Delhi RERA Crawler — rera.delhi.gov.in/registered_promoters_list
Type: static (Drupal 7 — httpx + BeautifulSoup)

Strategy:
- ALL project data lives inline in the listing table rows. No detail pages.
- Listing: table.views-table.cols-5, paginated via ?page=N (0-indexed).
- Each <tr> has five <td> cells:
    td.views-field-php-1                   → promoter name, address, email, phone
    td.views-field-field-project-address   → project name, location string
    td.views-field-field-rera-registrationno → reg no, valid-until date, construction
                                               status, certificate PDF link
    td.views-field-php                     → QPR history page link
- Pagination: ul.pager li.pager-next a present → more pages; stops at absence or
  no rows returned.
- Sentinel: DLRERA2023P0017 (TARC KAILASA / TARC PROJECTS LIMITED)
"""
from __future__ import annotations

import re
import time
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup, Tag
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.config import settings
from core.crawler_base import generate_project_key, random_delay, safe_get
from core.db import get_project_by_key, upsert_project, upsert_document, insert_crawl_error
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import build_document_urls, get_machine_context, normalize_project_payload
from core.s3 import compute_md5, upload_document, get_s3_url

LISTING_URL   = "https://rera.delhi.gov.in/registered_promoters_list"
BASE_URL      = "https://rera.delhi.gov.in"
DOMAIN        = "rera.delhi.gov.in"
_SENTINEL_REG = "DLRERA2023P0017"
# Matches e.g. DLRERA2023P0017 or DLRERA2022A0001
_REG_NO_RE  = re.compile(r"DLRERA\d{4}[PA]\d{4,5}", re.IGNORECASE)
_PIN_RE     = re.compile(r"\b(\d{6})\b")


def _get_listing_response(url: str, logger: CrawlerLogger, params: dict | None = None) -> httpx.Response | None:
    """Delhi returns a parseable listing page with a broken TLS chain and HTTP 500."""
    headers = {"User-Agent": settings.user_agents[0]}
    try:
        with httpx.Client(timeout=60.0, follow_redirects=True, verify=False) as client:
            resp = client.get(url, headers=headers, params=params)
        if resp.status_code >= 400 and "views-table" not in resp.text:
            logger.warning(
                f"Listing fetch returned HTTP {resp.status_code} without usable table markup",
                url=url,
            )
            return None
        return resp
    except Exception as exc:
        logger.warning(f"Listing fetch failed: {exc}", url=url)
        return None


def _delhi_get(url: str, logger: CrawlerLogger | None = None) -> httpx.Response | None:
    """GET for Delhi RERA sub-pages.

    The site often returns HTTP 500 while still serving valid HTML content.
    Unlike safe_get(), this wrapper does NOT call raise_for_status() so that
    callers can still parse the response body on non-200 codes.
    """
    headers = {"User-Agent": settings.user_agents[0]}
    for attempt in range(1, 4):
        try:
            with httpx.Client(timeout=30.0, follow_redirects=True, verify=False) as client:
                resp = client.get(url, headers=headers)
            return resp
        except Exception as exc:
            if logger:
                logger.warning(f"GET attempt {attempt}/3 failed: {exc}", url=url)
    return None


# ─── Row-level helpers ────────────────────────────────────────────────────────

def _strong_values(td: Tag) -> dict[str, str]:
    """Return {label: value} from a <td> containing <strong>Label:</strong>text pairs.

    Iterates each <strong> element; collects NavigableString siblings (skipping
    <br>, <a>, <img>) until the next <strong>.  <span> children are included via
    get_text so that Drupal date-display-single values are captured.
    """
    result: dict[str, str] = {}
    for strong in td.find_all("strong"):
        label = strong.get_text(strip=True).rstrip(":").strip().lower()
        if not label:
            continue
        parts: list[str] = []
        for node in strong.next_siblings:
            tag = getattr(node, "name", None)
            if tag == "strong":
                break
            if tag in ("br", "a", "img"):
                continue
            if tag is None:                             # NavigableString
                text = str(node).strip()
            else:                                       # span, b, etc.
                text = node.get_text(separator=" ", strip=True)
            if text:
                parts.append(text)
        result[label] = " ".join(parts).strip()
    return result


def _abs(href: str) -> str:
    """Convert a relative href to an absolute URL."""
    return href if href.startswith("http") else urljoin(BASE_URL, href)


def _parse_directors_page(html: str) -> list[dict]:
    """Parse co-promoter/director rows from promoter_directors/{node_id}.

    The page renders a table#view_directors with columns:
      No. | Designation | Personal Details (b tags) | Photograph (img)
    Returns a list of dicts with name, email, phone, designation, photo.
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", id="view_directors") or soup.find("table")
    if not table:
        return []

    results: list[dict] = []
    for tr in table.select("tbody tr"):
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue
        designation = cells[1].get_text(strip=True) if len(cells) > 1 else ""
        personal_td = cells[2]

        # Extract name / email / phone from <b>Label</b> : value pairs
        name = email = phone = ""
        for b in personal_td.find_all("b"):
            label = b.get_text(strip=True).lower().rstrip(":")
            # Collect the text node(s) immediately after the <b>
            val_parts: list[str] = []
            for node in b.next_siblings:
                tag = getattr(node, "name", None)
                if tag == "b":
                    break
                if tag in ("br", "img"):
                    continue
                text = str(node).strip().lstrip(":").strip() if tag is None else ""
                if text:
                    val_parts.append(text)
            val = " ".join(val_parts).strip()
            if label == "name":
                name = val
            elif label == "email":
                email = val
            elif label == "phone":
                phone = val

        photo: str | None = None
        if len(cells) > 3:
            img = cells[3].find("img")
            if img and img.get("src"):
                photo = _abs(img["src"])

        entry: dict = {k: v for k, v in {
            "name":        name,
            "email":       email,
            "phone":       phone,
            "designation": designation,
            "photo":       photo,
        }.items() if v}
        if entry.get("name"):
            results.append(entry)

    return results


def _parse_qpr_history(html: str) -> list[dict]:
    """Parse QPR submission history from online_view_periodic_progress_reports_history.

    The page renders a table with columns:
      S.No. | Report Due Date | Report Status | Project Name
    Report Status may contain <a> links to submitted QPR docs.
    Returns a list of dicts: {end_date, status, uploaded_documents?, updated?}
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []

    results: list[dict] = []
    for tr in table.select("tbody tr"):
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue
        end_date = cells[1].get_text(strip=True)
        status_td = cells[2]
        status = status_td.get_text(strip=True)
        qpr_docs = [
            {"link": _abs(a["href"]), "type": "QPR Report"}
            for a in status_td.find_all("a", href=True)
        ]

        entry: dict = {"end_date": end_date, "status": status}
        if qpr_docs:
            entry["uploaded_documents"] = qpr_docs
            entry["updated"] = True
        if end_date or status:
            results.append(entry)

    return results


def _extract_submitted_qprs_url(html: str) -> str | None:
    """Extract the 'View all submitted QPRs' URL from the QPR history page.

    The link lives in div.view-header and points to
    all-submiited-qprs-public-view/{project_node_id}.
    """
    soup = BeautifulSoup(html, "lxml")
    a = soup.select_one("div.view-header a[href*='all-submiited-qprs-public-view']")
    return _abs(a["href"]) if a else None


def _parse_submitted_qprs_page(html: str) -> list[dict]:
    """Parse all-submiited-qprs-public-view/{project_node_id}.

    Table columns:
      S.No. | QPR Details (linked to node) | Project Name | Quarter Duration
      Submission Details | Documents | Status
    Returns list of dicts: {qpr_id, detail_url, quarter_start, quarter_end,
                             submitted_on, uploaded_documents, status}.
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []

    results: list[dict] = []
    for tr in table.select("tbody tr"):
        cells = tr.find_all("td")
        if len(cells) < 6:
            continue

        # QPR ID and per-QPR detail-page link (node/{qpr_node_id})
        qpr_a      = cells[1].find("a", href=True)
        qpr_id     = qpr_a.get_text(strip=True) if qpr_a else ""
        detail_url = _abs(qpr_a["href"])         if qpr_a else None

        # Quarter dates
        start_date = end_date = ""
        for p in cells[3].find_all("p"):
            txt = p.get_text(strip=True)
            if "Start Date" in txt:
                start_date = txt.split(":", 1)[-1].strip()
            elif "End Date" in txt:
                end_date   = txt.split(":", 1)[-1].strip()

        # Submission date
        submitted_on = (
            cells[4].get_text(separator=" ", strip=True)
            .replace("Submitted On :", "").strip()
        )

        # Documents (skip mailto links) — standard uploaded_documents format
        docs = [
            {"link": _abs(a["href"]), "type": "QPR Submission Document"}
            for a in cells[5].find_all("a", href=True)
            if not a["href"].startswith("mailto")
        ]

        # Status
        status = cells[6].get_text(strip=True) if len(cells) > 6 else ""

        entry: dict = {k: v for k, v in {
            "qpr_id":             qpr_id,
            "detail_url":         detail_url,
            "quarter_start":      start_date,
            "quarter_end":        end_date,
            "submitted_on":       submitted_on,
            "uploaded_documents": docs or None,
            "status":             status,
        }.items() if v}
        if entry.get("qpr_id"):
            results.append(entry)

    return results


def _parse_qpr_detail_node(html: str) -> dict:
    """Parse a QPR detail node page (node/{qpr_node_id}).

    Extracts:
      qpr_id, quarter_start, quarter_end, submitted_on, status,
      tower_completion: [{tower, pct_completed}],
      amenity_completion: [{amenity, pct_completed}],
      uploaded_documents: [{"link": str, "type": str}]
    """
    soup = BeautifulSoup(html, "lxml")
    main = soup.find(id="main-content") or soup.find("article")
    if not main:
        return {}

    result: dict = {}

    # ── QPR header (div.group-qpr-details) ───────────────────────────────
    header = main.find("div", class_="group-qpr-details")
    if header:
        for tbl in header.find_all("table"):
            cells = tbl.find_all("td")
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True).rstrip(":")
            value = cells[1].get_text(strip=True)
            if label == "QPR ID":
                result["qpr_id"] = value
            elif label == "Quarter Start Date":
                result["quarter_start"] = value
            elif label == "Quarter End Date":
                result["quarter_end"] = value
            elif label == "QPR Submitted on":
                span = cells[1].find("span", class_="date-display-single")
                result["submitted_on"] = (span.get("content") or value) if span else value
            elif label == "Status of QPR":
                result["status"] = value

    # ── Tower completion ──────────────────────────────────────────────────
    tower_details: list[dict] = []
    for tower_div in main.find_all(
        "div",
        class_=lambda c: c and "field-collection-item-field-select-tower" in c,
    ):
        tower_name: str | None = None
        pct: float | None = None
        for tbl in tower_div.find_all("table"):
            cells = tbl.find_all("td")
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True)
            value = cells[1].get_text(strip=True)
            if label == "Name of Tower:":
                tower_name = value
            elif label == "% Completed:" and tower_name is not None and pct is None:
                try:
                    pct = float(value)
                except ValueError:
                    pass
        if tower_name is not None:
            entry: dict = {"tower": tower_name}
            if pct is not None:
                entry["pct_completed"] = pct
            tower_details.append(entry)
    if tower_details:
        result["tower_completion"] = tower_details

    # ── Amenity completion ────────────────────────────────────────────────
    amenities: list[dict] = []
    for tbl in main.find_all("table"):
        cells = tbl.find_all("td")
        if len(cells) < 2:
            continue
        if "Amenities" in cells[0].get_text():
            amenity_name = cells[1].get_text(strip=True)
            next_tbl = tbl.find_next_sibling("table")
            if next_tbl:
                nc = next_tbl.find_all("td")
                if len(nc) >= 2 and "% Completed" in nc[0].get_text():
                    try:
                        amenities.append({
                            "amenity":       amenity_name,
                            "pct_completed": float(nc[1].get_text(strip=True)),
                        })
                    except ValueError:
                        amenities.append({"amenity": amenity_name})
    if amenities:
        result["amenity_completion"] = amenities

    # ── Documents ─────────────────────────────────────────────────────────
    qpr_docs = [
        {"link": _abs(a["href"]), "type": "QPR Document"}
        for a in main.find_all("a", href=True)
        if "/files/qpr/" in a.get("href", "")
    ]
    if qpr_docs:
        result["uploaded_documents"] = qpr_docs

    return {k: v for k, v in result.items() if v not in (None, "", [], {})}


def _find_project_page_by_promoter_node(
    promoter_node_id: int,
    max_delta: int = 200,
    logger: CrawlerLogger | None = None,
) -> str | None:
    """Scan project_page nodes to find the one linked to promoter_node_id.

    For brand-new projects that have not yet submitted any QPRs, the QPR history
    page has no all-submiited-qprs-public-view link, so the project node ID cannot
    be derived from it.  However, the project_page Drupal node is always created a
    small number of nodes after the promoter node (historically 1–110).  We scan
    project_page/{promoter_node_id + N} for N in 1..max_delta and return the first
    URL whose HTML contains a link back to promoter_page/{promoter_node_id},
    confirming it is the correct project page.
    """
    target = f"promoter_page/{promoter_node_id}"
    headers = {"User-Agent": settings.user_agents[0]}
    try:
        with httpx.Client(timeout=15.0, follow_redirects=True, verify=False) as client:
            for delta in range(1, max_delta + 1):
                node_id = promoter_node_id + delta
                url = f"{BASE_URL}/project_page/{node_id}"
                try:
                    resp = client.get(url, headers=headers)
                    if target in resp.text:
                        if logger:
                            logger.info(
                                f"Found project_page/{node_id} via promoter scan"
                                f" (promoter={promoter_node_id}, delta={delta})",
                                step="project_page_scan",
                            )
                        return url
                except Exception as exc:
                    if logger:
                        logger.warning(
                            f"project_page/{node_id} fetch error: {exc}",
                            step="project_page_scan",
                        )
    except Exception as exc:
        if logger:
            logger.warning(
                f"Promoter-scan client error (promoter={promoter_node_id}): {exc}",
                step="project_page_scan",
            )
    return None


def _parse_project_page(html: str) -> dict:
    """Parse project_page/{project_node_id} — the rich project details page.

    Extracts from each named scroll section:
      scroll1  → general details (description, website, lat/lng, land_type, dates)
      scroll2  → area details (land_area, open_area, covered_area, parking)
      scroll3  → cost estimates (construction_cost_lakhs, project_cost_lakhs, total_cost_lakhs)
      scroll4  → project approval docs (Encumbrance, Sanction Plan)
      scroll5  → facilities & amenities table
      scroll6  → project entity (CA, architect, engineer)
      scroll7  → uploaded document URLs (with category headers)
      scroll8  → tower/floor inventory
      jssor_1  → project image slider

    Also extracts from the page header:
      promoter_page URL, RERA status, last_updated, registration certificate PDF.
    """
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()
    main = soup.find(id="main-content") or soup.find("form", id="project-page")
    if not main:
        return {}

    result: dict = {}

    # ── Header: promoter URL, cert PDF, last updated ──────────────────────────
    promo_a = main.find("a", href=lambda h: h and "promoter_page" in str(h))
    if promo_a:
        result["promoter_page_url"] = _abs(promo_a["href"])

    cert_a = main.find("a", href=lambda h: h and "certification_document" in str(h))
    if cert_a:
        result["registration_cert_url"] = _abs(cert_a["href"])

    for span in main.find_all("span"):
        txt = span.get_text(strip=True)
        if txt.startswith("Last Updated on"):
            result["last_updated"] = txt.replace("Last Updated on :", "").strip()
            break

    # ── Helper: parse col-md-3/col-md-9 label-value rows ─────────────────────
    def _kv(section_div: BeautifulSoup) -> dict[str, str]:
        """Parse Bootstrap grid rows into {label: value} pairs.

        Handles both the common col-md-3/col-md-9 layout and the 4-column
        col-md-3/col-md-3/col-md-3/col-md-3 layout used for Latitude/Longitude.
        """
        pairs: dict[str, str] = {}
        for row in section_div.find_all("div", class_="row"):
            cols = row.find_all("div", recursive=False)
            if not cols:
                continue
            # Collect all label spans in this row
            label_cols = [c for c in cols if c.find("span", class_="font-weight-semibold")]
            if not label_cols:
                continue
            # For each label column, its value is the immediately following sibling
            # that does NOT contain a label span.
            for lc in label_cols:
                span = lc.find("span", class_="font-weight-semibold")
                label = span.get_text(strip=True).rstrip(":").strip() if span else ""
                if not label:
                    continue
                # Find the next sibling div that has no label span
                idx = cols.index(lc)
                value_parts: list[str] = []
                for vc in cols[idx + 1:]:
                    if vc.find("span", class_="font-weight-semibold"):
                        break  # next label — stop
                    value_parts.append(vc.get_text(strip=True))
                value = " ".join(value_parts).strip()
                if value:
                    pairs[label] = value
        return pairs

    # ── scroll1: General Details + Timelines ─────────────────────────────────
    s1 = main.find(id="scroll1")
    if s1:
        kv = _kv(s1)
        # Use DB-schema column names directly so the normalizer maps them correctly.
        _map = {
            "Description":                "project_description",
            "Website":                    "website",
            "Sub District /Tehsil /City": "sub_district",
            "Latitude":                   "latitude",
            "Longitude":                  "longitude",
            "Project Type":               "project_type",
            "Type of Land":               "land_type",
            "Start Date":                 "estimated_commencement_date",
            "End Date":                   "project_end_date",  # kept separate; listing's valid_until → estimated_finish_date
        }
        for src, dst in _map.items():
            if kv.get(src):
                result[dst] = kv[src]

    # ── scroll2: Area + Parking ───────────────────────────────────────────────
    s2 = main.find(id="scroll2")
    if s2:
        kv = _kv(s2)
        # land_area / construction_area map directly to DB float columns.
        _map2 = {
            "Land Area":                  "land_area",
            "Open Area":                  "open_area_sqmt",    # no dedicated DB column; stored in data
            "Covered Area":               "construction_area",
            "No. of parking":             "total_parking",
            "No. of open parking":        "open_parking",
            "No. of covered parking":     "covered_parking",
            "No. of garage":              "garage_count",
        }
        for src, dst in _map2.items():
            if kv.get(src):
                result[dst] = kv[src]

    # ── scroll3: Cost → project_cost_detail JSONB ─────────────────────────────
    s3 = main.find(id="scroll3")
    if s3:
        kv = _kv(s3)
        cost_detail: dict = {}
        if kv.get("Cost of construction"):
            cost_detail["estimated_construction_cost"] = kv["Cost of construction"]
        if kv.get("Cost of project"):
            cost_detail["estimated_project_cost"] = kv["Cost of project"]
        if kv.get("Total cost of project"):
            cost_detail["total_project_cost"] = kv["Total cost of project"]
        if cost_detail:
            result["project_cost_detail"] = cost_detail

    # ── scroll5: Facilities & Amenities table ─────────────────────────────────
    s5 = main.find(id="scroll5")
    if s5:
        amenities: list[dict] = []
        for tr in s5.select("table tbody tr"):
            cells = tr.find_all("td")
            if len(cells) < 3:
                continue
            name   = cells[1].get_text(strip=True)
            detail = cells[2].get_text(strip=True)
            status = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            if name:
                amenities.append({k: v for k, v in {
                    "name": name, "detail": detail, "status": status,
                }.items() if v})
        if amenities:
            result["amenities"] = amenities

    # ── scroll6: Project Entity (CA, Architects, Engineers, etc.) ────────────
    s6 = main.find(id="scroll6")
    if s6:
        _TAB_ROLES = {
            "CA1":               "CA",
            "architecht1":       "Architect",
            "engineer1":         "Engineer",
            "st_engineer1":      "Structural Engineer",
            "associate_agent1":  "Real Estate Agent",
        }
        _LABEL_MAP = {
            "ca registration no.":       "registration_no",
            "coa registration no.":      "registration_no",
            "rera registration no":      "registration_no",
            "name":                      "name",
            "mobile no.":                "mobile",
            "mobile no":                 "mobile",
            "e-mail address":            "email",
            "email address":             "email",
        }
        professionals: list[dict] = []
        for tab_id, role in _TAB_ROLES.items():
            tab = s6.find(id=tab_id)
            if not tab:
                continue
            kv = _kv(tab)
            entry: dict = {"type": role}
            for raw_label, val in kv.items():
                mapped = _LABEL_MAP.get(raw_label.lower().rstrip("."))
                if mapped and val:
                    entry[mapped] = val
            if len(entry) > 1:   # has at least one field besides "type"
                professionals.append(entry)
        if professionals:
            result["professional_information"] = professionals

    # ── scroll4: Project Approval — Encumbrance + Sanction Plan ─────────────
    # scroll4 has two tables:
    #   Table 1 (2 cols): Encumbrance / Non Encumbrance Details section
    #   Table 2 (4 cols): Sanction Plan section (doc name in category header row)
    s4 = main.find(id="scroll4")
    scroll4_docs: list[dict] = []
    if s4:
        for tbl in s4.find_all("table"):
            current_category: str | None = None
            for tr in tbl.select("tbody tr"):
                cells = tr.find_all("td")
                if not cells:
                    continue
                # Category/header rows have colspan set on the first cell
                if cells[0].get("colspan"):
                    current_category = cells[0].get_text(strip=True)
                    continue
                # Find the button (may be in any cell — search all cells)
                btn = None
                for cell in cells:
                    btn = cell.find("button", attrs={"data-pdf_link": True})
                    if btn:
                        break
                if not btn or not btn.get("data-pdf_link"):
                    continue
                # doc_name: use cells[0] text if non-empty, else fall back to category
                doc_name = cells[0].get_text(strip=True) or current_category or "Project Document"
                scroll4_docs.append({
                    "link": _abs(btn["data-pdf_link"]),
                    "type": doc_name,
                })

    # ── scroll7: Uploaded documents ───────────────────────────────────────────
    # The table has 3 columns: Document Name | Status | Uploaded Documents.
    # Category header rows use colspan=3 on a single <td> — include them as
    # grouping markers so the output mirrors the on-screen document structure.
    # Each uploaded file is a <button data-pdf_link="..."> (not an <a> tag).
    s7 = main.find(id="scroll7")
    if s7:
        proj_docs: list[dict] = []
        for tr in s7.select("table tbody tr"):
            cells = tr.find_all("td")
            if not cells:
                continue
            # Category header row: single <td colspan="3"> containing an <h4>
            if cells[0].get("colspan"):
                category_name = cells[0].get_text(strip=True)
                if category_name:
                    proj_docs.append({"type": category_name})
                continue
            if len(cells) < 3:
                continue
            doc_name = cells[0].get_text(strip=True)
            btn = cells[2].find("button", attrs={"data-pdf_link": True})
            if btn and btn.get("data-pdf_link"):
                proj_docs.append({
                    "link": _abs(btn["data-pdf_link"]),
                    "type": doc_name or "Project Document",
                })
        # Append scroll4 docs (Sanction Plan, Encumbrance) after scroll7 docs
        proj_docs.extend(scroll4_docs)
        if proj_docs:
            result["uploaded_documents"] = proj_docs

    # ── jssor_1: Project image slider ─────────────────────────────────────────
    # The jssor_1 div contains <img data-u="image" src="..."> for each project
    # photo. Collect all distinct image URLs (including the pna.jpg placeholder).
    jssor = soup.find(id="jssor_1")
    if jssor:
        img_urls: list[str] = []
        seen_imgs: set[str] = set()
        for img in jssor.find_all("img", attrs={"data-u": "image"}):
            src = img.get("src", "").strip()
            if src and src not in seen_imgs:
                seen_imgs.add(src)
                img_urls.append(src if src.startswith("http") else urljoin(BASE_URL, src))
        if img_urls:
            result["project_images"] = img_urls

    # ── scroll8: Tower/Floor inventory ───────────────────────────────────────
    # Each tower is ONE top-level table whose <tbody> has 5 rows:
    #   tr[0]: <th> header row  (Sr.No, Name of Block/Tower, dates, parking)
    #   tr[1]: <td> data row    (tower name, completion date, parking counts)
    #   tr[2]: <td colspan=9>   containing nested Apartment Type table
    #   tr[3]: <td colspan=9>   containing nested Stage/Completion table
    #   tr[4]: <td colspan=9>   containing nested Facility/Amenity table
    s8 = main.find(id="scroll8")
    if s8:
        towers: list[dict] = []
        # Only top-level tables (not nested inside another table)
        for tbl in s8.find_all("table"):
            if tbl.find_parent("table") is not None:
                continue
            tbody = tbl.find("tbody")
            if not tbody:
                continue
            trs = tbody.find_all("tr", recursive=False)
            if not trs:
                continue

            # First row must be the header with "Name of Block/Tower"
            header_ths = [th.get_text(strip=True) for th in trs[0].find_all("th", recursive=False)]
            if "Name of Block/Tower" not in header_ths:
                continue

            # Second row: tower data
            if len(trs) < 2:
                continue
            data_cells = trs[1].find_all("td", recursive=False)
            tower_name      = data_cells[1].get_text(strip=True) if len(data_cells) > 1 else ""
            completion_date = data_cells[2].get_text(strip=True) if len(data_cells) > 2 else ""
            if not tower_name:
                continue

            tower: dict = {"tower": tower_name}
            if completion_date:
                tower["proposed_completion"] = completion_date

            # Rows 2–4: each contains one nested sub-table
            for nested_tr in trs[2:]:
                sub = nested_tr.find("table")
                if not sub:
                    continue
                sub_first_tr = sub.find("tr")
                if not sub_first_tr:
                    continue
                sub_ths = [th.get_text(strip=True) for th in sub_first_tr.find_all("th")]

                if "Apartment Type" in sub_ths:
                    # Floor-wise inventory
                    floors: list[dict] = []
                    current_floor: str | None = None
                    for row in sub.find_all("tr"):
                        tds = row.find_all("td")
                        if not tds:
                            continue
                        # Floor label row: a single wide <td> with "Floor No."
                        if len(tds) == 1 or (len(tds) > 0 and tds[0].get("colspan", "1") not in ("1", 1)):
                            txt = tds[0].get_text(strip=True)
                            if "Floor No." in txt:
                                current_floor = txt.split(":", 1)[-1].strip()
                            continue
                        if len(tds) >= 7 and current_floor is not None:
                            apt_type = tds[1].get_text(strip=True)
                            if apt_type and apt_type != "Apartment Type":
                                floors.append({k: v for k, v in {
                                    "floor":       current_floor,
                                    "type":        apt_type,
                                    "count":       tds[2].get_text(strip=True),
                                    "carpet_sqmt": tds[3].get_text(strip=True),
                                    "booked":      tds[6].get_text(strip=True),
                                    "available":   tds[7].get_text(strip=True) if len(tds) > 7 else "",
                                }.items() if v})
                    if floors:
                        tower["floor_inventory"] = floors

                elif "Stage" in sub_ths:
                    stages: list[dict] = []
                    for row in sub.find_all("tr"):
                        tds = row.find_all("td")
                        if len(tds) >= 4:
                            stage = tds[1].get_text(strip=True)
                            if stage and stage != "Stage":
                                stages.append({k: v for k, v in {
                                    "stage":    stage,
                                    "due_date": tds[2].get_text(strip=True),
                                    "started":  tds[3].get_text(strip=True),
                                    "pct_done": tds[4].get_text(strip=True) if len(tds) > 4 else "",
                                }.items() if v})
                    if stages:
                        tower["completion_stages"] = stages

            towers.append(tower)
        if towers:
            result["tower_inventory"] = towers

    return {k: v for k, v in result.items() if v not in (None, "", [], {})}


def _parse_row(tr: Tag) -> dict | None:
    """Parse one <tr> from the Delhi RERA listing table into a project dict.

    Returns None if no registration number can be extracted (row is unusable).

    Table columns (by CSS class):
      views-field-php-1                    → promoter name / address / email / phone
      views-field-field-project-address    → project name / location
      views-field-field-rera-registrationno → reg no / valid-until / status / cert PDF
      views-field-php                      → QPR history link
    """
    # ── Cell selectors ────────────────────────────────────────────────────────
    promo_td = tr.select_one("td.views-field-php-1")
    proj_td  = tr.select_one("td.views-field-field-project-address")
    reg_td   = tr.select_one("td.views-field-field-rera-registrationno")
    qpr_td   = tr.select_one("td.views-field-php")

    if not reg_td:
        return None

    # ── Registration number ───────────────────────────────────────────────────
    reg_text = reg_td.get_text(separator=" ", strip=True)
    m = _REG_NO_RE.search(reg_text)
    if not m:
        return None
    reg_no = m.group(0).upper()

    # ── Promoter cell ─────────────────────────────────────────────────────────
    promo_kv: dict[str, str] = _strong_values(promo_td) if promo_td else {}
    promoter_name  = promo_kv.get("name", "").strip()
    promoter_addr  = promo_kv.get("address", "").strip()
    promoter_email = promo_kv.get("email", "").strip()
    promoter_phone = (
        promo_kv.get("phone number", "") or promo_kv.get("phone", "")
    ).strip()

    # "View Photos" link → promoter_directors/{node_id} — used to fetch
    # co_promoter_details / members_details in the main run() loop.
    directors_url: str | None = None
    if promo_td:
        dir_a = promo_td.find("a", href=True)
        if dir_a:
            directors_url = _abs(dir_a["href"])

    # ── Project cell ──────────────────────────────────────────────────────────
    proj_kv: dict[str, str] = _strong_values(proj_td) if proj_td else {}
    project_name = proj_kv.get("name", "").strip()
    location_str = proj_kv.get("location", "").strip()

    # ── Registration cell ─────────────────────────────────────────────────────
    reg_kv = _strong_values(reg_td)

    # Valid-until: prefer ISO content attribute on date-display-single span
    valid_until: str | None = None
    date_span = reg_td.select_one("span.date-display-single")
    if date_span:
        valid_until = date_span.get("content") or date_span.get_text(strip=True) or None

    const_status = (
        reg_kv.get("construction status", "") or reg_kv.get("construction status:", "")
    ).strip() or None

    # Certificate PDF link
    cert_url: str | None = None
    cert_a = reg_td.select_one("span.file a[href]")
    if cert_a:
        cert_url = _abs(cert_a["href"])

    # Extension certificate (text note)
    ext_cert = reg_kv.get("extension certificate", "").strip() or None

    # ── QPR cell ──────────────────────────────────────────────────────────────
    # The cell may contain:
    #   <a href="online_view_periodic_progress_reports_history/...">View QPRs
    #     <a class="product_list" href="project_page/{node_id}">View Project</a>
    #   </a>
    # In newer layouts the "View Project" link may use a different class or
    # be a sibling anchor — match any <a href="project_page/..."> in the cell.
    qpr_url: str | None = None
    project_page_url: str | None = None
    if qpr_td:
        for a in qpr_td.find_all("a", href=True):
            href = str(a.get("href", ""))
            if "project_page/" in href and project_page_url is None:
                project_page_url = _abs(href)
            elif "online_view_periodic_progress_reports_history" in href and qpr_url is None:
                qpr_url = _abs(href)
            elif qpr_url is None and "project_page/" not in href:
                # First non-project-page link → QPR history
                qpr_url = _abs(href)

    # ── Build sub-dicts ───────────────────────────────────────────────────────
    contact: dict = {
        k: v for k, v in {"email": promoter_email, "phone": promoter_phone}.items() if v
    }

    loc_raw: dict = {}
    if location_str:
        loc_raw["raw_address"] = location_str
        pin_m = _PIN_RE.search(location_str)
        if pin_m:
            loc_raw["pin_code"] = pin_m.group(1)

    prom_addr_raw: dict = {}
    if promoter_addr:
        prom_addr_raw["registered_address"] = promoter_addr
        if not loc_raw.get("pin_code"):
            pin_m = _PIN_RE.search(promoter_addr)
            if pin_m:
                loc_raw["pin_code"] = pin_m.group(1)

    docs: list[dict] = []
    if cert_url:
        docs.append({"link": cert_url, "type": "Registration Certificate"})
    if qpr_url:
        docs.append({"link": qpr_url, "type": "QPR History"})

    data_snap: dict = {}
    if promoter_email:
        data_snap["email"] = promoter_email
    if cert_url:
        data_snap["link"] = cert_url

    out: dict = {
        "project_registration_no":  reg_no,
        "project_name":             project_name or None,
        "promoter_name":            promoter_name or None,
        "status_of_the_project":    const_status,
        "estimated_finish_date":    valid_until,
        "project_location_raw":     loc_raw or None,
        "promoter_address_raw":     prom_addr_raw or None,
        "promoter_contact_details": contact or None,
        "uploaded_documents":       docs or None,
        "data":                     data_snap or None,
        # Secondary-fetch URLs (used by run() to enrich each project):
        "_directors_url":           directors_url,     # → co_promoter_details / members_details
        "_qpr_url":                 qpr_url,           # kept for reference / fallback
        "_project_page_url":        project_page_url,  # → project details (direct, no QPR hop needed)
    }
    if ext_cert:
        out["extension_certificate"] = ext_cert   # stored in data jsonb via normalizer
    return {k: v for k, v in out.items() if v not in (None, "", [], {})}


def _parse_listing_page(html: str) -> list[dict]:
    """Extract all project dicts from a single listing page.

    The 'View Project' link (<a class="product_list">) in each row already
    carries the project_page/{node_id} URL, so no secondary AJAX lookup is needed.
    """
    soup = BeautifulSoup(html, "lxml")
    results: list[dict] = []
    table = soup.select_one("div.view-content table")
    if not table:
        return results
    for tr in table.select("tbody tr"):
        row = _parse_row(tr)
        if row:
            results.append(row)
    return results


def _has_next_page(html: str) -> bool:
    """Return True if the Drupal Views pager has a 'next' link."""
    soup = BeautifulSoup(html, "lxml")
    return bool(soup.select_one(
        "ul.pager li.pager-next a, "
        "ul.pager__items li.pager__item--next a"
    ))


def _fetch_project_page_urls_playwright(listing_url: str, logger: CrawlerLogger) -> dict[str, str]:
    """Use Playwright to load a listing page and click 'View Project' for each row.

    Returns a dict mapping reg_no → absolute project_page URL.  Called as a
    fallback when the static HTML fetch doesn't expose any project_page/ links
    (e.g. because the site renders them via JavaScript or the link class changed).
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("Playwright not installed — cannot click View Project buttons",
                       step="project_page")
        return {}

    result: dict[str, str] = {}
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(ignore_https_errors=True)
            page = ctx.new_page()
            page.goto(listing_url, wait_until="domcontentloaded", timeout=30_000)
            rows = page.query_selector_all("div.view-content table tbody tr")
            for row in rows:
                # Identify the project by reg_no
                reg_td = row.query_selector("td.views-field-field-rera-registrationno")
                if not reg_td:
                    continue
                m = _REG_NO_RE.search(reg_td.inner_text())
                if not m:
                    continue
                reg_no = m.group(0).upper()

                # Look for any link whose href contains project_page/ OR whose
                # visible text contains "View Project" (handles class/layout changes)
                proj_link = row.query_selector("a[href*='project_page/']")
                if not proj_link:
                    proj_link = row.query_selector("a:has-text('View Project')")
                if proj_link:
                    href = proj_link.get_attribute("href")
                    if href:
                        result[reg_no] = _abs(href)

            browser.close()
        logger.info(
            f"Playwright: found {len(result)} View Project URLs on {listing_url}",
            step="project_page",
        )
    except Exception as exc:
        logger.warning(f"Playwright View Project extraction failed: {exc}",
                       step="project_page")
    return result


# ─── Sentinel ─────────────────────────────────────────────────────────────────

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Delhi RERA.

    Loads state_projects_sample/delhi.json as the baseline, searches listing
    pages 0 and 1 for the sentinel project, then fully enriches that row
    (project page + directors) exactly as run() would — and verifies ≥ 80%
    field coverage against the baseline.
    """
    import json as _json
    import os as _os
    from core.sentinel_utils import check_field_coverage

    sentinel_reg = config.get("sentinel_registration_no", _SENTINEL_REG)

    sample_path = _os.path.join(
        _os.path.dirname(_os.path.dirname(__file__)),
        "state_projects_sample", "delhi.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check",
                       path=sample_path, step="sentinel")
        return True

    # Step 1: verify listing page 0 parses correctly (structural check)
    page0_resp = _get_listing_response(f"{LISTING_URL}?page=0", logger)
    if not page0_resp:
        logger.error("Sentinel: listing page unreachable", step="sentinel")
        insert_crawl_error(run_id, config["id"], "SENTINEL_FAILED",
                           "Listing page unreachable", url=LISTING_URL)
        return False
    page0_rows = _parse_listing_page(page0_resp.text)
    if not page0_rows:
        logger.error("Sentinel: no rows parsed — site structure may have changed",
                     step="sentinel")
        insert_crawl_error(run_id, config["id"], "SENTINEL_FAILED",
                           "No rows found on listing page", url=LISTING_URL)
        return False

    # Step 2: scan listing pages 0-4 to find the sentinel project row.
    # The row contains both the listing-level fields (promoter name, address, etc.)
    # and the _project_page_url injected by the batch AJAX lookup in _parse_listing_page.
    fresh: dict | None = None
    proj_page_url: str | None = None

    # Check page 0 rows first (already fetched)
    found = next(
        (r for r in page0_rows if r.get("project_registration_no", "").upper() == sentinel_reg.upper()),
        None,
    )
    if found:
        fresh = found
    else:
        for page_no in range(1, 5):
            page_url = f"{LISTING_URL}?page={page_no}"
            resp = _get_listing_response(page_url, logger)
            rows = _parse_listing_page(resp.text) if resp else []
            found = next(
                (r for r in rows if r.get("project_registration_no", "").upper() == sentinel_reg.upper()),
                None,
            )
            if found:
                fresh = found
                break

    if fresh is None:
        logger.warning(
            f"Sentinel: {sentinel_reg} not found on pages 0-4, using first row for structure check",
            step="sentinel",
        )
        fresh = page0_rows[0]

    proj_page_url = fresh.pop("_project_page_url", None)
    qpr_url_sentinel = fresh.pop("_qpr_url", None)
    directors_url_sentinel = fresh.pop("_directors_url", None)

    # Derive project_page_url from QPR history if not in listing row
    # (Delhi's listing no longer carries a product_list anchor)
    if not proj_page_url and qpr_url_sentinel:
        qpr_resp = _delhi_get(qpr_url_sentinel, logger=logger)
        if qpr_resp:
            submitted_url = _extract_submitted_qprs_url(qpr_resp.text)
            if submitted_url:
                m_node = re.search(r"all-submiited-qprs-public-view/(\d+)", submitted_url)
                if m_node:
                    proj_page_url = f"{BASE_URL}/project_page/{m_node.group(1)}"
                    logger.info(
                        f"Sentinel: derived project_page_url {proj_page_url}",
                        step="sentinel",
                    )

    logger.info("Sentinel: enriching row for coverage check", reg=sentinel_reg, step="sentinel")

    # Enrich with directors — same logic as run()
    if directors_url_sentinel:
        dir_resp = _delhi_get(directors_url_sentinel, logger=logger)
        if dir_resp:
            directors = _parse_directors_page(dir_resp.text)
            if directors:
                fresh["co_promoter_details"] = [
                    {k: v for k, v in d.items() if k != "photo"}
                    for d in directors
                ]
                fresh["members_details"] = directors

    # Enrich with project page — same logic as run()
    if proj_page_url:
        proj_resp = _delhi_get(proj_page_url, logger=logger)
        if proj_resp:
            proj_detail = _parse_project_page(proj_resp.text)
            if proj_detail:
                proj_docs = proj_detail.pop("uploaded_documents", None)
                for k, v in proj_detail.items():
                    if k not in fresh:
                        fresh[k] = v
                if proj_docs:
                    fresh["uploaded_documents"] = (fresh.get("uploaded_documents") or []) + proj_docs

    # Normalize the raw crawler output so field names match the DB-level baseline
    # (e.g. land_area_sqmt → land_area, description → project_description, etc.)
    from core.project_normalizer import normalize_project_payload
    payload = {
        **fresh,
        "url":    proj_page_url or f"{LISTING_URL}?page=0",
        "domain": DOMAIN,
        "state":  config.get("state", "delhi"),
    }
    payload = {k: v for k, v in payload.items() if v not in (None, "", [], {})}
    try:
        normalized = normalize_project_payload(payload, config)
    except Exception as exc:
        logger.warning(f"Sentinel: normalization failed ({exc}), comparing raw output",
                       step="sentinel")
        normalized = fresh

    if not check_field_coverage(normalized, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "delhi_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


# ─── Document processing ──────────────────────────────────────────────────────

def _process_documents(
    project_key: str,
    documents: list[dict],
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
) -> tuple[list[dict], int]:
    """Download, MD5-check, and upload each PDF document to S3.

    Returns:
        (enriched_documents, upload_count)
        enriched_documents: same list with 's3_link' injected for uploaded docs
        upload_count: number of documents actually uploaded
    """
    enriched: list[dict] = []
    upload_count = 0

    for doc in documents:
        url      = doc.get("link", "")
        doc_type = doc.get("type", "document")
        if not url or not url.lower().endswith(".pdf"):
            enriched.append(doc)
            continue

        slug     = re.sub(r"[^a-z0-9]+", "_", doc_type.lower()).strip("_") or "document"
        filename = f"{slug}.pdf"

        try:
            resp = safe_get(url, logger=logger, timeout=60.0, verify=False)
            if not resp or len(resp.content) < 100:
                enriched.append(doc)
                logger.warning(f"Document download failed or too small: {url}", step="documents")
                continue

            data   = resp.content
            md5    = compute_md5(data)
            s3_key = upload_document(project_key, filename, data, dry_run=settings.DRY_RUN_S3)
            if s3_key is None:
                enriched.append(doc)
                logger.warning(f"S3 upload returned None: {url}", step="documents")
                continue

            s3_url = get_s3_url(s3_key)
            upsert_document(
                project_key=project_key,
                document_type=doc_type,
                original_url=url,
                s3_key=s3_key,
                s3_bucket=settings.S3_BUCKET_NAME,
                file_name=filename,
                md5_checksum=md5,
                file_size_bytes=len(data),
            )
            enriched.append({**doc, "s3_link": s3_url})
            upload_count += 1
            logger.info(f"Document uploaded: {doc_type!r}", s3_key=s3_key, step="documents")
            logger.log_document(doc_type, url, "uploaded", s3_key=s3_key, file_size_bytes=len(data))

        except Exception as exc:
            enriched.append(doc)
            logger.error(f"Document processing error: {exc}", url=url, step="documents")
            insert_crawl_error(
                run_id, site_id, "S3_UPLOAD_FAILED", str(exc),
                url=url, project_key=project_key,
            )

    return enriched, upload_count


# ─── Main entry point ─────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    """
    Args:
        config  : site dict from sites_config.SITES
        run_id  : crawl_runs.id for this run
        mode    : 'daily_light' | 'weekly_deep'
    Returns:
        dict with keys: projects_found, projects_new, projects_updated,
                        projects_skipped, documents_uploaded, error_count
    """
    logger = CrawlerLogger(config["id"], run_id)
    counters = dict(
        projects_found=0, projects_new=0, projects_updated=0,
        projects_skipped=0, documents_uploaded=0, error_count=0,
    )
    machine_name, machine_ip = get_machine_context()
    item_limit   = settings.CRAWL_ITEM_LIMIT or 0
    items_done   = 0
    delay_range  = config.get("rate_limit_delay", (2, 4))
    max_pages    = settings.MAX_PAGES
    t_run = time.monotonic()

    # ── Sentinel health check ────────────────────────────────────────────────
    t0 = time.monotonic()
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counters["error_count"] += 1
        return counters
    logger.warning(f"Step timing [sentinel]: {time.monotonic()-t0:.2f}s", step="timing")

    # ── Resume from checkpoint ────────────────────────────────────────────────
    checkpoint = load_checkpoint(config["id"], mode)
    page = (checkpoint or {}).get("last_page", 0)
    if page:
        logger.info(f"Resuming from checkpoint: page {page}", step="checkpoint")

    # ── Pagination loop ───────────────────────────────────────────────────────
    t0 = time.monotonic()
    first_page_logged = False
    while True:
        page_url = f"{LISTING_URL}?page={page}"
        logger.info(f"Fetching listing page {page}", url=page_url, step="listing")

        resp = _get_listing_response(page_url, logger)
        if not resp:
            logger.error(f"Failed to fetch page {page}", step="listing")
            insert_crawl_error(
                run_id, config["id"], "HTTP_ERROR",
                f"page {page} fetch failed", url=page_url,
            )
            counters["error_count"] += 1
            break

        html = resp.text
        rows = _parse_listing_page(html)
        if not rows:
            logger.info(f"No rows on page {page} — pagination complete", step="listing")
            break

        counters["projects_found"] += len(rows)
        logger.info(f"Page {page}: {len(rows)} rows", step="listing")
        if not first_page_logged:
            logger.warning(f"Step timing [search]: {time.monotonic()-t0:.2f}s  rows={len(rows)}", step="timing")
            first_page_logged = True

        # ── Playwright fallback: click "View Project" for any rows missing the URL ──
        # The static HTML may not expose project_page/ links (class change / JS render).
        # One Playwright session per listing page covers all rows at once.
        if any(not r.get("_project_page_url") for r in rows) and settings.SCRAPE_DETAILS:
            pw_urls = _fetch_project_page_urls_playwright(page_url, logger)
            if pw_urls:
                for r in rows:
                    reg = r.get("project_registration_no", "").upper()
                    if not r.get("_project_page_url") and reg in pw_urls:
                        r["_project_page_url"] = pw_urls[reg]

        stop_all = False
        for row in rows:
            if item_limit and items_done >= item_limit:
                logger.info(f"Item limit {item_limit} reached — stopping", step="listing")
                stop_all = True
                break

            reg_no = row.get("project_registration_no", "").strip().upper()
            if not reg_no:
                counters["error_count"] += 1
                continue

            key = generate_project_key(reg_no)

            # ── daily_light: skip projects already in the DB ──────────────────
            if mode == "daily_light" and get_project_by_key(key):
                counters["projects_skipped"] += 1
                continue

            logger.set_project(key=key, reg_no=reg_no, url=page_url, page=page)
            try:
                # ── Enrich: fetch directors and project details page ──────────
                fetch_directors  = config.get("fetch_directors", True)
                fetch_qpr        = config.get("fetch_qpr_history", False)
                directors_url    = row.pop("_directors_url", None)
                proj_page_url    = row.pop("_project_page_url", None)
                qpr_url          = row.pop("_qpr_url", None)

                if fetch_directors and directors_url and settings.SCRAPE_DETAILS:
                    dir_resp = _delhi_get(directors_url, logger=logger)
                    if dir_resp:
                        directors = _parse_directors_page(dir_resp.text)
                        if directors:
                            row["co_promoter_details"] = [
                                {k: v for k, v in d.items() if k != "photo"}
                                for d in directors
                            ]
                            row["members_details"] = directors
                            logger.info(
                                f"Fetched {len(directors)} director(s) for {reg_no}",
                                step="directors",
                            )
                        random_delay(*delay_range)

                # ── Derive project_page_url from QPR history if listing has no link ──
                # Delhi's listing no longer includes a "View Project" (product_list)
                # anchor. Fetch the QPR history page — its view-header carries
                # all-submiited-qprs-public-view/{node_id} which gives us the
                # project node ID needed to build the project_page URL.
                if not proj_page_url and qpr_url and settings.SCRAPE_DETAILS:
                    qpr_resp = _delhi_get(qpr_url, logger=logger)
                    if qpr_resp:
                        submitted_url = _extract_submitted_qprs_url(qpr_resp.text)
                        if submitted_url:
                            m_node = re.search(r"all-submiited-qprs-public-view/(\d+)",
                                               submitted_url)
                            if m_node:
                                proj_page_url = f"{BASE_URL}/project_page/{m_node.group(1)}"
                                logger.info(
                                    f"Derived project_page_url from QPR history: {proj_page_url}",
                                    step="project_page",
                                )
                        # Also capture QPR history if enabled (avoid a second fetch)
                        if fetch_qpr and not row.get("status_update"):
                            qpr_history = _parse_qpr_history(qpr_resp.text)
                            if qpr_history:
                                row["status_update"] = qpr_history
                                logger.info(
                                    f"Fetched {len(qpr_history)} QPR entries for {reg_no}",
                                    step="qpr_history",
                                )
                    random_delay(*delay_range)
                elif fetch_qpr and qpr_url and settings.SCRAPE_DETAILS:
                    qpr_resp = _delhi_get(qpr_url, logger=logger)
                    if qpr_resp:
                        qpr_history = _parse_qpr_history(qpr_resp.text)
                        if qpr_history:
                            row["status_update"] = qpr_history
                            logger.info(
                                f"Fetched {len(qpr_history)} QPR entries for {reg_no}",
                                step="qpr_history",
                            )
                    random_delay(*delay_range)

                # ── Fallback: scan project_page nodes near the promoter node ──
                # Brand-new projects that have not yet started any QPR cycle have
                # no all-submiited-qprs-public-view link in their QPR history page,
                # so the node-ID derivation above yields nothing.  The project_page
                # Drupal node is always created slightly after the promoter node
                # (historically within 200 nodes).  Scan upward from
                # promoter_node+1 looking for the first project_page page that
                # links back to promoter_page/{promoter_node}, then use that URL.
                if not proj_page_url and directors_url and settings.SCRAPE_DETAILS:
                    m_promo = re.search(r"promoter_directors/(\d+)", directors_url)
                    if m_promo:
                        promoter_node_id = int(m_promo.group(1))
                        proj_page_url = _find_project_page_by_promoter_node(
                            promoter_node_id, logger=logger,
                        )
                        if proj_page_url:
                            logger.info(
                                f"project_page for {reg_no} discovered via promoter"
                                f" scan: {proj_page_url}",
                                step="project_page_scan",
                            )
                    random_delay(*delay_range)

                if proj_page_url and settings.SCRAPE_DETAILS:
                    proj_resp = _delhi_get(proj_page_url, logger=logger)
                    if proj_resp:
                        proj_detail = _parse_project_page(proj_resp.text)
                        if proj_detail:
                            # Merge uploaded_documents lists (don't overwrite)
                            proj_docs = proj_detail.pop("uploaded_documents", None)
                            for k, v in proj_detail.items():
                                if k not in row:
                                    row[k] = v
                            if proj_docs:
                                existing = row.get("uploaded_documents") or []
                                row["uploaded_documents"] = existing + proj_docs
                            logger.info(
                                f"Fetched {proj_page_url} ({len(proj_detail)} fields)"
                                f" for {reg_no}",
                                step="project_page",
                            )
                    random_delay(*delay_range)

                # ── Build, normalize, upsert ──────────────────────────────────
                try:
                    payload: dict = {
                        **row,
                        "url":    proj_page_url or page_url,
                        "domain": DOMAIN,
                        "state":  config.get("state", "delhi"),
                        "is_live": True,
                    }
                    payload = {k: v for k, v in payload.items() if v not in (None, "", [], {})}

                    normalized = normalize_project_payload(
                        payload, config,
                        machine_name=machine_name,
                        machine_ip=machine_ip,
                    )
                    record  = ProjectRecord(**normalized)
                    db_dict = record.to_db_dict()
                    status  = upsert_project(db_dict)
                    items_done += 1

                    if status == "new":
                        counters["projects_new"] += 1
                        logger.info(f"New project: {reg_no}", step="upsert")
                    elif status == "updated":
                        counters["projects_updated"] += 1
                        logger.info(f"Updated: {reg_no}", step="upsert")
                    else:
                        counters["projects_skipped"] += 1

                    # ── Documents ──────────────────────────────────────────────
                    docs = row.get("uploaded_documents") or []
                    if docs:
                        enriched, doc_count = _process_documents(
                            key, docs, run_id, config["id"], logger,
                        )
                        counters["documents_uploaded"] += doc_count
                        upsert_project({
                            "key":                key,
                            "url":                db_dict["url"],
                            "state":              db_dict["state"],
                            "domain":             db_dict["domain"],
                            "project_registration_no": db_dict["project_registration_no"],
                            "uploaded_documents": enriched,
                            "document_urls":      build_document_urls(enriched),
                        })

                except ValidationError as exc:
                    counters["error_count"] += 1
                    logger.error(f"Validation error for {reg_no}: {exc}", step="validate")
                    insert_crawl_error(
                        run_id, config["id"], "VALIDATION_FAILED", str(exc),
                        project_key=key, url=page_url,
                    )
                except Exception as exc:
                    counters["error_count"] += 1
                    logger.error(f"Unexpected error for {reg_no}: {exc}", step="upsert")
                    insert_crawl_error(
                        run_id, config["id"], "EXTRACTION_FAILED", str(exc),
                        project_key=key, url=page_url,
                    )
            finally:
                logger.clear_project()

            random_delay(*delay_range)

        # ── Checkpoint + advance ───────────────────────────────────────────
        save_checkpoint(config["id"], mode, page, None, run_id)

        if stop_all:
            break
        if max_pages is not None and page >= max_pages - 1:
            logger.info(f"Reached max_pages={max_pages}, stopping", step="listing")
            break
        if not _has_next_page(html):
            logger.info("No more pages in Drupal pager", step="listing")
            break

        page += 1
        random_delay(*delay_range)

    reset_checkpoint(config["id"], mode)
    logger.info(f"Delhi RERA complete: {counters}", step="done")
    logger.warning(f"Step timing [total_run]: {time.monotonic()-t_run:.2f}s", step="timing")
    return counters
