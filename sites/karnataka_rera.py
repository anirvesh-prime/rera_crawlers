"""
Karnataka RERA Crawler — rera.karnataka.gov.in
Type: static (httpx + BeautifulSoup)

How the portal works (observed from live HTML):
- The listing endpoint that actually honours the district filter is
  /projectViewDetails (the form on /viewAllProjects POSTs there).  The crawler
  POSTs each district name with payload
      {appNo: "", regNo: "", project: "", firm: "",
       district: <DistrictName>, subdistrict: "0"}
  and reads the HTML <table> at the top of the response — columns include
  ACKNOWLEDGEMENT NO, REGISTRATION NO, PROMOTER NAME, PROJECT NAME, STATUS,
  DISTRICT, TALUK, PROJECT TYPE, APPROVED ON, ...
  Only rows that carry a "showFileApplicationPreview" view link are drillable
  (rejected applications have the link stripped and are skipped).
  Note: /viewAllProjects itself always returns the unfiltered global catalog
  (~9.6k projects embedded as `localObj.appNo` JS arrays) regardless of any
  POST parameters — using it for district traversal silently mislabels every
  candidate with the wrong district.

- Detail fetch (TWO-STEP, as of 2025):
  Step 1: POST /projectViewDetails with appNo=<ack_no>
          → Returns search-results table containing the internal numeric DB ID
            (<a id="<numeric_id>" onclick="return showFileApplicationPreview(this);">)
            and the APPROVED ON date.
  Step 2: POST /projectDetails with action=<numeric_id>
          → Returns full project detail page (HTML ~200–450 KB).
  The portal no longer accepts the ack_no directly as action parameter (returns 400).

- The detail page uses a Bootstrap grid layout (col-md-3 div pairs) for most fields.
  Project Name / Ack No / Reg No appear in <span class="user_name"> elements.

- Registration certificate: GET /certificate?CER_NO=<registration_no>
- Document downloads: GET /download_jc?DOC_ID=<encoded_id>
  (skip links where DOC_ID query param is blank)

- Canonical URL stored per project: https://rera.karnataka.gov.in/projectViewDetails
  (no per-project URL path exists on the public portal)
"""
from __future__ import annotations

import base64
import mimetypes
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

from bs4 import BeautifulSoup, Tag
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import SeleniumSession, generate_project_key, random_delay
from core.db import (
    get_project_by_key,
    upsert_project,
    insert_crawl_error,
    upsert_document,
    update_crawl_run_progress,
)
from core.details_pool import get_detail_workers, process_details
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

BASE_URL    = "https://rera.karnataka.gov.in"
LISTING_URL = f"{BASE_URL}/viewAllProjects"      # search-form page
DETAIL_URL  = f"{BASE_URL}/projectDetails"
CERT_URL    = f"{BASE_URL}/certificate"
PROJECT_URL = f"{BASE_URL}/projectViewDetails"   # form action (search results page)
DOMAIN      = "rera.karnataka.gov.in"
STATE_CODE  = "KA"


# ── SeleniumSession wiring ────────────────────────────────────────────────────

_SESSION: SeleniumSession | None = None
# Detail-pool runs N workers against the single shared headless-Chrome driver;
# every search / detail interaction navigates the browser, so concurrent calls
# would race over the active URL and DOM. Serialise all browser-driven work
# through this lock — the driver itself is effectively single-threaded anyway.
_DRIVER_LOCK = threading.Lock()


def _session() -> SeleniumSession:
    """Return the active SeleniumSession, lazy-initialising on first use.

    The portal's /viewAllProjects and /projectViewDetails responses are ~6 MB
    each (uncompressed — the server doesn't honour ``Accept-Encoding: gzip``)
    and dominated by inline <script> blocks that push ~9.6k localObj entries
    for an autocomplete dataset the crawler never uses.  Executing those
    blocks dominates page-load time — ~5s on a fast laptop, >60s on the
    2 vCPU production droplet — so launch Chrome with
    ``--blink-settings=scriptEnabled=false`` to skip in-page JS entirely.

    With page JS off:
      * The form's <input name="btn1"> submit button still posts the form
        natively (handled by Blink, not page JS); we hide the document-ready
        loader overlay via ``execute_script`` so it doesn't intercept the
        click.  ``execute_script`` itself still runs because WebDriver's
        Runtime.evaluate path is independent of Blink's scriptEnabled.
      * The results ``<table id="approvedTable">`` is fully rendered
        server-side, so DataTables never runs and every row is in the static
        HTML — no row-expansion step is needed.
      * The detail-page modal (``showFileApplicationPreview``) is replaced
        by a direct ``fetch('/projectDetails')`` issued from
        ``execute_async_script`` — that runs in WebDriver's execution context
        regardless of Blink's scriptEnabled flag.
    """
    global _SESSION
    if _SESSION is None:
        _SESSION = SeleniumSession(
            ignore_certificate_errors=True,
            extra_chrome_args=("--blink-settings=scriptEnabled=false",),
        )
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


def download_response(url, *, method="GET", data=None, headers=None,
                      logger=None, **_ignored):
    """Binary-download shim — dispatches through the SeleniumSession so PDFs
    inherit the browser's cookies + TLS trust store.  Used only for document
    downloads; all page-content interaction now goes through real browser
    navigation (see ``_submit_search_form`` / ``_fetch_detail``).
    """
    return _session().download(url, method=method, data=data, headers=headers, logger=logger)

# All 31 Karnataka districts as they appear in the portal's <select> options.
# A district must be selected — blank search returns zero results.
DISTRICTS: list[str] = [
    "Bagalkot", "Ballari", "Belagavi",
    "Bengaluru  Rural",   # note: two spaces — matches portal option value exactly
    "Bengaluru Urban", "Bidar", "Chamarajanagar", "Chikkaballapura",
    "Chikkamagaluru", "Chitradurga", "Dakshina Kannada", "Davangere",
    "Dharwad", "Gadag", "Hassan", "Haveri", "Kalaburagi", "Kodagu",
    "Kolar", "Koppal", "Mandya", "Mysore", "Raichur", "Ramanagara",
    "Shivamogga", "Tumakuru", "Udupi", "Uttara Kannada", "Vijayanagara",
    "Vijayapura", "Yadgir",
]

# Map lowercased Karnataka portal labels (from detail HTML) → schema field names.
# Covers both the old fragment-style labels and the new Bootstrap-grid labels.
_LABEL_MAP: dict[str, str] = {
    # Project identity
    "project name":                                                 "project_name",
    "project type":                                                 "project_type",
    "type of project":                                              "project_type",
    "registration no":                                              "project_registration_no",
    "application no":                                               "acknowledgement_no",
    "acknowledgement no":                                           "acknowledgement_no",
    "status":                                                       "status_of_the_project",
    "project status":                                               "status_of_the_project",
    # Promoter
    "promoter / company / firm name":                               "promoter_name",
    "promoter name":                                                "promoter_name",
    "company name":                                                 "promoter_name",
    # GST / PAN / registration
    "gst no":                                                       "_gst_no",
    "gstin":                                                        "_gst_no",
    "pan no":                                                       "_pan_no",
    "pan":                                                          "_pan_no",
    "trade licence / registration no":                              "_trade_reg_no",
    "registration number":                                          "_trade_reg_no",
    "objective":                                                    "_objective",
    "main objectives":                                              "_objective",
    # Location
    "district":                                                     "_district",
    "taluk":                                                        "_taluk",
    "village":                                                      "_village",
    "pin code":                                                     "_pin_code",
    "survey / resurvey number":                                     "_survey_no",
    "latitude":                                                     "_latitude",
    "longitude":                                                    "_longitude",
    # Website
    "website":                                                      "_website",
    "promoter website":                                             "_website",
    # Project address (full address string for raw_address)
    "project address":                                              "_project_address",
    # Dates
    "date of commencement":                                         "actual_commencement_date",
    "project start date":                                           "actual_commencement_date",
    "estimated date of commencement":                               "estimated_commencement_date",
    "proposed date of commencement":                                "estimated_commencement_date",
    "estimated commencement date":                                  "estimated_commencement_date",
    "proposed start date":                                          "estimated_commencement_date",
    "proposed date of completion":                                  "estimated_finish_date",
    "proposed completion date":                                     "estimated_finish_date",
    "project end date":                                             "estimated_finish_date",
    "completion date":                                              "actual_finish_date",
    "date of approval":                                             "approved_on_date",
    # Costs — new verbose labels + legacy short labels
    "cost of land":                                                 "_cost_of_land",
    "cost of land (inr) (c1)( as certified by ca in form 1 )":     "_cost_of_land",
    "estimated construction cost":                                  "_est_construction_cost",
    "cost of layout development (inr) (c2)( as certified by ca in form 1 )": "_est_construction_cost",
    "total construction cost":                                      "_est_construction_cost",
    "total project cost":                                           "_total_project_cost",
    "total project cost (inr) (c1+c2)":                            "_total_project_cost",
    # Land
    "land area":                                                    "land_area",
    "extent":                                                       "land_area",
    "extent of land":                                               "land_area",
    "total land area":                                              "land_area",
    # Construction area
    "construction area":                                            "construction_area",
    "total construction area":                                      "construction_area",
    "built up area":                                                "construction_area",
    # Units / plots
    "number of plots":                                              "number_of_residential_units",
    "total number of plots":                                        "number_of_residential_units",
    "total no. of plots":                                           "number_of_residential_units",
    "no. of plots":                                                 "number_of_residential_units",
    "number of residential units":                                  "number_of_residential_units",
    "total no. of units":                                           "number_of_residential_units",
    "number of commercial units":                                   "number_of_commercial_units",
    "no. of commercial units":                                      "number_of_commercial_units",
    # Bank
    "bank name":                                                    "_bank_name",
    "account no":                                                   "_account_no",
    "account no.(70% account)":                                     "_account_no",
    "account name":                                                 "_account_name",
    "ifsc":                                                         "_ifsc",
    "ifsc code":                                                    "_ifsc",
    "branch":                                                       "_branch",
    # Progress
    "total completion percentage":                                  "_total_completion_pct",
    "extent of development carried till date":                      "_total_completion_pct",
    # Description
    "project description":                                          "project_description",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _clean(text: Any) -> str:
    if not text:
        return ""
    if isinstance(text, (list, tuple)):
        text = " ".join(_clean(t) for t in text)
    elif not isinstance(text, str):
        text = str(text)
    return re.sub(r"\s+", " ", text).strip()


def _row_values(row: dict) -> list:
    """Ordered cell values from a _parse_section_table row, excluding the
    auxiliary '__links' entries so positional indexing matches the columns."""
    return [v for k, v in row.items() if not str(k).endswith("__links")]


def _has_name_column(rows: list[dict]) -> bool:
    """True if the parsed table has a column header containing 'name'.
    Empty professional sections on the new portal (a bare <h1> with no table)
    bind to an unrelated later table; a genuine professional table always has
    a '<Role> Name' column, so this guards against that mis-binding."""
    if not rows:
        return False
    return any("name" in str(col).lower()
               for col in rows[0] if not str(col).endswith("__links"))


def _safe_float(val: str | None) -> float | None:
    if not val:
        return None
    try:
        return float(re.sub(r"[^\d.]", "", val))
    except (ValueError, TypeError):
        return None


# ── Listing ───────────────────────────────────────────────────────────────────

def _parse_search_table(html: str) -> list[dict]:
    """Parse the search-results <table> returned by POST /projectViewDetails.

    Returns a list of dicts (one per drillable row) with header-derived keys:
    ``acknowledgement_no``, ``project_registration_no``, ``project_name``,
    ``promoter_name``, ``district``, ``status``, ``project_type``,
    ``approved_on``.  Rows without a ``showFileApplicationPreview`` view link
    (e.g. REJECTED applications) are skipped — the detail fetch needs the
    numeric DB id that link carries.
    """
    soup = BeautifulSoup(html, "lxml")
    # DataTables with scrollable headers splits the source <table> into two
    # sibling tables: a header-only "scrollHead" copy and a body table
    # (``id="approvedTable"``) that actually carries the data rows.  Prefer
    # the table whose rows contain a ``showFileApplicationPreview`` anchor;
    # fall back to ``#approvedTable`` and finally to the first <table>.
    tbl = None
    for cand in soup.find_all("table"):
        if cand.find("a", onclick=lambda s: s and "showFileApplicationPreview" in s):
            tbl = cand
            break
    if tbl is None:
        tbl = soup.find("table", id="approvedTable") or soup.find("table")
    if not tbl:
        return []
    trs = tbl.find_all("tr")
    if len(trs) < 2:
        return []
    headers = [_clean(c.get_text()).lower()
               for c in trs[0].find_all(["th", "td"])]

    def col(*needles: str) -> int:
        for needle in needles:
            for i, h in enumerate(headers):
                if needle in h:
                    return i
        return -1

    ack_i   = col("acknowledg", "application")
    reg_i   = col("registration")
    name_i  = col("project name")
    prom_i  = col("promoter")
    dist_i  = col("district")
    stat_i  = col("status")
    type_i  = col("project type")
    appr_i  = col("approved on")

    out: list[dict] = []
    for row in trs[1:]:
        if not row.find("a", onclick=lambda s: s and "showFileApplicationPreview" in s):
            continue
        cells = [_clean(c.get_text()) for c in row.find_all("td")]
        def at(i: int) -> str:
            return cells[i] if 0 <= i < len(cells) else ""
        ack_no = at(ack_i)
        if not ack_no:
            continue
        out.append({
            "acknowledgement_no":      ack_no,
            "project_registration_no": at(reg_i) or None,
            "project_name":            at(name_i) or None,
            "promoter_name":           at(prom_i) or None,
            "district":                at(dist_i),
            "status":                  at(stat_i),
            "project_type":            at(type_i),
            "approved_on":             at(appr_i),
        })
    return out


def _submit_search_form(
    *,
    district: str = "0",
    app_no: str = "",
    reg_no: str = "",
    logger: CrawlerLogger,
    step: str,
) -> str | None:
    """Drive the actual browser through the /viewAllProjects search form and
    return the rendered results-page HTML.

    Loads the form page, fills the requested field(s) via DOM interaction
    (select-by-value on the District dropdown; ``send_keys`` on the Application
    No / Registration No inputs), clicks the Search submit button, waits for
    the results <table> to render on /projectViewDetails, and returns
    ``driver.page_source``.  Returns ``None`` on navigation timeout or
    WebDriver errors so callers can checkpoint-and-continue.

    Held under ``_DRIVER_LOCK`` so the detail-pool's parallel workers do not
    race over the single shared headless-Chrome driver.
    """
    from selenium.common.exceptions import (
        NoSuchElementException, TimeoutException, WebDriverException,
    )
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import Select, WebDriverWait

    # Bengaluru Urban returns ~4k rows / ~6 MB of HTML and the portal is slow,
    # so the form-submit navigation regularly exceeds short page-load budgets.
    # Use a generous page-load cap and retry once on TimeoutException.
    with _DRIVER_LOCK:
        last_exc: Exception | None = None
        for attempt in range(2):
            try:
                driver = _session().driver()
                driver.set_page_load_timeout(120.0)
                driver.get(LISTING_URL)
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.ID, "projectDist"))
                )
                if district and district != "0":
                    Select(driver.find_element(By.ID, "projectDist")).select_by_value(district)
                if app_no:
                    el = driver.find_element(By.ID, "regNo")     # name="appNo"
                    el.clear(); el.send_keys(app_no)
                if reg_no:
                    el = driver.find_element(By.ID, "regNo2")    # name="regNo"
                    el.clear(); el.send_keys(reg_no)
                # Page JS is disabled (see ``_session``), so the document-ready
                # handler that hides ``<div id="loader">`` never fires and the
                # overlay intercepts the button click.  Hide it manually and
                # submit the form natively — ``form.submit()`` is a Blink-
                # level action that doesn't need page JS.
                driver.execute_script(
                    "var l=document.getElementById('loader');"
                    " if(l){l.style.display='none';}"
                    " document.querySelector('input[name=\"btn1\"]').form.submit();"
                )
                # Wait for the actual results table — ``#approvedTable`` is
                # unique to /projectViewDetails (the form page has zero
                # ``<table>`` elements), so this doubles as a navigation gate.
                WebDriverWait(driver, 90).until(
                    EC.presence_of_element_located((By.ID, "approvedTable"))
                )
                # DataTables never runs (page JS off) so every row is in the
                # static HTML — no expand step needed.
                return driver.page_source
            except (TimeoutException, NoSuchElementException, WebDriverException) as exc:
                last_exc = exc
                # Abort any half-loaded navigation before the retry so the next
                # ``driver.get`` starts from a clean state.
                try:
                    driver.execute_script("window.stop();")
                except Exception:
                    pass
                if attempt == 0:
                    logger.info(
                        f"Browser search retry 1/1 after {exc.__class__.__name__}: "
                        f"district={district!r} app_no={app_no!r} reg_no={reg_no!r}",
                        step=step,
                    )
                    continue
        logger.warning(
            f"Browser search failed ({last_exc.__class__.__name__ if last_exc else 'unknown'}): "
            f"district={district!r} app_no={app_no!r} reg_no={reg_no!r}",
            step=step,
        )
        return None


def _post_listing(district: str, logger: CrawlerLogger) -> str | None:
    """Drive the browser through the search form for one district and return
    the rendered results-page HTML.

    The portal's /viewAllProjects page renders a form whose <select
    name='district'> filter is honoured only when the form is submitted; the
    raw endpoint returns the global catalogue.  Selecting the district from
    the rendered dropdown and clicking Search reproduces the user flow
    exactly.
    """
    return _submit_search_form(district=district, logger=logger, step="listing")


def _search_by_reg_no(reg_no: str, logger: CrawlerLogger) -> dict | None:
    """Direct lookup: drive the browser through the search form with the
    Registration No field filled in and parse the single-row search result
    into a listing-row dict.  Returns ``None`` when the portal yields no
    matching row.  Used by the ``--target-reg-no`` shortcut so targeted runs
    skip the district walk entirely.
    """
    html = _submit_search_form(reg_no=reg_no, logger=logger, step="listing")
    if not html:
        return None
    target = reg_no.strip().upper()
    for r in _parse_search_table(html):
        if (r.get("project_registration_no") or "").upper() != target:
            continue
        district = r.get("district") or ""
        return {
            "acknowledgement_no":       r["acknowledgement_no"],
            "project_registration_no":  r["project_registration_no"],
            "project_name":             r["project_name"],
            "promoter_name":            r["promoter_name"],
            "promoter_registration_no": None,
            "project_city":             district.upper(),
            "project_location_raw":     {"district": district} if district else {},
            "data": {
                "search_district":      district,
                "listing_fallback":     True,
                "target_lookup":        True,
                "listing_status":       r.get("status") or "",
                "listing_project_type": r.get("project_type") or "",
                "listing_approved_on":  r.get("approved_on") or "",
            },
        }
    return None


def _extract_listing_rows(html: str, district: str) -> list[dict]:
    """Recover per-project listing data from the search-results <table>.

    The table is rendered by POST /projectViewDetails for the requested
    district and contains ACK NO, REGISTRATION NO, PROJECT/PROMOTER NAME,
    STATUS, DISTRICT, TALUK, PROJECT TYPE and APPROVED ON columns directly —
    so the project key can be generated at listing time, avoiding a detail-page
    fetch for ``daily_light`` dedup checks.
    """
    rows: list[dict] = []
    for r in _parse_search_table(html):
        rows.append({
            "acknowledgement_no":       r["acknowledgement_no"],                # FIELD: acknowledgement_no <- search table "ACKNOWLEDGEMENT NO"
            "project_registration_no":  r["project_registration_no"],           # FIELD: project_registration_no <- search table "REGISTRATION NO"
            "project_name":             r["project_name"],                      # FIELD: project_name <- search table "PROJECT NAME"
            "promoter_name":            r["promoter_name"],                     # FIELD: promoter_name <- search table "PROMOTER NAME"
            "promoter_registration_no": None,                                   # FIELD: promoter_registration_no <- None (not in listing table)
            "project_city":             district.upper(),                       # FIELD: project_city <- searched district (upper)
            "project_location_raw":     {"district": district},                 # FIELD: project_location_raw.district <- searched district
            "data": {
                "search_district":      district,                               # FIELD: data.search_district <- searched district
                "listing_fallback":     True,                                   # FIELD: data.listing_fallback <- literal True
                "listing_status":       r.get("status") or "",                  # FIELD: data.listing_status <- search table "STATUS"
                "listing_project_type": r.get("project_type") or "",            # FIELD: data.listing_project_type <- search table "PROJECT TYPE"
                "listing_approved_on":  r.get("approved_on") or "",             # FIELD: data.listing_approved_on <- search table "APPROVED ON"
            },
        })
    return rows


# ── Detail page fetching ─────────────────────────────────────────────────────

def _fetch_detail(
    ack_no: str,
    logger: CrawlerLogger,
    reg_no: str | None = None,
) -> tuple[str | None, dict]:
    """
    Drive the rendered browser through the two-step detail flow:
      1. Submit the /viewAllProjects search form with Registration No = <reg_no>
         → parse the rendered results <table> → extract numeric DB id +
         approved_on / status / project_type from the matching row.
      2. Click the row's "View Project Details" anchor (which fires the portal's
         own ``showFileApplicationPreview`` AJAX into the ``#result`` div) and
         read the rendered detail HTML back out of that div.

    Returns (html | None, meta_dict).  meta_dict keys: approved_on_date,
    status_of_the_project, project_type_listing.
    """
    from selenium.common.exceptions import (
        NoSuchElementException, TimeoutException, WebDriverException,
    )
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    if not reg_no:
        logger.warning(
            f"No registration number available for {ack_no!r} — cannot fetch detail",
            step="detail",
        )
        return None, {}

    # Step 1: search by Registration No to land on the results page.
    search_html = _submit_search_form(reg_no=reg_no, logger=logger, step="detail")
    if not search_html:
        return None, {}

    soup = BeautifulSoup(search_html, "lxml")
    # DataTables with scrollX splits the results <table> into two siblings:
    # a header-only "scrollHead" clone and the body table (``id="approvedTable"``)
    # that actually carries the data rows.  Prefer the table whose rows contain
    # a ``showFileApplicationPreview`` anchor; fall back to ``#approvedTable``
    # and finally to the first <table>.
    tbl = None
    for cand in soup.find_all("table"):
        if cand.find("a", onclick=lambda s: s and "showFileApplicationPreview" in s):
            tbl = cand
            break
    if tbl is None:
        tbl = soup.find("table", id="approvedTable") or soup.find("table")
    if not tbl:
        logger.warning(f"No table in search results for {reg_no!r}", step="detail")
        return None, {}

    rows = tbl.find_all("tr")
    numeric_id: str | None = None
    meta: dict = {}

    if rows:
        hdr_cells = rows[0].find_all(["th", "td"])
        headers = [_clean(c.get_text()).lower() for c in hdr_cells]
        approved_idx = next((i for i, h in enumerate(headers) if "approved on" in h), -1)
        status_idx   = next((i for i, h in enumerate(headers) if h.strip() == "status"), -1)
        type_idx     = next((i for i, h in enumerate(headers) if "project type" in h), -1)
        for row in rows[1:]:
            a = row.find("a", onclick=lambda s: s and "showFileApplicationPreview" in s)
            if not a:
                continue
            numeric_id = a.get("id", "")
            cells = row.find_all("td")
            if approved_idx >= 0 and approved_idx < len(cells):
                raw_date = _clean(cells[approved_idx].get_text())
                parsed = parse_datetime(raw_date)
                meta["approved_on_date"] = (
                    parsed.strftime("%Y-%m-%d %H:%M:%S+00:00") if parsed else None
                )
            if status_idx >= 0 and status_idx < len(cells):
                meta["status_of_the_project"] = _clean(cells[status_idx].get_text())
            if type_idx >= 0 and type_idx < len(cells):
                meta["project_type_listing"] = _clean(cells[type_idx].get_text())
            break

    if not numeric_id:
        logger.warning(f"Numeric DB id not found for {reg_no!r}", step="detail")
        return None, meta

    # Step 2: issue the same POST /projectDetails request the portal's own
    # ``showFileApplicationPreview`` handler would have fired, but do it from
    # the browser's WebDriver context via ``fetch`` — page JS is disabled
    # (see ``_session``) so the original click handler isn't wired up, and
    # WebDriver's Runtime.evaluate is independent of Blink's scriptEnabled
    # flag.  Issuing the fetch from the page preserves the browser's cookie
    # and TLS state without any extra setup.
    with _DRIVER_LOCK:
        try:
            driver = _session().driver()
            # ``execute_async_script`` uses the driver's script timeout, not
            # the page-load timeout — give the fetch a generous budget since
            # the detail response can be ~400 KB and the portal is slow.
            driver.set_script_timeout(60.0)
            detail_html = driver.execute_async_script(
                "var id = arguments[0];"
                " var cb = arguments[arguments.length - 1];"
                " fetch('/projectDetails', {"
                "   method: 'POST',"
                "   headers: {"
                "     'Content-Type': 'application/x-www-form-urlencoded',"
                "     'X-Requested-With': 'XMLHttpRequest'"
                "   },"
                "   body: 'action=' + encodeURIComponent(id),"
                "   credentials: 'include'"
                " }).then(function(r){return r.text();})"
                "   .then(function(t){cb(t);})"
                "   .catch(function(){cb(null);});",
                numeric_id,
            )
        except (TimeoutException, NoSuchElementException, WebDriverException) as exc:
            logger.warning(
                f"Detail navigation failed for numeric_id={numeric_id!r}: "
                f"{exc.__class__.__name__}",
                step="detail",
            )
            return None, meta

    if not detail_html:
        logger.warning(
            f"Empty detail response for numeric_id={numeric_id!r}",
            step="detail",
        )
        return None, meta

    return detail_html, meta


# ── Detail page parsing ───────────────────────────────────────────────────────

def _extract_kv_pairs(soup: BeautifulSoup) -> dict[str, str]:
    """
    Extract label→value pairs from tr/td rows (still used for address fields
    like 'present address', 'permanent address', 'official address').
    """
    result: dict[str, str] = {}
    for row in soup.find_all("tr"):
        cells = row.find_all("td", recursive=False)
        if len(cells) >= 2:
            raw_key = _clean(cells[0].get_text()).rstrip(":")
            raw_val = _clean(cells[1].get_text())
            key = raw_key.lower().strip()
            if key and raw_val and len(key) < 120 and key not in result:
                result[key] = raw_val
    return result


def _extract_grid_kv(soup: BeautifulSoup) -> dict[str, str]:
    """
    Extract label→value pairs from Bootstrap grid div pairs.
    Handles col-md-3/col-md-3, col-md-3/col-md-9, and col-md-6/col-md-6 layouts.
    The new portal renders fields as adjacent div pairs where the first div
    contains a <p class="text-right"> label and the second contains the value.
    Iterates ALL direct child divs (not just col-md-3) to support all column widths.
    """
    result: dict[str, str] = {}
    for row_div in soup.find_all("div", class_="row"):
        cols = row_div.find_all("div", recursive=False)
        for i in range(0, len(cols) - 1, 2):
            label_div = cols[i]
            value_div = cols[i + 1]
            label_p = label_div.find("p")
            value_tag = value_div.find(["p", "pre"])
            if not label_p or not value_tag:
                continue
            # Strip the decorative colon-span from the label
            for span in label_p.find_all("span", class_="space_LR"):
                span.decompose()
            label = _clean(label_p.get_text()).rstrip(":").lower().strip()
            value = _clean(value_tag.get_text())
            if label and value and len(label) < 200 and label not in result:
                result[label] = value
    return result


def _extract_header_fields(soup: BeautifulSoup) -> dict[str, str]:
    """
    Extract Project Name / Acknowledgement Number / Registration Number from
    <span class="user_name"> elements in the detail page header.
    """
    result: dict[str, str] = {}
    for span in soup.find_all("span", class_="user_name"):
        text = _clean(span.get_text())
        m = re.match(r"^([^:]+)\s*:\s*(.+)$", text)
        if not m:
            continue
        label = m.group(1).strip().lower()
        value = m.group(2).strip()
        if "project name" in label:
            result["project_name"] = value                        # FIELD: project_name <- detail <span class="user_name"> "project name"
        elif "acknowledgement" in label:
            result["acknowledgement_no"] = value                  # FIELD: acknowledgement_no <- detail <span class="user_name"> "acknowledgement"
        elif "registration" in label:
            result["project_registration_no"] = value             # FIELD: project_registration_no <- detail <span class="user_name"> "registration"
    return result


def _parse_section_table(soup: BeautifulSoup, heading_keywords: list[str]) -> list[dict]:
    """
    Find the first <table> whose nearest preceding heading text contains any
    of the given keywords. Returns list of header-keyed row dicts.
    Searches h1 (new portal) as well as h2/h3/h4/b/strong/th (legacy).
    """
    for el in soup.find_all(["h1", "h2", "h3", "h4", "b", "strong", "th"]):
        if not any(kw in _clean(el.get_text()).lower() for kw in heading_keywords):
            continue
        tbl = el.find_next("table")
        if not tbl:
            continue
        rows = tbl.find_all("tr")
        if len(rows) < 2:
            continue
        headers = [_clean(c.get_text()) for c in rows[0].find_all(["th", "td"])]
        out = []
        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells:
                continue
            rd: dict = {}
            for i, cell in enumerate(cells):
                col = headers[i] if i < len(headers) else f"col_{i}"
                rd[col] = _clean(cell.get_text())
                links = [a["href"] for a in cell.find_all("a", href=True)
                         if "javascript" not in a["href"].lower()]
                if links:
                    rd[f"{col}__links"] = links
            if any(isinstance(v, str) and v for v in rd.values()):
                out.append(rd)
        return out
    return []


def _parse_detail(html: str, ack_no: str, search_district: str,
                  start_page: int, meta: dict | None = None) -> dict:
    """
    Parse the full detail page returned by POST /projectDetails.
    Uses Bootstrap grid (col-md-3) KV pairs for most fields plus
    tr/td KV pairs for address fields.
    meta may contain approved_on_date and status_of_the_project from listing.
    Returns a dict of normalized schema fields ready for merging.
    """
    if meta is None:
        meta = {}
    soup = BeautifulSoup(html, "lxml")

    # Merge both KV extraction strategies: grid (primary) + tr/td (for addresses)
    grid_kv = _extract_grid_kv(soup)
    td_kv   = _extract_kv_pairs(soup)
    # grid_kv takes priority; td_kv fills in what grid misses
    kv: dict[str, str] = {**td_kv, **grid_kv}

    out: dict = {}

    # ── 0. Header fields (project name / ack_no / reg_no) ───────────────────
    hdr = _extract_header_fields(soup)
    out.update(hdr)

    # ── 1. Map label→schema field ────────────────────────────────────────────
    for raw_key, val in kv.items():
        field = _LABEL_MAP.get(raw_key)
        if field and val and not out.get(field):
            out[field] = val

    def _pop_mapped(field: str, *fallback_labels: str) -> str:
        value = out.pop(field, None)
        if value:
            return str(value)
        for label in fallback_labels:
            fallback = kv.get(label, "")
            if fallback:
                return fallback
        return ""

    # ── 2. Apply listing metadata (approved_on, status) ─────────────────────
    if meta.get("approved_on_date") and not out.get("approved_on_date"):
        out["approved_on_date"] = meta["approved_on_date"]              # FIELD: approved_on_date <- listing meta "approved on" column
    if meta.get("status_of_the_project") and not out.get("status_of_the_project"):
        out["status_of_the_project"] = meta["status_of_the_project"]    # FIELD: status_of_the_project <- listing meta "status" column
    if meta.get("project_type_listing") and not out.get("project_type"):
        out["project_type"] = meta["project_type_listing"]              # FIELD: project_type <- listing meta "project type" column

    # ── 3. Parse date fields ─────────────────────────────────────────────────
    for f in ("actual_commencement_date", "estimated_commencement_date",
              "estimated_finish_date", "actual_finish_date", "approved_on_date"):
        raw = out.get(f)
        if raw:
            parsed = parse_datetime(raw)
            out[f] = parsed.strftime("%Y-%m-%d %H:%M:%S+00:00") if parsed else None
    # When promoter has applied for completion, set actual_finish_date from
    # estimated_finish_date (portal shows the same project end date for both).
    # Detect via the "Promoter has Applied for Completion" h1 heading on the page.
    if not out.get("actual_finish_date") and out.get("estimated_finish_date"):
        completion_applied = any(
            "applied for completion" in _clean(h1.get_text()).lower()
            for h1 in soup.find_all("h1")
        )
        if completion_applied:
            out["actual_finish_date"] = out["estimated_finish_date"]    # FIELD: actual_finish_date <- copy of estimated_finish_date when "applied for completion"

    # ── 4. Land area — find "X Acres, Y Gunta/ Z Sq Mtr(s)" pattern in table cells
    land_area_m2: float | None = None
    for td in soup.find_all("td"):
        td_text = _clean(td.get_text())
        m = re.search(r"\d+\s*(?:Acres?|Gunta)[^/]*/\s*(\d+(?:\.\d+)?)\s*Sq\s*Mtrs?",
                      td_text, re.I)
        if m:
            land_area_m2 = float(m.group(1))
            break
    if land_area_m2 is None and out.get("land_area"):
        land_area_m2 = _safe_float(str(out.get("land_area")))
    if land_area_m2 is not None:
        out["land_area"] = land_area_m2                       # FIELD: land_area <- regex td "Sq Mtrs" or _safe_float fallback
        out["land_area_details"] = {                          # FIELD: land_area_details <- nested land area dict
            "land_area": str(round(land_area_m2)),            # FIELD: land_area_details.land_area <- round(land_area_m2)
            "land_area_unit": None,                           # FIELD: land_area_details.land_area_unit <- None
            "construction_area": None,                        # FIELD: land_area_details.construction_area <- None
            "construction_area_unit": None,                   # FIELD: land_area_details.construction_area_unit <- None
        }

    # ── 5. Project location ──────────────────────────────────────────────────
    district = _pop_mapped("_district", "district")
    # Prefer the full project address string; fall back to village name
    project_address = _pop_mapped("_project_address", "project address")
    loc: dict = {k: v for k, v in {
        "district":               district,                                              # FIELD: project_location_raw.district <- _pop_mapped("_district", "district")
        "taluk":                  _pop_mapped("_taluk", "taluk"),                        # FIELD: project_location_raw.taluk <- _pop_mapped("_taluk", "taluk")
        "pin_code":               _pop_mapped("_pin_code", "pin code"),                  # FIELD: project_location_raw.pin_code <- _pop_mapped("_pin_code", "pin code")
        "latitude":               _pop_mapped("_latitude", "latitude"),                  # FIELD: project_location_raw.latitude <- _pop_mapped("_latitude", "latitude")
        "longitude":              _pop_mapped("_longitude", "longitude"),                # FIELD: project_location_raw.longitude <- _pop_mapped("_longitude", "longitude")
        "survey_resurvey_number": _pop_mapped("_survey_no", "survey / resurvey number"), # FIELD: project_location_raw.survey_resurvey_number <- _pop_mapped("_survey_no")
        "raw_address":            project_address or _pop_mapped("_village", "village"), # FIELD: project_location_raw.raw_address <- project_address or village
    }.items() if v}
    for coord_key, store_key in (("latitude", "processed_latitude"),
                                 ("longitude", "processed_longitude")):
        if loc.get(coord_key):
            fv = _safe_float(loc[coord_key])
            if fv is not None:
                loc[store_key] = fv
    if loc:
        out["project_location_raw"] = loc            # FIELD: project_location_raw <- filtered loc dict above
    if district:
        out["project_city"] = district.upper()       # FIELD: project_city <- district.upper()
    if loc.get("pin_code"):
        out["project_pin_code"] = loc["pin_code"]    # FIELD: project_pin_code <- loc["pin_code"]

    # ── 6. Promoter address ──────────────────────────────────────────────────
    prom_addr: dict = {}
    # Primary: Bootstrap grid "promoter address" field
    for key in ("promoter address",):
        if kv.get(key):
            prom_addr["raw_address"] = kv[key]          # FIELD: promoter_address_raw.raw_address <- kv["promoter address"]
            break
    if not prom_addr.get("raw_address"):
        for key in grid_kv:
            if "promoter" in key and "address" in key:
                prom_addr["raw_address"] = grid_kv[key] # FIELD: promoter_address_raw.raw_address <- grid_kv key containing "promoter"+"address"
                break
    # Location sub-fields (may come from promoter section of the grid)
    for sub, labels in [
        ("state",    ["promoter state", "promoter's state"]),
        ("taluk",    ["promoter taluk", "promoter's taluk", "taluk"]),
        ("district", ["promoter district", "promoter's district", "district"]),
        ("pin_code", ["promoter pin code", "pin code"]),
    ]:
        for label in labels:
            val = kv.get(label, "")
            if val:
                prom_addr[sub] = val
                break
    # Karnataka is always the state for this portal
    if not prom_addr.get("state"):
        prom_addr["state"] = "Karnataka"                            # FIELD: promoter_address_raw.state <- literal "Karnataka"
    if prom_addr:
        out["promoter_address_raw"] = prom_addr                     # FIELD: promoter_address_raw <- assembled prom_addr dict

    # ── 7. Promoter contact (website) ────────────────────────────────────────
    website = _pop_mapped("_website", "website", "promoter website")
    if website:
        # FIELD: promoter_contact_details <- nested {"website": website}
        out["promoter_contact_details"] = {"website": website}      # FIELD: promoter_contact_details.website <- _pop_mapped("_website", "website", "promoter website")

    # ── 8. Promoters details (GST, PAN, trade reg, objective) ────────────────
    pd_dict: dict = {
        "gst_no":          _pop_mapped("_gst_no", "gst no", "gstin"),                        # FIELD: promoters_details.gst_no <- _pop_mapped("_gst_no", "gst no", "gstin")
        "pan_no":          _pop_mapped("_pan_no", "pan no", "pan"),                          # FIELD: promoters_details.pan_no <- _pop_mapped("_pan_no", "pan no", "pan")
        "registration_no": _pop_mapped("_trade_reg_no", "trade licence / registration no",   # FIELD: promoters_details.registration_no <- _pop_mapped("_trade_reg_no", ...)
                                       "registration number"),
        "objective":       _pop_mapped("_objective", "objective"),                           # FIELD: promoters_details.objective <- _pop_mapped("_objective", "objective")
    }
    pd_dict = {k: v for k, v in pd_dict.items() if v}
    if pd_dict:
        out["promoters_details"] = pd_dict                          # FIELD: promoters_details <- filtered pd_dict

    # ── 9. Bank details ──────────────────────────────────────────────────────
    # Bank section uses "state" which can conflict; grab it from grid before popping
    bank_state = grid_kv.get("state", "")
    bank_district = grid_kv.get("district", district)
    bank_pin = grid_kv.get("pin code", "")
    bank: dict = {
        "bank_name":    _pop_mapped("_bank_name", "bank name"),                              # FIELD: bank_details.bank_name <- _pop_mapped("_bank_name", "bank name")
        "account_no":   _pop_mapped("_account_no", "account no", "account no.(70% account)"),  # FIELD: bank_details.account_no <- _pop_mapped("_account_no", "account no", ...)
        "account_name": _pop_mapped("_account_name", "account name"),                        # FIELD: bank_details.account_name <- _pop_mapped("_account_name", "account name")
        "IFSC":         _pop_mapped("_ifsc", "ifsc", "ifsc code"),                           # FIELD: bank_details.IFSC <- _pop_mapped("_ifsc", "ifsc", "ifsc code")
        "branch":       _pop_mapped("_branch", "branch"),                                    # FIELD: bank_details.branch <- _pop_mapped("_branch", "branch")
    }
    if bank_state:
        bank["state"] = bank_state                          # FIELD: bank_details.state <- grid_kv["state"]
    if bank_district:
        bank["district"] = bank_district                    # FIELD: bank_details.district <- grid_kv["district"] or location district
    if bank_pin:
        bank["pin_code"] = bank_pin                         # FIELD: bank_details.pin_code <- grid_kv["pin code"]
    bank = {k: v for k, v in bank.items() if v}
    if bank:
        out["bank_details"] = bank                          # FIELD: bank_details <- filtered bank dict

    # ── 10. Project cost ─────────────────────────────────────────────────────
    cost: dict = {
        "cost_of_land": _pop_mapped(                                                         # FIELD: project_cost_detail.cost_of_land <- _pop_mapped("_cost_of_land", "cost of land", ...)
            "_cost_of_land", "cost of land",
            "cost of land (inr) (c1)( as certified by ca in form 1 )"),
        "estimated_construction_cost": _pop_mapped(                                          # FIELD: project_cost_detail.estimated_construction_cost <- _pop_mapped("_est_construction_cost", ...)
            "_est_construction_cost", "estimated construction cost",
            "cost of layout development (inr) (c2)( as certified by ca in form 1 )",
            "total construction cost"),
        "total_project_cost": _pop_mapped(                                                   # FIELD: project_cost_detail.total_project_cost <- _pop_mapped("_total_project_cost", ...)
            "_total_project_cost", "total project cost",
            "total project cost (inr) (c1+c2)"),
    }
    cost = {k: v for k, v in cost.items() if v}
    if cost:
        out["project_cost_detail"] = cost                   # FIELD: project_cost_detail <- filtered cost dict

    # ── 11. Building / plot details ──────────────────────────────────────────
    # New page: "Development Details ( Plot Dimension wise break up )" h1
    # Columns: Sl No. | Plot Type | Number of Sites | Total Area (in Sq.Mtr)
    brows = _parse_section_table(
        soup,
        ["plot dimension", "plot detail", "plot type", "unit detail",
         "building detail", "development detail"])
    if brows:
        bd = []
        skip_keys = {"s.no", "sl.no", "sl no.", "#", "no.", "total"}
        for r in brows:
            # Use header-keyed values; fall back to positional
            vals = _row_values(r)
            flat_type = (r.get("Plot Type (Site Dimension in Mtr)")
                         or r.get("Type of Inventory") or r.get("Flat Type")
                         or (vals[1] if len(vals) > 1 else ""))
            total_area = (r.get("Total Area (in Sq.Mtr)")
                          or r.get("Carpet Area (Sq Mtr)") or r.get("Total Area")
                          or (vals[3] if len(vals) > 3 else ""))
            flat_type = _clean(str(flat_type))
            total_area = _clean(str(total_area))
            if flat_type and flat_type.lower() not in skip_keys:
                bd.append({"flat_type": flat_type, "total_area": total_area})
        if bd:
            out["building_details"] = bd                    # FIELD: building_details <- list of {flat_type,total_area} from plot/unit table

    # ── 11b. Proposed timeline — parse "Project Schedule" table ──────────────
    sched_rows = _parse_section_table(soup, ["project schedule"])
    if sched_rows:
        timeline = []
        for r in sched_rows:
            title = _clean(r.get("Project Work") or r.get("col_1") or "")
            status = _clean(r.get("Is Applicable ?") or r.get("col_2") or "")
            end_date_raw = _clean(r.get("Estimated To Date") or r.get("col_4") or "")
            if not title:
                continue
            parsed_end = parse_datetime(end_date_raw) if end_date_raw else None
            timeline.append({
                "title": title,
                "status": status or None,
                "proposed_end_date": (
                    parsed_end.strftime("%Y-%m-%d %H:%M:%S+00:00") if parsed_end else None
                ),
            })
        if timeline:
            out["proposed_timeline"] = timeline             # FIELD: proposed_timeline <- list of {title,status,proposed_end_date} from "Project Schedule" table

    # ── 12. Professional information ─────────────────────────────────────────
    # New page uses separate h1 sections per role; collect them all
    profs: list[dict] = []
    role_keywords = [
        ("project chartered accountant", "Accountant"),
        ("project engineer", "Engineers"),
        ("project architect", "Architect"),
        ("project contractor", "Contractor"),
    ]
    for kw, role_label in role_keywords:
        prows = _parse_section_table(soup, [kw])
        if not _has_name_column(prows):
            continue
        for r in prows:
            vals = _row_values(r)
            # Columns: Sl No. | Name | Address | Year | Licence No.
            name = _clean(vals[1]) if len(vals) > 1 else ""
            if not name:
                name = _clean(vals[0]) if vals else ""
            addr = _clean(vals[2]) if len(vals) > 2 else ""
            yr   = _clean(vals[3]) if len(vals) > 3 else ""
            lic  = _clean(vals[4]) if len(vals) > 4 else ""
            if name and name.lower() not in ("sl no.", "s.no", "#"):
                e = {k: v for k, v in {
                    "name": name, "role": role_label, "address": addr,
                    "effective_date": yr, "key_real_estate_projects": lic,
                }.items() if v}
                profs.append(e)
    # Fall back to legacy search
    if not profs:
        prows = _parse_section_table(soup, ["professional", "engineer", "architect"])
        if not _has_name_column(prows):
            prows = []
        for r in prows:
            vals = _row_values(r)
            e = {
                "name":                     _clean(vals[0]) if vals else "",
                "role":                     _clean(vals[1]) if len(vals) > 1 else "",
                "address":                  _clean(vals[2]) if len(vals) > 2 else "",
                "effective_date":           _clean(vals[3]) if len(vals) > 3 else "",
                "key_real_estate_projects": _clean(vals[4]) if len(vals) > 4 else "",
            }
            e = {k: v for k, v in e.items() if v}
            if e.get("name"):
                profs.append(e)
    if profs:
        out["professional_information"] = profs             # FIELD: professional_information <- list of professional role entries

    # ── 13. Co-promoter / land-owner details ─────────────────────────────────
    crows = _parse_section_table(soup, ["co-promoter", "co promoter", "land owner"])
    if crows:
        colist = []
        for r in crows:
            # New columns: Sl No., Land Owner Name, Land Owner Share, Survey Number,
            #              Present Address, Communication Address
            name         = (r.get("Land Owner Name") or r.get("Name", ""))
            survey_no    = (r.get("Survey Number") or r.get("Survey No", ""))
            land_share   = (r.get("Land Owner Share") or r.get("Land Share", ""))
            present_addr = r.get("Present Address", "")
            comm_addr    = r.get("Communication Address") or r.get("Comm Address", "")
            name = _clean(name)
            if name and name.lower() not in ("s.no", "sl.no", "#", "sl no."):
                e = {k: v for k, v in {
                    "name": name,
                    "survey_no": _clean(survey_no),
                    "land_share": _clean(land_share),
                    "present_address": _clean(present_addr),
                    "comm_address": _clean(comm_addr),
                }.items() if v}
                colist.append(e)
        if colist:
            out["co_promoter_details"] = colist             # FIELD: co_promoter_details <- list of co-promoter/land-owner entries from section table

    # ── 14. Authorised signatory ─────────────────────────────────────────────
    # New page: name from grid KV; addresses from tr/td KV
    sg_name = grid_kv.get("name of authorized signatory", "")
    if sg_name:
        sg = {
            "name":              sg_name,                                # FIELD: authorised_signatory_details.name <- grid_kv["name of authorized signatory"]
            "pan_no":            grid_kv.get("pan", ""),                 # FIELD: authorised_signatory_details.pan_no <- grid_kv["pan"]
            "present_address":   td_kv.get("present address", ""),       # FIELD: authorised_signatory_details.present_address <- td_kv["present address"]
            "official_address":  td_kv.get("official address", ""),      # FIELD: authorised_signatory_details.official_address <- td_kv["official address"]
            "permanent_address": td_kv.get("permanent address", ""),     # FIELD: authorised_signatory_details.permanent_address <- td_kv["permanent address"]
        }
        sg["raw_address"] = (sg.get("present_address") or sg.get("official_address") or "")  # FIELD: authorised_signatory_details.raw_address <- present_address or official_address
        sg = {k: v for k, v in sg.items() if v}
        if sg.get("name"):
            out["authorised_signatory_details"] = sg                     # FIELD: authorised_signatory_details <- filtered sg dict

    # ── 15. Construction progress (total completion %) ───────────────────────
    total_pct = _pop_mapped("_total_completion_pct", "total completion percentage",
                            "extent of development carried till date")
    if not total_pct:
        m2 = re.search(r"extent\s+of\s+development\s+carried\s+till\s+date\s*[:\s]+([^\n<]+)",
                       html, re.I)
        if m2:
            total_pct = m2.group(1).strip()
    if total_pct:
        if "%" not in total_pct:
            total_pct = f"{total_pct} %"
        out["construction_progress"] = [                                                 # FIELD: construction_progress <- single-element list with total completion %
            {"title": "total_completion_percentage", "progress_percentage": total_pct}
        ]

    # ── 16. Project images — img tags with alt containing "photo" ───────────
    images: list[str] = []
    seen_img_urls: set[str] = set()
    for img in soup.find_all("img"):
        alt = (img.get("alt") or "").lower()
        if "photo" not in alt:
            continue
        src = img.get("src") or ""
        if not src:
            continue
        full_url = src if src.startswith("http") else f"{BASE_URL}{src}"
        if "download_jc" in full_url.lower() and full_url not in seen_img_urls:
            seen_img_urls.add(full_url)
            images.append(full_url)
    if images:
        out["project_images"] = images                       # FIELD: project_images <- list of img src URLs with alt containing "photo"

    # ── 17. Raw data snapshot ────────────────────────────────────────────────
    out["data"] = {                                                                       # FIELD: data <- raw snapshot dict
        "district":                    search_district,                                   # FIELD: data.district <- search_district arg
        "START_PAGE":                  str(start_page),                                   # FIELD: data.START_PAGE <- start_page arg (str)
        "project_district":            district.upper() if district else "",              # FIELD: data.project_district <- district.upper() or ""
        "total_completion_percentage": total_pct or "",                                   # FIELD: data.total_completion_percentage <- total_pct
        "status":                      meta.get("status_of_the_project", ""),             # FIELD: data.status <- meta["status_of_the_project"]
    }

    return out



# ── Quarterly Progress Report snapshot ────────────────────────────────────────

# Bootstrap accordion headers inside the #quarter tab look like:
#   <b>Quarter Q4 ( 2025-26 ) Details (Submitted on 11-04-2026)</b>
# The crawler grabs the LATEST panel (by submission date), inlines every
# <img> inside it as a base64 data URI, and emits one self-contained HTML
# snapshot per project that downstream code uploads to S3 alongside the
# regular PDFs.
_QPR_QUARTER_RE   = re.compile(r"Quarter\s+Q(\d)\s*\(\s*(\d{4})\s*-\s*(\d{2})\s*\)", re.I)
_QPR_SUBMITTED_RE = re.compile(r"Submitted on\s+(\d{2}-\d{2}-\d{4})", re.I)
_QPR_DOC_TYPE     = "Quarterly Progress Report"

_QPR_INLINE_CSS = (
    'body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:24px;color:#222}'
    "h1,h2{color:#1f4e8a}"
    "table{border-collapse:collapse;margin:8px 0;width:100%}"
    "th,td{border:1px solid #ccc;padding:6px 10px;vertical-align:top}"
    "th{background:#f4f6fa;text-align:left}"
    "img{max-width:320px;height:auto;margin:4px;border:1px solid #ddd;padding:2px;background:#fff}"
    ".kheader{background:#f4f6fa;padding:12px 16px;border-radius:6px;margin-bottom:16px}"
    ".kheader table{border:none}.kheader td{border:none;padding:2px 8px}"
    ".text-right{text-align:right;font-weight:600}.space_LR{padding:0 4px}"
    ".panel{margin:12px 0}"
    ".panel-heading{background:#1f4e8a;color:#fff;padding:8px 14px;border-radius:6px 6px 0 0}"
    ".panel-heading b,.panel-heading b span{color:#fff !important;font-size:16px}"
    ".panel-body{border:1px solid #ddd;border-top:none;padding:14px;border-radius:0 0 6px 6px}"
)


def _qpr_find_panel_root(node: Tag) -> Tag | None:
    """Walk up to the enclosing Bootstrap accordion panel div for ``node``."""
    p: Tag | None = node
    while p is not None and p.parent is not None:
        p = p.parent
        if isinstance(p, Tag) and p.name == "div":
            cls = p.get("class") or []
            if "panel" in cls and not any(
                x in cls for x in ("panel-heading", "panel-body", "panel-title", "panel-group")
            ):
                return p
    return None


def _qpr_inline_image(img: Tag, logger: CrawlerLogger) -> bool:
    """Replace ``img['src']`` with a base64 data URI; returns True on success."""
    src = (img.get("src") or "").strip()
    if not src or src.startswith("data:"):
        return False
    if src.startswith("http"):
        full = src
    elif src.startswith("/"):
        full = f"{BASE_URL}{src}"
    else:
        full = f"{BASE_URL}/{src}"
    resp = download_response(full, logger=logger,
                             total_timeout=_DOC_TOTAL_TIMEOUT, max_bytes=_DOC_MAX_BYTES)
    if not resp or not getattr(resp, "content", None):
        return False
    ctype = ""
    headers = getattr(resp, "headers", None) or {}
    if hasattr(headers, "get"):
        ctype = (headers.get("content-type") or "").split(";", 1)[0].strip().lower()
    if not ctype or "html" in ctype:
        ctype = mimetypes.guess_type(src)[0] or "image/jpeg"
    img["src"] = f"data:{ctype};base64,{base64.b64encode(resp.content).decode('ascii')}"
    img.attrs.pop("srcset", None)
    return True


def _qpr_header_fields(soup: BeautifulSoup) -> dict[str, str]:
    """Pull Project Name / Ack No / Reg No from the detail page header strip."""
    out: dict[str, str] = {}
    for span in soup.find_all("span", class_="user_name"):
        text = " ".join(span.get_text(" ", strip=True).split())
        m = re.match(r"^([^:]+)\s*:\s*(.+)$", text)
        if m:
            out[m.group(1).strip().lower()] = m.group(2).strip()
    return out


def _build_qpr_snapshot(
    html: str, ack_no: str, reg_no: str, logger: CrawlerLogger,
) -> dict | None:
    """Return a synthetic ``uploaded_documents`` entry for the latest QPR panel.

    The returned dict carries ``_inline_bytes`` / ``_inline_filename`` so
    ``_handle_document`` skips the download step and uploads the pre-built
    HTML straight to S3.  Returns ``None`` when no QPR panel is present.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception as exc:
        logger.warning(f"QPR snapshot: html parse failed: {exc}", step="documents")
        return None
    qdiv = soup.find(id="quarter")
    if not qdiv:
        return None

    panels: list[tuple[datetime, int, int, Tag]] = []
    for b in qdiv.find_all("b"):
        text = " ".join(b.get_text(" ", strip=True).split())
        qm, sm = _QPR_QUARTER_RE.search(text), _QPR_SUBMITTED_RE.search(text)
        if not qm or not sm:
            continue
        panel = _qpr_find_panel_root(b)
        if panel is None:
            continue
        try:
            sub_date = datetime.strptime(sm.group(1), "%d-%m-%Y")
        except ValueError:
            continue
        panels.append((sub_date, int(qm.group(2)), int(qm.group(1)), panel))
    if not panels:
        return None

    panels.sort(key=lambda x: x[0], reverse=True)
    sub_date, fy, qn, latest = panels[0]
    label = f"Q{qn} ({fy}-{(fy + 1) % 100:02d}) submitted on {sub_date.strftime('%d-%m-%Y')}"

    ok = fail = 0
    for img in latest.find_all("img"):
        try:
            if _qpr_inline_image(img, logger):
                ok += 1
            else:
                fail += 1
        except Exception as exc:
            logger.warning(f"QPR snapshot: image inline error: {exc}", step="documents")
            fail += 1

    header = _qpr_header_fields(soup)
    from html import escape as _esc
    project_name = header.get("project name", "")
    title = f"{project_name or reg_no or ack_no} — Latest QPR ({label})"
    hdr_html = (
        f'<div class="kheader"><h1>{_esc(title)}</h1><table>'
        f"<tr><td><b>Project Name</b></td><td>{_esc(project_name)}</td></tr>"
        f"<tr><td><b>Acknowledgement No</b></td><td>{_esc(header.get('acknowledgement number', ack_no))}</td></tr>"
        f"<tr><td><b>Registration No</b></td><td>{_esc(header.get('registration number', reg_no))}</td></tr>"
        f"<tr><td><b>Snapshot Of</b></td><td>{_esc(label)}</td></tr></table></div>"
    )
    out_html = (
        f"<!doctype html><html><head><meta charset='utf-8'><title>{_esc(title)}</title>"
        f"<style>{_QPR_INLINE_CSS}</style></head><body>{hdr_html}{str(latest)}</body></html>"
    )
    data = out_html.encode("utf-8")
    logger.info(
        f"QPR snapshot built: {label} (images ok={ok} failed={fail}, size={len(data)/1024:.0f} KB)",
        step="documents",
    )
    return {
        "link": f"{PROJECT_URL}?appNo={ack_no}",
        "type": _QPR_DOC_TYPE,
        "dated_on": sub_date.strftime("%Y-%m-%d"),
        "_inline_bytes": data,
        "_inline_filename": "quarterly_progress_report.html",
    }


# ── Document extraction ───────────────────────────────────────────────────────

# Matches "*(Annexure - 60)" or "( Annexure - 60 )" suffixes on portal labels.
_ANNEXURE_RE = re.compile(r"\s*\*?\(?\s*Annexure\s*[-–]\s*\d+\s*\)?", re.I)
# Matches date strings like "06-03-2023" or "28/11/2003".
_DATE_CELL_RE = re.compile(r"^\d{1,2}[-/]\d{2}[-/]\d{2,4}$")
# Matches file extensions (.pdf, .xlsx, .jpeg, etc.).
_FILE_EXT_RE = re.compile(r"\.\w{2,5}$")


def _doc_label_from_row(parent_td, row_cells: list, section_heading: str,
                        link_text: str) -> str:
    """
    Determine the human-readable document type for a download link found inside
    a table cell.

    Strategy (handles three distinct table layouts on the Karnataka portal):

    1. Empty link text  →  the link has no visible label (e.g. licence-number
       columns in the Professional table, or document columns in Land Survey).
       Use the nearest preceding section heading (e.g. "Project Chartered
       Accountant", "Land Survey Details") — always more meaningful than a
       random data cell value.

    2. Non-empty link text  →  walk LEFT through the current row's cells to
       find the nearest cell whose text looks like a document-category label
       (not a serial number, not a date, not another filename).  This correctly
       handles:
         • Financial docs:  [Label | 2022.pdf | 2021.pdf | 2020.pdf]
           — year 2 & 3 get the same Label as year 1.
         • Other Docs:      [Doc Name | Date | file.pdf]
           — walks past the date to reach the doc name in col-0.
         • 4-col project docs: [Label1 | file1 | Label2 | file2]
           — each file's immediate left neighbour IS its own label.
       If no label is found (e.g. numbered "Other Documents" rows where col-0
       is a serial number), falls back to link_text (the filename itself).
    """
    if not link_text:
        # Empty link tag — use section heading as the category label.
        return section_heading or "Document"

    if parent_td not in row_cells:
        return link_text

    col_idx = row_cells.index(parent_td)
    # Walk left through preceding cells to find a proper label.
    for i in range(col_idx - 1, -1, -1):
        candidate = _clean(row_cells[i].get_text())
        if not candidate:
            continue
        # Serial number → we've reached the leftmost data; no label exists.
        if candidate.isdigit():
            break
        # Date cell (e.g. "06-03-2023") → skip and keep walking.
        if _DATE_CELL_RE.match(candidate):
            continue
        # Another filename → keep walking left to find the true label.
        if _FILE_EXT_RE.search(candidate):
            continue
        # Single-word codes without spaces (survey nos., case IDs like "820/3") → skip.
        if "/" in candidate and " " not in candidate:
            continue
        # Looks like a real document-category label — strip the annexure suffix.
        label = _ANNEXURE_RE.sub("", candidate).strip().rstrip("*").strip()
        return label if label else link_text

    # Nothing useful found to the left — use the filename as the type.
    return link_text


def _extract_documents(html: str, reg_no: str) -> list[dict]:
    """
    Extract all document links from the detail HTML.
    - Scans <a href> for /download_jc?DOC_ID= patterns; skips entries with empty DOC_ID.
    - Skips placeholder links whose filename is "Not Applicable.pdf" — the portal
      renders these for document categories that don't apply to a project (the server
      returns a tiny blank 5 KB PDF with no content).  Downloading these wastes S3
      storage and clutters the document list with meaningless files.
    - Resolves each document's human-readable type via _doc_label_from_row.
    - Adds the auto-generated RERA registration certificate entry.
    Returns list of {link, type} dicts.
    """
    # Matches filenames that explicitly indicate "not applicable" placeholders.
    # Anchored so "Some Not Applicable Doc.pdf" is not accidentally skipped.
    _NOT_APPLICABLE_RE = re.compile(r'^not\s*applicable(\.pdf)?$', re.I)

    soup = BeautifulSoup(html, "lxml")
    docs: list[dict] = []
    seen_links: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "download_jc" not in href.lower():
            continue
        # Skip entries with blank DOC_ID
        if "DOC_ID=" in href and (
            href.endswith("DOC_ID=") or "DOC_ID=&" in href
        ):
            continue
        full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
        if full_url in seen_links:
            continue
        seen_links.add(full_url)

        link_text = _clean(a.get_text())

        # Skip placeholder "Not Applicable.pdf" links — the promoter uploads these
        # when a required document category doesn't apply to their project.
        # The server returns a real but blank PDF; downloading it provides no value.
        if _NOT_APPLICABLE_RE.match(link_text):
            continue

        parent_td = a.find_parent("td")

        if parent_td is None:
            # Standalone link (e.g. inside a Bootstrap grid div, not a table).
            # The link text itself is the filename and serves as a reasonable label.
            doc_type = link_text or "Document"
        else:
            row = parent_td.find_parent("tr")
            row_cells = row.find_all("td") if row else []

            # Nearest section heading above this link's table.
            section_heading = ""
            table = row.find_parent("table") if row else None
            if table:
                for hdr in table.find_all_previous(["h1", "h2", "h3", "h4"]):
                    section_heading = _clean(hdr.get_text())
                    break

            doc_type = _doc_label_from_row(
                parent_td, row_cells, section_heading, link_text
            )

        docs.append({"link": full_url, "type": doc_type})  # FIELD: uploaded_documents[].link <- href, uploaded_documents[].type <- _doc_label_from_row

    # Auto-add registration certificate for approved projects
    if reg_no:
        cert_link = f"{CERT_URL}?CER_NO={reg_no}"
        if cert_link not in seen_links:
            docs.append({"link": cert_link, "type": "Rera Registration Certificate 1"})  # FIELD: uploaded_documents[].link <- CERT_URL?CER_NO=reg_no, type <- literal

    return docs


# ── Document download constants ───────────────────────────────────────────────

_DOC_CONNECT_TIMEOUT = 10.0   # seconds to establish TCP connection
_DOC_READ_TIMEOUT    = 20.0   # seconds between data chunks
_DOC_TOTAL_TIMEOUT   = 60.0   # hard cap: total download time in seconds
_DOC_MAX_BYTES       = 50 * 1024 * 1024  # 50 MB safety limit
_MAX_DOC_WORKERS     = 1      # serialised: single SeleniumSession driver is not thread-safe

# psycopg connections are not thread-safe — serialise all DB writes from doc threads.
_DB_LOCK = threading.Lock()


def _handle_document(
    project_key: str,
    doc: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
    client=None,
) -> dict | None:
    """Download a document, upload to S3, and record it in rera_project_documents.

    Docs carrying ``_inline_bytes`` (e.g. the QPR HTML snapshot built locally)
    bypass the network download and are uploaded straight to S3.
    """
    url = doc.get("link")
    doc_type = doc.get("type", "document")
    inline_bytes = doc.get("_inline_bytes")
    if not url and not inline_bytes:
        return None
    try:
        if inline_bytes is not None:
            data = inline_bytes
            # Derive filename from the (post-rename) label so the counter
            # suffix is preserved, but force the inline content's extension.
            slug = re.sub(r"[^a-z0-9]+", "_",
                          (doc.get("label") or doc.get("type") or "document").lower()).strip("_") or "document"
            inline_name = doc.get("_inline_filename") or "snapshot.html"
            ext = inline_name[inline_name.rfind("."):] if "." in inline_name else ".html"
            filename = f"{slug}{ext}"
        else:
            filename = build_document_filename(doc)
            resp = download_response(
                url,
                logger=logger,
                total_timeout=_DOC_TOTAL_TIMEOUT,
                max_bytes=_DOC_MAX_BYTES,
            )
            if not resp or len(resp.content) < 100:
                logger.warning("Document download empty or failed", url=url, step="documents")
                return None
            data = resp.content
        if len(data) > _DOC_MAX_BYTES:
            logger.warning(
                f"Document too large ({len(data)/1024/1024:.1f} MB), skipping",
                url=url, step="documents",
            )
            return None
        md5 = compute_md5(data)
        s3_key = upload_document(project_key, filename, data, dry_run=settings.DRY_RUN_S3)
        if s3_key is None:
            return None
        s3_url = get_s3_url(s3_key)
        with _DB_LOCK:
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
        logger.log_document(doc_type, url, "uploaded", s3_key=s3_key, file_size_bytes=len(data))
        # Strip internal _inline_* keys so they don't leak into the project record.
        result = {k: v for k, v in doc.items() if not k.startswith("_inline_")}
        result["s3_link"] = s3_url                                                # FIELD: uploaded_documents[].s3_link <- get_s3_url(s3_key)
        return result
    except Exception as exc:
        logger.warning(f"Document handling error: {exc}", url=url, step="documents")
        with _DB_LOCK:
            insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(exc),
                               url=url, project_key=project_key)
        return None


def _process_documents(
    project_key: str,
    documents: list[dict],
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
    state: str = "karnataka",
) -> tuple[list[dict], int]:
    # ── Phase 1: filter to policy-allowed documents (sequential, CPU-only) ─────
    counters: dict[str, int] = {}
    selected_pairs: list[tuple[dict, dict]] = []  # (original_doc, selected_doc)
    skipped_entries: list[dict] = []
    for doc in documents:
        sel = select_document_for_download(state, doc, counters)
        if sel:
            selected_pairs.append((doc, sel))
        else:
            skipped_entries.append({"link": doc.get("link"), "type": doc.get("type", "document")})  # FIELD: uploaded_documents[].link/type <- skipped doc (policy-rejected) entry
    if not selected_pairs:
        return skipped_entries, 0

    # ── Phase 2: downloads (serialised — single SeleniumSession driver) ────────
    dl_results: list[dict | None] = [None] * len(selected_pairs)
    with ThreadPoolExecutor(max_workers=_MAX_DOC_WORKERS) as executor:
        futures = {
            executor.submit(_handle_document, project_key, sel, run_id, site_id, logger, None): i
            for i, (_orig, sel) in enumerate(selected_pairs)
        }
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                dl_results[idx] = fut.result()
            except Exception as exc:
                logger.warning(f"Doc thread error: {exc}", step="documents")

    # ── Phase 3: reassemble in original order ─────────────────────────────────
    upload_count = 0
    enriched: list[dict] = []
    for (orig, _sel), result in zip(selected_pairs, dl_results):
        if result:
            enriched.append(result)
            upload_count += 1
        else:
            enriched.append({"link": orig.get("link"), "type": orig.get("type", "document")})  # FIELD: uploaded_documents[].link/type <- original doc (download failed, no s3_link)
    enriched.extend(skipped_entries)
    return enriched, upload_count



# ── Sentinel ──────────────────────────────────────────────────────────────────

def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Karnataka RERA.

    Uses the same direct ``/projectViewDetails`` search that powers the
    ``--target-reg-no`` flag: looks up the sentinel project by its registration
    number (``sentinel_registration_no`` from config) via
    :func:`_search_by_reg_no`, then fetches the full detail HTML and verifies
    ≥ 80% field coverage against ``state_projects_sample/karnataka.json``.

    Both the sentinel and the targeted-crawl path therefore exercise an
    identical lookup path against the portal — if one breaks the other will
    too, which is exactly what we want a health check to surface.
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
        "state_projects_sample", "karnataka.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check",
                       path=sample_path, step="sentinel")
        return True

    logger.info(f"Sentinel: direct reg_no lookup for {sentinel_reg}", step="sentinel")
    try:
        listing_row = _search_by_reg_no(sentinel_reg, logger)
        if listing_row is None:
            logger.warning(
                "Sentinel: reg_no lookup returned no match — likely transient "
                "(portal hangs / empty search response); skipping coverage "
                "check this run",
                step="sentinel",
            )
            return True
        ack_no = listing_row.get("acknowledgement_no") or ""
        if not ack_no:
            logger.warning(
                "Sentinel: lookup row carried no acknowledgement_no — skipping",
                step="sentinel",
            )
            return True

        html, meta = _fetch_detail(ack_no, logger)
        if not html:
            logger.warning(
                "Sentinel: detail fetch returned no HTML — likely transient "
                "(portal hangs mid-POST); skipping coverage check this run",
                step="sentinel",
            )
            return True
        fresh = _parse_detail(html, ack_no, DISTRICTS[0], start_page=0, meta=meta) or {}
    except Exception as exc:
        exc_str = str(exc)
        # httpx ReadTimeout / ConnectTimeout / network blip → transient; skip
        # rather than abort the entire crawl on a one-off portal hiccup.
        if "timeout" in exc_str.lower() or "connect" in exc_str.lower():
            logger.warning(
                f"Sentinel: transient network error — {exc}; "
                "skipping coverage check this run",
                step="sentinel",
            )
            return True
        logger.error(f"Sentinel: fetch/parse error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted", step="sentinel")
        return False

    # Verify the fetched project is actually the sentinel project
    scraped_reg = fresh.get("project_registration_no", "")
    if scraped_reg and scraped_reg.upper() != sentinel_reg.upper():
        logger.error(
            f"Sentinel: reg_no mismatch — expected {sentinel_reg!r}, got {scraped_reg!r}",
            step="sentinel",
        )
        insert_crawl_error(
            run_id, config.get("id", "karnataka_rera"),
            "SENTINEL_FAILED", f"reg_no mismatch: {scraped_reg!r}",
        )
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "karnataka_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


# ── Main entry point ──────────────────────────────────────────────────────────

# ── Lister phase ──────────────────────────────────────────────────────────────

def _collect_candidates(
    districts: list[str],
    start_district_idx: int,
    *,
    item_limit: int,
    delay_range: tuple[float, float],
    logger: CrawlerLogger,
    run_id: int,
    site_id: str,
    mode: str,
) -> tuple[list[tuple[int, int, dict]], int, int]:
    """
    Walk districts sequentially and collect listing rows for the per-project
    detail phase.  Stops as soon as ``item_limit`` rows have been collected;
    callers walk any remaining districts in parallel afterwards for the
    ``projects_found`` count.

    Returns ``(candidates, total_found, last_district_idx)`` where
    ``candidates`` is a list of ``(district_idx, page_number, listing_row)``
    tuples.  ``total_found`` counts every row enumerated by the listing
    parser in the districts that were walked here.
    """
    candidates: list[tuple[int, int, dict]] = []
    total_found = 0
    last_district_idx = start_district_idx
    cap = item_limit if item_limit > 0 else None
    target_reg_no = (settings.TARGET_REG_NO or "").strip()
    # Targeted run: skip the district walk entirely.  /projectViewDetails accepts
    # regNo as a direct lookup key (same endpoint the sentinel hits with appNo),
    # so one POST yields the single matching listing row.
    if target_reg_no:
        logger.info(
            f"Targeted run — direct reg_no lookup for {target_reg_no!r}",
            step="listing",
        )
        t0 = time.monotonic()
        row = _search_by_reg_no(target_reg_no, logger)
        logger.timing("search", time.monotonic() - t0, rows=(1 if row else 0))
        if row is None:
            logger.warning(
                f"Target reg_no={target_reg_no!r} not found via direct lookup",
                step="listing",
            )
            return [], 0, last_district_idx
        logger.info(
            f"Target matched: ack={row['acknowledgement_no']!r} "
            f"district={(row.get('project_location_raw') or {}).get('district', '')!r}",
            step="listing",
        )
        return [(start_district_idx, 0, row)], 1, last_district_idx
    first_district_logged = False
    t0 = time.monotonic()
    for district_idx in range(start_district_idx, len(districts)):
        last_district_idx = district_idx
        district = districts[district_idx]
        logger.info(
            f"District {district_idx + 1}/{len(districts)}: {district!r}",
            step="listing",
        )
        html = _post_listing(district, logger)
        if html is None:
            logger.error(
                f"Listing POST failed for district={district!r}",
                step="listing",
            )
            insert_crawl_error(
                run_id, site_id, "HTTP_ERROR",
                f"listing POST failed: district={district}",
                url=PROJECT_URL,
            )
            save_checkpoint(site_id, mode, district_idx + 1, None, run_id)
            continue
        listing_rows = _extract_listing_rows(html, district)
        logger.info(
            f"  {len(listing_rows)} drillable rows",
            district=district, step="listing",
        )
        if listing_rows:
            total_found += len(listing_rows)
            if not first_district_logged:
                logger.timing("search", time.monotonic() - t0, rows=len(listing_rows))
                first_district_logged = True
            for row in listing_rows:
                candidates.append((district_idx, 0, row))
                if cap is not None and len(candidates) >= cap:
                    save_checkpoint(site_id, mode, district_idx, None, run_id)
                    return candidates, total_found, last_district_idx
        save_checkpoint(site_id, mode, district_idx + 1, None, run_id)
        random_delay(*delay_range)
    return candidates, total_found, last_district_idx


# ── Details phase (per-candidate worker) ──────────────────────────────────────

def _process_candidate(
    district_idx: int,
    page_number: int,
    listing_row: dict,
    config: dict,
    run_id: int,
    site_id: str,
    mode: str,
    machine_name: str,
    machine_ip: str,
    state: str,
    logger: CrawlerLogger,
) -> dict:
    """Per-candidate worker — runs in the detail thread pool.

    Performs the two-step detail fetch, normalise + upsert, then S3-uploads
    selected documents (httpx-only; download_jc endpoints are stateless).
    Returns a counter-delta dict aggregated by the orchestrator.
    """
    deltas = {
        "projects_skipped": 0, "projects_new": 0, "projects_updated": 0,
        "documents_uploaded": 0, "error_count": 0,
    }
    ack_no = listing_row["acknowledgement_no"]
    listing_reg_no = listing_row.get("project_registration_no")
    project_key: str | None = None
    if listing_reg_no:
        project_key = generate_project_key(listing_reg_no)
        logger.set_project(key=project_key, reg_no=listing_reg_no,
                           url=PROJECT_URL, page=page_number)
        if mode == "daily_light" and get_project_by_key(project_key):
            deltas["projects_skipped"] += 1
            logger.info(
                f"Skipping — already in DB (daily_light): {listing_reg_no}",
                step="skip",
            )
            logger.clear_project()
            return deltas
    else:
        logger.set_project(reg_no=ack_no, url=PROJECT_URL, page=page_number)
    try:
        detail_html, fetch_meta = _fetch_detail(ack_no, logger, reg_no=listing_reg_no)
        if detail_html:
            detail = _parse_detail(
                detail_html, ack_no, DISTRICTS[district_idx], 0, meta=fetch_meta)
            detail_reg_no = detail.get("project_registration_no", "")
            reg_no = listing_reg_no or detail_reg_no
            if not listing_reg_no:
                if not reg_no:
                    logger.warning(
                        f"No registration number for {ack_no!r} — skipping",
                        step="detail",
                    )
                    deltas["error_count"] += 1
                    return deltas
                project_key = generate_project_key(reg_no)
                logger.set_project(key=project_key, reg_no=reg_no,
                                   url=PROJECT_URL, page=page_number)
                if mode == "daily_light" and get_project_by_key(project_key):
                    deltas["projects_skipped"] += 1
                    logger.info(
                        f"Skipping — already in DB (daily_light): {reg_no}",
                        step="skip",
                    )
                    return deltas
            uploaded_docs = _extract_documents(detail_html, reg_no)
            qpr_doc = _build_qpr_snapshot(detail_html, ack_no, reg_no, logger)
            if qpr_doc is not None:
                uploaded_docs.append(qpr_doc)                              # FIELD: uploaded_documents[] <- inline QPR snapshot (latest panel)
        else:
            if not listing_reg_no:
                logger.warning(
                    f"Detail fetch failed for {ack_no!r} and no listing reg_no — skipping",
                    step="detail",
                )
                deltas["error_count"] += 1
                return deltas
            logger.warning(
                f"Detail fetch failed for {ack_no!r}; skipping (no listing fallback)",
                step="detail",
            )
            deltas["error_count"] += 1
            return deltas

        merged: dict = {
            **detail,
            "acknowledgement_no": ack_no,                              # FIELD: acknowledgement_no <- ack_no from listing_row
            "url":    PROJECT_URL,                                     # FIELD: url <- module constant PROJECT_URL
            "domain": DOMAIN,                                          # FIELD: domain <- module constant DOMAIN
            "state":  state,                                           # FIELD: state <- state arg (config["state"])
            "data":   merge_data_sections(detail.get("data"), {}),     # FIELD: data <- merge_data_sections(detail["data"], {})
            "is_live": True,                                           # FIELD: is_live <- literal True
        }
        if uploaded_docs:
            merged["uploaded_documents"] = uploaded_docs               # FIELD: uploaded_documents <- _extract_documents result
        merged = {k: v for k, v in merged.items() if v is not None}

        try:
            normalized = normalize_project_payload(
                merged, config, machine_name=machine_name, machine_ip=machine_ip)
            record  = ProjectRecord(**normalized)
            db_dict = record.to_db_dict()
            status  = upsert_project(db_dict)
            if status == "new":
                deltas["projects_new"] += 1
                logger.info(f"New: {ack_no}", step="upsert")
            elif status == "updated":
                deltas["projects_updated"] += 1
                logger.info(f"Updated: {ack_no}", step="upsert")
            else:
                deltas["projects_skipped"] += 1
                logger.info(f"Skipped: {ack_no}", step="upsert")

            if uploaded_docs and (mode != "daily_light" or status == "new"):
                enriched, doc_count = _process_documents(
                    project_key, uploaded_docs, run_id, site_id, logger, state)
                deltas["documents_uploaded"] += doc_count
                if doc_count:
                    upsert_project({
                        "key": project_key,                            # FIELD: key <- generate_project_key(reg_no)
                        "uploaded_documents": enriched,                # FIELD: uploaded_documents <- enriched docs from _process_documents
                        "document_urls": build_document_urls(enriched),  # FIELD: document_urls <- build_document_urls(enriched)
                    })
        except ValidationError as exc:
            deltas["error_count"] += 1
            logger.error(f"Validation error for {ack_no}: {exc}", step="validate")
            insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(exc),
                               project_key=project_key, url=PROJECT_URL)
        except Exception as exc:
            deltas["error_count"] += 1
            logger.error(f"Unexpected error for {ack_no}: {exc}", step="upsert")
            insert_crawl_error(run_id, site_id, "CRAWLER_EXCEPTION", str(exc),
                               project_key=project_key, url=PROJECT_URL)
    finally:
        logger.clear_project()
    return deltas


# ── Main entry point ──────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    """Public entry point — ensures the Selenium driver is shut down after the run."""
    try:
        return _run(config, run_id, mode)
    finally:
        _quit_driver()


def _run(config: dict, run_id: int, mode: str) -> dict:
    """
    Karnataka RERA crawl: two-phase lister → details pipeline.

    Phase A (lister)  — walk districts sequentially, collecting up to
                        CRAWL_ITEM_LIMIT listing rows then stopping.
                        projects_found reflects the rows actually walked,
                        not the full state catalog.
    Phase B (details) — process the collected rows through the two-step
                        detail fetch, normalise, upsert and document download
                        (serialised because the SeleniumSession driver is not
                        thread-safe).
    """
    logger = CrawlerLogger(config["id"], run_id)
    counters = dict(
        projects_found=0, projects_new=0, projects_updated=0,
        projects_skipped=0, documents_uploaded=0, error_count=0,
    )
    machine_name, machine_ip = get_machine_context()
    site_id     = config["id"]
    item_limit  = settings.CRAWL_ITEM_LIMIT or 0
    delay_range = config.get("rate_limit_delay", (2, 5))
    state       = config.get("state", "karnataka")
    districts   = DISTRICTS
    t_run = time.monotonic()

    # ── Sentinel health check ────────────────────────────────────────────────
    # Skipped for targeted runs (--target-reg-no): the caller is exercising a
    # single project and a sentinel failure would abort before we get there.
    if (settings.TARGET_REG_NO or "").strip():
        logger.info(
            "Sentinel skipped (targeted run via --target-reg-no)",
            step="sentinel",
        )
    else:
        t0 = time.monotonic()
        if not _sentinel_check(config, run_id, logger):
            logger.error("Sentinel failed — aborting crawl", step="sentinel")
            counters["sentinel_passed"] = False
            counters["error_count"] += 1
            return counters
        counters["sentinel_passed"] = True
        logger.timing("sentinel", time.monotonic() - t0)

    checkpoint = load_checkpoint(config["id"], mode) or {}
    start_district_idx = int(checkpoint.get("last_page", 0))

    # ── Phase A: Lister — collect candidates up to item_limit ────────────────
    candidates, total_found_walked, last_district_idx = _collect_candidates(
        districts, start_district_idx,
        item_limit=item_limit,
        delay_range=delay_range,
        logger=logger, run_id=run_id, site_id=site_id, mode=mode,
    )
    counters["projects_found"] += total_found_walked
    update_crawl_run_progress(run_id, counters)
    logger.info(
        f"Lister phase: collected {len(candidates)} candidates from "
        f"districts {start_district_idx}–{last_district_idx} (found={total_found_walked})",
        step="listing",
    )

    # ── Phase B: Details — serialised processing (single Selenium driver) ────
    if candidates:
        # Force single-worker mode: the shared SeleniumSession is not safe to
        # share across threads. Concurrent driver access produces interleaved
        # navigations and lost cookies.
        n_workers = 1
        logger.info(
            f"Phase B: serial detail fetch ({len(candidates)} candidates, "
            f"{n_workers} worker)",
            step="detail_fetch",
        )
        tB = time.monotonic()

        def _worker(_idx: int, item: tuple[int, int, dict]) -> dict:
            d_idx, p_num, row = item
            return _process_candidate(
                d_idx, p_num, row, config, run_id, site_id, mode,
                machine_name, machine_ip, state, logger,
            )

        def _on_detail_result(_idx: int, deltas: dict | None, exc: Exception | None) -> None:
            # Fold each completed candidate's deltas into the running counters and
            # push them to crawl_runs so the dashboard updates per project, not just
            # once at the end.  Runs serially in this thread (see process_details).
            if exc is not None:
                counters["error_count"] += 1
                logger.exception("Worker raised", exc, step="project_loop")
            else:
                for k, v in (deltas or {}).items():
                    counters[k] = counters.get(k, 0) + v
            update_crawl_run_progress(run_id, counters)

        process_details(candidates, _worker, n_workers=n_workers,
                        on_result=_on_detail_result)
        logger.timing("details", time.monotonic() - tB,
                      items=len(candidates), workers=n_workers)

    reset_checkpoint(config["id"], mode)
    logger.info(f"Karnataka RERA crawl complete: {counters}", step="done")
    logger.timing("total_run", time.monotonic() - t_run)
    return counters

