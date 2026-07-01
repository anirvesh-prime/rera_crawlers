"""
Rajasthan RERA Crawler — rera.rajasthan.gov.in
Type: Pure Selenium (Angular SPA listing + detail page HTML scraping)

Strategy:
- Phase 1: Use Selenium to navigate the Angular listing page
  (ProjectList?status=3) and enumerate all registered projects via
  DataTables HTML scraping.
- Phase 2: For each project, navigate to the detail page with Selenium,
  wait for the Angular SPA to fully render, then parse the rendered HTML
  with BeautifulSoup to extract structured fields.
- Documents: collect all anchor/link elements pointing to PDFs or
  downloadable files from the rendered detail page HTML.
- No direct REST API calls are made; all data is obtained by rendering
  the public-facing website through a real browser.
"""
from __future__ import annotations

import re
import time
from collections.abc import Callable
from datetime import datetime, timezone, timedelta

from pydantic import ValidationError

from bs4 import BeautifulSoup

from core.crawler_base import (
    SeleniumSession,
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
    existing_uploaded_document_entry,
    get_machine_context,
    merge_data_sections,
    normalize_project_payload,
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings

# Rajasthan is in IST (UTC+5:30). /Date(ms)/ timestamps from the RERA API are
# stored as midnight IST values, so we must interpret them in IST and then
# write the result with a "+00:00" suffix (matching production convention).
_IST = timezone(timedelta(hours=5, minutes=30))


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


def _normalize_project_type(raw: str) -> str:
    """Normalize project type string to lowercase-hyphenated format.

    Examples: 'GROUP HOUSING' → 'group-housing', 'Residential' → 'residential'
    """
    return raw.strip().lower().replace(" ", "-")


def _normalize_date_str(val) -> str | None:
    """Normalize any date representation from the Rajasthan RERA APIs to the
    canonical ISO string ``YYYY-MM-DD HH:MM:SS+00:00`` (or ``+00:00`` suffix
    for existing ISO strings).  Returns *None* for invalid / empty / pre-epoch
    sentinel values so callers can skip them cleanly.

    Handled formats
    ---------------
    * ``/Date(<ms>)/``  — .NET JSON date; interpreted in IST (UTC+5:30)
    * ``dd-mm-yyyy``    — plain date string from listing / detail page
    * ``dd/mm/yyyy``    — alternate separator from listing page
    * ``YYYY-MM-DDTHH:MM:SS[.sss]`` — ISO with "T" separator (listing APPROVEDON)
    * ``YYYY-MM-DD HH:MM:SS``       — already-normalised (append ``+00:00``)
    """
    if val is None:
        return None
    v = str(val).strip()
    if v in ("", "null", "None", "0"):
        return None

    # /Date(ms)/ — .NET JSON format
    m = re.match(r"^/Date\((-?\d+)\)/$", v)
    if m:
        ms = int(m.group(1))
        if ms <= 0:
            return None  # sentinel for year 0001 or invalid
        try:
            dt = datetime.fromtimestamp(ms / 1000, tz=_IST)
            return dt.strftime("%Y-%m-%d %H:%M:%S") + "+00:00"
        except (ValueError, OSError):
            return None

    # dd-mm-yyyy  or  dd/mm/yyyy
    m = re.match(r"^(\d{2})[-/](\d{2})[-/](\d{4})$", v)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)} 00:00:00+00:00"

    # YYYY-MM-DDTHH:MM:SS[.sss…]  (ISO with 'T' separator)
    m = re.match(r"^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})", v)
    if m:
        return f"{m.group(1)} {m.group(2)}+00:00"

    # Already YYYY-MM-DD HH:MM:SS (with or without timezone suffix)
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", v):
        return v if "+" in v else v + "+00:00"

    # Unrecognised — return as-is so we don't silently discard data
    return v





def _format_inr(amount: float) -> str:
    """Format a float as Indian currency string: ₹X,XX,XX,XXX.XX"""
    rounded  = round(amount, 2)
    int_part = int(rounded)
    dec_str  = f"{rounded - int_part:.2f}"[1:]   # ".xx"
    s = str(int_part)
    if len(s) <= 3:
        return f"₹{s}{dec_str}"
    result, s = s[-3:], s[:-3]
    while s:
        result, s = s[-2:] + "," + result, s[:-2]
    return f"₹{result}{dec_str}"


BASE_URL         = "https://rera.rajasthan.gov.in"
STATE_CODE       = "RJ"
DOMAIN           = "rera.rajasthan.gov.in"
LISTING_PAGE_URL = f"{BASE_URL}/ProjectList?status=3"
GETPROJECTS_API_URL = "https://reraapi.rajasthan.gov.in/api/web/Home/GetProjects"
_GETPROJECTS_API_FRAGMENT = "/api/web/Home/GetProjects"

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Selenium listing table field → schema field
_LIST_API_TO_FIELD: dict[str, str] = {
    "reg_no":         "project_registration_no",
    "project_name":   "project_name",
    "promoter_name":  "promoter_name",
    "project_type":   "project_type",
    "district":       "project_city",
    "application_no": "acknowledgement_no",
    "approved_on":    "approved_on_date",
    "status":         "status_of_the_project",
}


def _clean(text) -> str:
    """Strip and collapse whitespace (used by the Selenium listing scraper)."""
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def _bare_registration_no(reg_no: str) -> str:
    """Strip Rajasthan listing-only suffixes such as ``(28/04/2026)``."""
    cleaned = _clean(reg_no)
    match = re.search(r"\bRAJ/[A-Z]+/\d{4}/\d+\b", cleaned, flags=re.I)
    if match:
        return match.group(0).upper()
    return re.sub(r"\s*\([^)]*\)\s*$", "", cleaned).strip()


def _identity_registration_no(reg_no: str) -> str:
    """Return the Rajasthan registration string used for project-key identity."""
    cleaned = _clean(reg_no)
    if re.match(r"^RAJ/[A-Z]+/\d{4}/\d+\s+\(\d{2}/\d{2}/\d{4}\)$", cleaned, flags=re.I):
        return cleaned.upper()
    return _bare_registration_no(cleaned)


def _flush_progress_logs(logger: CrawlerLogger) -> None:
    """Best-effort flush so remote dashboards see long-phase heartbeats."""
    try:
        logger._flush_db()
    except Exception:
        pass


def _first_number(text: str) -> float | None:
    """Return the first numeric token from text such as '2429.23 Sq Mtrs'."""
    m = re.search(r"-?\d+(?:,\d{2,3})*(?:\.\d+)?|-?\d+(?:\.\d+)?", text or "")
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


def _is_real_document(resp) -> bool:
    """Return True only if the response body looks like an actual document (not a soft-404 HTML page)."""
    if resp is None:
        return False
    content_type = resp.headers.get("Content-Type", "").lower()
    chunk = resp.content[:8] if resp.content else b""
    if chunk.startswith(b"%PDF"):
        return True
    if "text/html" in content_type or "text/plain" in content_type:
        return False
    return len(chunk) > 0


def _resolve_relative_url(path: str) -> str:
    """Resolve a relative document path to an absolute URL using BASE_URL."""
    clean = path.replace("~/", "").replace("~\\", "").replace("../", "").replace("..\\", "")
    if not clean.startswith("/"):
        clean = f"/{clean}"
    return f"{BASE_URL}{clean}"


def _build_doc_url(path: str | None) -> str | None:
    """Return an absolute URL for a document path (relative or absolute)."""
    if not path or path == "0":
        return None
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return _resolve_relative_url(path)


# ── Phase A: listing traversal helpers ───────────────────────────────────────

def _extract_rj_table_rows(page) -> list[dict]:
    """
    Extract visible project rows from the Angular DataTables listing.
    Dynamically maps column indices from header text.
    """
    soup = BeautifulSoup(page.content(), "lxml")
    rows: list[dict] = []
    table = soup.select_one("table[datatable], table.dataTable, #project-list-table, table")
    if not table:
        return rows

    headers = [th.get_text(strip=True).lower() for th in table.select("thead th")]
    col_map: dict[str, int] = {}
    for i, h in enumerate(headers):
        if "district" in h:
            col_map["district"] = i
        elif "project name" in h and "no" not in h:
            col_map["project_name"] = i
        elif "project type" in h:
            col_map["project_type"] = i
        elif "promoter" in h:
            col_map["promoter_name"] = i
        elif "application" in h:
            col_map["application_no"] = i
        elif "registration" in h:
            col_map["reg_no"] = i
        elif "approved" in h or "approvedon" in h:
            col_map["approved_on"] = i
        elif "status" in h:
            col_map["status"] = i

    reg_re = re.compile(r"\bRAJ/[A-Z]+/\d{4}/\d+(?:\s*\([^)]*\))?", re.I)
    fallback_cols = {
        "district": 1,
        "project_name": 2,
        "project_type": 3,
        "promoter_name": 4,
        "application_no": 5,
        "reg_no": 6,
        "approved_on": 7,
        "status": 8,
    }

    for tr in table.select("tbody tr"):
        cells = tr.select("td")
        if not cells:
            continue
        if len(cells) == 1 and re.search(r"no\s+data|loading|processing", cells[0].get_text(" ", strip=True), re.I):
            continue
        row: dict = {}
        active_col_map = col_map or fallback_cols
        for field, idx in active_col_map.items():
            if idx < len(cells):
                row[field] = cells[idx].get_text(strip=True)

        if not row.get("reg_no"):
            for cell in cells:
                match = reg_re.search(cell.get_text(" ", strip=True))
                if match:
                    row["reg_no"] = match.group(0).strip()
                    break

        detail_link = tr.select_one(
            "a[href*='ProjectDetail'], a[href*='ViewProjectNew'], "
            "a[href*='ViewProject'], a[href*='project']"
        )
        if detail_link and detail_link.get("href"):
            detail_url = _resolve_relative_url(detail_link["href"].strip())
            if detail_url:
                row["detail_url"] = detail_url

        if row.get("reg_no"):
            rows.append(row)

    return rows


# Browser-side network tracker used only to wait for the Angular listing XHR.
# The crawler does not call reraapi directly.
_GETPROJECTS_TRACKER_JS = f"""
    (() => {{
        if (window.__rjGetProjectsTrackerInstalled) return;
        window.__rjGetProjectsTrackerInstalled = true;
        window.__rjGetProjects = {{
            seen: false,
            pending: 0,
            completed: 0,
            failed: 0,
            lastUrl: ""
        }};
        const isTarget = (url) => String(url || "").includes("{_GETPROJECTS_API_FRAGMENT}");
        const markStart = (url) => {{
            if (!isTarget(url)) return false;
            window.__rjGetProjects.seen = true;
            window.__rjGetProjects.pending += 1;
            window.__rjGetProjects.lastUrl = String(url || "");
            return true;
        }};
        const markEnd = (hit, failed) => {{
            if (!hit) return;
            window.__rjGetProjects.pending = Math.max(0, window.__rjGetProjects.pending - 1);
            if (failed) window.__rjGetProjects.failed += 1;
            else window.__rjGetProjects.completed += 1;
        }};

        const originalFetch = window.fetch;
        if (originalFetch) {{
            window.fetch = function () {{
                const req = arguments[0];
                const url = req && req.url ? req.url : req;
                const hit = markStart(url);
                return originalFetch.apply(this, arguments).then(
                    (response) => {{ markEnd(hit, false); return response; }},
                    (error) => {{ markEnd(hit, true); throw error; }}
                );
            }};
        }}

        const OriginalXHR = window.XMLHttpRequest;
        if (OriginalXHR) {{
            window.XMLHttpRequest = function () {{
                const xhr = new OriginalXHR();
                let requestUrl = "";
                let hit = false;
                const originalOpen = xhr.open;
                xhr.open = function (method, url) {{
                    requestUrl = String(url || "");
                    return originalOpen.apply(xhr, arguments);
                }};
                xhr.addEventListener("loadstart", () => {{
                    hit = markStart(requestUrl);
                }});
                xhr.addEventListener("loadend", () => markEnd(hit, false));
                xhr.addEventListener("error", () => markEnd(hit, true));
                xhr.addEventListener("abort", () => markEnd(hit, true));
                return xhr;
            }};
        }}
    }})();
"""


def _install_getprojects_tracker(page) -> None:
    """Install a browser-side tracker before the listing page starts its XHRs."""
    driver = getattr(page, "_driver", None)
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": _GETPROJECTS_TRACKER_JS},
        )
    except Exception:
        pass


def _reset_getprojects_tracker(page) -> None:
    try:
        page.evaluate(f"""() => {{
            window.__rjGetProjects = {{
                seen: false,
                pending: 0,
                completed: 0,
                failed: 0,
                lastUrl: ""
            }};
            if (performance && performance.clearResourceTimings) {{
                performance.clearResourceTimings();
            }}
        }}""")
    except Exception:
        pass


def _wait_for_getprojects_request(
    page,
    logger: CrawlerLogger,
    *,
    timeout: int = 60_000,
    warn_on_timeout: bool = True,
) -> bool:
    """
    Wait until the public Angular page finishes its GetProjects request.

    This observes browser network activity/performance entries only; it never
    issues the Rajasthan API request itself.
    """
    try:
        page.wait_for_function(
            f"""() => {{
                const state = window.__rjGetProjects || {{}};
                const pending = Number(state.pending || 0);
                const completed = Number(state.completed || 0);
                const failed = Number(state.failed || 0);
                const perfDone = (performance.getEntriesByType("resource") || [])
                    .some((entry) =>
                        String(entry.name || "").includes("{_GETPROJECTS_API_FRAGMENT}")
                        && Number(entry.responseEnd || 0) > 0
                    );
                return pending === 0 && (completed > 0 || failed > 0 || perfDone);
            }}""",
            timeout=timeout,
        )
        logger.info("Rajasthan GetProjects request completed", step="listing")
        return True
    except Exception as exc:
        if warn_on_timeout:
            logger.warning(
                f"Timed out waiting for GetProjects request completion: {exc}",
                step="listing",
                url=GETPROJECTS_API_URL,
            )
        return False


def _wait_for_listing_table(page, logger: CrawlerLogger, *, timeout: int = 30_000) -> bool:
    try:
        page.wait_for_selector(
            "table[datatable], table.dataTable, #project-list-table, table tbody tr",
            timeout=timeout,
        )
        page.wait_for_load_state("networkidle", timeout=timeout)
        return True
    except Exception as exc:
        logger.warning(f"DataTables table not ready: {exc}", step="listing")
        return False


def _set_listing_page_size_to_max(page, logger: CrawlerLogger) -> int | None:
    """Use Selenium to select the largest visible listing page-size option."""
    selector = "select.form-select.d-inline-block.w-auto"
    try:
        page.wait_for_selector(selector, timeout=15_000)
        options = page.evaluate(
            """(selector) => {
                const select = document.querySelector(selector);
                if (!select) return [];
                return Array.from(select.options)
                    .map((opt) => ({value: opt.value, text: opt.textContent.trim()}))
                    .filter((opt) => /^\\d+$/.test(opt.value));
            }""",
            selector,
        )
        if not options:
            logger.warning("Rajasthan listing page-size select has no numeric options", step="listing")
            return None

        max_value = str(max(int(opt["value"]) for opt in options))
        current_value = page.evaluate(
            """(selector) => {
                const select = document.querySelector(selector);
                return select ? String(select.value || "") : "";
            }""",
            selector,
        )
        if current_value == max_value:
            logger.info(f"Rajasthan listing page size already {max_value}", step="listing")
            return int(max_value)

        _reset_getprojects_tracker(page)
        select_el = page.locator(selector).first._first_element(timeout_ms=10_000)
        try:
            page.evaluate(
                """(el) => {
                    el.scrollIntoView({block: "center", inline: "center"});
                }""",
                select_el,
            )
        except Exception:
            pass

        try:
            from selenium.webdriver.support.ui import Select
            Select(select_el).select_by_value(max_value)
        except Exception:
            page.evaluate(
                """(el, value) => {
                    el.value = value;
                    el.dispatchEvent(new Event("input", {bubbles: true}));
                    el.dispatchEvent(new Event("change", {bubbles: true}));
                }""",
                select_el,
                max_value,
            )

        logger.info(f"Set Rajasthan listing page size to {max_value}", step="listing")
        page.wait_for_timeout(1_000)
        _wait_for_getprojects_request(page, logger, timeout=8_000, warn_on_timeout=False)
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        return int(max_value)
    except Exception as exc:
        logger.warning(f"Could not set Rajasthan listing page size to maximum: {exc}", step="listing")
        return None


def _scrape_project_list(
    logger: CrawlerLogger,
    *,
    max_pages: int | None = None,
    enough_rows: int | None = None,
    check_existing: bool = False,
    max_checked_rows: int | None = None,
    on_progress: Callable[
        [int, int, int, str | None, str | None, str | None, str | None, str | None],
        None,
    ] | None = None,
) -> tuple[list[dict], int, int]:
    """
    Navigate the Rajasthan RERA Angular SPA listing page and inspect project rows.
    Returns candidate dicts with keys: reg_no, project_name, promoter_name,
    project_type, district, application_no, approved_on, status.

    When ``max_pages`` is set the walker stops after N listing pages.  When
    ``enough_rows`` is set it stops once that many candidates have been found.
    In daily_light, ``check_existing`` makes this a DB-backed lister: each
    row's reg_no is checked immediately and only DB-missing rows continue to
    detail parsing.
    """
    projects: list[dict] = []
    checked_rows = 0
    skipped_existing_total = 0
    seen_reg_nos: set[str] = set()

    def _accept_rows(page_rows: list[dict]) -> tuple[int, int]:
        nonlocal checked_rows, skipped_existing_total
        accepted = 0
        skipped_existing = 0
        for row in page_rows:
            if max_checked_rows is not None and checked_rows >= max_checked_rows:
                break
            raw_reg_no = row.get("reg_no") or ""
            reg_no = _bare_registration_no(raw_reg_no)
            if not reg_no:
                continue
            row["reg_no"] = reg_no
            row["raw_reg_no"] = str(raw_reg_no or "")
            identity_reg_no = _identity_registration_no(raw_reg_no)
            row["identity_reg_no"] = identity_reg_no
            project_key = generate_project_key(identity_reg_no)
            bare_project_key = (
                generate_project_key(reg_no)
                if identity_reg_no != reg_no
                else None
            )
            checked_rows += 1

            reg_key = reg_no.upper()
            if reg_key in seen_reg_nos:
                _publish_progress(reg_no, str(raw_reg_no or ""), project_key, bare_project_key)
                continue
            seen_reg_nos.add(reg_key)

            existing_match_key = None
            if check_existing:
                if get_project_by_key(project_key):
                    existing_match_key = project_key
                elif bare_project_key and get_project_by_key(bare_project_key):
                    existing_match_key = bare_project_key

            if existing_match_key:
                skipped_existing += 1
                skipped_existing_total += 1
                _publish_progress(
                    reg_no,
                    str(raw_reg_no or ""),
                    project_key,
                    bare_project_key,
                    existing_match_key,
                )
                continue
            projects.append(row)
            accepted += 1
            _publish_progress(reg_no, str(raw_reg_no or ""), project_key, bare_project_key)
        return accepted, skipped_existing

    def _publish_progress(
        reg_no: str | None = None,
        raw_reg_no: str | None = None,
        project_key: str | None = None,
        bare_project_key: str | None = None,
        existing_match_key: str | None = None,
    ) -> None:
        if not on_progress:
            return
        try:
            on_progress(
                checked_rows,
                skipped_existing_total,
                len(projects),
                reg_no,
                raw_reg_no,
                project_key,
                bare_project_key,
                existing_match_key,
            )
        except Exception as exc:
            logger.warning(f"Rajasthan listing progress update failed: {exc}", step="listing")

    try:
        logger.info("Starting Rajasthan browser session", step="timing")
        _flush_progress_logs(logger)
        page = page_adapter(_session())
        logger.info("Rajasthan browser session ready", step="timing")
        _flush_progress_logs(logger)
        _install_getprojects_tracker(page)
        logger.info("Starting Rajasthan listing scrape", step="timing")
        _flush_progress_logs(logger)
        _reset_getprojects_tracker(page)
        page.goto(LISTING_PAGE_URL, timeout=60_000)

        # Wait for the public Angular page's own GetProjects request before
        # parsing rows. This only observes browser network activity.
        if not _wait_for_getprojects_request(page, logger, timeout=60_000):
            return projects, checked_rows, skipped_existing_total
        if not _wait_for_listing_table(page, logger, timeout=30_000):
            return projects, checked_rows, skipped_existing_total
        _set_listing_page_size_to_max(page, logger)
        if not _wait_for_listing_table(page, logger, timeout=30_000):
            return projects, checked_rows, skipped_existing_total

        # ── Extract rows then paginate through every page ─────────────────
        # Selector covers: DataTables classic, Bootstrap 3/4/5,
        # Angular Material (mat-paginator), and aria-label variants.
        _NEXT_SEL = (
            "a.paginate_button.next:not(.disabled), "
            "li.paginate_button.next:not(.disabled) a, "
            "li.next:not(.disabled) a, "
            "a[aria-label='Next']:not(.disabled), "
            "button[aria-label='Next page']:not([disabled]), "
            "button[aria-label='next']:not([disabled]), "
            "button.mat-mdc-paginator-navigation-next:not([disabled]), "
            "button.mat-paginator-navigation-next:not([disabled]), "
            ".pagination .next:not(.disabled) a, "
            "a:has-text('Next'):not(.disabled)"
        )

        _before = checked_rows
        page_rows = _extract_rj_table_rows(page)
        accepted, skipped_existing = _accept_rows(page_rows)
        rows_checked_on_page = checked_rows - _before
        if not page_rows:
            try:
                diag = page.evaluate("""() => {
                    const table = document.querySelector('table[datatable], table.dataTable, #project-list-table, table');
                    const headers = table ? Array.from(table.querySelectorAll('thead th')).map(th => th.innerText.trim()) : [];
                    const bodyRows = table ? Array.from(table.querySelectorAll('tbody tr')).slice(0, 3).map(tr => tr.innerText.trim()) : [];
                    const allTables = document.querySelectorAll('table').length;
                    const bodyText = document.body ? document.body.innerText.slice(0, 1000) : '';
                    return {url: location.href, title: document.title, allTables, headers, bodyRows, bodyText};
                }""")
                logger.error(
                    "Rajasthan listing rendered zero project rows",
                    step="listing",
                    diagnostic=diag,
                )
            except Exception as exc:
                logger.error(
                    f"Rajasthan listing rendered zero project rows and diagnostics failed: {exc}",
                    step="listing",
                )
        logger.info(
            f"Listing page 1 saw {len(page_rows)} rows; checked {rows_checked_on_page}; "
            f"accepted {accepted}, existing {skipped_existing} "
            f"({checked_rows} checked, {len(projects)} candidates)",
            step="timing",
            page=1,
            rows=checked_rows,
            page_rows=len(page_rows),
            checked_on_page=rows_checked_on_page,
            candidates=len(projects),
        )
        _flush_progress_logs(logger)

        _page_num    = 1
        _stall_guard = 0    # consecutive pages with no new rows
        while True:
            if max_pages is not None and _page_num >= max_pages:
                logger.info(
                    f"max_pages={max_pages} reached — stopping pagination",
                    step="listing",
                )
                break
            if max_checked_rows is not None and checked_rows >= max_checked_rows:
                logger.info(
                    f"max_checked_rows={max_checked_rows} reached "
                    f"(checked {checked_rows}) — stopping pagination",
                    step="listing",
                )
                break
            if enough_rows is not None and len(projects) >= enough_rows:
                logger.info(
                    f"enough_rows={enough_rows} reached "
                    f"(have {len(projects)}) — stopping pagination",
                    step="listing",
                )
                break
            try:
                next_locator = page.locator(_NEXT_SEL)
                if next_locator.count() == 0:
                    logger.info("No next-button found — pagination complete")
                    break
                next_btn = next_locator.first
                # Use the underlying WebElement (Selenium) for .is_displayed() —
                # locator.first returns the locator; resolve to an element here.
                try:
                    next_el = next_btn._first_element()
                    if not next_el.is_displayed():
                        logger.info("Next-button not visible — pagination complete")
                        break
                    # Bootstrap wraps <a> in <li class="page-item disabled"> on
                    # the last page — the link is visible but the parent
                    # blocks clicks.
                    from selenium.webdriver.common.by import By
                    parent = next_el.find_element(By.XPATH, "..")
                    if "disabled" in (parent.get_attribute("class") or ""):
                        logger.info("Next-button parent is disabled — pagination complete")
                        break
                except Exception:
                    pass

                _before = checked_rows
                _seen_before = len(seen_reg_nos)
                _click_locator_stably(page, next_btn, timeout=15_000)
                page.wait_for_load_state("networkidle", timeout=15_000)
                page.wait_for_timeout(1_000)
                page_rows = _extract_rj_table_rows(page)
                accepted, skipped_existing = _accept_rows(page_rows)
                rows_checked_on_page = checked_rows - _before
                _page_num += 1
                logger.info(
                    f"Listing page {_page_num} saw {len(page_rows)} rows; checked {rows_checked_on_page}; "
                    f"accepted {accepted}, existing {skipped_existing} "
                    f"({checked_rows} checked, {len(projects)} candidates)",
                    step="timing",
                    page=_page_num,
                    rows=checked_rows,
                    page_rows=len(page_rows),
                    checked_on_page=rows_checked_on_page,
                    candidates=len(projects),
                )
                _flush_progress_logs(logger)

                # Guard: stop if no new data arrived (disabled button stayed
                # visible, or click had no effect).
                if len(seen_reg_nos) == _seen_before:
                    _stall_guard += 1
                    if _stall_guard >= 2:
                        logger.warning("Pagination stalled (no new rows) — stopping")
                        break
                else:
                    _stall_guard = 0
            except Exception as e:
                logger.warning(f"Pagination stopped: {e}")
                break

    except Exception as exc:
        logger.error(f"Selenium listing scrape failed: {exc}")

    logger.info(
        f"Rajasthan page inspection: checked {checked_rows} rows; "
        f"{len(projects)} candidates for detail"
    )
    return projects, checked_rows, skipped_existing_total


# ── HTML label → schema field map for the rendered detail page ────────────────
_DETAIL_LABEL_FIELD_MAP: dict[str, str] = {
    # Project basics
    "project name": "project_name",
    "name of the project": "project_name",
    "project title": "project_name",
    "registration no": "project_registration_no",
    "registration no.": "project_registration_no",
    "registration number": "project_registration_no",
    "rera registration no": "project_registration_no",
    "rera registration number": "project_registration_no",
    "rajasthan rera reg. no.": "project_registration_no",
    "rajasthan rera reg. no": "project_registration_no",
    "promoter name": "promoter_name",
    "promoter details": "promoter_name",
    "name of promoter": "promoter_name",
    "developer name": "promoter_name",
    "project type": "project_type",
    "type of project": "project_type",
    "project category": "project_type",
    "district": "project_city",
    "district name": "project_city",
    "application no": "acknowledgement_no",
    "application no.": "acknowledgement_no",
    "application number": "acknowledgement_no",
    "acknowledgement no": "acknowledgement_no",
    "project status": "status_of_the_project",
    "status": "status_of_the_project",
    # Dates
    "approved on": "approved_on_date",
    "date of approval": "approved_on_date",
    "registration date": "approved_on_date",
    "date of registration": "submitted_date",
    "application date": "submitted_date",
    "submitted date": "submitted_date",
    "revised date of completion": "estimated_finish_date",
    "estimated finish date": "estimated_finish_date",
    "date of completion": "estimated_finish_date",
    "expected date of completion": "estimated_finish_date",
    "commencement date": "estimated_commencement_date",
    "actual commencement date": "actual_commencement_date",
    "actual finish date": "actual_finish_date",
    # Areas
    "plot area": "land_area",
    "land area": "land_area",
    "phase area": "land_area",
    "phase area of units": "land_area",
    "total land area": "land_area",
    "total area of project": "land_area",
    "built up area": "construction_area",
    "builtup area": "construction_area",
    "total built up area": "construction_area",
    "construction area": "construction_area",
    "saleable area": "construction_area",
    # Units
    "total residential units": "number_of_residential_units",
    "number of residential units": "number_of_residential_units",
    "no. of residential units": "number_of_residential_units",
    "total commercial units": "number_of_commercial_units",
    "number of commercial units": "number_of_commercial_units",
    # Location
    "taluk": "taluk",
    "tehsil": "taluk",
    "tahsil": "taluk",
    "village": "village",
    "village name": "village",
    "locality": "locality",
    "pin code": "pin_code",
    "pincode": "pin_code",
    # Description
    "project description": "project_description",
    "description": "project_description",
}

_DOC_EXTENSIONS = frozenset({
    ".pdf", ".doc", ".docx", ".xlsx", ".xls", ".jpg", ".jpeg", ".png", ".zip",
})
_DOC_PATH_KEYWORDS = ("download", "/content/", "/uploads/", "getfile", "viewdoc", "getdocument")

# Anchor texts that carry no meaningful document-type information.
# When a link has one of these generic texts we fall back to the parent
# table-row's first cell (which contains the real document category).
_GENERIC_ANCHOR_TEXTS = frozenset({
    "view", "view photo", "download", "click here", "here", "open", "pdf", "",
})


def _extract_kv_from_html(soup: BeautifulSoup) -> dict[str, str]:
    """Extract key-value pairs from multiple Bootstrap/Angular HTML patterns."""
    kv: dict[str, str] = {}

    # Strategy 0: current Rajasthan ViewProjectNew print layout.
    # Labels are inline spans inside <p>/<td>, e.g.
    # <span class="label">Project Name:</span> VENTURA
    for label_el in soup.select("span.label"):
        label = _clean(label_el.get_text()).rstrip(":").strip()
        if not label or len(label) > 80:
            continue
        parent = label_el.parent
        if not parent or not hasattr(parent, "get_text"):
            continue
        full_text = _clean(parent.get_text(" ", strip=True))
        raw_label = _clean(label_el.get_text(" ", strip=True))
        value = full_text
        if raw_label and value.lower().startswith(raw_label.lower()):
            value = _clean(value[len(raw_label):])
        if not value and label_el.next_sibling:
            value = _clean(str(label_el.next_sibling))
        if value:
            kv.setdefault(label, value)

    # Strategy 5 (highest priority): Rajasthan RERA 2.0 Angular pattern
    # <div class="details"><span class="label">…</span><span class="value">…</span></div>
    for div in soup.find_all("div", class_="details"):
        label_el = div.select_one(".label")
        value_el = div.select_one(".value")
        if not label_el or not value_el:
            continue
        label = _clean(label_el.get_text())
        # Exclude embedded <a> link text (e.g. "View" anchors inside the value span)
        value_parts = [
            t.strip() for t in value_el.strings
            if t.strip() and getattr(t.parent, "name", None) != "a"
        ]
        value = _clean(" ".join(value_parts))
        if label and value and len(label) < 80:
            kv.setdefault(label, value)

    # Strategy 1: <tr> with <th>label</th><td>value</td>
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) >= 2:
            label = _clean(cells[0].get_text())
            value = _clean(cells[1].get_text())
            if label and value and len(label) < 80:
                kv.setdefault(label, value)

    # Strategy 2: Bootstrap row with two-column label + value layout
    for row in soup.find_all(class_=re.compile(r"\brow\b")):
        cols = [c for c in row.children
                if hasattr(c, "get_text") and hasattr(c, "get")
                and re.search(r"\bcol\b", " ".join(c.get("class", [])))]
        if len(cols) >= 2:
            label = _clean(cols[0].get_text())
            value = _clean(cols[1].get_text())
            if label and value and len(label) < 80:
                kv.setdefault(label, value)

    # Strategy 3: <label> element followed by sibling/parent-sibling with value
    for label_el in soup.find_all("label"):
        label_text = _clean(label_el.get_text())
        if not label_text or len(label_text) > 80:
            continue
        sibling = label_el.find_next_sibling()
        if not sibling:
            parent = label_el.parent
            sibling = parent.find_next_sibling() if parent else None
        if sibling and hasattr(sibling, "get_text"):
            value = _clean(sibling.get_text())
            if value:
                kv.setdefault(label_text, value)

    # Strategy 4: <dl><dt>label</dt><dd>value</dd></dl>
    for dl in soup.find_all("dl"):
        for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
            label = _clean(dt.get_text())
            value = _clean(dd.get_text())
            if label and value:
                kv.setdefault(label, value)

    return kv


def _parse_viewproject_html(soup: BeautifulSoup) -> dict:  # noqa: C901
    """Parse the full-detail ViewProject popup page (table-based layout).

    The popup at /ViewProject?id=...&type=U contains all the rich project data
    that is hidden behind the 'Updated project details' View link on the main
    ProjectDetail page. It uses <table> sections identified by a header row.
    """
    compact_fallback = _parse_detail_html(soup)
    out: dict = {}

    def _tbl_heading(tbl) -> str:
        rows = tbl.find_all("tr")
        if rows:
            return _clean(rows[0].get_text()).lower()
        return ""

    def _find_table(*keywords: str):
        for tbl in soup.find_all("table"):
            h = _tbl_heading(tbl)
            if all(kw.lower() in h for kw in keywords):
                return tbl
        return None

    def _parse_kv_rows(tbl, skip: int = 1) -> dict:
        kv: dict = {}
        rows = tbl.find_all("tr")[skip:]
        for tr in rows:
            cells = [_clean(td.get_text()) for td in tr.find_all(["td", "th"])]
            if len(cells) >= 4:
                if cells[0]:
                    kv[cells[0]] = cells[1]
                if cells[2]:
                    kv[cells[2]] = cells[3]
            elif len(cells) == 2 and cells[0]:
                kv[cells[0]] = cells[1]
        return kv

    def _parse_allottee_building_details() -> list[dict]:
        text = soup.get_text("\n", strip=True)
        pattern = re.compile(
            r"Building\s*:\s*(?P<building>.*?)\s*"
            r"\(\s*Apartment\s*:\s*(?P<apartment>.*?)\s*,\s*"
            r"Block\s*:\s*(?P<block>.*?)\s*,\s*"
            r"Carpet\s+Area\s*:\s*(?P<carpet>[\d.]+)\s*sq\.?\s*meters?\s*\)\s*"
            r"Number\s+Of\s+Apartments\s*:\s*(?P<total>\d+)\s*,\s*"
            r"Booked\s*:\s*(?P<booked>\d+)",
            re.I | re.S,
        )
        units: list[dict] = []
        seen: set[tuple[str, str, str, str, str]] = set()
        for match in pattern.finditer(text):
            flat_type = _clean(match.group("apartment"))
            block = _clean(match.group("block"))
            carpet = _clean(match.group("carpet"))
            total = _clean(match.group("total"))
            booked = _clean(match.group("booked"))
            key = (flat_type, block, carpet, total, booked)
            if key in seen:
                continue
            seen.add(key)
            units.append({
                "flat_type": flat_type,
                "block_name": block,
                "carpet_area": carpet,
                "no_of_units": total,
                "booking_detail": booked,
            })
        return units

    # ── Bank details ──────────────────────────────────────────────────────────
    bank_tbl = _find_table("detail of separate bank account")
    if bank_tbl:
        kv = _parse_kv_rows(bank_tbl)
        bank: dict = {}
        for k, v in kv.items():
            n = k.lower()
            if "bank name" in n:           bank["bank_name"]    = v  # FIELD: bank_details.bank_name <- bank_tbl label "bank name"
            elif "branch name" in n:       bank["branch"]        = v  # FIELD: bank_details.branch <- bank_tbl label "branch name"
            elif "ifsc" in n:              bank["IFSC"]          = v  # FIELD: bank_details.IFSC <- bank_tbl label "ifsc"
            elif "a/c number" in n:        bank["account_no"]    = v  # FIELD: bank_details.account_no <- bank_tbl label "a/c number"
            elif "account holder" in n:    bank["account_name"]  = v  # FIELD: bank_details.account_name <- bank_tbl label "account holder"
            elif "bank address" in n:      bank["address"]       = v  # FIELD: bank_details.address <- bank_tbl label "bank address"
        if bank:
            out["bank_details"] = bank  # FIELD: bank_details <- ViewProject "bank account" table

    # ── Land area & units ─────────────────────────────────────────────────────
    land_tbl = _find_table("land details")
    if land_tbl:
        kv = _parse_kv_rows(land_tbl)
        for k, v in kv.items():
            n = k.lower()
            if ("total area" in n or "phase area" in n) and "land_area" not in out:
                try:
                    out["land_area"] = float(v.replace(",", ""))  # FIELD: land_area <- land_tbl "total area"/"phase area"
                except (ValueError, TypeError):
                    pass
            if ("sanctioned number" in n or "number of apartments" in n) and v not in ("0", ""):
                try:
                    # FIELD: number_of_residential_units <- land_tbl "sanctioned"/"apartments"
                    out.setdefault("number_of_residential_units", int(float(v)))
                except (ValueError, TypeError):
                    pass

    # ── Construction area ─────────────────────────────────────────────────────
    sba_tbl = _find_table("total built up area")
    if sba_tbl:
        kv = _parse_kv_rows(sba_tbl)
        for k, v in kv.items():
            if "built up area" in k.lower():
                try:
                    out["construction_area"] = float(v.replace(",", ""))  # FIELD: construction_area <- sba_tbl "built up area"
                except (ValueError, TypeError):
                    pass

    # ── Project location ──────────────────────────────────────────────────────
    loc_tbl = _find_table("location of project")
    if loc_tbl:
        kv = _parse_kv_rows(loc_tbl)
        loc: dict = {}
        _loc_map = {
            "state": "state", "district": "district",
            "tehsil": "taluk", "village": "village",
            "plot": "house_no_building_name", "khasra": "house_no_building_name",
            "ward": "ward", "street": "locality", "locality": "locality",
            "post office": "post_office", "pincode": "pin_code", "pin code": "pin_code",
        }
        for k, v in kv.items():
            if not v or v in ("-", "NA", ""):
                continue
            n = k.lower()
            for kw, field in _loc_map.items():
                if kw in n:
                    loc.setdefault(field, v)
                    break
        if loc:
            parts = [v for fld, v in loc.items() if fld not in ("state",) and v]
            loc["raw_address"] = ", ".join(parts)
            out["project_location_raw"] = loc  # FIELD: project_location_raw <- ViewProject "location of project" table

    # ── Project description (Remark) ──────────────────────────────────────────
    remark_tbl = _find_table("remark about project")
    if remark_tbl:
        rows = remark_tbl.find_all("tr")
        if len(rows) >= 2:
            desc = _clean(rows[1].get_text())
            if desc:
                out["project_description"] = desc  # FIELD: project_description <- ViewProject "remark about project" table

    # ── Project cost ──────────────────────────────────────────────────────────
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if not rows:
            continue
        hdrs = [_clean(c.get_text()).lower() for c in rows[0].find_all(["td", "th"])]
        if "title" in hdrs and "value" in hdrs:
            cost: dict = {}
            for tr in rows[1:]:
                cells = [_clean(td.get_text()) for td in tr.find_all(["td", "th"])]
                if len(cells) < 3 or not cells[1] or not cells[2]:
                    continue
                title, val = cells[1].lower(), cells[2]
                # FIELD: project_cost_detail.cost_of_land <- cost row "land cost"
                if "land cost" in title:               cost["cost_of_land"] = val
                elif "estimated" in title and "construction" in title:
                    # FIELD: project_cost_detail.estimated_construction_cost <- cost row "estimated construction"
                    cost["estimated_construction_cost"] = val
                elif "estimated" in title and "project" in title:
                    # FIELD: project_cost_detail.estimated_project_cost <- cost row "estimated project"
                    cost["estimated_project_cost"] = val
            if cost:
                out["project_cost_detail"] = cost  # FIELD: project_cost_detail <- ViewProject title/value cost table
            break

    # ── Construction progress ─────────────────────────────────────────────────
    # The "Updates as of" ViewProject page has a table whose header row contains
    # both a "title" column and a "percentage" (or "%") column.
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if not rows:
            continue
        hdrs = [_clean(c.get_text()).lower() for c in rows[0].find_all(["td", "th"])]
        has_title = any("title" in h for h in hdrs)
        has_pct   = any("%" in h or "percentage" in h for h in hdrs)
        if not (has_title and has_pct):
            continue
        title_i = next((i for i, h in enumerate(hdrs) if "title" in h), None)
        pct_i   = next((i for i, h in enumerate(hdrs) if "%" in h or "percentage" in h), None)
        date_i  = next((i for i, h in enumerate(hdrs) if "date" in h), None)
        rem_i   = next((i for i, h in enumerate(hdrs) if "remark" in h), None)
        progress: list[dict] = []
        for tr in rows[1:]:
            cells = [_clean(td.get_text()) for td in tr.find_all(["td", "th"])]
            if not cells:
                continue
            entry: dict = {}
            if title_i is not None and title_i < len(cells) and cells[title_i]:
                entry["title"] = cells[title_i]  # FIELD: construction_progress.title <- progress table title column
            if pct_i is not None and pct_i < len(cells) and cells[pct_i]:
                entry["progress_percentage"] = cells[pct_i]  # FIELD: construction_progress.progress_percentage <- progress table % column
            if date_i is not None and date_i < len(cells) and cells[date_i]:
                entry["date_of_reporting"] = cells[date_i]  # FIELD: construction_progress.date_of_reporting <- progress table date column
            if rem_i is not None and rem_i < len(cells) and cells[rem_i]:
                entry["remarks"] = cells[rem_i]  # FIELD: construction_progress.remarks <- progress table remark column
            if entry.get("title"):
                progress.append(entry)
        if progress:
            out["construction_progress"] = progress  # FIELD: construction_progress <- ViewProject "Updates as of" progress table
        break

    # ── Building / apartment details ──────────────────────────────────────────
    # Prefer Allottee Details summaries. They contain one stable row per unit
    # type, while the Building Details table nests allottee/document rows and can
    # shift columns in the rendered HTML.
    allottee_units = _parse_allottee_building_details()
    if allottee_units:
        out["building_details"] = allottee_units  # FIELD: building_details <- Allottee Details "Building : ..." summaries
    else:
        bldg_tbl = _find_table("building details")
        if bldg_tbl:
            rows = bldg_tbl.find_all("tr")
            hdrs: list[str] = []
            units: list[dict] = []
            for tr in rows:
                cells = [_clean(td.get_text()) for td in tr.find_all(["td", "th"])]
                if not cells or not any(cells):
                    continue
                if "apartment type" in cells[0].lower() and not hdrs:
                    hdrs = [c.lower() for c in cells]
                    continue
                if hdrs and len(cells) >= 3:
                    u: dict = {}
                    for i, h in enumerate(hdrs[:len(cells)]):
                        v = cells[i] if i < len(cells) else ""
                        if not v or v == "View":
                            continue
                        if "apartment type" in h:       u["flat_type"]     = v  # FIELD: building_details.flat_type <- bldg_tbl "apartment type"
                        elif "block number" in h:       u["block_name"]    = v  # FIELD: building_details.block_name <- bldg_tbl "block number"
                        elif "carpet area" in h:        u["carpet_area"]   = v  # FIELD: building_details.carpet_area <- bldg_tbl "carpet area"
                        elif "balcony" in h:            u["balcony_area"]  = v  # FIELD: building_details.balcony_area <- bldg_tbl "balcony"
                        elif "terrace" in h:            u["open_area"]     = v  # FIELD: building_details.open_area <- bldg_tbl "terrace"
                        # FIELD: building_details.no_of_units <- bldg_tbl "proposed number"
                        elif "proposed number" in h:    u["no_of_units"]   = v
                    if u.get("flat_type") and u.get("carpet_area"):
                        units.append(u)
            if units:
                out["building_details"] = units  # FIELD: building_details <- ViewProject "building details" table

    # ── Professional information ───────────────────────────────────────────────
    # Tables with e-mail/name/contact-number headers, preceded by a role heading
    professionals: list[dict] = []
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if not rows:
            continue
        hdrs = [_clean(c.get_text()).lower() for c in rows[0].find_all(["td", "th"])]
        if "e-mail address" not in hdrs or "name" not in hdrs:
            continue
        ei = next((i for i, h in enumerate(hdrs) if "e-mail" in h), None)
        ni = next((i for i, h in enumerate(hdrs) if h == "name"), None)
        ai = next((i for i, h in enumerate(hdrs) if "address" in h), None)
        pi = next((i for i, h in enumerate(hdrs) if "contact number" in h), None)
        # Try to infer role from a preceding heading cell in the same table
        role_row = tbl.find_previous(["h5", "h4", "th", "caption"])
        role = _clean(role_row.get_text()) if role_row else ""
        for tr in rows[1:]:
            cells = [_clean(td.get_text()) for td in tr.find_all(["td", "th"])]
            if not cells:
                continue
            prof: dict = {}
            if ni is not None and ni < len(cells) and cells[ni]:
                prof["name"] = cells[ni]  # FIELD: professional_information.name <- professional table "name" column
            if ei is not None and ei < len(cells) and cells[ei]:
                prof["email"] = cells[ei]  # FIELD: professional_information.email <- professional table "e-mail" column
            if ai is not None and ai < len(cells) and cells[ai]:
                prof["address"] = cells[ai]  # FIELD: professional_information.address <- professional table "address" column
            if pi is not None and pi < len(cells) and cells[pi]:
                prof["phone"] = cells[pi]  # FIELD: professional_information.phone <- professional table "contact number" column
            if role:
                prof["role"] = role  # FIELD: professional_information.role <- preceding heading text
            if prof.get("name"):
                professionals.append(prof)
    if professionals:
        out["professional_information"] = professionals  # FIELD: professional_information <- ViewProject professional tables

    # ── Members / partners ────────────────────────────────────────────────────
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if not rows:
            continue
        hdrs = [_clean(c.get_text()).lower() for c in rows[0].find_all(["td", "th"])]
        if "designation" in hdrs and "name" in hdrs:
            ni = next((i for i, h in enumerate(hdrs) if h == "name"), None)
            di = next((i for i, h in enumerate(hdrs) if "designation" in h), None)
            members: list[dict] = []
            for tr in rows[1:]:
                cells = [_clean(td.get_text()) for td in tr.find_all(["td", "th"])]
                if not cells:
                    continue
                m: dict = {}
                if ni is not None and ni < len(cells):
                    m["name"] = cells[ni]  # FIELD: members_details.name <- members table "name" column
                if di is not None and di < len(cells):
                    m["position"] = cells[di]  # FIELD: members_details.position <- members table "designation" column
                if m.get("name"):
                    members.append(m)
            if members:
                out["members_details"] = members  # FIELD: members_details <- ViewProject members/partners table
            break

    # ── Promoter / organisation ───────────────────────────────────────────────
    org_tbl = _find_table("organization")
    if org_tbl:
        kv = _parse_kv_rows(org_tbl)
        promoter: dict = {}
        for k, v in kv.items():
            n = k.lower()
            if "organization name" in n:    promoter["name"]         = v  # FIELD: promoters_details.name <- org_tbl "organization name"
            # FIELD: promoters_details.type_of_firm <- org_tbl "organization type"
            elif "organization type" in n:  promoter["type_of_firm"] = v
        if promoter:
            out["promoters_details"] = promoter  # FIELD: promoters_details <- ViewProject "organization" table

    # The popup can also contain compact inline labels for dates/status. Do not
    # use it wholesale here: on the legacy table layout some generic labels map
    # poorly (for example "Project Name" can resolve to "Registration No.").
    for field in (
        "project_registration_no",
        "status_of_the_project",
        "actual_commencement_date",
        "estimated_finish_date",
        "actual_finish_date",
        "approved_on_date",
    ):
        val = compact_fallback.get(field)
        if val not in (None, "", [], {}) and not out.get(field):
            out[field] = val

    return out


def _parse_detail_docs(soup: BeautifulSoup) -> list[dict]:
    """Collect all document/download links from the rendered detail page HTML.

    For links whose anchor text is generic (e.g. "View", "View Photo") the
    function inspects the parent <tr> row and uses the first non-generic cell
    text as the document label — this is the pattern used on the ViewProject
    table-based page where document type is in cell[0] and the link is in the
    last cell.
    """
    docs: list[dict] = []
    seen: set[str] = set()
    root = (
        soup.select_one("app-viewprojectnew #pdfContent")
        or soup.select_one("#pdfContent")
        or soup.select_one(".page-content")
        or soup.select_one(".content-wrapper")
        or soup.select_one("main")
        or soup
    )
    for a in root.find_all("a", href=True):
        href = str(a.get("href", "")).strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript"):
            continue
        hl = href.lower()
        is_doc = (
            any(hl.endswith(ext) for ext in _DOC_EXTENSIONS)
            or any(kw in hl for kw in _DOC_PATH_KEYWORDS)
        )
        if not is_doc:
            continue
        url = _build_doc_url(href)
        if not url or url in seen:
            continue
        seen.add(url)

        anchor_text = _clean(a.get_text()) or ""
        label = anchor_text

        # When anchor text is generic ("View", "Download" …), look for the
        # document category in the first non-generic cell of the parent <tr>.
        if label.lower() in _GENERIC_ANCHOR_TEXTS:
            parent_tr = a.find_parent("tr")
            if parent_tr:
                cells = [
                    _clean(td.get_text())
                    for td in parent_tr.find_all(["td", "th"])
                ]
                _bad_doc_labels = {"na", "n/a", "none", "-", "--", "0"}
                row_label = next(
                    (
                        c for c in cells
                        if c
                        and c.lower() not in _GENERIC_ANCHOR_TEXTS
                        and c.lower() not in _bad_doc_labels
                    ),
                    "",
                )
                if row_label:
                    label = row_label

        if (
            not label
            or label.lower() in _GENERIC_ANCHOR_TEXTS
            or label.lower() in {"na", "n/a", "none", "-", "--", "0"}
        ):
            label = "document"

        docs.append({"label": label, "url": url})
    return docs


def _try_expand_tabs(page) -> None:
    """Click through all inactive tabs/accordions to expose hidden content."""
    selectors = [
        "div.tab:not(.selected)",               # Rajasthan RERA 2.0 Angular tabs
        "li.nav-item a.nav-link:not(.active)",
        "a[data-toggle='tab']:not(.active)",
        ".nav-tabs li:not(.active) a",
        "[role='tab'][aria-selected='false']",
        ".accordion-toggle",
        ".panel-title a",
    ]
    for selector in selectors:
        try:
            tabs = page.locator(selector).all()
            for tab in tabs:
                try:
                    _click_locator_stably(page, tab, timeout=5_000)
                    page.wait_for_timeout(800)
                except Exception:
                    pass
        except Exception:
            pass


def _click_locator_stably(page, locator, *, timeout: int = 15_000) -> None:
    """Click a locator after centering it; fall back to JS for visible Chrome.

    Rajasthan's public site has sticky/mega-menu layers that can intercept
    native Selenium clicks in non-headless mode. The intended button is still
    the resolved element, so a JS-dispatched click is the correct fallback.
    """
    element = locator._first_element(timeout_ms=timeout)
    try:
        page.evaluate(
            """(el) => {
                el.scrollIntoView({block: 'center', inline: 'center'});
                window.scrollBy(0, -80);
            }""",
            element,
        )
        page.wait_for_timeout(300)
    except Exception:
        pass
    try:
        element.click()
    except Exception:
        page.evaluate("(el) => el.click()", element)


def _parse_detail_html(soup: BeautifulSoup) -> dict:
    """Map extracted key-value pairs from the detail page HTML to schema fields."""
    out: dict = {}
    kv = _extract_kv_from_html(soup)

    for raw_label, raw_value in kv.items():
        norm = raw_label.lower().strip().rstrip(":").strip()
        field = _DETAIL_LABEL_FIELD_MAP.get(norm)
        if not field:
            continue
        val = str(raw_value).strip()
        if not val or val.lower() in ("", "null", "none", "n/a", "-", "--", "0"):
            continue
        if field.endswith("_date"):
            val = _normalize_date_str(val) or val
        elif field == "project_registration_no":
            m = re.search(r"RAJ/P/\d{4}/\d+", val, re.I)
            if m:
                val = m.group(0)
        elif field == "promoter_name":
            val = re.sub(
                r"\s+(?:individual|partnership\s+firm|company|society|trust|llp)\b.*$",
                "",
                val,
                flags=re.I,
            ).strip()
        elif field == "project_type":
            val = _normalize_project_type(val)
        elif field in ("number_of_residential_units", "number_of_commercial_units"):
            try:
                val = int(float(val))
            except (ValueError, TypeError):
                continue
        elif field in ("land_area", "construction_area"):
            num = _first_number(val)
            if num is None:
                continue
            val = num
        out[field] = val

    def _kv(label: str) -> str:
        return next(
            (
                str(v).strip()
                for k, v in kv.items()
                if k.lower().strip().rstrip(":") == label
                and str(v).strip()
            ),
            "",
        )

    # Current ViewProjectNew layout exposes a compact project-address block.
    loc: dict = {}
    project_address = _kv("project address")
    if project_address:
        loc["raw_address"] = project_address
        pin_match = re.search(r"\b(\d{6})\b", project_address)
        if pin_match:
            loc["pin_code"] = pin_match.group(1)
        village_match = re.search(r"\bVillage-\s*([^,]+)", project_address, re.I)
        if village_match:
            loc["village"] = _clean(village_match.group(1))
        plot_match = re.search(r"(?:Khasra No\./\s*)?Plot No\.?\s*([^,]+)", project_address, re.I)
        if plot_match:
            loc["house_no_building_name"] = _clean(plot_match.group(1))
    if _kv("tehsil"):
        loc["taluk"] = _kv("tehsil")
    if _kv("district"):
        loc["district"] = _kv("district")
        out.setdefault("project_city", _kv("district"))
    if _kv("state"):
        loc["state"] = _kv("state")
        out.setdefault("project_state", _kv("state"))
    if loc:
        out["project_location_raw"] = loc

    promoter_type = _kv("promoter type")
    if promoter_type:
        out.setdefault("promoters_details", {})["type_of_firm"] = promoter_type
    promoter_name = _kv("promoter name")
    if promoter_name:
        out.setdefault("promoters_details", {})["name"] = promoter_name
    mobile = _kv("mobile number")
    if mobile:
        out["promoter_contact_details"] = {"phone": mobile}
    office_address = _kv("office address")
    if office_address:
        out["promoter_address_raw"] = {"raw_address": office_address}

    partners = _kv("partners")
    if partners:
        out["members_details"] = [
            {"name": name}
            for name in (_clean(part) for part in partners.split(","))
            if name
        ]

    estimated_cost = _kv("project estimated cost (rs.)")
    if estimated_cost:
        out["project_cost_detail"] = {
            "estimated_project_cost": estimated_cost.replace(",", "")
        }

    return out


def _fetch_viewproject_html(page, logger: CrawlerLogger) -> str | None:
    """
    On the ProjectDetail page, find the 'Updated project details' (or
    'Project details as at time of registration') ViewProject link, resolve its
    fully encoded URL via JavaScript, then open it in a new tab and return the HTML.

    Using element.href (not getAttribute) gives the browser-resolved,
    correctly percent-encoded URL, avoiding double-encoding of base64 IDs.

    Returns the popup HTML string, or None if the link isn't found.
    """
    # Wait for Angular to finish rendering the detail divs.
    # The site uses "as on DD/MM/YYYY" (not "as of") in the label — the regex
    # covers both variants.
    _VIEWPROJECT_JS = """() => {
        const links = Array.from(document.querySelectorAll('a[href*="ViewProject"]'))
            // ViewProjectNew is the compact printable summary. The richer legacy
            // popup remains available as /ViewProject?id=...&type=U/O.
            .filter(a => /\\/ViewProject\\?/i.test(a.href));

        // Collect all "Updated project" links and parse their date (as of / as on).
        const DATE_RE = /as\\s+(?:of|on)\\s+(\\d{1,2})[\\/-](\\d{1,2})[\\/-](\\d{4})/i;
        let bestLink = null;
        let bestVal  = -1;

        for (const a of links) {
            const p = a.closest('div.details, tr, td, p, li, div') || a.parentElement;
            const text = p ? p.textContent : a.textContent;
            if (!text || !/Updated project/i.test(text)) continue;
            const m = DATE_RE.exec(text);
            if (m) {
                // Build a numeric YYYYMMDD so we can compare without Date parsing.
                const val = parseInt(m[3]) * 10000
                          + parseInt(m[2]) * 100
                          + parseInt(m[1]);
                if (val > bestVal) { bestVal = val; bestLink = a; }
            } else if (!bestLink) {
                // No date found — keep as a fallback candidate.
                bestLink = a;
            }
        }

        // If no "Updated project" links at all, fall back to the first ViewProject link.
        const chosen = bestLink
            || links.find(a => /type=U/i.test(a.href))
            || links[0];
        return chosen ? chosen.href : null;
    }"""

    page.wait_for_timeout(3_000)
    viewproject_url: str | None = page.evaluate(_VIEWPROJECT_JS)

    if not viewproject_url:
        # Tabs may not have been clicked yet (Angular renders them lazily).
        # Click any remaining inactive tabs and retry once.
        # Also log diagnostic info to help debug future structure changes.
        try:
            diag = page.evaluate("""() => {
                const allLinks = Array.from(document.querySelectorAll('a[href]'))
                    .map(a => a.href).filter(h => h && !h.startsWith('javascript'));
                const vpLinks = allLinks.filter(h => h.toLowerCase().includes('viewproject'));
                const detailDivs = Array.from(document.querySelectorAll('div.details')).length;
                const tabs = Array.from(document.querySelectorAll('div.tab')).length;
                return {allLinks: allLinks.length, vpLinks, detailDivs, tabs};
            }""")
            logger.info(
                f"ViewProject retry diagnostic: "
                f"total_links={diag.get('allLinks', '?')} "
                f"vp_links={diag.get('vpLinks', [])} "
                f"detail_divs={diag.get('detailDivs', '?')} "
                f"tabs={diag.get('tabs', '?')}",
                step="detail",
            )
        except Exception:
            pass

        logger.info(
            "ViewProject link not found — clicking tabs and retrying", step="detail"
        )
        try:
            tabs = page.locator("div.tab:not(.selected)").all()
            for tab in tabs:
                try:
                    _click_locator_stably(page, tab, timeout=5_000)
                    page.wait_for_timeout(1_000)
                except Exception:
                    pass
        except Exception:
            pass
        page.wait_for_timeout(3_000)
        viewproject_url = page.evaluate(_VIEWPROJECT_JS)

    if not viewproject_url:
        logger.warning("ViewProject link not found on detail page", step="detail")
        return None

    logger.info(f"Fetching ViewProject page: {viewproject_url}", step="detail")
    try:
        popup_page = page.context.new_page()
        popup_page.goto(viewproject_url, timeout=60_000, wait_until="networkidle")
        popup_page.wait_for_timeout(2_000)
        html = popup_page.content()
        popup_page.close()
        logger.info("ViewProject page scraped", step="detail")
        return html
    except Exception as exc:
        logger.warning(f"ViewProject page fetch failed: {exc}", step="detail")
        return None


def _scrape_detail_html_via_browser(
    page, project_ref: str, logger: CrawlerLogger
) -> tuple[dict, list[dict]]:
    """
    Scrape the currently-loaded project detail page (Angular SPA already rendered).
    Call _navigate_to_project_detail first to land on the right URL.

    Also clicks the 'Updated project details' View link to open the full-detail
    popup (/ViewProject) and merges that richer data into the result.

    Returns (data_dict, doc_links).
    """
    try:
        # The current ProjectDetail page already contains the rich
        # /ViewProject?id=...&type=U link. Fetch it before clicking any tabs:
        # a "Project at a Glance" tab/link can route the browser to
        # ViewProjectNew, which no longer has those rich ViewProject links.
        page.wait_for_timeout(2_000)
        initial_soup = BeautifulSoup(page.content(), "lxml")
        data = _parse_detail_html(initial_soup)
        docs = _parse_detail_docs(initial_soup)
        popup_html = _fetch_viewproject_html(page, logger)

        # Wait for Angular to render the tab bar before attempting to click tabs.
        # Without this, _try_expand_tabs finds no elements and silently exits
        # while the ViewProject link remains hidden behind an unclicked tab.
        try:
            page.wait_for_selector("div.tab", timeout=8_000)
        except Exception:
            pass  # proceed even if the selector never appears
        _try_expand_tabs(page)
        page.wait_for_timeout(2_000)
        soup = BeautifulSoup(page.content(), "lxml")
        expanded_data = _parse_detail_html(soup)
        protected_fields = {
            "project_name",
            "project_registration_no",
            "project_type",
            "promoter_name",
        }
        for key, value in expanded_data.items():
            if key in protected_fields and data.get(key):
                continue
            data[key] = value
        docs.extend(_parse_detail_docs(soup))

        # ── Open the full-detail popup and merge richer fields ────────────────
        if popup_html:
            popup_soup = BeautifulSoup(popup_html, "lxml")
            rich = _parse_viewproject_html(popup_soup)
            # Rich data overrides sparse surface-level data
            data.update(rich)
            # Also collect any doc links from the popup
            docs.extend(_parse_detail_docs(popup_soup))

        deduped_docs: list[dict] = []
        seen_doc_urls: set[str] = set()
        for doc in docs:
            url = doc.get("url")
            if not url or url in seen_doc_urls:
                continue
            seen_doc_urls.add(url)
            deduped_docs.append(doc)
        docs = deduped_docs

        logger.info(
            f"Detail page scraped: {len(data)} fields, {len(docs)} docs",
            step="detail",
        )
        return data, docs
    except Exception as exc:
        logger.error(f"Detail page scrape failed: {exc}", step="detail")
        return {}, []


def _navigate_to_project_detail(page, reg_no: str, logger: CrawlerLogger) -> str:
    """
    Navigate to a project's detail page by using the Rajasthan RERA listing's
    Angular search form to filter by registration number, then clicking View.

    The listing uses three Angular form inputs (projectName, promoterName,
    registrationNo) rather than a DataTables global search box. Filling
    registrationNo and clicking the Search button triggers the filter.

    Returns the detail page URL on success, or empty string on failure.
    """
    # The reg_no extracted from the listing table may carry a date suffix such as
    # " (28/04/2026)".  The site's search input expects only the bare number, e.g.
    # "RAJ/P/2025/4508".  Strip any trailing parenthesised group before searching.
    search_reg_no = _bare_registration_no(reg_no)

    try:
        page.goto(LISTING_PAGE_URL, timeout=60_000)
        page.wait_for_selector("table tbody tr", timeout=30_000)
        page.wait_for_load_state("networkidle", timeout=20_000)

        # Fill the registration-number search field and click Search
        try:
            reg_input = page.locator("input[name='registrationNo']").first
            reg_input.fill(search_reg_no)
            search_btn = page.locator(
                "button.btn-primary.w-100, button:has-text('Search')"
            ).first
            _click_locator_stably(page, search_btn, timeout=15_000)
            page.wait_for_timeout(2_000)
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception as se:
            logger.warning(f"Registration search failed for {reg_no}: {se}")

        # Guard: confirm the table has real data rows before attempting to click.
        # An empty search returns a single "No data available" / "No records" row.
        try:
            first_row_text = page.locator("tbody tr").first.inner_text(timeout=5_000)
            if not first_row_text or any(
                phrase in first_row_text.lower()
                for phrase in ("no data", "no records", "no matching")
            ):
                logger.warning(f"No matching rows in listing for {reg_no!r} (searched: {search_reg_no!r})")
                return ""
        except Exception:
            pass  # can't confirm — proceed optimistically

        # Click the View button/link on the first visible matching row.
        # The element may be a <button> or an <a> tag depending on site version.
        view_locator = page.locator("tbody tr").first.locator(
            "button, a[href*='ProjectDetail'], a:has-text('View'), a:has-text('Details')"
        ).first
        _click_locator_stably(page, view_locator, timeout=15_000)
        page.wait_for_load_state("networkidle", timeout=30_000)
        page.wait_for_timeout(1_500)

        detail_url = page.url
        if "ProjectDetail" in detail_url:
            return detail_url

        logger.warning(
            f"View click for {reg_no} did not reach ProjectDetail — got {detail_url}"
        )
        return ""
    except Exception as exc:
        logger.error(f"Navigation to detail failed for {reg_no}: {exc}")
        return ""


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Rajasthan RERA — Selenium-only.

    1. Navigates to the public listing page and searches for sentinel_registration_no.
    2. Clicks the View button to land on the project's detail page.
    3. Extracts fields via HTML scraping.
    4. Verifies ≥ 80% coverage against the stored baseline.
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
        "state_projects_sample", "rajasthan.json",
    )
    try:
        with open(sample_path) as fh:
            baseline: dict = _json.load(fh)
    except FileNotFoundError:
        logger.warning("Sample baseline not found — skipping coverage check",
                       path=sample_path, step="sentinel")
        return True

    logger.info(
        f"Sentinel: navigating to listing to find {sentinel_reg}", step="sentinel"
    )
    try:
        page = page_adapter(_session())
        detail_url = _navigate_to_project_detail(page, sentinel_reg, logger)
        if not detail_url:
            logger.error(
                "Sentinel: could not navigate to project detail page",
                step="sentinel",
            )
            insert_crawl_error(
                run_id, config.get("id", "rajasthan_rera"),
                "SENTINEL_FAILED", "Could not navigate to detail page",
            )
            return False

        logger.info(f"Sentinel: landed on {detail_url}", step="sentinel")
        fresh, _ = _scrape_detail_html_via_browser(page, sentinel_reg, logger)
    except Exception as exc:
        exc_str = str(exc)
        # Selenium / network timeout → transient; skip rather than abort crawl
        if "timeout" in exc_str.lower() or "net::" in exc_str.lower():
            logger.warning(
                f"Sentinel: Selenium timeout (likely transient) — {exc}; "
                "skipping coverage check this run",
                step="sentinel",
            )
            return True
        logger.error(f"Sentinel: Selenium failed — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data extracted from detail page", step="sentinel")
        insert_crawl_error(
            run_id, config.get("id", "rajasthan_rera"),
            "SENTINEL_FAILED", "No data from detail page (Selenium)",
        )
        return False

    logger.info(f"Sentinel: checking coverage for {sentinel_reg}", step="sentinel")
    # The current public ViewProjectNew page no longer exposes several fields
    # present in the legacy sample (bank/professional/description sections).
    # Keep a state-specific gate high enough to catch parser breakage while not
    # failing every run for fields the website no longer publishes.
    if not check_field_coverage(fresh, baseline, threshold=0.65, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "rajasthan_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


# ── Phase B: detail traversal and persistence helpers ────────────────────────

def _handle_document(project_key: str, doc: dict, run_id: int,
                     site_id: str, logger: CrawlerLogger,
                     client=None) -> dict | None:
    """Download a document, upload to S3, persist to DB. Returns normalized document metadata or None."""
    url   = doc.get("url")
    label = doc.get("label", "document")
    if not url:
        return None
    reused, existing_s3_key = existing_uploaded_document_entry(project_key, {**doc, "url": url, "type": label})
    if reused:
        logger.info("Document reused", label=label, s3_key=existing_s3_key, step="documents")
        logger.log_document(label, url, "reused", s3_key=existing_s3_key)
        return reused
    filename = build_document_filename(doc)
    try:
        resp = download_response(url, logger=logger, timeout=15, client=client)
        if not resp or len(resp.content) < 100:
            return None
        content = resp.content
        md5     = compute_md5(content)
        s3_key  = upload_document(project_key, filename, content, dry_run=settings.DRY_RUN_S3)
        if s3_key is None:
            return None
        s3_url  = get_s3_url(s3_key)
        upsert_document(project_key=project_key, document_type=label, original_url=document_identity_url(doc) or url,
                        s3_key=s3_key, s3_bucket=settings.S3_BUCKET_NAME,
                        file_name=filename, md5_checksum=md5, file_size_bytes=len(content))
        logger.info("Document handled", label=label, s3_key=s3_key, step="documents")
        logger.log_document(label, url, "uploaded", s3_key=s3_key, file_size_bytes=len(content))
        return document_result_entry(doc, s3_url, filename)
    except Exception as e:
        logger.error(f"Document failed: {e}", url=url)
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(e),
                           project_key=project_key, url=url)
        return None


def _listing_seed_data(proj: dict) -> dict:
    """Convert one listing table row into normalized base project fields."""
    data: dict = {}
    for list_f, schema_f in _LIST_API_TO_FIELD.items():
        val = str(proj.get(list_f, "") or "").strip()
        if not val:
            continue
        if schema_f == "project_registration_no":
            val = _bare_registration_no(val)
        elif schema_f.endswith("_date"):
            val = _normalize_date_str(val) or val
        elif schema_f == "project_type":
            val = _normalize_project_type(val)
        data[schema_f] = val
    return data


def _open_project_detail_page(detail_page, proj: dict, reg_no: str,
                              logger: CrawlerLogger) -> str:
    """
    Open the detail page for a listing row via Selenium.

    The lister only reads row data/hrefs. Actual detail navigation is isolated
    here so light crawls can skip existing rows before any detail link is opened.
    """
    detail_url = (proj.get("detail_url") or "").strip()
    if detail_url:
        detail_page.goto(detail_url, timeout=60_000, wait_until="domcontentloaded")
        try:
            detail_page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            pass
        return detail_url
    return _navigate_to_project_detail(detail_page, reg_no, logger)


def _process_project_detail(
    proj: dict,
    *,
    index: int,
    total_projects: int,
    detail_page,
    config: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
    machine_name: str,
    machine_ip: str,
    client=None,
) -> tuple[str, int, str, int]:
    """Detail-stage workflow: navigate, parse, validate/upsert, then documents."""
    reg_no = _bare_registration_no(proj.get("reg_no") or f"RJ-{index}")
    identity_reg_no = proj.get("identity_reg_no") or _identity_registration_no(proj.get("raw_reg_no") or reg_no)
    key = generate_project_key(identity_reg_no)
    display_index = index + 1
    validation_error_count = 0

    logger.info(
        f"Project {display_index}/{total_projects}: starting detail scrape",
        step="timing",
        project_index=display_index,
        total_projects=total_projects,
    )
    _flush_progress_logs(logger)

    data = _listing_seed_data({**proj, "reg_no": reg_no})
    detail_url = _open_project_detail_page(detail_page, proj, reg_no, logger)
    logger.info(
        f"Project {display_index}/{total_projects}: detail navigation complete",
        step="timing",
        project_index=display_index,
        total_projects=total_projects,
        detail_url=detail_url,
    )
    _flush_progress_logs(logger)

    detail_fields: dict = {}
    doc_links: list[dict] = []
    if detail_url:
        detail_fields, doc_links = _scrape_detail_html_via_browser(
            detail_page, reg_no, logger)
    logger.info(
        f"Project {display_index}/{total_projects}: detail parse complete "
        f"({len(detail_fields)} fields, {len(doc_links)} docs)",
        step="timing",
        project_index=display_index,
        total_projects=total_projects,
        field_count=len(detail_fields),
        document_count=len(doc_links),
    )
    _flush_progress_logs(logger)

    # Detail page fields override listing fields because they are more complete.
    data.update(detail_fields)
    project_url = detail_url or LISTING_PAGE_URL
    data.update({
        "key":              key,                              # FIELD: key <- generate_project_key(reg_no)
        "state":            config["state"],                  # FIELD: state <- config["state"]
        "project_state":    "Rajasthan",                      # FIELD: project_state <- literal "Rajasthan"
        "domain":           DOMAIN,                           # FIELD: domain <- module DOMAIN constant
        "config_id":        config["config_id"],              # FIELD: config_id <- config["config_id"]
        "url":              project_url,                      # FIELD: url <- detail_url or LISTING_PAGE_URL
        "is_live":          True,                             # FIELD: is_live <- literal True
        "machine_name":     machine_name,                     # FIELD: machine_name <- get_machine_context()
        "crawl_machine_ip": machine_ip,                       # FIELD: crawl_machine_ip <- get_machine_context()
    })

    # FIELD: data.govt_type <- literal "state"
    # FIELD: data.is_processed <- literal False
    prod_data_fields: dict = {"govt_type": "state", "is_processed": False}
    if detail_url:
        prod_data_fields["details_page"]           = detail_url            # FIELD: data.details_page <- detail_url
        # FIELD: data.land_area_unit <- literal "In sq. meters"
        prod_data_fields["land_area_unit"]         = "In sq. meters"
        # FIELD: data.construction_area_unit <- literal "in sq. meters"
        prod_data_fields["construction_area_unit"] = "in sq. meters"

    _proj_type = data.get("project_type", "")
    if _proj_type:
        prod_data_fields["type"] = _proj_type.replace("-", " ").title()  # FIELD: data.type <- data["project_type"] title-cased

    _sub_date = data.get("submitted_date", "")
    _reg_no_for_temp = data.get("project_registration_no", reg_no)
    if _sub_date and _reg_no_for_temp:
        try:
            _dt = datetime.fromisoformat(_sub_date.replace("+00:00", ""))
            prod_data_fields["temp"] = (  # FIELD: data.temp <- f"{project_registration_no} ({submitted_date dd/mm/yyyy})"
                f"{_reg_no_for_temp} ({_dt.strftime('%d/%m/%Y')})"
            )
        except (ValueError, TypeError):
            pass

    _pb_name    = data.get("promoter_name", "")
    _pb_contact = data.get("promoter_contact_details") or {}
    _pb_phone   = _pb_contact.get("phone", "")
    _pb_email   = _pb_contact.get("email", "")
    _promoter_block = [x for x in [_pb_name, _pb_phone, _pb_email] if x]
    if _promoter_block:
        prod_data_fields["promoter_block"] = _promoter_block  # FIELD: data.promoter_block <- [promoter_name, phone, email]

    data["data"] = merge_data_sections(  # FIELD: data <- merge_data_sections(prod_data_fields, {source, detail_url})
        prod_data_fields,
        # FIELD: data.source <- literal "selenium_html"
        # FIELD: data.detail_url <- detail_url
        {"source": "selenium_html", "detail_url": detail_url},
    )

    logger.info("Normalizing and validating", step="normalize")
    try:
        normalized = normalize_project_payload(
            data, config, machine_name=machine_name, machine_ip=machine_ip)
        record  = ProjectRecord(**normalized)
        db_dict = record.to_db_dict()
    except (ValidationError, ValueError) as e:
        logger.warning("Validation failed — raw fallback", error=str(e))
        insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(e),
                           project_key=key, url=detail_url, raw_data=data)
        validation_error_count = 1
        db_dict = normalize_project_payload(
            {**data, "data": {"validation_fallback": True,
                              "raw": data.get("data")}},
            config, machine_name=machine_name, machine_ip=machine_ip,
        )

    action = upsert_project(db_dict)
    logger.info(f"DB result: {action}", step="db_upsert")

    documents_uploaded = 0
    if doc_links and settings.SKIP_DOCUMENTS:
        logger.info(
            f"Skipping {len(doc_links)} documents (--skip-documents)",
            step="documents",
        )
    elif doc_links:
        logger.info(f"Downloading {len(doc_links)} documents", step="documents")
        uploaded_documents = []
        doc_name_counts: dict[str, int] = {}
        for doc in doc_links:
            selected_doc = select_document_for_download(
                config["state"], doc, doc_name_counts, domain=DOMAIN)
            if selected_doc:
                uploaded_doc = _handle_document(
                    key, selected_doc, run_id, site_id, logger, client=client)
                if uploaded_doc:
                    uploaded_documents.append(uploaded_doc)
                    documents_uploaded += 1
                else:
                    uploaded_documents.append(
                        # FIELD: uploaded_documents.link <- doc["url"]
                        # FIELD: uploaded_documents.type <- doc["label"] (fallback "document")
                        {"link": doc.get("url"), "type": doc.get("label", "document")})
            else:
                uploaded_documents.append(
                    # FIELD: uploaded_documents.link <- doc["url"]
                    # FIELD: uploaded_documents.type <- doc["label"] (fallback "document")
                    {"link": doc.get("url"), "type": doc.get("label", "document")})
        if uploaded_documents:
            upsert_project({
                # FIELD: key <- db_dict["key"]
                # FIELD: url <- db_dict["url"]
                "key": db_dict["key"], "url": db_dict["url"],
                # FIELD: state <- db_dict["state"]
                # FIELD: domain <- db_dict["domain"]
                "state": db_dict["state"], "domain": db_dict["domain"],
                # FIELD: project_registration_no <- db_dict["project_registration_no"]
                "project_registration_no": db_dict["project_registration_no"],
                "uploaded_documents": uploaded_documents,  # FIELD: uploaded_documents <- list of handled/fallback doc entries
                # FIELD: document_urls <- build_document_urls(uploaded_documents)
                "document_urls": build_document_urls(uploaded_documents),
            })

    return action, documents_uploaded, detail_url, validation_error_count


def run(config: dict, run_id: int, mode: str) -> dict:
    """Public entry point — ensures the Selenium driver is shut down after the run."""
    try:
        return _run(config, run_id, mode)
    finally:
        _quit_driver()


def _run(config: dict, run_id: int, mode: str) -> dict:
    """
    Rajasthan crawl workflow.

    1. Optional sentinel health check for full runs.
    2. Listing traversal only: wait for GetProjects, maximize page size,
       paginate, and collect candidate rows.
    3. Detail traversal only for candidates. In daily_light, existing DB rows
       are skipped before any detail URL is opened.
    4. Normalize/upsert project data, then process documents if enabled.
    """
    logger   = CrawlerLogger(config["id"], run_id)
    site_id  = config["id"]
    counts   = dict(projects_found=0, projects_new=0, projects_updated=0,
                    projects_skipped=0, documents_uploaded=0, error_count=0)
    item_limit      = settings.CRAWL_ITEM_LIMIT or 0
    machine_name, machine_ip = get_machine_context()
    t_run = time.monotonic()

    # ── Targeted run handling ────────────────────────────────────────────────
    # --target-reg-no restricts the run to one or more specific projects
    # (comma-separated, case-insensitive). The reg-no is present on every listing
    # row, so the listing is filtered down to the requested project(s) and the
    # sentinel check is skipped (mirrors karnataka_rera / uttarakhand_rera).
    target_regs = get_target_reg_nos()

    # ── Sentinel health check ────────────────────────────────────────────────
    if target_regs or mode == "daily_light":
        logger.info("Sentinel skipped (targeted run or daily_light)", step="sentinel")
        counts["sentinel_passed"] = True
    else:
        t0 = time.monotonic()
        logger.info("Starting sentinel check", step="timing")
        _flush_progress_logs(logger)
        if not _sentinel_check(config, run_id, logger):
            logger.error("Sentinel failed — aborting crawl", step="sentinel")
            counts["sentinel_passed"] = False
            counts["error_count"] += 1
            return counts
        counts["sentinel_passed"] = True
        logger.timing("sentinel", time.monotonic() - t0)

    # ── Phase A: Lister — inspect visible reg_nos via Selenium listing scrape ─
    # In daily_light, the lister checks the DB while paging and only returns
    # rows whose reg_no is absent. Without an item limit, it must still inspect
    # every listing page to discover unseen reg_nos.
    t0 = time.monotonic()
    list_max_pages = settings.MAX_PAGES if settings.MAX_PAGES else None
    light_check_existing = mode == "daily_light" and not target_regs
    list_enough = None if light_check_existing else (item_limit if item_limit else None)
    max_checked_rows = item_limit if (light_check_existing and item_limit) else None
    last_listing_dashboard_update = 0

    def _update_listing_dashboard(
        checked_rows: int,
        skipped_existing_rows: int,
        candidate_rows: int,
        reg_no: str | None = None,
        raw_reg_no: str | None = None,
        project_key: str | None = None,
        bare_project_key: str | None = None,
        existing_match_key: str | None = None,
    ) -> None:
        nonlocal last_listing_dashboard_update
        if not light_check_existing:
            return
        counts["projects_found"] = checked_rows
        counts["projects_skipped"] = skipped_existing_rows
        should_update_dashboard = (
            checked_rows <= 10
            or checked_rows - last_listing_dashboard_update >= 25
            or (max_checked_rows is not None and checked_rows >= max_checked_rows)
        )
        if should_update_dashboard:
            update_crawl_run_progress(run_id, counts)
            last_listing_dashboard_update = checked_rows
        print(
            "[INFO] [rajasthan_rera] [listing] "
            "Rajasthan daily_light listing progress: "
            f"reg_no={reg_no or '-'}, "
            f"key={project_key or '-'}, "
            f"existing_match_key={existing_match_key or '-'}, "
            f"raw_reg_no={raw_reg_no or '-'}, "
            f"checked={checked_rows}, existing={skipped_existing_rows}, "
            f"candidates={candidate_rows}",
            flush=True,
        )

    listed_projects, checked_listing_rows, skipped_existing_rows = _scrape_project_list(
        logger,
        max_pages=list_max_pages,
        enough_rows=list_enough,
        check_existing=light_check_existing,
        max_checked_rows=max_checked_rows,
        on_progress=_update_listing_dashboard if light_check_existing else None,
    )
    if checked_listing_rows == 0:
        logger.error("Rajasthan listing returned zero projects — aborting crawl", step="listing")
        insert_crawl_error(
            run_id,
            site_id,
            "LISTING_EMPTY",
            "Rajasthan listing returned zero projects",
            url=LISTING_PAGE_URL,
        )
        counts["error_count"] += 1
        return counts
    if not listed_projects and light_check_existing:
        counts["projects_found"] = checked_listing_rows
        counts["projects_skipped"] = skipped_existing_rows
        update_crawl_run_progress(run_id, counts)
        logger.timing("search", time.monotonic() - t0, rows=checked_listing_rows)
        logger.info("Rajasthan daily_light: all checked registrations already exist in DB")
        logger.timing("total_run", time.monotonic() - t_run)
        return counts

    # ── Targeted filtering ─────────────────────────────────────────────────────
    # Restrict the listing to the requested registration number(s).
    if target_regs:
        listed_projects = [
            p for p in listed_projects
            if _bare_registration_no(p.get("reg_no") or "").upper() in target_regs
        ]
        matched_regs = {
            _bare_registration_no(p.get("reg_no") or "").upper() for p in listed_projects
        }
        for missing in sorted(target_regs - matched_regs):
            logger.warning(f"Target reg_no={missing!r} not found in listing", step="listing")
        logger.info(
            f"Targeted run — {len(matched_regs)} of {len(target_regs)} requested "
            f"project(s) matched", step="listing",
        )

    counts["projects_found"] = checked_listing_rows if light_check_existing else len(listed_projects)
    if light_check_existing:
        counts["projects_skipped"] = skipped_existing_rows
    update_crawl_run_progress(run_id, counts)
    total_listing = len(listed_projects)
    if item_limit and not light_check_existing:
        listed_projects = listed_projects[:item_limit]
        logger.info(
            f"Rajasthan: CRAWL_ITEM_LIMIT={item_limit} — "
            f"{len(listed_projects)} of {total_listing} projects",
        )
    logger.timing("search", time.monotonic() - t0, rows=counts["projects_found"])

    # ── Phase B: detail traversal — only candidates from the lister get opened ─
    # Document downloads share the module-level SeleniumSession.
    session = None  # signature compatibility for _handle_document(...)
    detail_page = page_adapter(_session())
    total_projects = len(listed_projects)

    for i, proj in enumerate(listed_projects):
        reg_no = _bare_registration_no(proj.get("reg_no") or f"RJ-{i}")
        identity_reg_no = proj.get("identity_reg_no") or _identity_registration_no(proj.get("raw_reg_no") or reg_no)
        key = generate_project_key(identity_reg_no)
        detail_url = (proj.get("detail_url") or "").strip()
        logger.set_project(key=key, reg_no=reg_no, url=LISTING_PAGE_URL, page=i)

        if mode == "daily_light" and get_project_by_key(key):
            logger.info("Skipping — already in DB (daily_light)", step="skip")
            counts["projects_skipped"] += 1
            logger.clear_project()
            update_crawl_run_progress(run_id, counts)
            continue

        try:
            action, uploaded_count, detail_url, validation_errors = _process_project_detail(
                proj,
                index=i,
                total_projects=total_projects,
                detail_page=detail_page,
                config=config,
                run_id=run_id,
                site_id=site_id,
                logger=logger,
                machine_name=machine_name,
                machine_ip=machine_ip,
                client=session,
            )
            if action == "new":
                counts["projects_new"] += 1
            else:
                counts["projects_updated"] += 1
            counts["documents_uploaded"] += uploaded_count
            counts["error_count"] += validation_errors
            random_delay(*config.get("rate_limit_delay", (1, 3)))
        except Exception as exc:
            logger.exception("Project processing failed", exc, step="project_loop",
                             reg_no=reg_no)
            insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                               project_key=key, url=detail_url or LISTING_PAGE_URL)
            counts["error_count"] += 1
        finally:
            logger.clear_project()
            update_crawl_run_progress(run_id, counts)

    logger.info(f"Rajasthan RERA complete: {counts}")
    logger.timing("total_run", time.monotonic() - t_run)
    return counts
