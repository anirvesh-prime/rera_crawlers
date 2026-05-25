"""
Telangana RERA Crawler — rerait.telangana.gov.in
Type: Playwright (ASP.NET search form + CAPTCHA)

Strategy:
- Submit the search form at /SearchList/Search with CAPTCHA solved via captcha_solver.
- Parse the server-rendered results table; paginate via ASP.NET __doPostBack.
- For each row: extract the encrypted q-param (PrintPreview URL) and the base64
  data_cert that encodes ProjectID / AppID / UserID.
- Navigate to the PrintPreview page with Playwright; parse HTML for all fields.
- Derive stable registration number from the detail page, falling back to TG-{AppID}.
- Download selected documents and upload to S3.
"""
from __future__ import annotations

import base64
import re
import time
from datetime import timezone
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse, unquote

from bs4 import BeautifulSoup
from pydantic import ValidationError

from core.captcha_solver import captcha_to_text
from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import (
    PlaywrightSession,
    download_response,
    generate_project_key,
    get_legacy_ssl_context,
    random_delay,
    safe_get,
)
from core.db import get_project_by_key, upsert_project, insert_crawl_error, upsert_document
from core.details_pool import get_detail_workers, process_details
from core.document_policy import select_document_for_download
from core.logger import CrawlerLogger
from core.models import ProjectRecord
from core.project_normalizer import (
    build_document_filename,
    build_document_urls,
    clean_string,
    document_identity_url,
    document_result_entry,
    get_machine_context,
    merge_data_sections,
    normalize_project_payload,
    parse_datetime,
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL    = "https://rerait.telangana.gov.in"
SEARCH_URL  = f"{BASE_URL}/SearchList/Search"
DOMAIN      = "rerait.telangana.gov.in"
STATE       = "telangana"
STATE_CODE  = "TS"

_CERT_CHAR_D     = 52   # CharacterD value for registration certificate download
_PREVIEW_CHAR_D  = 87   # CharacterD value for project preview PDF

_PAGE_TIMEOUT_MS   = 90_000
_NAV_TIMEOUT_MS    = 120_000
_CAPTCHA_MAX_TRIES = 5

_CAPTCHA_SELECTORS = [
    "img[src*='captcha' i]",
    "img[src*='Captcha' i]",
    "canvas",
    "#captchaImage",
    "img[id*='captcha' i]",
]

# Regex to strip trailing colons / whitespace from label text
_LABEL_RE = re.compile(r"[:.\s]+$")

# ── Utility helpers ────────────────────────────────────────────────────────────

def _clean(val: Any) -> str | None:
    """Whitespace-normalised, None-safe string."""
    return clean_string(val)


def _label(raw: str | None) -> str:
    """Normalise a label extracted from HTML."""
    if not raw:
        return ""
    return _LABEL_RE.sub("", raw).strip().lower()


def _decode_data_cert(b64: str) -> dict[str, str]:
    """
    Decode a base64-encoded query string like
    ProjectID=…&Division=…&UserID=…&RoleID=…&AppID=…&Action=…&CharacterD=…&ExtAppID=
    into a plain dict.
    """
    try:
        decoded = base64.b64decode(b64 + "==").decode("utf-8", errors="replace")
        return dict(pair.split("=", 1) for pair in decoded.split("&") if "=" in pair)
    except Exception:
        return {}


# Stable params kept in doc_decoded and in rebuilt cert/preview URLs.  The
# Telangana portal periodically appends new session-scoped trailers (originally
# CharacterD + ExtAppID; later IsAbyence was added).  Pinning the set of kept
# keys makes doc_decoded stable across those server-side additions so existing
# DB keys are not invalidated whenever a new trailer appears.
_STABLE_DOC_KEYS = ("ProjectID", "Division", "UserID", "RoleID", "AppID", "Action")


def _compute_doc_decoded(raw_cert: str) -> str:
    """
    Decode *raw_cert* (a base64 query-string) and keep only the canonical
    stable parameters so the result is invariant across sessions and across
    Telangana adding/removing trailing session-scoped params like CharacterD,
    ExtAppID, IsAbyence.
    """
    try:
        decoded = base64.b64decode(raw_cert + "==").decode("utf-8", errors="replace")
        kept: list[str] = []
        for pair in decoded.split("&"):
            if "=" not in pair:
                continue
            k, _ = pair.split("=", 1)
            if k in _STABLE_DOC_KEYS:
                kept.append(pair)
        return "&".join(kept)
    except Exception:
        return ""


def _build_cert_url(params: dict[str, str], char_d: int) -> str:
    """Rebuild a GetShowCertificateFileContent URL with a specific CharacterD."""
    p = {k: params[k] for k in _STABLE_DOC_KEYS if k in params}
    p["CharacterD"] = str(char_d)
    p["ExtAppID"]   = ""
    qs = "&".join(f"{k}={v}" for k, v in p.items())
    b64 = base64.b64encode(qs.encode()).decode().rstrip("=")
    return f"{BASE_URL}/SearchList/GetShowCertificateFileContent?QueryStringID={b64}"


def _build_preview_url(params: dict[str, str], char_d: int) -> str:
    """Rebuild a GetshowFileApplicationPreviewFileContent URL."""
    p = {k: params[k] for k in _STABLE_DOC_KEYS if k in params}
    p["CharacterD"] = str(char_d)
    p["ExtAppID"]   = ""
    qs = "&".join(f"{k}={v}" for k, v in p.items())
    b64 = base64.b64encode(qs.encode()).decode().rstrip("=")
    return f"{BASE_URL}//SearchList/GetshowFileApplicationPreviewFileContent/{b64}"


def _extract_print_preview_url(tag: Any) -> str | None:
    """Return the PrintPreview URL embedded in an onclick or href attribute."""
    for attr in ("onclick", "href"):
        val = tag.get(attr, "") if hasattr(tag, "get") else ""
        m = re.search(r"PrintPreview[^'\"]*q=[^'\"]+", val)
        if m:
            raw = m.group(0)
            if not raw.startswith("http"):
                raw = BASE_URL + "/" + raw.lstrip("/")
            return raw
    return None


# ── CAPTCHA + search form ─────────────────────────────────────────────────────

def _solve_captcha(page: Any, logger: CrawlerLogger) -> str | None:
    """
    Extract and solve the CAPTCHA from the search page.

    Strategy:
      1. element.screenshot() captures the rendered #captchaImage pixels via
         Playwright — no CORS/canvas-taint issues.
      2. A brief 3.5 s stabilisation wait is required: the page JS
         auto-refreshes the CAPTCHA once ~2.5 s after load (no page reload,
         just a src change).  Capturing before this settles means solving the
         old CAPTCHA while the server session already holds the new one.
      3. Send the full-size data:image/png;base64,… string directly to
         captcha_source="model_captcha".  Downscaling to 90×28 was found to
         destroy enough detail that the solver intermittently returned empty
         text; sending the raw screenshot yields a 100% response rate.
    """
    try:
        # Wait for the CAPTCHA image to be present and fully loaded
        page.wait_for_selector("#captchaImage", state="visible", timeout=15_000)
        page.wait_for_function(
            """() => {
                const img = document.querySelector('#captchaImage');
                return img && img.complete && img.naturalWidth > 0;
            }""",
            timeout=15_000,
        )

        # ── Stabilisation delay ──────────────────────────────────────────────
        # The page JS replaces the CAPTCHA image ~2.5 s after load.  Wait long
        # enough for that one-time refresh to complete so we solve the same
        # image the server session is holding.
        page.wait_for_timeout(3_500)

        captcha_el = page.query_selector("#captchaImage")
        if not captcha_el:
            logger.warning("CAPTCHA image element not found", step="captcha")
            return None

        png_bytes = captcha_el.screenshot()
        if not png_bytes or len(png_bytes) < 100:
            logger.warning("CAPTCHA element screenshot returned empty", step="captcha")
            return None

        # Send the full-size screenshot as a data URL — no downscale.
        # Resizing to 90×28 was found to destroy enough detail that the solver
        # intermittently returns empty text; the full-size image yields 100% responses.
        full_data_url = "data:image/png;base64," + base64.b64encode(png_bytes).decode()

        solved = (captcha_to_text(full_data_url, default_captcha_source="model_captcha") or "").strip()
        if solved:
            logger.info(f"CAPTCHA solved: {solved!r}", step="captcha")
            return solved
        logger.warning("CAPTCHA solver returned empty text", step="captcha")
        return None
    except Exception as exc:
        logger.warning(f"CAPTCHA extraction failed: {exc}", step="captcha")
        return None


def _submit_search(page: Any, logger: CrawlerLogger) -> bool:
    """
    Fill and submit the Telangana RERA search form with no filters.
    Returns True if the results table appeared within the timeout.
    """
    for attempt in range(1, _CAPTCHA_MAX_TRIES + 1):
        try:
            logger.info(f"CAPTCHA attempt {attempt}/{_CAPTCHA_MAX_TRIES}", step="captcha")
            page.wait_for_selector(
                ", ".join(_CAPTCHA_SELECTORS),
                state="visible",
                timeout=_PAGE_TIMEOUT_MS,
            )
            captcha_text = _solve_captcha(page, logger)
            if not captcha_text:
                logger.warning("CAPTCHA solver returned empty text", step="captcha")
                page.reload(wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
                continue

            captcha_input = None
            for sel in ("input[name*='captcha' i]", "input[id*='captcha' i]",
                        "input[placeholder*='captcha' i]", "#txtCaptcha", "input[type='text']"):
                try:
                    captcha_input = page.wait_for_selector(sel, timeout=3_000)
                    if captcha_input:
                        break
                except Exception:
                    pass

            if not captcha_input:
                logger.warning("CAPTCHA input field not found", step="captcha")
                page.reload(wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
                continue

            captcha_input.fill(captcha_text)
            logger.info(
                f"Submitting CAPTCHA answer: {captcha_text!r}",
                step="captcha",
            )

            btn_clicked: str | None = None
            for btn_sel in (
                # Specific first — avoids matching 'Advanced Search' button
                "#btnSearch",
                "input[type='submit'][name='Command']",
                "input[type='submit'][value='Search']",
                "input[type='submit']",
                "button[type='submit']",
            ):
                try:
                    btn = page.query_selector(btn_sel)
                    if btn:
                        btn.click()
                        btn_clicked = btn_sel
                        break
                except Exception:
                    pass
            logger.info(f"Button clicked: {btn_clicked!r}", step="search")

            try:
                page.wait_for_selector("table", timeout=60_000)
                logger.info("Search submitted successfully", step="search")
                return True
            except Exception:
                # Dismiss "Captcha is not valid." modal if present (click OK)
                # so we stay in the same session and can re-solve without a reload.
                try:
                    ok_btn = page.query_selector("button:has-text('OK'), input[value='OK']")
                    if ok_btn:
                        ok_btn.click()
                        page.wait_for_timeout(500)
                except Exception:
                    pass

                landed_url = page.url
                try:
                    error_text = page.inner_text("body") or ""
                    error_snippet = " | ".join(
                        ln.strip() for ln in error_text.splitlines()
                        if ln.strip() and len(ln.strip()) > 3
                    )[:400]
                except Exception:
                    error_snippet = "<could not read body>"

                logger.warning(
                    f"No results table after submit (attempt {attempt}); "
                    f"url={landed_url!r} | page_text={error_snippet!r}",
                    step="search",
                )

        except Exception as exc:
            logger.warning(f"Search attempt {attempt} error: {exc}", step="search")
            try:
                page.reload(wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
            except Exception:
                pass

    return False


def _goto_next_page(page: Any, fast: bool = False) -> bool:
    """
    Click the ASP.NET postback 'next page' link in the pager if present.
    Returns True if a next-page link was found and clicked.

    When ``fast=True`` we skip the networkidle wait and use a tighter timeout
    on the page-number watcher — useful when only the next page's HTML is
    needed (no detail-page work).
    """
    try:
        current_page = _get_current_page(page)
        next_link = page.query_selector(
            "#btnNext:not([disabled]), "
            "button[name='Command'][value='Next']:not([disabled]), "
            "input[name='Command'][value='Next']:not([disabled]), "
            "a:text-matches('^>$|^Next$|^»$', 'i')"
        )
        if not next_link:
            return False
        next_link.click()
        try:
            page.wait_for_load_state("domcontentloaded", timeout=15_000 if fast else 30_000)
        except Exception:
            pass
        if not fast:
            try:
                page.wait_for_load_state("networkidle", timeout=30_000)
            except Exception:
                pass
        try:
            page.wait_for_function(
                """(prev) => {
                    const el = document.querySelector('#CurrentPage');
                    if (!el) return false;
                    const current = parseInt(el.value || el.textContent || '0', 10);
                    return Number.isFinite(current) && current > prev;
                }""",
                current_page,
                timeout=10_000 if fast else 30_000,
            )
        except Exception:
            pass
        return _get_current_page(page) > current_page
    except Exception:
        return False


def _get_current_page(page: Any) -> int:
    """Return the current Telangana search results page number."""
    try:
        html = page.content()
        soup = BeautifulSoup(html, "lxml")
        current = soup.find(attrs={"id": "CurrentPage"})
        if current:
            raw = current.get("value") or current.get_text(strip=True)
            if raw and raw.isdigit():
                return int(raw)
        text = soup.get_text(" ", strip=True)
        m = re.search(r"\bPage\s*:?\s*(\d+)\s+of\s+\d+\b", text, re.I)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return 1


def _get_total_pages(page: Any) -> int:
    """Try to parse total page count from pager HTML; returns 1 if not found."""
    try:
        html = page.content()
        soup = BeautifulSoup(html, "lxml")
        total = soup.find(attrs={"id": "TotalPages"})
        if total:
            raw = total.get("value") or total.get_text(strip=True)
            if raw and raw.isdigit():
                return int(raw)
        pager = soup.find(lambda t: t.name in ("tr", "td", "div")
                          and re.search(r"\bPage\b.*\bof\b", t.get_text(), re.I))
        if pager:
            m = re.search(r"of\s+(\d+)", pager.get_text(), re.I)
            if m:
                return int(m.group(1))
        # Count page-number links
        numbers = []
        for a in soup.find_all("a"):
            t = a.get_text(strip=True)
            if re.fullmatch(r"\d+", t):
                numbers.append(int(t))
        return max(numbers) if numbers else 1
    except Exception:
        return 1


# ── Listing table parsing ─────────────────────────────────────────────────────

def _find_listing_column_indexes(table) -> tuple[int | None, int | None]:
    """Locate the project/promoter-name column indexes from the listing table headers.

    Returns (project_name_idx, promoter_name_idx).  Each is None when the
    corresponding header cannot be found, in which case the caller falls back to
    extracting those fields from the detail page.
    """
    proj_idx: int | None = None
    prom_idx: int | None = None
    for thead_tr in table.find_all("tr"):
        ths = thead_tr.find_all("th")
        if not ths:
            continue
        for i, th in enumerate(ths):
            label = re.sub(r"\s+", " ", th.get_text(" ", strip=True)).lower()
            if proj_idx is None and "project name" in label:
                proj_idx = i
            if prom_idx is None and "promoter name" in label:
                prom_idx = i
        if proj_idx is not None or prom_idx is not None:
            break
    return proj_idx, prom_idx


def _parse_listing_rows(html: str) -> list[dict]:
    """
    Parse the search results HTML table.
    Each row yields a dict with PrintPreview URL, data_cert, AppID, ProjectID, etc.
    """
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict] = []
    seen_app_ids: set[str] = set()

    for table in soup.find_all("table"):
        proj_idx, prom_idx = _find_listing_column_indexes(table)
        tbody = table.find("tbody") or table
        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")
            if not cells:
                continue

            pp_url: str | None = None
            raw_cert: str | None = None

            # Extract PrintPreview URL from any tag in the row.
            for tag in list(tr.find_all(["a", "button", "input", "span"])) + [tr]:
                if pp_url is None:
                    pp_url = _extract_print_preview_url(tag)

            # data_cert lives on the "View Certificate" anchor.  Telangana now
            # stores it as a raw base64 string in the ``data-qstr`` attribute
            # (old layout wrapped it as ``?QueryStringID=…`` in href/onclick or
            # embedded in a ``GetshowFileApplicationPreviewFileContent/<b64>``
            # path).  Search any anchor in the row that uses ``showFile``-style
            # callbacks so we are robust to column reordering as well as the
            # storage variant — the older href/QueryStringID form is retained
            # as a fallback for any cached or alternate-layout responses.
            for a in tr.find_all("a"):
                qstr = str(a.get("data-qstr", "") or "").strip().rstrip("=")
                if qstr and re.fullmatch(r"[A-Za-z0-9+/=]+", qstr):
                    raw_cert = qstr
                    break
                for attr in ("href", "onclick", "data-url"):
                    v = str(a.get(attr, "") or "")
                    m = re.search(r"QueryStringID=([A-Za-z0-9+/=]+)", v)
                    if not m:
                        m = re.search(
                            r"GetshowFileApplicationPreviewFileContent/([A-Za-z0-9+/=]+)", v
                        )
                    if m:
                        raw_cert = m.group(1)
                        break
                if raw_cert:
                    break

            if not pp_url and not raw_cert:
                continue

            params: dict[str, str] = _decode_data_cert(raw_cert) if raw_cert else {}
            app_id  = params.get("AppID", "")
            proj_id = params.get("ProjectID", "")

            if app_id and app_id in seen_app_ids:
                continue
            if app_id:
                seen_app_ids.add(app_id)

            base_params = {k: v for k, v in params.items()
                           if k not in ("CharacterD", "ExtAppID")}

            texts = [_clean(c.get_text(separator=" ", strip=True)) for c in cells]
            texts = [t for t in texts if t]

            # Pull project_name/promoter_name from the resolved header positions
            # so the daily_light skip can compute the composite key without
            # fetching the PrintPreview detail page.  Missing/empty values fall
            # back to None and force the slow path post-detail.
            def _cell(idx: int | None) -> str | None:
                if idx is None or idx >= len(cells):
                    return None
                return _clean(cells[idx].get_text(separator=" ", strip=True))

            rows.append({
                "print_preview_url": pp_url,
                "data_cert": raw_cert,
                "cert_url": _build_cert_url(base_params, _CERT_CHAR_D) if base_params else None,
                "preview_pdf_url": _build_preview_url(base_params, _PREVIEW_CHAR_D) if base_params else None,
                "app_id": app_id,
                "project_id": proj_id,
                "user_id": params.get("UserID", ""),
                "division": params.get("Division", ""),
                "role_id": params.get("RoleID", ""),
                "listing_texts": texts,
                "project_name": _cell(proj_idx),
                "promoter_name": _cell(prom_idx),
            })

    return rows


# ── PrintPreview detail page parser ──────────────────────────────────────────

def _get_section(soup: BeautifulSoup, *header_patterns: str) -> BeautifulSoup:
    """
    Return the ``x_content`` div of the Bootstrap x_panel whose h2/h3 title
    matches any of the given regex patterns.  The Telangana PrintPreview uses
    ``<div class="x_panel"><div class="x_title"><h2>Section</h2>…</div>
    <div class="x_content">…data…</div></div>`` as its section structure.
    Falls back to the whole soup if no matching section is found.
    """
    for pattern in header_patterns:
        p = re.compile(pattern, re.I)
        h = soup.find(
            lambda t: t.name in ("h2", "h3", "h4") and p.search(t.get_text(strip=True))
        )
        if h:
            x_title = h.find_parent("div", class_=re.compile(r"x_title"))
            if x_title:
                x_content = x_title.find_next_sibling("div", class_=re.compile(r"x_content"))
                if x_content:
                    return x_content
    return soup


def _lv(soup: BeautifulSoup, *label_patterns: str) -> str | None:
    """
    Find a label whose text matches any of the given patterns (case-insensitive)
    and return the adjacent value cell's text.

    Supports three layouts:
    1. ``<th>/<td>`` table pairs.
    2. Bootstrap ``form-group`` col-div pairs used by the Telangana PrintPreview:
       ``<div class="form-group"><div class="col-…">Label</div>
         <div class="col-…">Value</div>…</div>``
    3. Col-div pairs NOT wrapped in a form-group (e.g. Built-Up Area Details):
       ``<div class="row"><div class="col-…"><label>Label</label></div>
         <div class="col-…">Value</div>…</div>``
    """
    for pattern in label_patterns:
        pat = re.compile(pattern, re.I)
        # 1. th/td/label/b/strong table-style pairs
        th = soup.find(
            lambda t: t.name in ("th", "td", "label", "b", "strong")
            and pat.search(t.get_text(strip=True))
        )
        if th:
            sib = th.find_next_sibling("td")
            if sib:
                return _clean(sib.get_text(separator=" ", strip=True))
            parent = th.parent
            if parent:
                sibs = parent.find_all("td")
                idx = sibs.index(th) if th in sibs else -1
                if idx >= 0 and idx + 1 < len(sibs):
                    return _clean(sibs[idx + 1].get_text(separator=" ", strip=True))
            # 3. Label inside a col-div → next sibling col-div holds the value
            col_parent = th.find_parent("div", class_=re.compile(r"\bcol"))
            if col_parent:
                next_col = col_parent.find_next_sibling("div", class_=re.compile(r"\bcol"))
                if next_col:
                    val = _clean(next_col.get_text(separator=" ", strip=True))
                    if val:
                        return val
        # 2. Bootstrap form-group col-div pairs
        for fg in soup.find_all("div", class_=re.compile(r"form-group")):
            cols = fg.find_all("div", class_=re.compile(r"\bcol"))
            for i in range(len(cols) - 1):
                lbl_text = cols[i].get_text(strip=True)
                if pat.search(lbl_text) and len(lbl_text) < 120:
                    val = _clean(cols[i + 1].get_text(separator=" ", strip=True))
                    if val:
                        return val
    return None


def _lv_for(section: Any, *for_patterns: str) -> str | None:
    """
    Find a ``<label for="…">`` whose ``for`` attribute matches one of the
    given regex patterns and return the value of the adjacent col-div.

    The Telangana PrintPreview reuses the same ``<label for="…">`` ids for
    semantically equivalent fields whose visible label text varies between
    promoter types (e.g. Individual uses "Promoter Name" with
    ``for="PersonalInfoModel_IndivisualName"``; Organization uses just "Name"
    with ``for="PersonalInfoModel_CompanyName"``).  Matching on the stable
    ``for`` id lets us extract the value without enumerating every visible
    label variant.
    """
    if not section:
        return None
    for pattern in for_patterns:
        pat = re.compile(pattern, re.I)
        lbl = section.find("label", attrs={"for": pat})
        if not lbl:
            continue
        col_parent = lbl.find_parent("div", class_=re.compile(r"\bcol"))
        if not col_parent:
            continue
        next_col = col_parent.find_next_sibling("div", class_=re.compile(r"\bcol"))
        if next_col:
            val = _clean(next_col.get_text(separator=" ", strip=True))
            if val:
                return val
    return None


def _parse_label_value_section(container: Any) -> dict[str, str]:
    """Generic label-value extraction from a Bootstrap / table section."""
    result: dict[str, str] = {}
    if not container:
        return result
    for row in container.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) >= 2:
            lbl = _label(cells[0].get_text(strip=True))
            val = _clean(cells[1].get_text(separator=" ", strip=True))
            if lbl and val:
                result[lbl] = val
    # Bootstrap col layout: look for paired label/value divs
    for col in container.find_all(class_=re.compile(r"col")):
        txt = col.get_text(separator="\n", strip=True).split("\n")
        if len(txt) >= 2:
            lbl = _label(txt[0])
            val = _clean(" ".join(txt[1:]))
            if lbl and val:
                result.setdefault(lbl, val)
    return result


def _parse_table_rows(table: Any) -> list[dict[str, str]]:
    """
    Parse a generic HTML table into a list of row dicts.
    Header row (th) defines keys; subsequent tr rows define values.
    """
    if not table:
        return []
    rows = table.find_all("tr")
    if not rows:
        return []
    headers = [_label(th.get_text(strip=True)) for th in rows[0].find_all(["th", "td"])]
    records: list[dict[str, str]] = []
    for tr in rows[1:]:
        cells = tr.find_all(["td", "th"])
        row = {}
        for i, cell in enumerate(cells):
            if i < len(headers) and headers[i]:
                val = _clean(cell.get_text(separator=" ", strip=True))
                if val:
                    row[headers[i]] = val
        if row:
            records.append(row)
    return records


# ── PrintPreview content validation + resilient fetch ────────────────────────

_MIN_PREVIEW_BYTES = 20_000
"""Real PrintPreview pages are several hundred KB; error/redirect pages are < 200 bytes."""

_PREVIEW_SECTION_RE = re.compile(
    r"project\s*information|promoter\s*information|land\s*details", re.I
)


def _is_valid_preview_html(html: str) -> bool:
    """
    Return True if *html* looks like a genuine Telangana PrintPreview page.

    Filters out the two known failure modes:
    - ``/Error/UnauthorizedPage``  (109 bytes — session expired / q-param invalid)
    - ``/Error/ErrorPage``         (~9 KB — generic server-side error)

    A valid page must be at least _MIN_PREVIEW_BYTES long AND contain at
    least one of the expected Bootstrap x_panel section headings.
    """
    if len(html) < _MIN_PREVIEW_BYTES:
        return False
    lower = html.lower()
    if "unauthorized" in lower or "errorpage" in lower:
        return False
    return bool(_PREVIEW_SECTION_RE.search(lower))


def _fetch_print_preview_html(
    detail_page: Any,
    pp_url: str,
    logger: CrawlerLogger,
    max_retries: int = 3,
) -> str | None:
    """
    Navigate to the PrintPreview page and return its HTML.

    Accepts a reusable Playwright page (detail_page) rather than a browser so
    the same page object is navigated for every project instead of creating and
    destroying a new page on each call.  This eliminates per-project page
    allocation overhead and halves the networkidle timeout (the Telangana
    PrintPreview is server-rendered and typically settles in 3-8 s).

    Validates that the response is a real project page (not an error/redirect
    from an expired or invalid session).  Retries up to *max_retries* times
    with linear back-off (2 s, 4 s) so transient network hiccups or brief
    server-side session issues are recovered automatically.

    Returns None only when every attempt fails, so the caller can decide
    to skip the project rather than silently upsert an empty record.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                f"Fetching PrintPreview (attempt {attempt}/{max_retries})",
                step="detail_fetch",
            )
            detail_page.goto(pp_url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
            try:
                detail_page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass  # networkidle timeout is non-fatal; content may still be complete

            html = detail_page.content()
            if _is_valid_preview_html(html):
                return html

            landed = detail_page.url
            logger.warning(
                f"PrintPreview returned invalid content "
                f"(attempt {attempt}/{max_retries}, landed={landed!r}, "
                f"content_len={len(html)})",
                step="detail_fetch",
            )
        except Exception as exc:
            logger.warning(
                f"PrintPreview navigation error (attempt {attempt}/{max_retries}): {exc}",
                step="detail_fetch",
            )
            # Reset the page to a blank state so the next attempt starts clean.
            try:
                detail_page.goto("about:blank", timeout=5000)
            except Exception:
                pass

        if attempt < max_retries:
            time.sleep(2 * attempt)   # 2 s, 4 s before the 2nd and 3rd retry

    return None


def _scrape_print_preview(soup: BeautifulSoup, row: dict) -> dict[str, Any]:
    """
    Extract all project fields from the PrintPreview page HTML.
    Maps Telangana portal labels to the canonical schema keys.

    The page uses Bootstrap x_panel sections with form-group col-div label/value
    pairs for most scalar fields, plus standard HTML tables for tabular data.
    We extract data from section-specific soup fragments to avoid confusion when
    the same label (e.g. "District") appears in both the Promoter and Project
    location sections.
    """
    # Section-specific soups (x_content of matching x_panel)
    proj_sec  = _get_section(soup, r"project\s*information")
    prom_sec  = _get_section(soup, r"promoter\s*information")
    addr_sec  = _get_section(soup, r"address\s*details")
    land_sec  = _get_section(soup, r"land\s*details")
    built_sec = _get_section(soup, r"built.up\s*area\s*details", r"built.*area.*detail")

    lv       = lambda *pats: _lv(soup, *pats)        # whole-doc fallback
    proj_lv  = lambda *pats: _lv(proj_sec, *pats)
    prom_lv  = lambda *pats: _lv(prom_sec, *pats)
    addr_lv  = lambda *pats: _lv(addr_sec, *pats)
    land_lv  = lambda *pats: _lv(land_sec, *pats)
    built_lv = lambda *pats: _lv(built_sec, *pats)

    data: dict[str, Any] = {}

    # ── Core fields (Project Information section) ─────────────────────────────
    data["project_name"]          = proj_lv(r"project\s*name")
    data["project_type"]          = proj_lv(r"project\s*type")
    data["status_of_the_project"] = proj_lv(r"project\s*status", r"\bstatus\b")
    data["project_registration_no"] = lv(r"plan\s*approval\s*number")
    data["approved_on_date"]      = proj_lv(r"approved\s*date")
    data["estimated_finish_date"] = proj_lv(
        r"revised\s*proposed\s*date",
        r"proposed\s*date.*completion",
        r"proposed.*completion",
    )
    data["actual_finish_date"]          = proj_lv(r"actual.*complet")
    data["estimated_commencement_date"] = proj_lv(r"commencement\s*date")

    # ── Location (Address Details section — project location, not promoter) ───
    district   = addr_lv(r"district")
    village    = addr_lv(r"village", r"mandal")
    locality   = addr_lv(r"locality")
    pin_code   = addr_lv(r"pin\s*code")
    state_name = addr_lv(r"\bstate\b") or "Telangana"

    raw_addr_parts = [p for p in [locality, district, state_name,
                                   f"Pincode: {pin_code}" if pin_code else None] if p]
    data["project_city"]      = district
    data["project_pin_code"]  = pin_code
    data["project_location_raw"] = {
        "state":       state_name,
        "district":    district,
        "village":     village,
        "locality":    locality,
        "pin_code":    pin_code,
        "raw_address": ", ".join(raw_addr_parts) if raw_addr_parts else None,
    }

    # ── Promoter (Promoter Information section) ───────────────────────────────
    # The section header is "Promoter Information - Individual" or
    # "Promoter Information - Organization"; the form labels differ between
    # the two layouts:
    #   Individual   : "Promoter Name" / "Middle Name" / "Last Name"
    #                  (label for=PersonalInfoModel_IndivisualName / …MName / …LName)
    #   Organization : just "Name"
    #                  (label for=PersonalInfoModel_CompanyName)
    # Fall back through the visible-label lookup first, then the stable
    # for-id lookup, so both variants resolve.
    first_name = (
        prom_lv(r"promoter\s*name")
        or _lv_for(prom_sec, r"PersonalInfoModel_IndivisualName$",
                              r"PersonalInfoModel_CompanyName$")
    )
    middle_name = (
        prom_lv(r"\bmiddle\s*name\b")
        or _lv_for(prom_sec, r"PersonalInfoModel_IndivisualMName$")
    )
    last_name = (
        prom_lv(r"\blast\s*name\b", r"\bsurname\b")
        or _lv_for(prom_sec, r"PersonalInfoModel_IndivisualLName$")
    )
    data["promoter_name"] = " ".join(filter(None, [first_name, middle_name, last_name])) or None

    # Address labels also diverge between Individual and Organization.  Look
    # up via for-id when the visible-label probe misses (e.g. Organization
    # uses CompanyHouseNo / CompanyBuilding / CompanyLocality / CompanyDistrict /
    # CompanyState / CompanyPinCode).
    prom_house = (
        prom_lv(r"building\s*name", r"house\s*number", r"house\s*no")
        or _lv_for(prom_sec, r"PersonalInfoModel_CompanyHouseNo$",
                              r"PersonalInfoModel_CompanyBuilding$",
                              r"PersonalInfoModel_IndivisualHouseNo$",
                              r"PersonalInfoModel_IndivisualBuilding$")
    )
    prom_locality = (
        prom_lv(r"\blocality\b")
        or _lv_for(prom_sec, r"PersonalInfoModel_CompanyLocality$",
                              r"PersonalInfoModel_IndivisualLocality$")
    )
    prom_district = (
        prom_lv(r"\bdistrict\b")
        or _lv_for(prom_sec, r"PersonalInfoModel_CompanyDistrictValue$",
                              r"PersonalInfoModel_IndivisualDistrictValue$")
    )
    prom_pin = (
        prom_lv(r"pin\s*code")
        or _lv_for(prom_sec, r"PersonalInfoModel_CompanyPinCode$",
                              r"PersonalInfoModel_IndivisualPinCode$")
    )
    prom_state = (
        prom_lv(r"^state$")
        or _lv_for(prom_sec, r"PersonalInfoModel_CompanyState$",
                              r"PersonalInfoModel_IndivisualState$")
        or "Telangana"
    )

    prom_raw_parts = [p for p in [prom_house, prom_locality, prom_district, prom_state,
                                   f"Pincode: {prom_pin}" if prom_pin else None] if p]
    data["promoter_address_raw"] = {
        "raw_address":            ", ".join(prom_raw_parts) if prom_raw_parts else None,
        "district":               prom_district,
        "locality":               prom_locality,
        "pin_code":               prom_pin,
        "state":                  prom_state,
        "house_no_building_name": prom_house,
    }
    phone = prom_lv(r"office\s*number", r"\bphone\b", r"\bmobile\b")
    email = prom_lv(r"e[-\s]?mail")
    data["promoter_contact_details"] = {k: v for k, v in {"phone": phone, "email": email}.items() if v}

    # The sample stores just the first-name fragment as promoters_details.name
    data["promoters_details"] = {"name": first_name} if first_name else None

    # ── Land / area ────────────────────────────────────────────────────────────
    # land_area: total land area from "Land Details" section
    # construction_area: approved built-up area from "Built-Up Area Details" section
    #   (distinct from Net Area in Land Details, which is land net of road widening)
    data["land_area"]         = land_lv(r"total\s*area", r"land\s*area")
    data["construction_area"] = (
        built_lv(r"approved.*built.*up", r"built[\s-]*up\s*area")
        or land_lv(r"approved.*built.*up", r"built[\s-]*up\s*area")
    )

    land_area_unit  = "Total Area(In sqmts)"
    const_area_unit = "Approved Built up Area (In Sqmts)"
    data["land_area_details"] = {
        "land_area":             data["land_area"],
        "land_area_unit":        land_area_unit,
        "construction_area":     data["construction_area"],
        "construction_area_unit": const_area_unit,
    } if data["land_area"] else None

    # ── Building / plot details table ─────────────────────────────────────────
    # Match plot tables specifically (has "Area of Plot" or "Proposed Number of Plots")
    # to avoid matching the Land Owner table which has a generic "Type" column.
    building_details: list[dict] = []
    for table in soup.find_all("table"):
        hdrs = [_label(th.get_text(strip=True)) for th in table.find_all("th")]
        hdrs_joined = " ".join(hdrs).lower()
        if re.search(r"number.*of.*plot|area.*of.*plot|proposed.*number.*plot", hdrs_joined):
            for r in _parse_table_rows(table):
                entry: dict[str, Any] = {"flat_type": "plot"}
                for h, v in r.items():
                    # Check booked/alloted BEFORE the generic number.*plot pattern
                    # so "number of plots booked/alloted/sold" → booking_detail,
                    # not no_of_plots.
                    if re.search(r"book|allot|sold", h, re.I):
                        entry["booking_detail"] = v
                    elif re.search(r"proposed.*number|number.*plot", h, re.I):
                        entry["no_of_plots"] = v
                    elif re.search(r"area.*of.*plot|plot.*area", h, re.I):
                        entry["total_area"] = v
                if len(entry) > 1:   # more than just flat_type
                    building_details.append(entry)

    # Apartment / flat tables (residential projects).
    # Telangana PrintPreview nests these under "Building Details" with columns:
    #   Sr.No. | Floor ID | Mortgage Area | Apartment Type |
    #   Saleable Area (in Sqmts) | Number of Apartment | Number of Booked Apartment
    # Keys are mapped to the canonical building_details allowed set so they
    # survive the normalize_structured_json whitelist filter.
    for table in soup.find_all("table"):
        hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
        hdrs_joined = " ".join(hdrs).lower()
        if re.search(r"floor.*id|apartment.*type|saleable.*area", hdrs_joined, re.I):
            for r in _parse_table_rows(table):
                entry: dict[str, Any] = {}
                for h, v in r.items():
                    kl = h.lower()
                    if re.search(r"apartment.*type|flat.*type", kl):
                        entry["flat_type"] = v
                    elif re.search(r"saleable.*area|carpet.*area", kl):
                        entry["total_area"] = v          # allowed key
                    elif re.search(r"number.*booked|booked.*apart", kl):
                        entry["booking_detail"] = v      # allowed key (booked count)
                    elif re.search(r"number.*apart|no.*apart", kl):
                        entry["no_of_units"] = v         # allowed key
                    elif re.search(r"floor.*id", kl):
                        entry["floor_no"] = v            # allowed key
                if entry.get("flat_type") or entry.get("total_area"):
                    building_details.append(entry)

    data["building_details"] = building_details or None

    # ── Co-promoters (Land Owner / Investor table) ────────────────────────────
    # The table headers include "Promoter(Land Owner/ Investor)".
    # Use the "Promoter Name" column (not "Project Name") for the name.
    co_promoters: list[dict] = []
    for table in soup.find_all("table"):
        hdrs_text = " ".join(th.get_text(strip=True) for th in table.find_all("th")).lower()
        if re.search(r"land.*owner|promoter.*investor", hdrs_text, re.I):
            for r in _parse_table_rows(table):
                name = r.get("promoter name") or next(
                    (v for k, v in r.items()
                     if "promoter" in k.lower() and "name" in k.lower()), None
                )
                # Skip repeated header rows (ASP.NET GridView repeats headers)
                if name and not re.search(r"^promoter\s*name$", name, re.I):
                    co_promoters.append({"name": name})
    data["co_promoter_details"] = co_promoters or None

    # ── Professionals ─────────────────────────────────────────────────────────
    professionals: list[dict] = []
    for table in soup.find_all("table"):
        hdrs_text = " ".join(th.get_text(strip=True) for th in table.find_all("th")).lower()
        if re.search(r"architect|engineer|professional", hdrs_text, re.I):
            for r in _parse_table_rows(table):
                name = r.get("professional name") or r.get("name") or next(
                    (v for k, v in r.items() if "name" in k), None
                )
                role = r.get("professional type") or r.get("role") or next(
                    (v for k, v in r.items() if "type" in k or "role" in k), None
                )
                if name:
                    professionals.append({"name": name, "role": role or ""})
    data["professional_information"] = professionals or None

    # ── Construction progress ─────────────────────────────────────────────────
    # The "Project Details" x_panel has a table:
    #   Name | Proposed | Booked | WorkDone(In %)
    # The "Development Work" panel has:
    #   Common areas And Facilities | Available | Percent | Details
    progress: list[dict] = []
    for table in soup.find_all("table"):
        hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
        hdrs_joined = " ".join(hdrs).lower()
        if re.search(r"workdone|work.*done|booked|percent", hdrs_joined, re.I):
            for r in _parse_table_rows(table):
                entry: dict[str, Any] = {}
                for k, v in r.items():
                    kl = k.lower()
                    if re.search(r"^name$|^common\s*area|title|description|item|activity|task", kl):
                        entry["title"] = v
                    elif re.search(r"remark|detail|status|available", kl):
                        entry["remarks"] = v
                    elif re.search(r"workdone|work.*done|percent|progress", kl):
                        entry["progress_percentage"] = v
                # Only include entries that have at least a remarks or progress value
                if entry and ("remarks" in entry or "progress_percentage" in entry):
                    progress.append(entry)
    data["construction_progress"] = progress or None

    # ── Status update ─────────────────────────────────────────────────────────
    data["status_update"] = {"booking_details": []}

    # ── Documents ─────────────────────────────────────────────────────────────
    docs: list[dict] = []
    seen_doc_urls: set[str] = set()

    # 1. Classic <a href> links (older PrintPreview versions).
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(k in href for k in ("/GetUserDocumentFileContent/", "/Preview/", ".pdf")):
            full_url = urljoin(BASE_URL, href)
            if full_url not in seen_doc_urls:
                seen_doc_urls.add(full_url)
                label_text = _clean(a.get_text(strip=True)) or "Document"
                docs.append({"link": full_url, "type": label_text})

    # 2. Telangana "Uploaded Documents" table — documents are loaded via a POST
    #    AJAX call (showFileDoc → /PrintPreview/GetUserDocumentIframe) using a
    #    UPID stored in a hidden input.  Extract those hidden inputs and store
    #    enough metadata for _handle_document to POST-download each file.
    proj_id_el = soup.find("input", {"id": "ProjectID"})
    proj_id_raw = (proj_id_el or {}).get("value", "") if proj_id_el else ""
    div_parts   = proj_id_raw.split("/")
    division    = div_parts[1] if len(div_parts) > 1 else "1"
    post_endpoint = f"{BASE_URL}/PrintPreview/GetUserDocumentIframe"

    for table in soup.find_all("table"):
        hdrs_text = " ".join(th.get_text(strip=True) for th in table.find_all("th")).lower()
        if "document name" not in hdrs_text or "uploaded document" not in hdrs_text:
            continue
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            # Document name from the <span title="…"> or td text
            name_span = tds[0].find("span", title=True)
            doc_name = name_span["title"] if name_span else _clean(tds[0].get_text(strip=True))

            upid_el = tds[0].find("input", id=re.compile(r"^UPID_\d+$"))
            if not upid_el:
                continue
            try:
                upid = int(upid_el.get("value", "-1"))
            except (ValueError, TypeError):
                upid = -1
            if upid <= 0:
                continue  # "Not Uploaded" rows have UPID = -1

            # Only include rows where a "View" button is present
            btn = tds[1].find("button") if len(tds) > 1 else None
            if btn is None:
                continue

            doc_key = f"upid:{upid}"
            if doc_key in seen_doc_urls:
                continue
            seen_doc_urls.add(doc_key)
            docs.append({
                "link": post_endpoint,
                "type": _clean(doc_name) or "Document",
                "upid": upid,
                "division": division,
            })

    data["_raw_docs"] = docs

    # ── Data blob (land area units) ───────────────────────────────────────────
    data["_land_area_unit"]   = land_area_unit
    data["_const_area_unit"]  = const_area_unit

    return data


# ── Document download + S3 upload ─────────────────────────────────────────────

def _fetch_upid_document(
    upid: int,
    division: str,
    cookies: dict,
    logger: CrawlerLogger,
) -> bytes | None:
    """
    Download a Telangana document that requires a POST to GetUserDocumentIframe.

    Flow:
      1. POST {"ID": upid, "Division": division, "RoleID": 1, "CurrentUserID": 0}
         → server returns an HTML page with an <iframe src="…pdf…"> or a PDF redirect.
      2. If the response is HTML, extract the iframe src and GET that URL.
      3. If the response is already PDF bytes, return them directly.
    """
    import httpx
    from bs4 import BeautifulSoup as _BS

    post_url = f"{BASE_URL}/PrintPreview/GetUserDocumentIframe"
    payload  = {"ID": upid, "Division": division, "RoleID": 1, "CurrentUserID": 0}
    headers  = {
        "User-Agent":   "Mozilla/5.0",
        "Referer":      BASE_URL,
        "Content-Type": "application/json",
    }
    try:
        with httpx.Client(
            timeout=60.0, follow_redirects=True, verify=False, cookies=cookies
        ) as client:
            # Fast-fail: GetUserDocumentIframe frequently 500s or hangs on
            # the Telangana portal. The default retries=3, timeout=60 ladder
            # burns up to 360s per dead UPID, multiplied by 5-20 docs per new
            # project. One attempt with a 25s/30s cap lets weekly_deep recover
            # broken docs without blocking the run on systemic portal failures.
            resp = download_response(
                post_url,
                method="POST",
                json_data=payload,
                headers=headers,
                retries=1,
                timeout=25.0,
                total_timeout=30.0,
                verify=False,
                client=client,
            )
            if not resp or not resp.is_success:
                logger.warning(
                    f"UPID POST failed: status={getattr(resp, 'status_code', 'n/a')}",
                    step="documents",
                )
                return None

            content_type = resp.headers.get("content-type", "")
            if "pdf" in content_type or resp.content[:4] == b"%PDF":
                return resp.content  # server returned PDF directly

            # Parse the iframe HTML for a src URL containing the actual file
            iframe_html = resp.text
            _soup = _BS(iframe_html, "lxml")
            iframe = _soup.find("iframe")
            if not iframe or not iframe.get("src"):
                # Try finding an <a> or embed tag
                embed = _soup.find("embed") or _soup.find("object")
                src = (embed or {}).get("src") or (embed or {}).get("data")
            else:
                src = iframe["src"]

            if not src:
                logger.warning(f"UPID {upid}: no src found in iframe response", step="documents")
                return None

            pdf_url = src if src.startswith("http") else urljoin(BASE_URL, src)
            pdf_resp = download_response(
                pdf_url,
                headers={"User-Agent": "Mozilla/5.0", "Referer": BASE_URL},
                retries=1,
                timeout=25.0,
                total_timeout=30.0,
                verify=False,
                client=client,
            )
            if pdf_resp and pdf_resp.is_success and len(pdf_resp.content) > 100:
                return pdf_resp.content

    except Exception as exc:
        logger.warning(f"UPID {upid} download error: {exc}", step="documents")
    return None


def _handle_document(
    project_key: str,
    doc: dict,
    run_id: int,
    site_id: str,
    logger: CrawlerLogger,
    *,
    cookies: dict | None = None,
) -> dict | None:
    """Download one document, upload to S3, return a document_result_entry or None."""
    url = doc.get("link") or doc.get("url")
    if not url:
        return None
    label = doc.get("type", "document")
    filename = build_document_filename({**doc, "source_url": url})
    try:
        import httpx
        headers = {"User-Agent": "Mozilla/5.0", "Referer": BASE_URL}

        # Telangana POST-based documents (UPID from hidden inputs)
        upid = doc.get("upid")
        if upid:
            data = _fetch_upid_document(
                upid=upid,
                division=str(doc.get("division", "1")),
                cookies=cookies or {},
                logger=logger,
            )
        else:
            with httpx.Client(
                timeout=60.0,
                follow_redirects=True,
                verify=False,
                cookies=cookies or {},
            ) as client:
                resp = download_response(
                    url,
                    headers=headers,
                    timeout=60.0,
                    verify=False,
                    client=client,
                )
                if not resp or not resp.is_success or len(resp.content) < 100:
                    return None
                data = resp.content

        if not data or len(data) < 100:
            return None

        md5 = compute_md5(data)
        s3_key = upload_document(project_key, filename, data, dry_run=settings.DRY_RUN_S3)
        if not s3_key:
            return None
        s3_url = get_s3_url(s3_key)
        upsert_document(
            project_key=project_key,
            document_type=label,
            original_url=url,
            s3_key=s3_key,
            s3_bucket=settings.S3_BUCKET_NAME,
            file_name=filename,
            md5_checksum=md5,
            file_size_bytes=len(data),
        )
        logger.info("Document uploaded", label=label, s3_key=s3_key, step="documents")
        logger.log_document(label, url, "uploaded", s3_key=s3_key, file_size_bytes=len(data))
        return document_result_entry({**doc, "source_url": url, "type": label}, s3_url, filename)
    except Exception as exc:
        logger.warning("Document handling error", url=url, error=str(exc))
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(exc),
                           project_key=project_key, url=url)
        return None


def _build_uploaded_documents(row: dict, detail: dict) -> list[dict]:
    """
    Assemble the raw uploaded_documents list from listing-row URLs and
    document links found in the PrintPreview HTML.

    UPID-based documents (POST downloads) all share the same endpoint URL so
    they cannot be deduplicated by link.  Instead we use ``upid:{id}`` as the
    deduplication key for those, and the URL for classic direct-link documents.
    """
    docs: list[dict] = []
    seen: set[str] = set()

    def _add_direct(link: str | None, doc_type: str) -> None:
        """Add a classic GET-download document, deduped by URL."""
        if not link or link in seen:
            return
        seen.add(link)
        docs.append({"link": link, "type": doc_type})

    # Certificate PDF (CharacterD=52)
    _add_direct(row.get("cert_url"), "Registration Certificate 1")
    # Preview PDF (CharacterD=87)
    _add_direct(row.get("preview_pdf_url"), "Rera Project Details")

    # Document links found on the PrintPreview HTML page.
    # UPID-based docs carry a "upid" key — use that for dedup; direct-link docs
    # use the URL.
    for doc in detail.get("_raw_docs") or []:
        upid = doc.get("upid")
        if upid is not None:
            key = f"upid:{upid}"
            if key in seen:
                continue
            seen.add(key)
            docs.append(dict(doc))          # preserve upid + division for download
        else:
            _add_direct(doc.get("link"), doc.get("type", "Document"))

    return docs


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Lightweight sentinel for Telangana RERA.

    Telangana's PrintPreview URLs carry a session-scoped ``q`` parameter that is
    only valid within the Playwright browser session that generated it.  They
    cannot be re-fetched via a plain httpx request, so any check that stores the
    sample URL and tries to GET it later will always receive an UnauthorizedPage
    redirect and fail — in production as well as in dry-run mode.

    Instead the sentinel verifies two things that are stable and do not require
    an active session:
      1. The search page (``SEARCH_URL``) responds with HTTP 200.
      2. The page HTML contains a CAPTCHA image element — confirming that the
         site structure expected by the crawler is still intact.
    """
    logger.info("Sentinel: checking search page accessibility",
                url=SEARCH_URL, step="sentinel")
    try:
        resp = safe_get(SEARCH_URL, retries=2, logger=logger, verify=False)
        if not resp:
            logger.error("Sentinel: search page unreachable",
                         url=SEARCH_URL, step="sentinel")
            insert_crawl_error(
                run_id, config.get("id", "telangana_rera"),
                "SENTINEL_FAILED", "Search page unreachable",
            )
            return False

        lower = resp.text.lower()
        if "captchaimage" not in lower and "captcha" not in lower:
            logger.error(
                "Sentinel: CAPTCHA element missing — site structure may have changed",
                url=SEARCH_URL, step="sentinel",
            )
            insert_crawl_error(
                run_id, config.get("id", "telangana_rera"),
                "SENTINEL_FAILED", "Search page missing expected CAPTCHA element",
            )
            return False

    except Exception as exc:
        logger.error(f"Sentinel: unexpected error — {exc}", step="sentinel")
        return False

    logger.info("Sentinel check passed", step="sentinel")
    return True


# ── Main run() ────────────────────────────────────────────────────────────────

def run(config: dict, run_id: int, mode: str) -> dict:
    """
    Main entry point for the Telangana RERA crawler.

    Flow:
    1. Launch Playwright; navigate to /SearchList/Search.
    2. Solve CAPTCHA and submit search form (no filters → all registered projects).
    3. Parse listing rows from each page; paginate via ASP.NET postback.
    4. For each row: navigate to PrintPreview, parse detail HTML.
    5. Normalize → upsert to DB.
    6. Download selected documents → upload to S3.
    7. Checkpoint after each listing page.
    """
    site_id = config["id"]
    logger  = CrawlerLogger(site_id, run_id)
    counts  = dict(projects_found=0, projects_new=0, projects_updated=0,
                   projects_skipped=0, documents_uploaded=0, error_count=0)

    item_limit    = settings.CRAWL_ITEM_LIMIT or 0
    items_done    = 0
    delay_min, delay_max = config.get("rate_limit_delay", (2, 4))
    machine_name, machine_ip = get_machine_context()
    t_run = time.monotonic()

    # ── Sentinel health check ────────────────────────────────────────────────
    t0 = time.monotonic()
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counts["sentinel_passed"] = False
        counts["error_count"] += 1
        return counts
    counts["sentinel_passed"] = True
    logger.timing("sentinel", time.monotonic() - t0)

    checkpoint = load_checkpoint(site_id, mode)
    start_page = (checkpoint["last_page"] + 1) if checkpoint else 1
    if checkpoint:
        logger.info(f"Resuming from checkpoint page {start_page}")

    with PlaywrightSession(headless=True, ignore_https_errors=True) as browser:
        # ── Open search page ─────────────────────────────────────────────────
        page = browser.new_page()
        # One reusable detail page shared across all projects — avoids the
        # per-project cost of creating and destroying a new browser page.
        detail_page = browser.new_page()
        t0 = time.monotonic()
        try:
            logger.info("Navigating to Telangana RERA search page")
            page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
        except Exception as exc:
            logger.error(f"Search page failed to load: {exc}")
            insert_crawl_error(run_id, site_id, "HTTP_ERROR", str(exc), url=SEARCH_URL)
            return counts

        # ── Submit search form (solves CAPTCHA) ──────────────────────────────
        if not _submit_search(page, logger):
            insert_crawl_error(run_id, site_id, "CAPTCHA_FAILED",
                               "Could not submit search form after max tries")
            return counts

        # ── Determine total pages ────────────────────────────────────────────
        total_pages = _get_total_pages(page)
        logger.timing("search", time.monotonic() - t0, pages=total_pages)
        max_pages   = settings.MAX_PAGES
        effective_end = (min(total_pages, start_page + max_pages - 1)
                         if max_pages else total_pages)
        logger.info(f"Total pages: {total_pages} | crawling up to {effective_end} | "
                    f"item_limit={item_limit or 'unlimited'}")

        # Skip listing pages before start_page by clicking "next"
        for _ in range(start_page - 1):
            if not _goto_next_page(page):
                break

        # When item_limit is hit we stop the listing walk entirely —
        # projects_found then reflects only the pages actually walked, not the
        # full Telangana catalog.
        processing_done = False
        current_page = start_page

        while current_page <= effective_end:
            logger.info(f"Listing page {current_page}/{effective_end}")
            html  = page.content()
            rows  = _parse_listing_rows(html)
            logger.info(f"  {len(rows)} project rows on page {current_page}")
            counts["projects_found"] += len(rows)

            for row in rows:
                app_id = row.get("app_id") or row.get("project_id") or ""
                pp_url = row.get("print_preview_url")

                if not pp_url:
                    counts["error_count"] += 1
                    continue

                if item_limit and items_done >= item_limit:
                    logger.info(
                        f"Item limit {item_limit} reached — stopping listing walk"
                    )
                    processing_done = True
                    break

                # Count every row toward the limit BEFORE skip checks so daily_light
                # (which skips every already-DB project) still honors CRAWL_ITEM_LIMIT.
                items_done += 1
                key: str | None = None
                logger.set_project(url=pp_url, page=current_page)

                # ── Fast path: compute composite key from listing-row fields ──
                # When the listing exposes project_name, promoter_name, and a
                # decodable data_cert, the same composite key formula used
                # post-detail can run here, letting daily_light skip already-known
                # projects without fetching the PrintPreview page.  All other
                # components (STATE, doc_decoded) are derivable without the
                # detail page.  Falls back to the slow post-detail path when any
                # listing-side component is missing.
                listing_project_name  = _clean(row.get("project_name"))
                listing_promoter_name = _clean(row.get("promoter_name"))
                listing_doc_decoded   = _compute_doc_decoded(row.get("data_cert") or "")
                listing_key: str | None = None
                if listing_project_name and listing_promoter_name and listing_doc_decoded:
                    listing_key_input = "|".join([
                        listing_project_name, listing_promoter_name, STATE, listing_doc_decoded,
                    ])
                    listing_key = generate_project_key(listing_key_input)
                    logger.set_project(key=listing_key, url=pp_url, page=current_page)
                    if mode == "daily_light" and get_project_by_key(listing_key):
                        logger.info("Skipping — already in DB (daily_light)", step="skip")
                        counts["projects_skipped"] += 1
                        logger.clear_project()
                        continue

                try:
                    # ── Navigate to PrintPreview ──────────────────────────────
                    # No per-project delay — the PrintPreview navigation itself
                    # takes 3-8 s, and a per-page delay already fires at the
                    # pagination step below.
                    pp_html = _fetch_print_preview_html(detail_page, pp_url, logger)
                    if pp_html is None:
                        logger.error(
                            "PrintPreview unavailable after retries — skipping project",
                            step="detail_fetch",
                        )
                        insert_crawl_error(
                            run_id, site_id, "DETAIL_FETCH_FAILED",
                            "PrintPreview returned invalid content after max retries",
                            url=pp_url,
                        )
                        counts["error_count"] += 1
                        logger.clear_project()
                        continue
                    pp_soup = BeautifulSoup(pp_html, "lxml")
                    detail_data = _scrape_print_preview(pp_soup, row)

                    # ── Store registration number (informational — not the key) ──
                    reg_no = _clean(detail_data.get("project_registration_no"))
                    if reg_no:
                        detail_data["project_registration_no"] = reg_no

                    # ── Assemble document list ────────────────────────────────
                    raw_docs = _build_uploaded_documents(row, detail_data)
                    detail_data.pop("_raw_docs", None)

                    # ── Build data blob ────────────────────────────────────────
                    land_area_unit  = detail_data.pop("_land_area_unit", "Total Area(In sqmts)")
                    const_area_unit = detail_data.pop("_const_area_unit",
                                                      "Approved Built up Area (In Sqmts)")

                    # doc_decoded: base64-decode data_cert and strip the last two
                    # session-scoped parameters (CharacterD, ExtAppID) so it is
                    # stable across crawl sessions.
                    raw_cert    = row.get("data_cert") or ""
                    doc_decoded = _compute_doc_decoded(raw_cert)

                    detail_data["data"] = {
                        "data_cert": raw_cert,
                        "govt_type": "state",
                        "doc_decoded": doc_decoded,
                        "is_processed": False,
                        "land_area_unit": land_area_unit,
                        "construction_area_unit": const_area_unit,
                    }
                    detail_data["land_area_details"] = {
                        "land_area": detail_data.get("land_area"),
                        "land_area_unit": land_area_unit,
                        "construction_area": detail_data.get("construction_area"),
                        "construction_area_unit": const_area_unit,
                    }

                    # ── Key: project_name | promoter_name | state | doc_decoded ──
                    # Telangana is the sole exception to the single-reg-no formula.
                    # All four components are required — skip if any is absent.
                    project_name  = _clean(detail_data.get("project_name"))
                    promoter_name = _clean(detail_data.get("promoter_name"))
                    if not project_name or not promoter_name or not doc_decoded:
                        missing = [
                            f for f, v in [
                                ("project_name",  project_name),
                                ("promoter_name", promoter_name),
                                ("doc_decoded",   doc_decoded),
                            ] if not v
                        ]
                        logger.error(
                            f"Missing key fields {missing} — skipping project",
                            step="key_generation",
                        )
                        insert_crawl_error(
                            run_id, site_id, "KEY_FIELDS_MISSING",
                            f"Cannot generate key — missing: {missing}",
                            url=pp_url,
                        )
                        counts["error_count"] += 1
                        logger.clear_project()
                        continue

                    key_input = "|".join([project_name, promoter_name, STATE, doc_decoded])
                    key = generate_project_key(key_input)
                    # When the listing exposed all key components, the listing-derived
                    # key must match the detail-derived key — otherwise daily_light will
                    # silently re-fetch this project forever.  Log loudly when they
                    # diverge; the listing-derived key wins so the DB record is
                    # findable by the next daily_light run.
                    if listing_key and listing_key != key:
                        logger.warning(
                            "Listing key disagrees with detail key — keeping listing key",
                            step="key_generation",
                            listing_key=listing_key,
                            detail_key=key,
                            listing_project_name=listing_project_name,
                            detail_project_name=project_name,
                            listing_promoter_name=listing_promoter_name,
                            detail_promoter_name=promoter_name,
                        )
                        key = listing_key
                    logger.set_project(key=key, reg_no=reg_no or "", url=pp_url, page=current_page)

                    if mode == "daily_light" and get_project_by_key(key):
                        logger.info("Skipping — already in DB (daily_light)", step="skip")
                        counts["projects_skipped"] += 1
                        logger.clear_project()
                        continue

                    # ── Core metadata ─────────────────────────────────────────
                    detail_data["key"]              = key
                    detail_data["state"]            = STATE
                    detail_data["project_state"]    = STATE
                    detail_data["domain"]           = DOMAIN
                    detail_data["config_id"]        = config.get("config_id")
                    detail_data["crawl_machine_ip"] = machine_ip
                    detail_data["machine_name"]     = machine_name
                    detail_data["url"]              = pp_url or SEARCH_URL
                    detail_data["is_live"]          = True
                    detail_data["uploaded_documents"] = raw_docs or None

                    # ── Normalize + validate ──────────────────────────────────
                    try:
                        normalized = normalize_project_payload(
                            detail_data, config,
                            machine_name=machine_name, machine_ip=machine_ip,
                        )
                        record  = ProjectRecord(**normalized)
                        db_dict = record.to_db_dict()
                    except (ValidationError, ValueError) as ve:
                        logger.warning("Validation failed — using fallback", error=str(ve),
                                       step="normalize")
                        insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(ve),
                                           project_key=key, url=pp_url)
                        counts["error_count"] += 1
                        try:
                            detail_data["data"] = merge_data_sections(
                                detail_data.get("data"), {"validation_fallback": True})
                            db_dict = normalize_project_payload(
                                detail_data, config,
                                machine_name=machine_name, machine_ip=machine_ip,
                            )
                        except Exception:
                            logger.clear_project()
                            continue

                    # ── Upsert to DB ──────────────────────────────────────────
                    action = upsert_project(db_dict)
                    if action == "new": counts["projects_new"] += 1
                    else:               counts["projects_updated"] += 1
                    logger.info(f"DB result: {action}", step="db_upsert")

                    # ── Documents ─────────────────────────────────────────────
                    doc_name_counts: dict[str, int] = {}
                    uploaded_results: list[dict] = []
                    for doc in raw_docs:
                        selected = select_document_for_download(
                            STATE, doc, doc_name_counts, domain=DOMAIN)
                        if selected:
                            result = _handle_document(
                                db_dict["key"], selected, run_id, site_id, logger)
                            if result:
                                uploaded_results.append(result)
                                counts["documents_uploaded"] += 1

                    if uploaded_results:
                        upsert_project({
                            "key":                    db_dict["key"],
                            "url":                    db_dict.get("url"),
                            "state":                  STATE,
                            "domain":                 DOMAIN,
                            "project_registration_no": db_dict["project_registration_no"],
                            "uploaded_documents":     uploaded_results,
                            "document_urls":          build_document_urls(uploaded_results),
                        })

                except Exception as exc:
                    logger.exception("Project processing failed", exc, step="project_loop")
                    insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                                       project_key=key, url=pp_url)
                    counts["error_count"] += 1
                finally:
                    logger.clear_project()

            # ── Checkpoint after each listing page ────────────────────────────
            save_checkpoint(site_id, mode, current_page, None, run_id)

            # ── Paginate ──────────────────────────────────────────────────────
            if processing_done:
                break
            if current_page < effective_end:
                random_delay(delay_min, delay_max)
                if not _goto_next_page(page):
                    logger.info("No next-page link found — stopping pagination")
                    break

            current_page += 1

    reset_checkpoint(site_id, mode)
    logger.info("Telangana RERA crawl finished", **counts)
    logger.timing("total_run", time.monotonic() - t_run)
    return counts
