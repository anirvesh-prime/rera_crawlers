"""
Rajasthan RERA Crawler — rera.rajasthan.gov.in
Type: Playwright (Angular SPA listing + detail page HTML scraping)

Strategy:
- Use Playwright to navigate the Angular listing page (ProjectList?status=3)
  and enumerate all registered projects via DataTables HTML scraping.
- For each project, navigate to the detail page with Playwright, wait for the
  Angular SPA to fully render, then parse the HTML with BeautifulSoup.
- Documents: collect all anchor links pointing to PDFs, downloads, or
  ViewProject paths from the rendered detail page.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta

import httpx

from pydantic import ValidationError

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import generate_project_key, random_delay, safe_get, safe_post
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
)
from core.s3 import compute_md5, upload_document, get_s3_url
from core.config import settings

# Rajasthan is in IST (UTC+5:30). /Date(ms)/ timestamps from the RERA API are
# stored as midnight IST values, so we must interpret them in IST and then
# write the result with a "+00:00" suffix (matching production convention).
_IST = timezone(timedelta(hours=5, minutes=30))


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
    * ``dd-mm-yyyy``    — plain date string from GetProjectById
    * ``dd/mm/yyyy``    — alternate separator from listing API
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
API_BASE         = "https://reraapi.rajasthan.gov.in/api/web"
APP_BASE         = "https://reraapp.rajasthan.gov.in"
STATE_CODE       = "RJ"
DOMAIN           = "rera.rajasthan.gov.in"
LISTING_PAGE_URL = f"{BASE_URL}/ProjectList?status=3"
_API_KEY     = "MySuperSecretApiKey_123"
_API_HEADERS = {
    "x-api-key": _API_KEY,
    "Accept":    "application/json, text/plain, */*",
    "Origin":    BASE_URL,
    "Referer":   f"{BASE_URL}/",
}

# Playwright table field → schema field
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

# GetProjectById detail API field → schema field
_DETAIL_API_TO_FIELD: dict[str, str] = {
    "ProjectName":             "project_name",
    "PromoterName":            "promoter_name",
    "RevisedDateOfComplation": "estimated_finish_date",
    "DateofRegistration":      "submitted_date",
    "ProjectCategory":         "project_type",
    "RegistrationNo":          "project_registration_no",
}

# Ordered host list for resolving relative document paths
_DOC_HOSTS = [APP_BASE, BASE_URL]


def _clean(text) -> str:
    """Strip and collapse whitespace (used by the Playwright listing scraper)."""
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


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


def _resolve_relative_url(path: str, hosts: list[str] = _DOC_HOSTS) -> str | None:
    """Turn a relative path into an absolute URL using the primary document host (APP_BASE)."""
    clean = path.replace("~/", "").replace("~\\", "").replace("../", "").replace("..\\", "")
    if not clean.startswith("/"):
        clean = f"/{clean}"
    primary = hosts[0] if hosts else APP_BASE
    return f"{primary}{clean}"


def _build_cert_url(cert_path: str) -> str | None:
    """Convert a relative UploadedCertificatePath to an absolute download URL."""
    if not cert_path or cert_path == "0":
        return None
    if cert_path.startswith("http://") or cert_path.startswith("https://"):
        return cert_path
    return _resolve_relative_url(cert_path)


def _build_app_url(path: str | None) -> str | None:
    if not path or path == "0":
        return None
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return _resolve_relative_url(path)


def _fetch_project_detail(enc_id: str, logger: CrawlerLogger,
                          client: httpx.Client | None = None) -> dict:
    """Call GetProjectById API and return explicit fields plus raw structured payloads."""
    resp = safe_post(
        f"{API_BASE}/Home/GetProjectById",
        json_data={"ProjectId": enc_id},
        headers=_API_HEADERS, retries=2, timeout=12, client=client,
    )
    if not resp:
        return {}
    try:
        proj = resp.json().get("Data", {}).get("Project", [{}])[0]
    except Exception:
        return {}

    out: dict = {}
    for api_f, schema_f in _DETAIL_API_TO_FIELD.items():
        val = proj.get(api_f)
        if val is not None and str(val).strip() and str(val) not in ("0", "None"):
            v = str(val).strip()
            if schema_f.endswith("_date"):
                v = _normalize_date_str(v) or v
            elif schema_f == "project_type":
                v = _normalize_project_type(v)
            out[schema_f] = v

    if proj.get("ProjectLocation"):
        out["project_location_raw"] = {"project_location": proj["ProjectLocation"]}

    promoter_address = {
        "details_of_promoter": proj.get("DetailsofPromoter"),
        "promoter_type":       proj.get("PromoterType"),
    }
    promoter_contact = {
        "phone": proj.get("promotermobileno"),
        "email": proj.get("promoteremail"),
    }
    if any(promoter_address.values()):
        out["promoter_address_raw"] = {k: v for k, v in promoter_address.items() if v}
    if any(promoter_contact.values()):
        out["promoter_contact_details"] = {k: v for k, v in promoter_contact.items() if v}

    if proj.get("TotalBuildingCount"):
        out["building_details"] = {
            "total_buildings":         proj.get("TotalBuildingCount"),
            "sanctioned_buildings":    proj.get("SanctionedbuildingCount"),
            "not_sanctioned_buildings":proj.get("NotSanctionedbuildingCount"),
            "open_space_area":         proj.get("AggregateAreaOpenSpace"),
        }
    if any(proj.get(k) for k in ("Rectified_PhaseArea", "AggregateAreaOpenSpace")):
        out["land_area_details"] = {
            "rectified_phase_area":      proj.get("Rectified_PhaseArea"),
            "aggregate_open_space_area": proj.get("AggregateAreaOpenSpace"),
        }
        try:
            if proj.get("Rectified_PhaseArea") is not None:
                out["land_area"] = float(proj["Rectified_PhaseArea"])
        except (ValueError, TypeError):
            pass
    out["data"] = {"source_api": "GetProjectById", "raw": proj}
    return out


def _fetch_project_website_detail(enc_id: str, logger: CrawlerLogger,
                                   client: httpx.Client | None = None) -> dict:
    resp = safe_get(f"{APP_BASE}/HomeWebsite/ProjectDtlsWebsite/{enc_id}",
                    logger=logger, timeout=15, client=client)
    if not resp:
        return {}
    try:
        payload = resp.json()
    except Exception:
        return {}
    if not payload.get("success"):
        return {}
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


def _fetch_last_updated_date(logger: CrawlerLogger,
                             client: httpx.Client | None = None) -> str:
    resp = safe_get(f"{APP_BASE}/HomeWebsite/GetPatchLastUpdatedDateWebSite/",
                    logger=logger, timeout=12, client=client)
    if not resp:
        return ""
    try:
        return resp.json().get("data", "") or ""
    except Exception:
        return ""


def _fetch_view_project_data(project_id: str, doc_type: str, logger: CrawlerLogger,
                             client: httpx.Client | None = None) -> dict:
    """Call ViewProjectWebsite?id=<project_id>&type=<O|U>. type=U is the primary source."""
    resp = safe_get(
        f"{APP_BASE}/HomeWebsite/ViewProjectWebsite?id={project_id}&type={doc_type}",
        logger=logger, timeout=15, client=client,
    )
    if not resp:
        return {}
    try:
        data = resp.json()
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _iter_website_documents(node, *, docs: list[dict], seen: set[str],
                            section: str | None = None) -> None:
    if isinstance(node, dict):
        doc_url = _build_app_url(node.get("DocumentUrl"))
        if doc_url:
            label = (node.get("ApplicationDocumentName") or node.get("DocumentName")
                     or node.get("MasterType") or section or "document")
            if doc_url not in seen:
                seen.add(doc_url)
                entry = {"label": str(label).strip(), "url": doc_url,
                         "remarks": node.get("DocumentName"),
                         "upload_date": node.get("CreatedOn"),
                         "category": node.get("MasterType") or section}
                docs.append({k: v for k, v in entry.items() if v not in (None, "")})
        for key, value in node.items():
            next_section = section
            if isinstance(key, str) and key.startswith("Get") and key.endswith("List"):
                next_section = key
            _iter_website_documents(value, docs=docs, seen=seen, section=next_section)
    elif isinstance(node, list):
        for item in node:
            _iter_website_documents(item, docs=docs, seen=seen, section=section)


def _iter_view_project_documents(node, *, docs: list[dict], seen: set[str],
                                  section: str | None = None) -> None:
    if isinstance(node, dict):
        certi = _build_app_url(node.get("CertiPath"))
        if certi and certi not in seen:
            seen.add(certi)
            docs.append({"label": "RERA Registration Certificate", "url": certi,
                         "category": "certificate"})
        drawing = _build_app_url(node.get("DrawingsFileURL"))
        if drawing and drawing not in seen:
            seen.add(drawing)
            docs.append({"label": "Common Area Drawing", "url": drawing,
                         "category": section or "ProjectCommanArea"})
        bldg_url  = _build_app_url(node.get("Url"))
        bldg_name = node.get("DocumentName")
        if bldg_url and bldg_name and bldg_url not in seen:
            seen.add(bldg_url)
            docs.append({"label": str(bldg_name).strip(), "url": bldg_url,
                         "category": section or "GetBuildingDetails"})
        doc_url = _build_app_url(node.get("DocumentUrl"))
        if doc_url and doc_url not in seen:
            seen.add(doc_url)
            label = (node.get("ApplicationDocumentName") or node.get("DocumentName")
                     or node.get("MasterType") or section or "document")
            entry = {"label": str(label).strip(), "url": doc_url,
                     "remarks": node.get("DocumentName"),
                     "upload_date": node.get("CreatedOn"),
                     "category": node.get("MasterType") or section}
            docs.append({k: v for k, v in entry.items() if v not in (None, "")})
        for key, value in node.items():
            next_section = section
            if isinstance(key, str) and (
                (key.startswith("Get") and key.endswith(("List", "Details")))
                or key in ("ProjectCommanArea", "ProjectDocuments", "PromoterDocumentList",
                           "ProjectCommanAreaNew")
            ):
                next_section = key
            _iter_view_project_documents(value, docs=docs, seen=seen, section=next_section)
    elif isinstance(node, list):
        for item in node:
            _iter_view_project_documents(item, docs=docs, seen=seen, section=section)



def _extract_view_project_fields(view_data: dict) -> dict:  # noqa: C901
    """Extract structured DB fields from a ViewProjectWebsite (type=U) JSON response."""
    out: dict = {}

    # ── GetProjectBasic ────────────────────────────────────────────────────────
    basic_raw = view_data.get("GetProjectBasic")
    if isinstance(basic_raw, list) and basic_raw:
        basic: dict = basic_raw[0]
    elif isinstance(basic_raw, dict):
        basic = basic_raw
    else:
        basic = {}

    if basic:
        _date_candidates: list[tuple[str, ...]] = [
            ("actual_commencement_date",    "ActualCommencementDate", "ActualCommencementDateNew"),
            ("actual_finish_date",          "ActualfinishDate", "ActualfinishDateNew"),
            ("estimated_finish_date",
             "RevisedDateOfComplation", "EstimatedFinishDate",
             "RevisedDateOfComplationNew", "EstimatedFinishDateNew"),
            ("estimated_commencement_date",
             "DateOfComplation", "CommencementDate", "ProposedStartDate",
             "ProposedCommencementDate", "EstimatedStartDate", "DateOfComplationNew"),
            ("submitted_date",
             "ApprovedOn", "ApplicationDate", "DateofApplication",
             "SubmittedDate", "RegistrationDate"),
        ]
        for row in _date_candidates:
            schema_f, *api_keys = row
            if schema_f in out:
                continue
            for api_f in api_keys:
                val = basic.get(api_f)
                if not val or str(val).strip() in ("null", "None", "0", ""):
                    continue
                normalized = _normalize_date_str(str(val).strip())
                if normalized:
                    out[schema_f] = normalized
                    break

        for api_f_list, schema_f in (
            (("TotalResidentialUnit", "NoofResidentialUnit", "TotalUnit", "NoofUnit",
              "TotalResidentialUnits", "NumberofResidentialUnit"), "number_of_residential_units"),
            (("TotalCommercialUnit", "NoofCommercialUnit", "TotalCommercialUnits",
              "NumberofCommercialUnit"), "number_of_commercial_units"),
        ):
            for api_f in api_f_list:
                val = basic.get(api_f)
                if val is not None and str(val).strip() not in ("", "null", "None", "0"):
                    try:
                        out[schema_f] = int(val)
                    except (ValueError, TypeError):
                        pass
                    break

        for api_f_list, schema_f in (
            (("BuiltupArea", "TotalBuiltupArea", "ConstructionArea", "TotalConstArea",
              "BuiltUpArea", "TotalBuiltUpArea", "BuiltUpAreaFSI", "SaleableArea"), "construction_area"),
            (("Rectified_PhaseArea", "PhaseArea", "Area", "LandArea",
              "TotalLandArea", "PlotArea"), "land_area"),
        ):
            for api_f in api_f_list:
                val = basic.get(api_f)
                if val is not None and str(val).strip() not in ("", "null", "None", "0"):
                    try:
                        out[schema_f] = float(val)
                    except (ValueError, TypeError):
                        pass
                    break

        agg_open = basic.get("AggregateAreaOpenSpace")
        if agg_open is not None:
            try:
                _open_f = float(agg_open)
                if _open_f > 0:
                    ld = out.get("land_area_details") or {}
                    ld["open_space_area"] = _open_f
                    out["land_area_details"] = ld
            except (ValueError, TypeError):
                pass

        desc = (basic.get("ProjectDescription") or basic.get("Description") or
                basic.get("ProjectDesc") or basic.get("ProjectRemark") or "").strip()
        if desc and desc.lower() not in ("null", "none"):
            out["project_description"] = desc

        def _s(*keys: str) -> str:
            for k in keys:
                v = basic.get(k)
                if v is None:
                    continue
                sv = str(v).strip()
                if sv in ("", "null", "None", "0"):
                    continue
                return sv
            return ""

        plot_no    = _s("PlotNo")
        village    = _s("VillageName")
        district   = _s("DistrictName")
        pin_code   = _s("PinCode")
        state_name = _s("StateName") or "Rajasthan"
        locality   = _s("LocalityName", "StreetName", "Locality", "StreetAddress")
        taluk      = next(
            (str(basic.get(k, "")).strip()
             for k in ("TahsilName", "TehsilName", "TalukName", "Tehsil", "Taluka")
             if basic.get(k) is not None
             and str(basic.get(k, "")).strip() not in ("", "null", "None", "0")
             and not str(basic.get(k, "")).strip().isdigit()),
            ""
        )
        house_no = _s("HouseNo", "HouseName", "BuildingNo", "ProjectPlotNo")

        if plot_no or village or district:
            parts: list[str] = []
            if plot_no:
                parts.append(f"Khasra No./ Plot No.{plot_no}")
            if village:
                parts.append(f"Village- {village}")
            if locality:
                parts.append(locality)
            location_str = f"{district} - {pin_code}" if pin_code else district
            if location_str:
                parts.append(f", {location_str} ({state_name})")
            raw_addr_str = " , ".join(parts)
            out["raw_address"] = raw_addr_str

            loc: dict = {}
            if house_no:
                loc["house_no_building_name"] = house_no
            elif plot_no:
                if re.match(r"^(PLOT|KHASRA|SURVEY|FLAT|BLOCK)\b", plot_no, re.IGNORECASE):
                    loc["house_no_building_name"] = plot_no
                else:
                    loc["house_no_building_name"] = f"PLOT NO {plot_no}"
            if village:
                loc["village"] = village
            if locality:
                loc["locality"] = locality
            if taluk:
                loc["taluk"] = taluk
            if district:
                loc["district"] = district
            if pin_code:
                loc["pin_code"] = pin_code
            loc["state"] = state_name
            loc["raw_address"] = raw_addr_str
            out["project_location_raw"] = loc

        _BANK_ACCOUNTS = [
            ("Collection Account (100%)", "BankName", "BranchName", "IFSCCode",
             "BankAccountNo", "BankAddress", "AccountHolderName"),
            ("RERA Retention Account (70%)", "BankNameRetention", "BranchNameRetention",
             "IFSCCodeRetention", "BankAccountNoRetention",
             "BankAddressRetention", "AccountHolderNameRetention"),
            ("Promoter's Account", "BankNamePromoter", "BranchNamePromoter",
             "IFSCCodePromoter", "BankAccountNoPromoter",
             "BankAddressPromoter", "AccountHolderNamePromoter"),
        ]
        _bank_list = []
        for _acct_type, _bn, _br, _ifsc, _ano, _baddr, _holder in _BANK_ACCOUNTS:
            _bname_val = (basic.get(_bn) or "").strip()
            if not _bname_val:
                continue
            _be: dict = {"account_type": _acct_type, "bank_name": _bname_val}
            for _fkey, _fval_key in (("branch", _br), ("IFSC", _ifsc), ("account_no", _ano),
                                      ("address", _baddr), ("account_name", _holder)):
                _v = (basic.get(_fval_key) or "").strip()
                if _v:
                    _be[_fkey] = _v
            _bank_list.append(_be)
        if _bank_list:
            out["bank_details"] = _bank_list

    # ── PlotDetails ───────────────────────────────────────────────────────────
    plot_details_raw = view_data.get("PlotDetails")
    if isinstance(plot_details_raw, list):
        plot_details = []
        for item in plot_details_raw:
            if not isinstance(item, dict):
                continue
            plot_area   = item.get("PlotArea")
            total_plots = item.get("TotalPlots")
            if plot_area is not None:
                plot_details.append({
                    "carpet_area": str(plot_area),
                    "no_of_units": str(total_plots) if total_plots is not None else "0",
                })
        if plot_details:
            out["plot_details"] = plot_details

    # ── Rich building details ─────────────────────────────────────────────────
    for bk in ("GetBuildingDetails", "BuildingDetails", "ProjectBuildingDetails",
               "GetBuildings", "Buildings"):
        bldg_raw = view_data.get(bk)
        if isinstance(bldg_raw, list) and bldg_raw:
            bldg_list = []
            total_res_units = 0
            for bldg_item in bldg_raw:
                if not isinstance(bldg_item, dict):
                    continue
                bldg_name = (bldg_item.get("Name") or bldg_item.get("BuildingName") or "").strip()
                _n_basement = int(bldg_item.get("NumberOfBaseMent") or 0)
                _n_plinth   = int(bldg_item.get("NumberOfPlinth")   or 0)
                _n_podium   = int(bldg_item.get("NumberOfPodium")   or 0)
                _n_stilt    = int(bldg_item.get("NumberOfStilts")   or 0)
                _n_upper    = int(bldg_item.get("NumberOfSlabOfSS") or 0)
                if _n_upper == 0:
                    _nbs = str(bldg_item.get("NumberOfBlocksString") or "")
                    _parts = [p.strip() for p in _nbs.split(",") if p.strip().isdigit()]
                    if len(_parts) >= 2:
                        try:
                            _n_upper = int(_parts[1])
                        except (ValueError, IndexError):
                            pass
                _floor_meta: dict = {}
                if _n_basement: _floor_meta["basement_floors"] = _n_basement
                if _n_plinth:   _floor_meta["plinth_floors"]   = _n_plinth
                if _n_podium:   _floor_meta["podium_floors"]   = _n_podium
                if _n_stilt:    _floor_meta["stilt_floors"]    = _n_stilt
                if _n_upper:    _floor_meta["upper_floors"]    = _n_upper
                _total_floors = _n_basement + _n_plinth + _n_podium + _n_stilt + _n_upper
                if _total_floors:
                    _floor_meta["total_floors"] = _total_floors

                apt_raw = (bldg_item.get("GetAppartmentDetails") or
                           bldg_item.get("GetAppartmentDetailsNew") or [])
                if apt_raw:
                    for apt in apt_raw:
                        if not isinstance(apt, dict):
                            continue
                        entry: dict = {}
                        flat_type = (apt.get("ApartmentType") or apt.get("FlatType") or
                                     apt.get("UnitType") or "")
                        carpet    = apt.get("CarpetArea") or apt.get("Carpetarea")
                        balcony   = apt.get("AreaOfBalconyVaramda") or apt.get("BalconyArea")
                        open_a    = apt.get("AreaOfVerandah")
                        if open_a is None:
                            open_a = apt.get("OpenArea")
                        no_units  = apt.get("NumberOfApartments") or apt.get("NoOfUnit")
                        no_booked = apt.get("NumberOfApartmentsBooked")
                        block     = (apt.get("BulidingBlockText") or apt.get("BlockName") or
                                     bldg_name or "")
                        if str(flat_type).strip():
                            entry["flat_type"] = str(flat_type).strip()
                        if carpet is not None:
                            entry["carpet_area"] = str(carpet)
                        if balcony is not None:
                            entry["balcony_area"] = str(balcony)
                        try:
                            entry["open_area"] = f"{float(open_a):.2f}" if open_a is not None else "0.00"
                        except (ValueError, TypeError):
                            entry["open_area"] = str(open_a) if open_a is not None else "0.00"
                        if no_units is not None:
                            entry["no_of_units"] = str(no_units)
                            try:
                                total_res_units += int(no_units)
                            except (ValueError, TypeError):
                                pass
                        if no_booked is not None:
                            entry["no_of_units_booked"] = str(no_booked)
                        if str(block).strip():
                            entry["block_name"] = str(block).strip()
                        if _floor_meta:
                            entry.update(_floor_meta)
                        if entry:
                            bldg_list.append(entry)
                else:
                    entry = {}
                    flat_type = bldg_item.get("FlatType") or bldg_item.get("ApartmentType") or ""
                    carpet    = bldg_item.get("CarpetArea")
                    no_units  = bldg_item.get("NoOfUnit") or bldg_item.get("TotalUnit")
                    if str(flat_type).strip():
                        entry["flat_type"] = str(flat_type).strip()
                    if carpet is not None:
                        entry["carpet_area"] = str(carpet)
                    if no_units is not None:
                        entry["no_of_units"] = str(no_units)
                    if entry:
                        bldg_list.append(entry)
            if bldg_list:
                out["building_details"] = bldg_list
            if total_res_units > 0 and "number_of_residential_units" not in out:
                out["number_of_residential_units"] = total_res_units
            break

    # ── Construction progress ─────────────────────────────────────────────────
    def _parse_dotnet_date(val) -> str:
        s = str(val).strip()
        _m = re.match(r'^/Date\((-?\d+)\)/$', s)
        if _m:
            try:
                dt = datetime.fromtimestamp(int(_m.group(1)) / 1000, tz=timezone.utc)
                return dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
            except (ValueError, OSError):
                pass
        return s

    _all_gantt: list = []
    for _gk in ("GanttChartModel", "GanttChartModelcomm"):
        _g = view_data.get(_gk)
        if isinstance(_g, list):
            _all_gantt.extend(_g)
    if _all_gantt:
        prog_list = []
        for item in _all_gantt:
            if not isinstance(item, dict):
                continue
            milestone = (item.get("Milestone") or "").strip()
            if not milestone:
                continue
            pe: dict = {"title": milestone}
            from_d = item.get("FromDate")
            to_d   = item.get("ToDate")
            if from_d and str(from_d).strip() not in ("", "null", "None"):
                pe["from_date"] = _parse_dotnet_date(from_d)
            if to_d and str(to_d).strip() not in ("", "null", "None"):
                pe["to_date"] = _parse_dotnet_date(to_d)
            prog_list.append(pe)
        if prog_list:
            out["construction_progress"] = prog_list

    # ── Co-promoter details ───────────────────────────────────────────────────
    for ck in ("GetCoPromoterDetails", "CoPromoterDetails", "GetCopromoterList",
               "CopromoterList", "CopromotorDetails"):
        co_raw = view_data.get(ck)
        if co_raw:
            out["co_promoter_details"] = co_raw if isinstance(co_raw, list) else [co_raw]
            break

    # ── Cost details ──────────────────────────────────────────────────────────
    cost_raw = view_data.get("GetProjectCostDetail")
    if cost_raw:
        cost_items = cost_raw if isinstance(cost_raw, list) else [cost_raw]
        cost_out: dict = {}
        _land_amt = 0.0
        _dev_amt  = 0.0
        for _ci in cost_items:
            if not isinstance(_ci, dict):
                continue
            _ci_type = (_ci.get("Type") or "").strip().lower().replace(" ", "")
            _ci_amt  = _ci.get("EstimatedAmount")
            _ci_pid  = _ci.get("ParticulerId", 0)
            if _ci_type and _ci_amt is not None and _ci_pid == 1:
                try:
                    amt = float(_ci_amt)
                except (ValueError, TypeError):
                    continue
                if "landcost" in _ci_type and _land_amt == 0.0:
                    _land_amt = amt
                elif "developmentcost" in _ci_type and _dev_amt == 0.0:
                    _dev_amt = amt
        if _land_amt > 0 or _dev_amt > 0:
            if _land_amt > 0:
                cost_out["cost_of_land"] = _format_inr(_land_amt)
            if _dev_amt > 0:
                cost_out["estimated_construction_cost"] = _format_inr(_dev_amt)
            if _land_amt > 0 and _dev_amt > 0:
                cost_out["estimated_project_cost"] = _format_inr(_land_amt + _dev_amt)
        else:
            def _inr_or_str(v) -> str:
                try:
                    return _format_inr(float(v))
                except (ValueError, TypeError):
                    return str(v).strip()
            for _ci in cost_items:
                if not isinstance(_ci, dict):
                    continue
                land  = _ci.get("CostofLand") or _ci.get("CostOfLand") or _ci.get("LandCost")
                total = _ci.get("TotalProjectCost") or _ci.get("EstimatedProjectCost")
                const = _ci.get("EstimatedCostofConstruction") or _ci.get("EstimatedConstructionCost")
                if land  is not None: cost_out["cost_of_land"]               = _inr_or_str(land)
                if total is not None: cost_out["estimated_project_cost"]      = _inr_or_str(total)
                if const is not None: cost_out["estimated_construction_cost"] = _inr_or_str(const)
                if cost_out:
                    break
        if not cost_out and cost_items:
            cost_out = cost_items[0] if isinstance(cost_items[0], dict) else {}
        out["project_cost_detail"] = cost_out or None

    return out  # ← completed in next block


def _extract_view_project_fields_part2(view_data: dict, out: dict) -> dict:  # noqa: C901
    """Continuation of _extract_view_project_fields: professionals, facilities, promoter."""

    # ── Professional information ──────────────────────────────────────────────
    professionals_raw = view_data.get("ProjectProFessionAlDetail")
    if isinstance(professionals_raw, dict):
        _PROF_ROLE_MAP = {
            "Architect": "Architect", "Engineer": "Structural Engineer",
            "NewEngineer": "Structural Engineer", "Contractor": "Contractor",
            "CA": "Chartered Accountant", "Plumbing": "Plumbing Consultant",
            "HVAC": "HVAC Consultant", "MEPConsultants": "MEP Consultant",
            "ProjectAgent": "Agent", "Other": "Other",
        }
        normalized_profs = []
        _seen_prof_names: set = set()
        for _role_key, _role_label in _PROF_ROLE_MAP.items():
            _role_list = professionals_raw.get(_role_key)
            if not isinstance(_role_list, list):
                continue
            for _pi in _role_list:
                if not isinstance(_pi, dict):
                    continue
                _pname = (_pi.get("Name") or _pi.get("ProfessionalName") or "").strip()
                if not _pname:
                    continue
                _dedup_key = (_role_key.replace("New", ""), _pname.lower())
                if _dedup_key in _seen_prof_names:
                    continue
                _seen_prof_names.add(_dedup_key)
                pe: dict = {"role": _role_label, "name": _pname}
                for _fk, _fv in (("email", _pi.get("Email") or _pi.get("EmailId") or ""),
                                   ("phone", _pi.get("ContactNumber") or _pi.get("MobileNo") or ""),
                                   ("address", _pi.get("Address") or ""),
                                   ("registration_no", _pi.get("COARegistrationNo") or _pi.get("RegistrationNo") or "")):
                    if str(_fv).strip():
                        pe[_fk] = str(_fv).strip()
                normalized_profs.append(pe)
        if normalized_profs:
            out["professional_information"] = normalized_profs
    elif isinstance(professionals_raw, list):
        normalized_profs = []
        for _pi in professionals_raw:
            if not isinstance(_pi, dict):
                continue
            _pname = (_pi.get("Name") or _pi.get("ProfessionalName") or _pi.get("name") or "").strip()
            _prole = (_pi.get("TypeofProfessional") or _pi.get("Role") or _pi.get("role") or "").strip()
            if not _pname:
                continue
            pe = {"name": _pname}
            if _prole: pe["role"] = _prole
            for _fk, _fv in (("email", _pi.get("Email") or _pi.get("EmailId") or _pi.get("email") or ""),
                               ("phone", _pi.get("ContactNumber") or _pi.get("MobileNo") or _pi.get("phone") or ""),
                               ("address", _pi.get("Address") or _pi.get("address") or "")):
                if str(_fv).strip():
                    pe[_fk] = str(_fv).strip()
            normalized_profs.append(pe)
        if normalized_profs:
            out["professional_information"] = normalized_profs

    # ── Common area facilities ────────────────────────────────────────────────
    fac_out: dict = {}
    _pca = view_data.get("ProjectCommanArea")
    _pca = _pca if isinstance(_pca, dict) else {}
    dev_works_raw = _pca.get("ProjectDevelopementWork") or []
    if isinstance(dev_works_raw, list) and dev_works_raw:
        amenities = []
        for _dw in dev_works_raw:
            if not isinstance(_dw, dict):
                continue
            _name = str(_dw.get("Name") or "").strip()
            if not _name:
                continue
            item: dict = {"name": _name, "proposed": bool(_dw.get("Proposed"))}
            _completion = _dw.get("Completion")
            if _completion is not None:
                try:
                    item["completion_percent"] = float(_completion)
                except (ValueError, TypeError):
                    pass
            amenities.append(item)
        if amenities:
            fac_out["amenities"] = amenities

    _caic = view_data.get("CommonAreaItemsCharged") or _pca.get("CommonAreaItemsCharged") or []
    if isinstance(_caic, list) and _caic:
        common_areas = [
            {"name": str(_ca.get("Items") or "").strip(), "included": bool(_ca.get("Checked"))}
            for _ca in _caic if isinstance(_ca, dict) and str(_ca.get("Items") or "").strip()
        ]
        if common_areas:
            fac_out["common_areas"] = common_areas

    _park_raw = _pca.get("ProjectCommonAreaDetails") or []
    if isinstance(_park_raw, list) and _park_raw:
        parking = []
        for _pr in _park_raw:
            if not isinstance(_pr, dict):
                continue
            _type = str(_pr.get("TypeName") or "").strip()
            if not _type:
                continue
            _cars  = int(_pr.get("NoOfCars") or 0)
            _two   = int(_pr.get("NoOfTwoWeelers") or 0)
            _cycles = int(_pr.get("NoOfCycles") or 0)
            _mech  = int(_pr.get("MechanicalCarParking") or 0)
            _vis_c = int(_pr.get("NoOfVisitorCarParking") or 0)
            _vis_s = int(_pr.get("NoOfVisitorScooterParking") or 0)
            _al_c  = int(_pr.get("CarParkingAllocated") or 0)
            _al_s  = int(_pr.get("ScooterParkingAllocated") or 0)
            if any(v > 0 for v in (_cars, _two, _cycles, _mech, _vis_c, _vis_s, _al_c, _al_s)):
                pentry: dict = {"type": _type}
                if _cars:    pentry["cars"] = _cars
                if _two:     pentry["two_wheelers"] = _two
                if _cycles:  pentry["cycles"] = _cycles
                if _mech:    pentry["mechanical"] = _mech
                if _vis_c:   pentry["visitor_cars"] = _vis_c
                if _vis_s:   pentry["visitor_two_wheelers"] = _vis_s
                if _al_c:    pentry["allocated_cars"] = _al_c
                if _al_s:    pentry["allocated_two_wheelers"] = _al_s
                parking.append(pentry)
        if parking:
            fac_out["parking"] = parking

    if fac_out:
        out["provided_faciltiy"] = fac_out

    # ── Litigation / complaints ───────────────────────────────────────────────
    litigations = view_data.get("ProjectLitigations")
    if litigations:
        out["complaints_litigation_details"] = litigations

    # ── PromoterDetails ───────────────────────────────────────────────────────
    promoter_raw = view_data.get("PromoterDetails")
    if isinstance(promoter_raw, dict):
        pd: dict = {}
        office_no = (promoter_raw.get("OfficeNo") or "").strip()
        website   = (promoter_raw.get("WebSiteURL") or "").strip()
        if office_no: pd["office_no"] = office_no
        if website:   pd["website"]   = website

        partners = promoter_raw.get("PartnerModel")
        if isinstance(partners, list) and partners:
            clean_partners = [
                {k: v for k, v in p.items() if v and k in ("PartnerName", "Designation")}
                for p in partners if isinstance(p, dict)
            ]
            if clean_partners:
                pd["partners"] = clean_partners
                out["members_details"] = [
                    {k2: v2 for k2, v2 in (
                        ("name",     p.get("PartnerName", "").strip()),
                        ("position", p.get("Designation", "").strip()),
                    ) if v2}
                    for p in partners if isinstance(p, dict)
                ]

        addr = promoter_raw.get("Address")
        if isinstance(addr, dict):
            _addr_key_map = {
                "PlotNumber": "house_no_building_name", "StreetName": "locality",
                "VillageName": "village", "DistrictName": "district",
                "Taluka": "taluk", "StateName": "state", "ZipCode": "pin_code",
            }
            addr_parts = {schema_k: (addr.get(raw_k) or "").strip()
                          for raw_k, schema_k in _addr_key_map.items()
                          if (addr.get(raw_k) or "").strip()}
            if addr_parts:
                pd["address"] = addr_parts
                out["promoter_address_raw"] = [addr_parts]

        past_exp = promoter_raw.get("PastExprienceDetails")
        if isinstance(past_exp, list) and past_exp:
            pd["past_experience"] = past_exp

        if pd:
            out["promoter_details"] = pd

        # Prefer InformationType (authoritative numeric enum) over OrgType string
        _info_type_map = {1: "Individual", 2: "Company", 3: "Partnership Firm",
                          4: "LLP", 5: "Society", 6: "AOP/BOI", 7: "HUF", 8: "Trust"}
        org_type = ""
        _it = promoter_raw.get("InformationType")
        if _it is not None:
            try:
                org_type = _info_type_map.get(int(_it), "")
            except (ValueError, TypeError):
                org_type = ""
        if not org_type:
            org_type = (promoter_raw.get("OrgType") or "").strip()
            if org_type.lower() in ("null", "none"):
                org_type = ""
        prom_d: dict = {}
        if org_type and org_type.lower() not in ("null", "none"):
            prom_d["type_of_firm"] = org_type
        if prom_d:
            out["promoters_details"] = prom_d

    return out


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

    for tr in table.select("tbody tr"):
        cells = tr.select("td")
        if not cells:
            continue
        row: dict = {}
        for field, idx in col_map.items():
            if idx < len(cells):
                row[field] = cells[idx].get_text(strip=True)

        # Extract enc_id from the "View" button's href or onclick attribute
        for a in tr.select("a[href], a[onclick]"):
            href = a.get("href", "") or a.get("onclick", "")
            m = re.search(r"[?&/]id=?([A-Za-z0-9+/%_=-]{8,})", href)
            if m:
                row["enc_id"] = m.group(1)
                break

        if row.get("reg_no"):
            rows.append(row)

    return rows


def _scrape_project_list_playwright(logger: CrawlerLogger) -> list[dict]:
    """
    Navigate the Rajasthan RERA Angular SPA listing page and extract all projects.
    Returns list of dicts with keys: enc_id, reg_no, project_name, promoter_name,
    project_type, district, application_no, approved_on, status.
    """
    projects: list[dict] = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()
            page.goto(LISTING_PAGE_URL, timeout=60_000)

            # Wait for Angular DataTable to render
            try:
                page.wait_for_selector(
                    "table[datatable], table.dataTable, #project-list-table, table tbody tr",
                    timeout=30_000,
                )
            except Exception:
                logger.warning("DataTables table not found — listing may be empty")
                browser.close()
                return projects
            page.wait_for_load_state("networkidle", timeout=30_000)

            # Try to set DataTables page size to maximum
            try:
                page.select_option(
                    "select[name*='DataTables_Table'], select[name*='_length'], select.dt-length-select",
                    value="1000",
                )
                page.wait_for_timeout(3_000)
                page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                logger.warning("Could not change DataTables page size — will paginate")

            # Extract rows from current view and paginate
            projects.extend(_extract_rj_table_rows(page))
            while True:
                try:
                    next_btn = page.locator(
                        "a.paginate_button.next:not(.disabled), "
                        "li.paginate_button.next:not(.disabled) a"
                    ).first
                    if not next_btn or not next_btn.is_visible():
                        break
                    next_btn.click()
                    page.wait_for_load_state("networkidle", timeout=15_000)
                    page.wait_for_timeout(1_000)
                    projects.extend(_extract_rj_table_rows(page))
                except Exception as e:
                    logger.warning(f"Pagination stopped: {e}")
                    break

            browser.close()
    except Exception as exc:
        logger.error(f"Playwright listing scrape failed: {exc}")

    logger.info(f"Rajasthan page inspection: found {len(projects)} projects")
    return projects




































def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """
    Data-quality sentinel for Rajasthan RERA.
    Loads state_projects_sample/rajasthan.json as the baseline, re-fetches the
    sentinel project's detail via the REST API (enc_id from sample URL),
    and verifies ≥ 80% field coverage.
    """
    import json as _json
    import os as _os
    from urllib.parse import urlparse as _urlparse, parse_qs as _parse_qs
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

    detail_url = baseline.get("url", "")
    if not detail_url:
        logger.warning("Sentinel: no detail URL in sample — skipping", step="sentinel")
        return True

    # Extract enc_id from URL query param ?id=...
    qs = _parse_qs(_urlparse(detail_url).query)
    enc_id = (qs.get("id") or [""])[0]
    if not enc_id:
        logger.warning("Sentinel: could not extract enc_id from URL — skipping",
                       url=detail_url, step="sentinel")
        return True

    logger.info(f"Sentinel: fetching detail for {sentinel_reg}", enc_id=enc_id, step="sentinel")
    try:
        fresh = _fetch_project_detail(enc_id, logger) or {}
    except Exception as exc:
        logger.error(f"Sentinel: fetch error — {exc}", step="sentinel")
        return False

    if not fresh:
        logger.error("Sentinel: no data returned from detail API", step="sentinel")
        return False

    if not check_field_coverage(fresh, baseline, threshold=0.80, logger=logger):
        insert_crawl_error(
            run_id, config.get("id", "rajasthan_rera"),
            "SENTINEL_FAILED",
            f"Coverage below 80% for sentinel project {sentinel_reg}",
        )
        return False

    logger.info("Sentinel check passed", reg=sentinel_reg, step="sentinel")
    return True


def _handle_document(project_key: str, doc: dict, run_id: int,
                     site_id: str, logger: CrawlerLogger,
                     client: httpx.Client | None = None) -> dict | None:
    """Download a document, upload to S3, persist to DB. Returns normalized document metadata or None."""
    url   = doc.get("url")
    label = doc.get("label", "document")
    if not url:
        return None
    filename = build_document_filename(doc)
    try:
        resp = safe_get(url, logger=logger, timeout=15, client=client)
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
        logger.info("Document handled", label=label, s3_key=s3_key)
        return document_result_entry(doc, s3_url, filename)
    except Exception as e:
        logger.error(f"Document failed: {e}", url=url)
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(e),
                           project_key=project_key, url=url)
        return None


def run(config: dict, run_id: int, mode: str) -> dict:
    """Playwright listing scrape + REST API detail fetching."""
    logger   = CrawlerLogger(config["id"], run_id)
    site_id  = config["id"]
    counts   = dict(projects_found=0, projects_new=0, projects_updated=0,
                    projects_skipped=0, documents_uploaded=0, error_count=0)
    item_limit      = settings.CRAWL_ITEM_LIMIT or 0
    items_processed = 0
    machine_name, machine_ip = get_machine_context()

    # ── Sentinel health check ────────────────────────────────────────────────
    if not _sentinel_check(config, run_id, logger):
        logger.error("Sentinel failed — aborting crawl", step="sentinel")
        counts["error_count"] += 1
        return counts

    checkpoint       = load_checkpoint(site_id, mode) or {}
    resume_after_key = checkpoint.get("last_project_key")
    resume_pending   = bool(resume_after_key)

    # Phase 1: collect project list via Playwright listing scrape
    listed_projects = _scrape_project_list_playwright(logger)
    if not listed_projects:
        return counts
    if item_limit:
        listed_projects = listed_projects[:item_limit]
        logger.info(f"Rajasthan: CRAWL_ITEM_LIMIT={item_limit} — {len(listed_projects)} projects")
    else:
        max_pages = settings.MAX_PAGES
        if max_pages:
            listed_projects = listed_projects[:max_pages * 50]
            logger.info(f"Rajasthan: limiting to {len(listed_projects)} projects (max_pages={max_pages})")
    counts["projects_found"] = len(listed_projects)

    _timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0)
    session  = httpx.Client(timeout=_timeout, follow_redirects=True)

    # Phase 2: fetch each project's details via REST API (no Playwright needed)
    for i, proj in enumerate(listed_projects):
        enc_id = proj.get("enc_id", "")
        reg_no = proj.get("reg_no") or f"RJ-{i}"
        key    = generate_project_key(reg_no)
        if resume_pending:
            if key == resume_after_key:
                resume_pending = False
            counts["projects_skipped"] += 1
            continue

        detail_url = (
            f"https://rera.rajasthan.gov.in/view-project-summary?id={enc_id}&type=U"
            if enc_id else f"{BASE_URL}/ProjectList?status=3"
        )
        logger.set_project(key=key, reg_no=reg_no, url=detail_url, page=i)

        if mode == "daily_light" and get_project_by_key(key):
            logger.info("Skipping — already in DB (daily_light)", step="skip")
            counts["projects_skipped"] += 1
            logger.clear_project()
            continue

        try:
            # Seed from listing fields
            data: dict = {}
            for list_f, schema_f in _LIST_API_TO_FIELD.items():
                val = str(proj.get(list_f, "") or "").strip()
                if val:
                    if schema_f.endswith("_date"):
                        val = _normalize_date_str(val) or val
                    elif schema_f == "project_type":
                        val = _normalize_project_type(val)
                    data[schema_f] = val

            # Fetch rich detail from ViewProjectWebsite API (primary source)
            view_data: dict = {}
            if enc_id:
                view_data = _fetch_view_project_data(enc_id, "U", logger, client=session)

            if view_data:
                view_fields = _extract_view_project_fields(view_data)
                view_fields = _extract_view_project_fields_part2(view_data, view_fields)
                data.update(view_fields)

            data.update({
                "key":             key,
                "state":           config["state"],
                "project_state":   "Rajasthan",
                "domain":          DOMAIN,
                "config_id":       config["config_id"],
                "url":             detail_url,
                "is_live":         True,
                "machine_name":    machine_name,
                "crawl_machine_ip": machine_ip,
            })

            # Collect document links from API response
            doc_links: list[dict] = []
            if view_data:
                seen_urls: set[str] = set()
                _iter_view_project_documents(view_data, docs=doc_links, seen=seen_urls)

            prod_data_fields: dict = {"govt_type": "state", "is_processed": False}
            if enc_id:
                prod_data_fields["details_page"]             = detail_url
                prod_data_fields["land_area_unit"]           = "In sq. meters"
                prod_data_fields["construction_area_unit"]   = "in sq. meters"

            # Populate data blob supplementary fields used by downstream consumers
            _proj_type = data.get("project_type", "")
            if _proj_type:
                prod_data_fields["type"] = _proj_type.replace("-", " ").title()
            _basic_raw = view_data.get("GetProjectBasic") if view_data else None
            _basic = (_basic_raw[0] if isinstance(_basic_raw, list) and _basic_raw
                      else (_basic_raw if isinstance(_basic_raw, dict) else {}))
            _total_bldg = _basic.get("TotalBuildingCount") if _basic else None
            if _total_bldg is not None:
                try:
                    prod_data_fields["no_of_plots"] = str(int(_total_bldg))
                except (ValueError, TypeError):
                    pass
            _sub_date = data.get("submitted_date", "")
            _reg_no_for_temp = data.get("project_registration_no", reg_no)
            if _sub_date and _reg_no_for_temp:
                try:
                    _dt = datetime.fromisoformat(_sub_date.replace("+00:00", ""))
                    prod_data_fields["temp"] = f"{_reg_no_for_temp} ({_dt.strftime('%d/%m/%Y')})"
                except (ValueError, TypeError):
                    pass

            _pb_name    = data.get("promoter_name", "")
            _pb_contact = data.get("promoter_contact_details") or {}
            _pb_phone   = _pb_contact.get("phone", "")
            _pb_email   = _pb_contact.get("email", "")
            _promoter_block = [x for x in [_pb_name, _pb_phone, _pb_email] if x]
            if _promoter_block:
                prod_data_fields["promoter_block"] = _promoter_block

            data["data"] = merge_data_sections(
                prod_data_fields,
                {"source": "api_view_project", "enc_id": enc_id},
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
                counts["error_count"] += 1
                db_dict = normalize_project_payload(
                    {**data, "data": {"validation_fallback": True, "raw": data.get("data")}},
                    config, machine_name=machine_name, machine_ip=machine_ip,
                )

            action = upsert_project(db_dict)
            items_processed += 1
            if action == "new":       counts["projects_new"] += 1
            elif action == "updated": counts["projects_updated"] += 1
            else:                     counts["projects_skipped"] += 1
            logger.info(f"DB result: {action}", step="db_upsert")

            if mode != "daily_light" and doc_links:
                logger.info(f"Downloading {len(doc_links)} documents", step="documents")
                uploaded_documents = []
                doc_name_counts: dict[str, int] = {}
                for doc in doc_links:
                    selected_doc = select_document_for_download(
                        config["state"], doc, doc_name_counts, domain=DOMAIN)
                    if selected_doc:
                        uploaded_doc = _handle_document(
                            key, selected_doc, run_id, site_id, logger, client=session)
                        if uploaded_doc:
                            uploaded_documents.append(uploaded_doc)
                            counts["documents_uploaded"] += 1
                        else:
                            uploaded_documents.append({"link": doc.get("url"), "type": doc.get("label", "document")})
                    else:
                        uploaded_documents.append({"link": doc.get("url"), "type": doc.get("label", "document")})
                if uploaded_documents:
                    upsert_project({
                        "key": db_dict["key"], "url": db_dict["url"],
                        "state": db_dict["state"], "domain": db_dict["domain"],
                        "project_registration_no": db_dict["project_registration_no"],
                        "uploaded_documents": uploaded_documents,
                        "document_urls": build_document_urls(uploaded_documents),
                    })

            if i % 100 == 0:
                save_checkpoint(site_id, mode, i, key, run_id)
            random_delay(*config.get("rate_limit_delay", (1, 3)))

        except Exception as exc:
            logger.exception("Project processing failed", exc, step="project_loop",
                             enc_id=enc_id)
            insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                               project_key=key, url=detail_url)
            counts["error_count"] += 1
        finally:
            logger.clear_project()

    session.close()
    reset_checkpoint(site_id, mode)
    logger.info(f"Rajasthan RERA complete: {counts}")
    return counts
