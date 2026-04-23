"""
Rajasthan RERA Crawler — rera.rajasthan.gov.in
Type: JSON REST API (Angular SPA)

Strategy:
- POST to reraapi.rajasthan.gov.in/api/web/Home/GetProjects with x-api-key header
- Single call returns all registered projects (no pagination required)
- Each project has a detail page at rera.rajasthan.gov.in/ProjectDetail/{id}
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

import httpx

from pydantic import ValidationError

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

def _format_inr(amount: float) -> str:
    """Format a float as Indian currency string: ₹X,XX,XX,XXX.XX"""
    rounded   = round(amount, 2)
    int_part  = int(rounded)
    dec_str   = f"{rounded - int_part:.2f}"[1:]   # ".xx"
    s = str(int_part)
    if len(s) <= 3:
        return f"₹{s}{dec_str}"
    result, s = s[-3:], s[:-3]
    while s:
        result, s = s[-2:] + "," + result, s[:-2]
    return f"₹{result}{dec_str}"


BASE_URL       = "https://rera.rajasthan.gov.in"
API_BASE       = "https://reraapi.rajasthan.gov.in/api/web"
APP_BASE       = "https://reraapp.rajasthan.gov.in"
STATE_CODE     = "RJ"
DOMAIN         = "rera.rajasthan.gov.in"
_API_KEY       = "MySuperSecretApiKey_123"
_API_HEADERS   = {
    "x-api-key":   _API_KEY,
    "Accept":      "application/json, text/plain, */*",
    "Origin":      BASE_URL,
    "Referer":     f"{BASE_URL}/",
}

# Listing API field → schema field
_LIST_API_TO_FIELD: dict[str, str] = {
    "ProjectName":     "project_name",
    "PromoterName":    "promoter_name",
    "DistrictName":    "project_city",
    "ProjectTypeName": "project_type",
    "REGISTRATIONNO":  "project_registration_no",
    "APPROVEDON":      "approved_on_date",
    "AppStatus":       "status_of_the_project",
    "ApplicationNo":   "acknowledgement_no",
    "RevisedDateOfComplation": "estimated_finish_date",
}

# GetProjectById detail API field → schema field
_DETAIL_API_TO_FIELD: dict[str, str] = {
    "ProjectName":          "project_name",
    "PromoterName":         "promoter_name",
    "RevisedDateOfComplation": "estimated_finish_date",
    "DateofRegistration":   "approved_on_date",
    "ProjectCategory":      "project_type",
    "RegistrationNo":       "project_registration_no",
}



def _fetch_project_detail(enc_id: str, logger: CrawlerLogger,
                          client: httpx.Client | None = None) -> dict:
    """Call GetProjectById API and return explicit fields plus raw structured payloads."""
    resp = safe_post(
        f"{API_BASE}/Home/GetProjectById",
        json_data={"ProjectId": enc_id},
        headers=_API_HEADERS, retries=2, timeout=12,
        client=client,
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
            out[schema_f] = str(val).strip()

    if proj.get("ProjectLocation"):
        out["project_location_raw"] = {"project_location": proj.get("ProjectLocation")}

    promoter_address = {
        "details_of_promoter": proj.get("DetailsofPromoter"),
        "promoter_type": proj.get("PromoterType"),
    }
    # Use "phone" key (matches production schema) instead of "mobile"
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
            "total_buildings": proj.get("TotalBuildingCount"),
            "sanctioned_buildings": proj.get("SanctionedbuildingCount"),
            "not_sanctioned_buildings": proj.get("NotSanctionedbuildingCount"),
            "open_space_area": proj.get("AggregateAreaOpenSpace"),
        }
    if any(proj.get(key) for key in ("Rectified_PhaseArea", "AggregateAreaOpenSpace")):
        out["land_area_details"] = {
            "rectified_phase_area": proj.get("Rectified_PhaseArea"),
            "aggregate_open_space_area": proj.get("AggregateAreaOpenSpace"),
        }
        # Promote land area to top-level float column
        try:
            if proj.get("Rectified_PhaseArea") is not None:
                out["land_area"] = float(proj["Rectified_PhaseArea"])
        except (ValueError, TypeError):
            pass
    out["data"] = {"source_api": "GetProjectById", "raw": proj}
    return out


def _fetch_all_projects(logger: CrawlerLogger) -> list[dict]:
    payload = {
        "DistrictId": 0, "TeshilId": 0, "ProjectName": None,
        "PromoterName": None, "RegistrationNo": None,
        "ProjectType": 0, "ApplicationStatus": "3", "Year": 0,
    }
    resp = safe_post(f"{API_BASE}/Home/GetProjects", json_data=payload,
                     headers=_API_HEADERS, retries=3, timeout=45)
    if not resp:
        logger.error("Failed to fetch Rajasthan project list from API")
        return []
    try:
        d = resp.json()
        data = d.get("Data", [])
        logger.info(f"Rajasthan API returned {len(data)} projects")
        return data
    except Exception as e:
        logger.error(f"Failed to parse Rajasthan API response: {e}")
        return []


# Ordered list of hosts to probe when resolving relative document paths.
# Checked left-to-right; first host that returns HTTP < 400 wins.
_DOC_HOSTS = [APP_BASE, BASE_URL]


def _is_real_document(resp) -> bool:
    """
    Return True only if the response body looks like an actual document.

    Government sites routinely serve soft-404 HTML pages with HTTP 200, so
    status code alone cannot be trusted. We instead peek at the first few bytes:
      - PDF files start with the magic bytes b'%PDF'
      - For other content types we reject anything whose Content-Type is HTML
    """
    if resp is None:
        return False
    content_type = resp.headers.get("Content-Type", "").lower()
    # Read just enough bytes to check the magic signature without pulling the full file
    chunk = resp.content[:8] if resp.content else b""
    if chunk.startswith(b"%PDF"):
        return True
    if "text/html" in content_type or "text/plain" in content_type:
        return False
    # Non-HTML content type with some body — treat as a real document
    return len(chunk) > 0


def _resolve_relative_url(path: str, hosts: list[str] = _DOC_HOSTS) -> str | None:
    """
    Turn a relative path (e.g. '~/Content/uploads/Certificate/Signed_xxx.pdf')
    into an absolute URL using the primary document host (APP_BASE).

    No HTTP probing is performed — the primary host (APP_BASE) always serves
    Rajasthan RERA documents, so probing every host is wasted round-trips.
    The `hosts` parameter is kept for backwards-compatibility with tests.
    """
    clean = path.replace("~/", "").replace("~\\", "").replace("../", "").replace("..\\", "")
    if not clean.startswith("/"):
        clean = f"/{clean}"
    primary = hosts[0] if hosts else APP_BASE
    return f"{primary}{clean}"


def _build_cert_url(cert_path: str) -> str | None:
    """Convert relative UploadedCertificatePath to an absolute download URL."""
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


def _fetch_project_website_detail(enc_id: str, logger: CrawlerLogger,
                                   client: httpx.Client | None = None) -> dict:
    resp = safe_get(f"{APP_BASE}/HomeWebsite/ProjectDtlsWebsite/{enc_id}", logger=logger, timeout=15,
                    client=client)
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


def _iter_website_documents(node, *, docs: list[dict], seen: set[str], section: str | None = None) -> None:
    if isinstance(node, dict):
        doc_url = _build_app_url(node.get("DocumentUrl"))
        if doc_url:
            label = (
                node.get("ApplicationDocumentName")
                or node.get("DocumentName")
                or node.get("MasterType")
                or section
                or "document"
            )
            if doc_url not in seen:
                seen.add(doc_url)
                entry = {
                    "label": str(label).strip(),
                    "url": doc_url,
                    "remarks": node.get("DocumentName"),
                    "upload_date": node.get("CreatedOn"),
                    "category": node.get("MasterType") or section,
                }
                docs.append({k: v for k, v in entry.items() if v not in (None, "")})
        for key, value in node.items():
            next_section = section
            if isinstance(key, str) and key.startswith("Get") and key.endswith("List"):
                next_section = key
            _iter_website_documents(value, docs=docs, seen=seen, section=next_section)
    elif isinstance(node, list):
        for item in node:
            _iter_website_documents(item, docs=docs, seen=seen, section=section)


def _fetch_last_updated_date(logger: CrawlerLogger,
                             client: httpx.Client | None = None) -> str:
    """Fetch the global 'Updated project details as on' date from the RERA app API."""
    resp = safe_get(
        f"{APP_BASE}/HomeWebsite/GetPatchLastUpdatedDateWebSite/",
        logger=logger, timeout=12, client=client,
    )
    if not resp:
        return ""
    try:
        return resp.json().get("data", "") or ""
    except Exception:
        return ""


def _fetch_view_project_data(project_id: str, doc_type: str, logger: CrawlerLogger,
                             client: httpx.Client | None = None) -> dict:
    """
    Call ViewProjectWebsite?id=<project_id>&type=<O|U>.
    type=U → current/updated project data (primary source for DB fields and documents).
    type=O → snapshot at the time of registration (secondary, used only for extra historic docs).
    """
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


def _iter_view_project_documents(
    node, *, docs: list[dict], seen: set[str], section: str | None = None
) -> None:
    """
    Traverse a ViewProjectWebsite JSON tree and collect document URLs.
    Handles all document URL fields not covered by _iter_website_documents:
      - CertiPath         → RERA Registration Certificate
      - DrawingsFileURL   → Common Area Drawing
      - Url + DocumentName → Building plan PDFs (only when DocumentName is present)
      - DocumentUrl       → standard uploaded documents (same as _iter_website_documents)
    """
    if isinstance(node, dict):
        # Registration certificate
        certi = _build_app_url(node.get("CertiPath"))
        if certi and certi not in seen:
            seen.add(certi)
            docs.append({"label": "RERA Registration Certificate", "url": certi,
                         "category": "certificate"})

        # Common area drawings
        drawing = _build_app_url(node.get("DrawingsFileURL"))
        if drawing and drawing not in seen:
            seen.add(drawing)
            docs.append({"label": "Common Area Drawing", "url": drawing,
                         "category": section or "ProjectCommanArea"})

        # Building plan documents (Url field paired with DocumentName)
        bldg_url = _build_app_url(node.get("Url"))
        bldg_name = node.get("DocumentName")
        if bldg_url and bldg_name and bldg_url not in seen:
            seen.add(bldg_url)
            docs.append({"label": str(bldg_name).strip(), "url": bldg_url,
                         "category": section or "GetBuildingDetails"})

        # Standard uploaded documents (ApplicationDocumentName / DocumentName / DocumentUrl)
        doc_url = _build_app_url(node.get("DocumentUrl"))
        if doc_url and doc_url not in seen:
            seen.add(doc_url)
            label = (
                node.get("ApplicationDocumentName")
                or node.get("DocumentName")
                or node.get("MasterType")
                or section
                or "document"
            )
            entry = {
                "label": str(label).strip(), "url": doc_url,
                "remarks": node.get("DocumentName"),
                "upload_date": node.get("CreatedOn"),
                "category": node.get("MasterType") or section,
            }
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
    """
    Extract structured DB fields from a ViewProjectWebsite (type=U) response.
    Returns only fields that have a non-empty value.

    Extracted top-level schema columns:
      actual_commencement_date, actual_finish_date, estimated_commencement_date,
      submitted_date, number_of_residential_units, number_of_commercial_units,
      construction_area, project_description, project_location_raw,
      promoter_address_raw, promoters_details, members_details,
      building_details (rich list), construction_progress, bank_details,
      co_promoter_details, plot_details, project_cost_detail,
      professional_information, provided_faciltiy, complaints_litigation_details,
      promoter_details.

    Also returns prod_data_fields helpers:
      raw_address (string), plot_details (list)
    """
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
        # Date fields — try multiple API key variants; first non-empty wins
        _date_candidates: list[tuple[str, ...]] = [
            ("actual_commencement_date", "ActualCommencementDate"),
            ("actual_finish_date",       "ActualfinishDate"),
            ("estimated_commencement_date", "CommencementDate", "ProposedStartDate",
             "ProposedCommencementDate", "EstimatedStartDate"),
            ("submitted_date", "ApplicationDate", "DateofApplication",
             "SubmittedDate", "RegistrationDate"),
        ]
        for row in _date_candidates:
            schema_f, *api_keys = row
            if schema_f in out:
                continue
            for api_f in api_keys:
                val = basic.get(api_f)
                if val and str(val).strip() not in ("null", "None", "0", ""):
                    v_str = str(val).strip()
                    # Parse .NET JSON date: /Date(<ms>)/
                    _dotnet = re.match(r'^/Date\((-?\d+)\)/$', v_str)
                    if _dotnet:
                        try:
                            dt = datetime.fromtimestamp(int(_dotnet.group(1)) / 1000, tz=timezone.utc)
                            v_str = dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
                        except (ValueError, OSError):
                            pass
                    out[schema_f] = v_str
                    break

        # Unit counts
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

        # Area fields
        for api_f_list, schema_f in (
            (("BuiltupArea", "TotalBuiltupArea", "ConstructionArea", "TotalConstArea",
              "BuiltUpArea", "TotalBuiltUpArea"), "construction_area"),
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

        # Project description
        desc = (basic.get("ProjectDescription") or basic.get("Description") or
                basic.get("ProjectDesc") or "").strip()
        if desc and desc.lower() not in ("null", "none"):
            out["project_description"] = desc

        # Location fields — build both a structured dict and a legacy string
        # _s() coerces any value (including ints returned by the API) to a str
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
        # Taluka field often holds a numeric DB ID — skip purely numeric values
        taluk      = next(
            (str(basic.get(k, "")).strip()
             for k in ("TahsilName", "TehsilName", "TalukName", "Tehsil", "Taluka")
             if basic.get(k) is not None
             and str(basic.get(k, "")).strip() not in ("", "null", "None", "0")
             and not str(basic.get(k, "")).strip().isdigit()),
            ""
        )
        house_no   = _s("HouseNo", "HouseName", "BuildingNo")

        if plot_no or village or district:
            # Legacy address string (stored in prod_data_fields for backward compat)
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
            out["raw_address"] = raw_addr_str  # for prod_data_fields

            # Structured project_location_raw JSONB dict (matches production schema)
            loc: dict = {}
            if house_no:
                loc["house_no_building_name"] = house_no
            elif plot_no:
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

        # Bank details — stored as three flat account sets in GetProjectBasic
        _BANK_ACCOUNTS = [
            ("collection", "BankName", "BranchName", "IFSCCode", "BankAccountNo",
             "BankAddress", "AccountHolderName"),
            ("retention", "BankNameRetention", "BranchNameRetention", "IFSCCodeRetention",
             "BankAccountNoRetention", "BankAddressRetention", "AccountHolderNameRetention"),
            ("promoter", "BankNamePromoter", "BranchNamePromoter", "IFSCCodePromoter",
             "BankAccountNoPromoter", "BankAddressPromoter", "AccountHolderNamePromoter"),
        ]
        _bank_list = []
        for _acct_type, _bn, _br, _ifsc, _ano, _baddr, _holder in _BANK_ACCOUNTS:
            _bname_val = (basic.get(_bn) or "").strip()
            if not _bname_val:
                continue
            _be: dict = {"account_type": _acct_type, "bank_name": _bname_val}
            _v = (basic.get(_br) or "").strip()
            if _v: _be["branch"] = _v
            _v = (basic.get(_ifsc) or "").strip()
            if _v: _be["IFSC"] = _v
            _v = (basic.get(_ano) or "").strip()
            if _v: _be["account_no"] = _v
            _v = (basic.get(_baddr) or "").strip()
            if _v: _be["address"] = _v
            _v = (basic.get(_holder) or "").strip()
            if _v: _be["account_name"] = _v
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

    # ── Rich building details (unit-level list from ViewProjectWebsite) ────────
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
                # Nested apartment/unit type details are in GetAppartmentDetails
                apt_raw = (bldg_item.get("GetAppartmentDetails") or
                           bldg_item.get("GetAppartmentDetailsNew") or [])
                if apt_raw:  # non-empty: use nested apartment-level data
                    for apt in apt_raw:
                        if not isinstance(apt, dict):
                            continue
                        entry: dict = {}
                        flat_type = (apt.get("ApartmentType") or apt.get("FlatType") or
                                     apt.get("UnitType") or "")
                        carpet    = apt.get("CarpetArea") or apt.get("Carpetarea")
                        balcony   = apt.get("AreaOfBalconyVaramda") or apt.get("BalconyArea")
                        open_a    = apt.get("AreaOfVerandah") or apt.get("OpenArea")
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
                        if open_a is not None:
                            entry["open_area"] = str(open_a)
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
                        if entry:
                            bldg_list.append(entry)
                else:
                    # Fallback: top-level building fields (older API shape)
                    entry = {}
                    flat_type = (bldg_item.get("FlatType") or bldg_item.get("ApartmentType") or "")
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
            # Populate total residential units if not already set from GetProjectBasic
            if total_res_units > 0 and "number_of_residential_units" not in out:
                out["number_of_residential_units"] = total_res_units
            break  # use first matching key

    # ── Construction progress (from Gantt chart milestones) ───────────────────
    def _parse_dotnet_date(val) -> str:
        """Convert /Date(<ms>)/ to ISO-8601 UTC string, or return the raw string."""
        s = str(val).strip()
        _m = re.match(r'^/Date\((-?\d+)\)/$', s)
        if _m:
            try:
                dt = datetime.fromtimestamp(int(_m.group(1)) / 1000, tz=timezone.utc)
                return dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
            except (ValueError, OSError):
                pass
        return s

    _gantt_res  = view_data.get("GanttChartModel")
    _gantt_comm = view_data.get("GanttChartModelcomm")
    _all_gantt: list = []
    if isinstance(_gantt_res, list):
        _all_gantt.extend(_gantt_res)
    if isinstance(_gantt_comm, list):
        _all_gantt.extend(_gantt_comm)
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

        # Primary: Rajasthan API uses a Type-keyed list with EstimatedAmount
        # Type values: "LandCost ", "DevelopmentCost " (with trailing space)
        # ParticulerId == 1 identifies the top-level summary row for each Type
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
            # Fallback: flat-dict format (older API / backward compat)
            def _inr_or_str(v) -> str:
                try:
                    return _format_inr(float(v))
                except (ValueError, TypeError):
                    return str(v).strip()
            for _ci in cost_items:
                if not isinstance(_ci, dict):
                    continue
                land  = (_ci.get("CostofLand") or _ci.get("CostOfLand") or
                         _ci.get("LandCost") or _ci.get("cost_of_land"))
                total = (_ci.get("TotalProjectCost") or _ci.get("EstimatedProjectCost") or
                         _ci.get("total_project_cost") or _ci.get("estimated_project_cost"))
                const = (_ci.get("EstimatedCostofConstruction") or _ci.get("EstimatedConstructionCost") or
                         _ci.get("ConstructionCost") or _ci.get("estimated_construction_cost"))
                if land  is not None: cost_out["cost_of_land"]               = _inr_or_str(land)
                if total is not None: cost_out["estimated_project_cost"]      = _inr_or_str(total)
                if const is not None: cost_out["estimated_construction_cost"] = _inr_or_str(const)
                if cost_out:
                    break

        if not cost_out and cost_items:
            # Absolute fallback: store the first raw item as-is
            cost_out = cost_items[0] if isinstance(cost_items[0], dict) else {}
        out["project_cost_detail"] = cost_out or None

    # ── Professional information ───────────────────────────────────────────────
    # ProjectProFessionAlDetail is a dict keyed by role (Architect, Engineer, CA…)
    # where each value is a list of professional records.
    professionals_raw = view_data.get("ProjectProFessionAlDetail")
    if isinstance(professionals_raw, dict):
        _PROF_ROLE_MAP = {
            "Architect":      "Architect",
            "Engineer":       "Structural Engineer",
            "NewEngineer":    "Structural Engineer",
            "Contractor":     "Contractor",
            "CA":             "Chartered Accountant",
            "Plumbing":       "Plumbing Consultant",
            "HVAC":           "HVAC Consultant",
            "MEPConsultants": "MEP Consultant",
            "ProjectAgent":   "Agent",
            "Other":          "Other",
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
                # Deduplicate same person appearing in Engineer + NewEngineer
                _dedup_key = (_role_key.replace("New", ""), _pname.lower())
                if _dedup_key in _seen_prof_names:
                    continue
                _seen_prof_names.add(_dedup_key)
                pe: dict = {"role": _role_label, "name": _pname}
                _pemail   = (_pi.get("Email") or _pi.get("EmailId") or "").strip()
                _pphone   = (_pi.get("ContactNumber") or _pi.get("MobileNo") or "").strip()
                _paddress = (_pi.get("Address") or "").strip()
                _preg     = (_pi.get("COARegistrationNo") or _pi.get("RegistrationNo") or "").strip()
                if _pemail:   pe["email"]           = _pemail
                if _pphone:   pe["phone"]           = _pphone
                if _paddress: pe["address"]         = _paddress
                if _preg:     pe["registration_no"] = _preg
                normalized_profs.append(pe)
        if normalized_profs:
            out["professional_information"] = normalized_profs
    elif isinstance(professionals_raw, list):
        # Fallback: older flat-list format (also handles already-lowercase keys)
        normalized_profs = []
        for _pi in professionals_raw:
            if not isinstance(_pi, dict):
                continue
            _pname = (_pi.get("Name") or _pi.get("ProfessionalName") or
                      _pi.get("name") or "").strip()
            _prole = (_pi.get("TypeofProfessional") or _pi.get("Role") or
                      _pi.get("role") or "").strip()
            if not _pname:
                continue
            pe: dict = {"name": _pname}
            if _prole: pe["role"] = _prole
            _pemail = (_pi.get("Email") or _pi.get("EmailId") or _pi.get("email") or "").strip()
            _pphone = (_pi.get("ContactNumber") or _pi.get("MobileNo") or _pi.get("phone") or "").strip()
            _paddress = (_pi.get("Address") or _pi.get("address") or "").strip()
            if _pemail:   pe["email"]   = _pemail
            if _pphone:   pe["phone"]   = _pphone
            if _paddress: pe["address"] = _paddress
            normalized_profs.append(pe)
        if normalized_profs:
            out["professional_information"] = normalized_profs

    # ── Common area facilities ────────────────────────────────────────────────
    facilities = view_data.get("GetProjectAreaFacilities")
    if facilities:
        # API wraps items under {"ProjectId": ..., "ProjectDetail": [...]}
        if isinstance(facilities, dict) and "ProjectDetail" in facilities:
            facilities = facilities.get("ProjectDetail") or facilities
        if facilities:
            out["provided_faciltiy"] = facilities

    # ── Litigation / complaints ───────────────────────────────────────────────
    litigations = view_data.get("ProjectLitigations")
    if litigations:
        out["complaints_litigation_details"] = litigations

    # ── PromoterDetails — promoter_details + members_details + promoters_details
    #                    + promoter_address_raw ──────────────────────────────
    promoter_raw = view_data.get("PromoterDetails")
    if isinstance(promoter_raw, dict):
        pd: dict = {}

        office_no = (promoter_raw.get("OfficeNo") or "").strip()
        website   = (promoter_raw.get("WebSiteURL") or "").strip()
        if office_no:
            pd["office_no"] = office_no
        if website:
            pd["website"] = website

        partners = promoter_raw.get("PartnerModel")
        if isinstance(partners, list) and partners:
            clean_partners = [
                {k: v for k, v in p.items() if v and k in ("PartnerName", "Designation")}
                for p in partners if isinstance(p, dict)
            ]
            if clean_partners:
                pd["partners"] = clean_partners
                # Also populate members_details (name/position format)
                out["members_details"] = [
                    {k2: v2 for k2, v2 in (
                        ("name",     p.get("PartnerName", "").strip()),
                        ("position", p.get("Designation", "").strip()),
                    ) if v2}
                    for p in partners if isinstance(p, dict)
                ]

        # Promoter address → both promoter_details.address and promoter_address_raw
        addr = promoter_raw.get("Address")
        if isinstance(addr, dict):
            _addr_key_map = {
                "PlotNumber":   "house_no_building_name",
                "StreetName":   "locality",
                "VillageName":  "village",
                "DistrictName": "district",
                "Taluka":       "taluk",
                "StateName":    "state",
                "ZipCode":      "pin_code",
            }
            addr_parts = {}
            for raw_k, schema_k in _addr_key_map.items():
                val = (addr.get(raw_k) or "").strip()
                if val:
                    addr_parts[schema_k] = val
            if addr_parts:
                pd["address"] = addr_parts
                # Structured list format matching production schema
                out["promoter_address_raw"] = [addr_parts]

        past_exp = promoter_raw.get("PastExprienceDetails")
        if isinstance(past_exp, list) and past_exp:
            pd["past_experience"] = past_exp

        if pd:
            out["promoter_details"] = pd

        # promoters_details (firm type / org name summary)
        org_type = (promoter_raw.get("OrgType") or "").strip()
        # InformationType integer → human-readable firm type (when OrgType is null)
        if not org_type or org_type.lower() in ("null", "none"):
            _info_type_map = {
                1: "Individual", 2: "Company", 3: "Partnership Firm",
                4: "LLP", 5: "Society", 6: "AOP/BOI", 7: "HUF", 8: "Trust",
            }
            _it = promoter_raw.get("InformationType")
            if _it is not None:
                try:
                    org_type = _info_type_map.get(int(_it), "")
                except (ValueError, TypeError):
                    org_type = ""
        org_name = (promoter_raw.get("OrgName") or "").strip()
        prom_d: dict = {}
        if org_type and org_type.lower() not in ("null", "none"):
            prom_d["type_of_firm"] = org_type
        if org_name and org_name.lower() not in ("null", "none"):
            prom_d["name"] = org_name
        if prom_d:
            out["promoters_details"] = prom_d

    return out


def _extract_project_website_documents(
    website_data: dict,
    updated_date: str = "",
) -> list[dict]:
    docs: list[dict] = []
    seen: set[str] = set()

    project_id = website_data.get("ProjectId")
    reg_date = website_data.get("DateofRegistration", "")

    if project_id:
        # "Project details as at the time of registration dated <date>" (type=O)
        reg_label = (
            f"Project details as at the time of registration dated {reg_date}"
            if reg_date
            else "project_details_at_registration"
        )
        url_o = f"{BASE_URL}/ViewProject?id={project_id}&type=O"
        if url_o not in seen:
            seen.add(url_o)
            entry: dict = {"label": reg_label, "url": url_o}
            if reg_date:
                entry["date"] = reg_date
            docs.append(entry)

        # "Updated project details as on <date>" (type=U)
        upd_label = (
            f"Updated project details as on {updated_date}"
            if updated_date
            else "updated_project_details"
        )
        url_u = f"{BASE_URL}/ViewProject?id={project_id}&type=U"
        if url_u not in seen:
            seen.add(url_u)
            entry_u: dict = {"label": upd_label, "url": url_u}
            if updated_date:
                entry_u["date"] = updated_date
            docs.append(entry_u)

    _iter_website_documents(website_data, docs=docs, seen=seen)
    return docs


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger) -> bool:
    """Verify the sentinel project still appears in the live Rajasthan API project list."""
    sentinel_reg = config.get("sentinel_registration_no", "")
    if not sentinel_reg:
        logger.warning("No sentinel configured — skipping")
        return True
    key = generate_project_key(sentinel_reg)
    existing = get_project_by_key(key)
    if not existing:
        logger.warning("Sentinel not in DB yet — skipping check")
        return True
    projects = _fetch_all_projects(logger)
    if not projects:
        logger.error("Sentinel: could not fetch project list from API")
        return False
    reg_numbers = {str(p.get("REGISTRATIONNO", "")).strip() for p in projects}
    if sentinel_reg not in reg_numbers:
        logger.error("Sentinel reg number not found in Rajasthan API project list", reg=sentinel_reg)
        return False
    logger.info("Sentinel check passed", reg=sentinel_reg)
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
    logger = CrawlerLogger(config["id"], run_id)
    site_id = config["id"]
    counts = dict(projects_found=0, projects_new=0, projects_updated=0,
                  projects_skipped=0, documents_uploaded=0, error_count=0)

    item_limit    = settings.CRAWL_ITEM_LIMIT or 0  # 0 = unlimited
    items_processed = 0

    if not _sentinel_check(config, run_id, logger):
        insert_crawl_error(run_id, site_id, "SENTINEL_FAILED", "Sentinel check failed")
        return counts

    checkpoint = load_checkpoint(site_id, mode) or {}
    resume_after_key = checkpoint.get("last_project_key")
    resume_pending = bool(resume_after_key)
    api_projects = _fetch_all_projects(logger)
    if not api_projects:
        return counts
    # Apply item_limit before max_pages so the env variable takes precedence
    if item_limit:
        api_projects = api_projects[:item_limit]
        logger.info(f"Rajasthan: CRAWL_ITEM_LIMIT={item_limit} applied — processing {len(api_projects)} projects")
    else:
        max_pages = settings.MAX_PAGES
        if max_pages:
            api_projects = api_projects[: max_pages * 50]
            logger.info(f"Rajasthan: limiting to first {len(api_projects)} projects (max_pages={max_pages})")
    counts["projects_found"] = len(api_projects)
    machine_name, machine_ip = get_machine_context()

    # One shared HTTP session for the entire run — avoids per-request TLS handshakes
    _timeout = httpx.Timeout(connect=10.0, read=15.0, write=10.0, pool=5.0)
    session = httpx.Client(timeout=_timeout, follow_redirects=True)

    # Fetch the global "Updated project details as on <date>" label once per run
    updated_date = _fetch_last_updated_date(logger, client=session)
    if updated_date:
        logger.info(f"Rajasthan: updated_project_details date = {updated_date}")
    else:
        logger.warning("Rajasthan: could not fetch last-updated date; labels will use generic text")

    for i, proj in enumerate(api_projects):
        pid    = str(proj.get("Id", ""))
        enc_id = proj.get("EncryptedProjectId", "")
        reg_no = proj.get("REGISTRATIONNO") or f"RJ-{pid}"
        key = generate_project_key(reg_no)
        if resume_pending:
            if key == resume_after_key:
                resume_pending = False
            counts["projects_skipped"] += 1
            continue

        detail_url = f"{BASE_URL}/ProjectDetail?id={enc_id}" if enc_id else f"{BASE_URL}/ProjectList?status=3"
        logger.set_project(key=key, reg_no=reg_no, url=detail_url, page=i)

        if mode == "daily_light" and get_project_by_key(key):
            logger.info("Skipping — already in DB (daily_light)", step="skip")
            counts["projects_skipped"] += 1
            logger.clear_project()
            continue

        try:
            data: dict = {
                "key": key,
                "state": config["state"], "project_state": config["state"],
                "domain": DOMAIN, "config_id": config["config_id"],
                "url": detail_url,
                "is_live": False, "machine_name": machine_name,
                "crawl_machine_ip": machine_ip,
            }
            for api_f, schema_f in _LIST_API_TO_FIELD.items():
                val = proj.get(api_f)
                if val and str(val).strip():
                    data[schema_f] = str(val).strip()

            cert_url = _build_cert_url(proj.get("UploadedCertificatePath", ""))
            doc_links = []
            if cert_url:
                doc_links.append({"label": "registration_certificate", "url": cert_url})

            # PROD-compatible metadata fields (always present for all projects)
            prod_data_fields: dict = {
                "govt_type":   "state",
                "is_processed": False,
            }

            if enc_id:
                logger.info("Fetching project detail API", step="detail_fetch")
                detail = _fetch_project_detail(enc_id, logger, client=session)
                data.update({k: v for k, v in detail.items() if k != "data" and v is not None and not k.startswith("_")})

                # Build promoter_block array matching PROD format:
                # [promoter_name, phone, email] — sourced from GetProjectById API
                _pb_name  = data.get("promoter_name", "")
                _pb_contact = data.get("promoter_contact_details") or {}
                _pb_phone = _pb_contact.get("phone", "")
                _pb_email = _pb_contact.get("email", "")
                _promoter_block = [x for x in [_pb_name, _pb_phone, _pb_email] if x]
                if _promoter_block:
                    prod_data_fields["promoter_block"] = _promoter_block

                logger.info("Fetching project website detail", step="detail_fetch")
                website_detail = _fetch_project_website_detail(enc_id, logger, client=session)

                # details_page and area unit fields are present for all enc_id projects
                prod_data_fields["details_page"] = (
                    f"https://rera.rajasthan.gov.in/view-project-summary?id={enc_id}&type=U"
                )
                prod_data_fields["land_area_unit"]         = "In sq. meters"
                prod_data_fields["construction_area_unit"] = "in sq. meters"

                if website_detail:
                    doc_links.extend(_extract_project_website_documents(website_detail, updated_date=updated_date))

                    # Fetch ViewProjectWebsite for additional documents and structured data.
                    # type=U (updated) is the primary source for DB fields and documents.
                    project_id = website_detail.get("ProjectId")
                    vp_updated: dict = {}
                    vp_original: dict = {}  # kept for data JSONB only (not fetched — see perf note)
                    if project_id:
                        # Build shared seen-set so ViewProject docs don't duplicate existing ones
                        vp_seen: set[str] = {d["url"] for d in doc_links if d.get("url")}

                        logger.info("Fetching ViewProjectWebsite (updated)", step="detail_fetch")
                        vp_updated = _fetch_view_project_data(project_id, "U", logger, client=session)
                        if vp_updated:
                            vp_docs: list[dict] = []
                            _iter_view_project_documents(vp_updated, docs=vp_docs, seen=vp_seen)
                            doc_links.extend(vp_docs)
                            # Populate structured DB fields (don't overwrite already-set values)
                            vp_fields = _extract_view_project_fields(vp_updated)
                            for field, val in vp_fields.items():
                                if field in ("raw_address", "plot_details"):
                                    continue
                                # Allow the structured dict from ViewProject to overwrite
                                # a simpler string value set earlier by GetProjectById
                                if field == "project_location_raw" and isinstance(val, dict):
                                    data[field] = val
                                elif field not in data or not data[field]:
                                    data[field] = val
                            # raw_address and plot_details go into the data JSONB, not top-level columns
                            if vp_fields.get("raw_address"):
                                prod_data_fields["raw_address"] = vp_fields["raw_address"]
                            if vp_fields.get("plot_details"):
                                prod_data_fields["plot_details"] = vp_fields["plot_details"]
                        # NOTE: type=O (historic snapshot) call intentionally skipped for performance.
                        # It adds ~15s per project and its documents are almost always duplicates of type=U.

                    # ── data JSONB enrichment (temp / type / no_of_plots) ─────────
                    _reg_no = data.get("project_registration_no", "")
                    _aod    = data.get("approved_on_date", "")
                    # Convert dd-mm-yyyy → dd/mm/yyyy for the temp stamp
                    _date_stamp = _aod.replace("-", "/") if _aod else ""
                    if _reg_no:
                        prod_data_fields["temp"] = (
                            f"{_reg_no} ({_date_stamp})" if _date_stamp else _reg_no
                        )
                    # Prefer detail-API casing (e.g. "Group Housing") over list-API ALL-CAPS
                    _raw_type = data.get("project_type", "") or proj.get("ProjectTypeName", "")
                    if _raw_type:
                        prod_data_fields["type"] = str(_raw_type).strip()
                    _n_units = data.get("number_of_residential_units")
                    if _n_units is not None and str(_n_units) not in ("", "None"):
                        prod_data_fields["no_of_plots"] = str(_n_units)

                    data["data"] = merge_data_sections(
                        prod_data_fields,
                        data.get("data"), detail.get("data"),
                        {
                            "source_api": "ProjectDtlsWebsite",
                            "raw_website": website_detail,
                            "raw_view_updated": vp_updated or None,
                            "raw_view_original": vp_original or None,
                        },
                    )
                else:
                    data["data"] = merge_data_sections(prod_data_fields, data.get("data"), detail.get("data"))
            else:
                data["data"] = merge_data_sections(prod_data_fields, data.get("data"))

            logger.info("Normalizing and validating", step="normalize")
            try:
                normalized = normalize_project_payload(data, config, machine_name=machine_name, machine_ip=machine_ip)
                record  = ProjectRecord(**normalized)
                db_dict = record.to_db_dict()
            except (ValidationError, ValueError) as e:
                logger.warning("Validation failed — using raw fallback", step="normalize", error=str(e))
                insert_crawl_error(run_id, site_id, "VALIDATION_FAILED", str(e),
                                   project_key=key, url=data.get("url"), raw_data=data)
                counts["error_count"] += 1
                db_dict = normalize_project_payload(
                    {**data, "data": {"validation_fallback": True, "raw": data.get("data")}},
                    config, machine_name=machine_name, machine_ip=machine_ip,
                )

            logger.info("Upserting to DB", step="db_upsert")
            action = upsert_project(db_dict)
            items_processed += 1
            if action == "new":       counts["projects_new"] += 1
            elif action == "updated": counts["projects_updated"] += 1
            else:                     counts["projects_skipped"] += 1
            logger.info(f"DB result: {action}", step="db_upsert")

            logger.info(f"Downloading {len(doc_links)} documents", step="documents")
            uploaded_documents = []
            doc_name_counts: dict[str, int] = {}
            for doc in doc_links:
                selected_doc = select_document_for_download(config["state"], doc, doc_name_counts, domain=DOMAIN)
                if selected_doc:
                    uploaded_doc = _handle_document(key, selected_doc, run_id, site_id, logger, client=session)
                    if uploaded_doc:
                        uploaded_documents.append(uploaded_doc)
                        counts["documents_uploaded"] += 1
                    else:
                        uploaded_documents.append({"link": doc.get("url"), "type": doc.get("label") or doc.get("type", "document")})
                else:
                    uploaded_documents.append({"link": doc.get("url"), "type": doc.get("label") or doc.get("type", "document")})
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
                             pid=pid, enc_id=enc_id)
            insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                               project_key=key, url=detail_url)
            counts["error_count"] += 1
        finally:
            logger.clear_project()
    session.close()
    reset_checkpoint(site_id, mode)
    logger.info(f"Rajasthan RERA complete: {counts}")
    return counts
