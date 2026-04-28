"""
Gujarat RERA Crawler — gujrera.gujarat.gov.in
Type: JSON REST API (Angular SPA)

Strategy:
- Enumerate projects by iterating sequential integer IDs via:
    GET /project_reg/public/alldatabyprojectid/{id}
  Skip IDs where data.projRegNo is null (gap in DB sequence).
- Fetch full details via:
    GET /project_reg/public/getproject-details/{id}
- Fetch document UIDs via:
    GET /project_reg/public/getproject-doc/{id}
- Download documents via:
    GET /vdms/download/{uid}

The listing search endpoint (/project_reg/public/global-search) has a persistent
SQL grammar error on the server, so we avoid it entirely.
"""
from __future__ import annotations

import json
import re
import subprocess
from datetime import datetime, timezone, timedelta

import httpx
from pydantic import ValidationError

from core.checkpoint import load_checkpoint, save_checkpoint, reset_checkpoint
from core.crawler_base import generate_project_key, get_legacy_ssl_context, random_delay, safe_get
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

BASE_URL  = "https://gujrera.gujarat.gov.in"
API_BASE  = f"{BASE_URL}/project_reg/public"
VDMS_BASE = f"{BASE_URL}/vdms/download"
DOMAIN    = "gujrera.gujarat.gov.in"
STATE     = "Gujarat"

# Max project ID to probe. The public registry is now beyond 30k IDs, so the
# old 25k cap truncates current projects and breaks sample-aligned dry runs.
_MAX_PROJECT_ID = 50_000

# IST offset — dates in the API are returned in IST (+05:30)
_IST = timezone(timedelta(hours=5, minutes=30))

# ── Document UID key → human-readable label ─────────────────────────────────
_FINDOC_UID_LABELS: dict[str, str] = {
    "auditedBalSheetDoc1UId":          "Audited Balance Sheet Year 1",
    "auditedBalSheetDoc2_UId":         "Audited Balance Sheet Year 2",
    "auditedBalSheetDoc3UId":          "Audited Balance Sheet Year 3",
    "auditedProfitLossSheetDoc1UId":   "Audited P&L Sheet Year 1",
    "auditedProfitLossSheetDoc2UId":   "Audited P&L Sheet Year 2",
    "auditedProfitLossSheetDoc3UId":   "Audited P&L Sheet Year 3",
    "cashFlowStmtFileDoc1UId":         "Cash Flow Statement Year 1",
    "cashFlowStmtFileDoc2UId":         "Cash Flow Statement Year 2",
    "cashFlowStmtFileDoc3UId":         "Cash Flow Statement Year 3",
    "cashFlowStmtYear1UId":            "Cash Flow Statement (Alt) Year 1",
    "cashFlowStmtYear2UId":            "Cash Flow Statement (Alt) Year 2",
    "cashFlowStmtYear3UId":            "Cash Flow Statement (Alt) Year 3",
    "auditedReportDoc1UId":            "Auditors Report Year 1",
    "auditedReportDoc2UId":            "Auditors Report Year 2",
    "auditedReportDoc3UId":            "Auditors Report Year 3",
    "auditedReportYear1UId":           "Audited Report Year 1",
    "auditedReportYear2UId":           "Audited Report Year 2",
    "auditedReportYear3UId":           "Audited Report Year 3",
    "auditedBalSheetYear1UId":         "Balance Sheet (Alt) Year 1",
    "auditedBalSheetYear2UId":         "Balance Sheet (Alt) Year 2",
    "auditedBalSheetYear3UId":         "Balance Sheet (Alt) Year 3",
    "auditedProfitLossSheetYear1UId":  "P&L Sheet (Alt) Year 1",
    "auditedProfitLossSheetYear2UId":  "P&L Sheet (Alt) Year 2",
    "auditedProfitLossSheetYear3UId":  "P&L Sheet (Alt) Year 3",
    "directorReportDoc1UId":           "Director Report Year 1",
    "directorReportDoc2UId":           "Director Report Year 2",
    "directorReportDoc3UId":           "Director Report Year 3",
    "auditorsDoc1UId":                 "Auditors Document Year 1",
    "auditorsDoc2UId":                 "Auditors Document Year 2",
    "auditorsDoc3UId":                 "Auditors Document Year 3",
    "anyOtherDocumentUId":             "Other Financial Document",
    "statutoryDocumentUId":            "Statutory Document",
}

_PROJDOC_UID_LABELS: dict[str, str] = {
    "performaForSaleOfAgreementUId":    "Proforma for Sale Agreement",
    "auditorsDoc1UId":                  "Auditors Document Year 1",
    "auditorsDoc2UId":                  "Auditors Document Year 2",
    "auditorsDoc3UId":                  "Auditors Document Year 3",
    "incomeTaxReturn1UId":              "Income Tax Return Year 1",
    "incomeTaxReturn2UId":              "Income Tax Return Year 2",
    "incomeTaxReturn3UId":              "Income Tax Return Year 3",
    "projectSpecificDocUId":            "Project Specific Document",
    "drainageAffidavitUid":             "Drainage Affidavit",
    "directorReportDoc1UId":            "Director Report Year 1",
    "directorReportDoc2UId":            "Director Report Year 2",
    "directorReportDoc3UId":            "Director Report Year 3",
    "directorReportYear1UId":           "Director Report (Alt) Year 1",
    "directorReportYear2UId":           "Director Report (Alt) Year 2",
    "directorReportYear3UId":           "Director Report (Alt) Year 3",
    "panCardDocUId":                    "PAN Card",
    "photoGraphDocUId":                 "Photograph",
    "commencementCertDocUId":           "Commencement Certificate",
    "approveSacPlanDocUId":             "Approved SAC Plan",
    "approveLayoutPlanDocUId":          "Approved Layout Plan",
    "agreementFileDocUId":              "Agreement Document",
    "landLocationFileDocUId":           "Land Location Document",
    "encumbranceCertificateDocUId":     "Encumbrance Certificate",
    "areaDevelopmentDocUId":            "Area Development Document",
    "performaOfAllotmentLetterDocUId":  "Proforma of Allotment Letter",
    "brochureOfCurrentProjectDocUId":   "Project Brochure",
    "projectRelatedDocUId":             "Project Related Document",
    "declarationFormbDocUId":           "Declaration Form B",
    "declarationFormB1UId":             "Declaration Form B1",
    "declarationFormB2UId":             "Declaration Form B2",
    "approvedBuildingPlanPlottingPlanUId": "Approved Building/Plotting Plan",
    "allNOCsfromAuthoritiesUId":        "All NOCs from Authorities",
    "titleClearanceCertificateUId":     "Title Clearance Certificate",
    "titleReportUId":                   "Title Report",
    "developmentAgreementUId":          "Development Agreement",
    "propertyCardUId":                  "Property Card",
    "propertyCard2UId":                 "Property Card 2",
    "propertyCard3UId":                 "Property Card 3",
    "buCertificateUId":                 "BU Certificate",
    "ganttchartForm1AUId":              "Gantt Chart Form 1A",
    "alloteeConsentDocUId":             "Allottee Consent Document",
    "statutoryDocumentUId":             "Statutory Document",
    "anyOtherDocumentUId":              "Other Document",
    "performaforSaledeedUId":           "Proforma for Sale Deed",
    "projectphotoUId":                  "Project Photo",
    "sanctionedLayoutPlanDocUId":       "Sanctioned Layout Plan",
}

# Professional list key in getproject-details.data → role label
_PROF_SECTION_ROLES: dict[str, str] = {
    "englist":   "Structural Engineer",
    "dev":       "Developer",
    "calist":    "Chartered Accountant",
    "agentlist": "Agent",
    "acrchlist": "Architect",
    "contr":     "Contractor",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _normalize_date(val) -> str | None:
    """Normalize dates from Gujarat RERA API to canonical ISO string.

    Handles:
      - ISO with T separator: "2018-01-01T00:00:00.000+0530"
      - dd-mm-yyyy: "20-09-2017"
      - yyyy-MM-dd
      - Already normalized strings
    """
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
    m = re.match(r"^(\d{4}-\d{2}-\d{2})$", v)
    if m:
        return f"{v} 00:00:00+00:00"
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", v):
        return v if "+" in v else v + "+00:00"
    return v


def _s(d: dict, *keys: str) -> str:
    """Return the first non-empty string value found among keys in dict d."""
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        sv = str(v).strip()
        if sv and sv not in ("null", "None", "0"):
            return sv
    return ""


def _collect_doc_uids(doc_section: dict, uid_label_map: dict[str, str]) -> list[dict]:
    """Extract non-null UID entries from a flat doc-section dict."""
    docs: list[dict] = []
    seen_uids: set[str] = set()
    for uid_key, label in uid_label_map.items():
        uid = doc_section.get(uid_key)
        if not uid or uid in seen_uids:
            continue
        seen_uids.add(uid)
        docs.append({"label": label, "url": f"{VDMS_BASE}/{uid}", "uid": uid})
    return docs


def _first_non_empty_dict(detail_data: dict, *name_hints: str) -> dict | None:
    hints = tuple(h.lower() for h in name_hints)
    for key, value in detail_data.items():
        if not isinstance(value, dict):
            continue
        key_l = str(key).lower()
        if any(hint in key_l for hint in hints):
            return value
    return None


def _first_non_empty_list(detail_data: dict, *name_hints: str) -> list[dict]:
    hints = tuple(h.lower() for h in name_hints)
    for key, value in detail_data.items():
        if not isinstance(value, list):
            continue
        key_l = str(key).lower()
        if any(hint in key_l for hint in hints):
            dict_items = [item for item in value if isinstance(item, dict)]
            if dict_items:
                return dict_items
    return []


def _fetch_basic(proj_id: int, client: httpx.Client) -> dict | None:
    """Call alldatabyprojectid — returns basic project metadata or None."""
    payload = _fetch_api_json(f"{API_BASE}/alldatabyprojectid/{proj_id}", client=client)
    if not payload or payload.get("status") != 200:
        return None
    data = payload.get("data") or {}
    if not data.get("projRegNo"):
        return None
    return data


def _fetch_details(proj_id: int, client: httpx.Client, logger: CrawlerLogger) -> dict:
    """Call getproject-details — returns raw data dict."""
    payload = _fetch_api_json(f"{API_BASE}/getproject-details/{proj_id}", client=client)
    if not payload:
        return {}
    try:
        return payload.get("data") or {}
    except Exception as e:
        logger.warning(f"Failed to parse getproject-details/{proj_id}: {e}")
        return {}


def _fetch_docs(proj_id: int, client: httpx.Client) -> dict:
    """Call getproject-doc — returns {findoc: dict, projectdoc: dict}."""
    payload = _fetch_api_json(f"{API_BASE}/getproject-doc/{proj_id}", client=client)
    if not payload:
        return {}
    try:
        return payload.get("data") or {}
    except Exception:
        return {}


def _fetch_qpr_details(proj_id: int, client: httpx.Client) -> dict:
    """Call the QPR project-details endpoint used by the public preview page."""
    payload = _fetch_api_json(
        f"{API_BASE}/project-app/get-project-details-for-qpr/{proj_id}",
        client=client,
    )
    if not payload:
        return {}
    try:
        return payload.get("data") or {}
    except Exception:
        return {}


def _fetch_promoter_profile(promoter_id: int, client: httpx.Client) -> dict:
    """Fetch promoter profile details used by the public preview page."""
    payload = _fetch_api_json(f"{BASE_URL}/user_reg/promoter/promoter{promoter_id}", client=client)
    return payload or {}


def _curl_json(url: str) -> dict | None:
    try:
        proc = subprocess.run(
            ["curl", "-L", "--silent", "--show-error", "--max-time", "20", url],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None
    try:
        return json.loads(proc.stdout)
    except Exception:
        return None


def _curl_bytes(url: str) -> bytes | None:
    try:
        proc = subprocess.run(
            ["curl", "-L", "--silent", "--show-error", "--max-time", "30", url],
            check=True,
            capture_output=True,
        )
    except Exception:
        return None
    return proc.stdout or None


def _fetch_api_json(url: str, *, client: httpx.Client) -> dict | None:
    resp = safe_get(url, retries=2, timeout=12, client=client)
    if resp:
        try:
            return resp.json()
        except Exception:
            return None
    return _curl_json(url)


def _extract_fields(  # noqa: C901
    basic: dict,
    detail_data: dict,
    qpr_data: dict | None = None,
    promoter_profile: dict | None = None,
) -> dict:
    """Map Gujarat API response fields to the projects table schema."""
    pd = detail_data.get("projectDetail") or {}
    qpr = qpr_data or {}
    promoter = promoter_profile or {}
    out: dict = {}

    out["project_registration_no"] = basic.get("projRegNo", "")
    out["project_name"]   = basic.get("projectName") or pd.get("projectName")
    out["project_type"]   = basic.get("projectType") or pd.get("projectType")
    out["promoter_name"]  = basic.get("promoterName")
    out["acknowledgement_no"] = basic.get("projectAckNo")

    out["status_of_the_project"] = pd.get("projectStatus")
    out["project_city"]  = pd.get("distName")
    out["project_state"] = pd.get("stateName") or STATE
    out["project_pin_code"] = _s(pd, "pinCode") or None

    out["approved_on_date"]         = _normalize_date(basic.get("approvedDate"))
    out["submitted_date"]           = _normalize_date(basic.get("appSubmissionDate"))
    out["actual_commencement_date"] = _normalize_date(pd.get("startDate"))
    out["actual_finish_date"]       = _normalize_date(pd.get("completionDate"))

    land_value = pd.get("totAreaOfLand")
    if land_value in (None, "", "null", "None"):
        land_value = pd.get("totLandAreaForProjectUnderReg")
    if land_value in (None, "", "null", "None"):
        land_value = pd.get("totAreaOfLandLayout")
    try:
        if land_value is not None:
            out["land_area"] = float(land_value)
    except (ValueError, TypeError):
        pass

    construction_value = pd.get("totCarpetAreaForProjectUnderReg")
    if construction_value in (None, "", "null", "None"):
        construction_value = pd.get("totCarpetArea")
    if construction_value in (None, "", "null", "None"):
        construction_value = pd.get("totCoverdArea")
    try:
        if construction_value is not None:
            out["construction_area"] = float(construction_value)
    except (ValueError, TypeError):
        pass

    desc = _s(pd, "projectDesc")
    if desc:
        out["project_description"] = desc

    cost: dict = {}
    for api_f, label in (
        ("totalProjectCost", "total_project_cost"),
        ("estimatedCost",    "estimated_project_cost"),
        ("costOfLand",       "cost_of_land"),
    ):
        v = pd.get(api_f)
        if v is not None and str(v).strip() not in ("", "0", "null", "None"):
            cost[label] = str(v)
    qpr_total_cost = qpr.get("totalProjectCost")
    if qpr_total_cost not in (None, "", "0", "null", "None"):
        cost.setdefault("total_project_cost", str(qpr_total_cost))
        cost.setdefault("estimated_project_cost", str(qpr_total_cost))
    if cost:
        out["project_cost_detail"] = cost

    # Location
    moje  = _s(pd, "moje")
    sub   = _s(pd, "subDistName")
    dist  = _s(pd, "distName")
    pin   = _s(pd, "pinCode")
    addr1 = _s(pd, "projectAddress")
    addr2 = _s(pd, "projectAddress2")
    plot  = _s(pd, "plotNo", "finalPlotNo")
    tp    = _s(pd, "tPNo")
    loc: dict = {}
    if plot:
        loc["house_no_building_name"] = plot
    if tp:
        loc["tp_no"] = tp
    if moje:
        loc["village"] = moje
    if sub:
        loc["taluk"] = sub
    if dist:
        loc["district"] = dist
    if pin:
        loc["pin_code"] = pin
    loc["state"] = STATE
    raw_parts = [p for p in [addr1, addr2, moje, sub, dist, STATE, pin] if p]
    if raw_parts:
        loc["raw_address"] = ", ".join(raw_parts)
    if loc:
        out["project_location_raw"] = loc

    # Promoter contact
    promo_contact: dict = {}
    email = (basic.get("promoterEmailId") or "").strip()
    phone = (basic.get("promoterMobileNo") or "").strip()
    if email:
        promo_contact["email"] = email
    if phone:
        promo_contact["phone"] = phone
    if promo_contact:
        out["promoter_contact_details"] = promo_contact

    promo_type = (basic.get("promoterType") or "").strip()
    promo_addr_source = _first_non_empty_dict(detail_data, "promoteraddress", "promoter_address")
    promo_addr_parts = [
        _s(promoter, "address"),
        _s(promoter, "address2"),
        _s(basic, "promoterAddress", "promoterAdd", "address", "address1"),
        _s(pd, "promoterAddress", "promoterAdd", "address", "address1"),
        _s(pd, "address2"),
        _s(pd, "distName"),
        _s(pd, "stateName"),
        _s(pd, "pinCode"),
    ]
    promo_addr: dict = {}
    if promo_addr_source:
        raw_address = ", ".join(
            part for part in (
                _s(promo_addr_source, "address", "address1", "addr1", "line1"),
                _s(promo_addr_source, "address2", "addr2", "line2"),
                _s(promo_addr_source, "city", "distName", "district"),
                _s(promo_addr_source, "state", "stateName"),
                _s(promo_addr_source, "pinCode", "pincode"),
            ) if part
        )
        if raw_address:
            promo_addr["raw_address"] = raw_address
        for src, tgt in (
            ("city", "city"),
            ("distName", "district"),
            ("district", "district"),
            ("state", "state"),
            ("stateName", "state"),
            ("pinCode", "pin_code"),
            ("pincode", "pin_code"),
        ):
            value = _s(promo_addr_source, src)
            if value:
                promo_addr[tgt] = value
    elif any(promo_addr_parts):
        promo_addr["raw_address"] = " ".join(part for part in promo_addr_parts if part)
        district = _s(promoter, "districtName") or _s(pd, "distName")
        state = _s(promoter, "stateName") or _s(pd, "stateName")
        pin = _s(promoter, "pinCode") or _s(pd, "pinCode")
        if district:
            promo_addr["district"] = district
        if state:
            promo_addr["state"] = state
        if pin:
            promo_addr["pin_code"] = pin
    if promo_addr:
        out["promoter_address_raw"] = promo_addr

    # Professionals
    profs: list[dict] = []
    seen_names: set[tuple] = set()
    for section_key, role_label in _PROF_SECTION_ROLES.items():
        section = detail_data.get(section_key)
        if not isinstance(section, list):
            continue
        for item in section:
            if not isinstance(item, dict):
                continue
            name = _s(item, "name", "fullName", "agentName", "contrName")
            if not name:
                continue
            dedup = (role_label, name.lower())
            if dedup in seen_names:
                continue
            seen_names.add(dedup)
            entry: dict = {"role": role_label, "name": name}
            email_p = _s(item, "email", "emailId")
            phone_p = _s(item, "mobileNo", "contactNo", "mobile")
            reg_p   = _s(item, "licenceNo", "registrationNo", "reraRegNo")
            if email_p: entry["email"]           = email_p
            if phone_p: entry["phone"]           = phone_p
            if reg_p:   entry["registration_no"] = reg_p
            profs.append(entry)
    if profs:
        out["professional_information"] = profs

    development_rows = [item for item in detail_data.get("dev") or [] if isinstance(item, dict)]
    if development_rows:
        building_details: list[dict] = []
        facilities: list[dict] = []
        status_updates: list[dict] = []

        for dev_row in development_rows:
            for unit in dev_row.get("internalDev") or []:
                if not isinstance(unit, dict):
                    continue
                flat_type = _s(unit, "typeOfInventory")
                if not flat_type:
                    continue
                entry: dict[str, str] = {"flat_type": flat_type}
                if unit.get("noOfInventory") not in (None, ""):
                    entry["no_of_units"] = str(unit["noOfInventory"])
                if unit.get("carpetArea") not in (None, ""):
                    entry["carpet_area"] = str(unit["carpetArea"])
                if unit.get("areaOfExclusive") not in (None, ""):
                    entry["open_area"] = str(unit["areaOfExclusive"])
                if unit.get("areaOfExclusiveOpenTerrace") not in (None, ""):
                    entry["balcony_area"] = str(unit["areaOfExclusiveOpenTerrace"])
                building_details.append(entry)

            external_dev = dev_row.get("externalDev") or {}
            if isinstance(external_dev, dict):
                for field_name, label in (
                    ("roadSysetmDevBy", "Road System"),
                    ("waterSupplyBy", "Water Supply"),
                    ("sewegeAndDrainageSystemDevBy", "Sewage and Drainage System"),
                    ("electricityAndTrasfomerSupply", "Electricity Supply and Transformer"),
                    ("solidWasteSupplyBy", "Solid Waste Management"),
                ):
                    value = _s(external_dev, field_name)
                    if value:
                        facilities.append({"facility": label, "status": value})

            status_entry: dict[str, object] = {"updated": True}
            if building_details:
                status_entry["building_details"] = building_details
            if facilities:
                status_entry["amenity_detail"] = facilities
            if status_entry.keys() != {"updated"}:
                status_updates.append(status_entry)

        if building_details:
            out["building_details"] = building_details
        if facilities:
            out["provided_faciltiy"] = facilities
        if status_updates:
            out["status_update"] = status_updates

    residential_units = _s(
        pd,
        "totalResidentialUnits",
        "noOfResidentialUnits",
        "totalResiUnits",
        "residentialUnits",
        "totResidentialUnit",
    )
    if not residential_units:
        qpr_internal = qpr.get("internalDevDetails") or []
        for item in qpr_internal:
            if not isinstance(item, dict):
                continue
            value = item.get("noOfInventory")
            if value not in (None, "", "0"):
                residential_units = str(value)
                break
    if residential_units:
        try:
            out["number_of_residential_units"] = int(residential_units.replace(",", ""))
        except ValueError:
            pass

    promoters_details: dict = {}
    promoter_reg = _s(
        promoter,
        "companyRegistrationNumber",
        "promoterRegistrationNo",
        "firmRegNo",
        "registrationNo",
    ) or _s(basic, "promoterRegNo", "promoterRegistrationNo", "firmRegNo")
    promoter_pan = _s(promoter, "panNo", "pan") or _s(basic, "promoterPan", "panNo", "pan")
    if out.get("promoter_name"):
        promoters_details["name"] = out["promoter_name"]
    if promo_type:
        promoters_details["type_of_firm"] = promo_type
    if promoter_reg:
        promoters_details["reg_no"] = promoter_reg
    if promoter_pan:
        promoters_details["pan"] = promoter_pan
    if promoters_details:
        out["promoters_details"] = promoters_details

    facility_rows = _first_non_empty_list(detail_data, "amenit", "facilit")
    if facility_rows:
        facilities: list[dict] = []
        for item in facility_rows:
            facility = _s(item, "facility", "amenity", "name", "facilityName", "amenityName")
            status = _s(item, "status", "availability", "isAvailable")
            if facility:
                entry = {"facility": facility}
                if status:
                    entry["status"] = status
                facilities.append(entry)
        if facilities:
            out["provided_faciltiy"] = facilities

    signatory_rows = _first_non_empty_list(detail_data, "signatory", "authorised", "authorized")
    if not signatory_rows and isinstance(promoter.get("authorizedSignatoryList"), list):
        signatory_rows = promoter["authorizedSignatoryList"]
    if signatory_rows:
        signatories: list[dict] = []
        for item in signatory_rows:
            name = _s(item, "name", "signatoryName", "authName")
            if not name:
                name = " ".join(
                    part
                    for part in (
                        _s(item, "authsignFirstName"),
                        _s(item, "authsignMiddleName"),
                        _s(item, "authsignLastName"),
                    )
                    if part
                ).strip()
            if not name:
                continue
            entry = {"name": name}
            for src, tgt in (
                ("email", "email"),
                ("mobile", "phone"),
                ("mobileNo", "phone"),
                ("phone", "phone"),
                ("photo", "photo"),
                ("authsignEmailId", "email"),
                ("authsignMobileNumber", "phone"),
            ):
                value = _s(item, src)
                if value:
                    entry[tgt] = value
            auth_photo_uid = _s(item, "authsignPhotUId")
            if auth_photo_uid and "photo" not in entry:
                entry["photo"] = f"{VDMS_BASE}/{auth_photo_uid}"
            signatories.append(entry)
        if signatories:
            out["authorised_signatory_details"] = signatories

    co_promoter_rows = _first_non_empty_list(detail_data, "copromoter", "co_promoter", "landowner", "land_owner")
    if not co_promoter_rows and isinstance(promoter.get("assosiateList"), list):
        co_promoter_rows = promoter["assosiateList"]
    if co_promoter_rows:
        co_promoters: list[dict] = []
        for item in co_promoter_rows:
            name = _s(item, "name", "coPromoterName", "landOwnerName")
            if not name:
                name = " ".join(
                    part
                    for part in (
                        _s(item, "associateFirstName"),
                        _s(item, "associateMiddleName"),
                        _s(item, "associateLastName", "lastName"),
                    )
                    if part
                ).strip()
            if not name:
                continue
            entry = {"name": name}
            for src, tgt in (
                ("email", "email"),
                ("mobile", "phone"),
                ("mobileNo", "phone"),
                ("phone", "phone"),
                ("assocaiteEmailId", "email"),
                ("assocaiteMobileNumber", "phone"),
            ):
                value = _s(item, src)
                if value:
                    entry[tgt] = value
            co_promoters.append(entry)
        if co_promoters:
            out["co_promoter_details"] = co_promoters

    out["land_area_details"] = {
        "land_area":              str(land_value if land_value is not None else ""),
        "land_area_unit":         "Sq. Mtrs.",
        "construction_area":      construction_value if construction_value is not None else "",
        "construction_area_unit": "Sq. Mtrs.",
        "open_parking_area":      str(pd.get("openParkingArea", "") or ""),
    }

    return out


def _collect_all_docs(basic: dict, doc_data: dict) -> list[dict]:
    """Build the full list of document entries for a project."""
    docs: list[dict] = []
    seen_uids: set[str] = set()

    for uid_key, label in (
        ("certificateUid",       "RERA Registration Certificate"),
        ("altcertificateUid",    "Alternate RERA Certificate"),
        ("extcertificateUid",    "Extension Certificate"),
        ("altsec15certificateUid", "Alternate Sec-15 Certificate"),
    ):
        uid = (basic.get(uid_key) or "").strip()
        if uid and uid not in seen_uids:
            seen_uids.add(uid)
            cat = "certificate" if uid_key == "certificateUid" else "other"
            docs.append({"label": label, "url": f"{VDMS_BASE}/{uid}", "uid": uid, "category": cat})

    for doc in _collect_doc_uids(doc_data.get("findoc") or {}, _FINDOC_UID_LABELS):
        if doc["uid"] not in seen_uids:
            seen_uids.add(doc["uid"])
            docs.append(doc)

    for doc in _collect_doc_uids(doc_data.get("projectdoc") or {}, _PROJDOC_UID_LABELS):
        if doc["uid"] not in seen_uids:
            seen_uids.add(doc["uid"])
            docs.append(doc)

    return docs


def _handle_document(
    project_key: str, doc: dict, run_id: int,
    site_id: str, logger: CrawlerLogger,
    client: httpx.Client,
) -> dict | None:
    url   = doc.get("url")
    label = doc.get("label", "document")
    if not url:
        return None
    filename = build_document_filename(doc)
    try:
        resp = safe_get(url, retries=1, timeout=20, client=client)
        content = resp.content if resp else None
        if not content or len(content) < 100:
            content = _curl_bytes(url)
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
        logger.info("Document handled", label=label, s3_key=s3_key)
        return document_result_entry(doc, s3_url, filename)
    except Exception as e:
        logger.error(f"Document failed: {e}", url=url)
        insert_crawl_error(run_id, site_id, "S3_UPLOAD_FAILED", str(e),
                           project_key=project_key, url=url)
        return None


def _sentinel_check(config: dict, run_id: int, logger: CrawlerLogger,
                    client: httpx.Client) -> bool:
    sentinel_reg = config.get("sentinel_registration_no", "")
    if not sentinel_reg:
        logger.warning("No sentinel configured — skipping")
        return True
    key = generate_project_key(sentinel_reg)
    if not get_project_by_key(key):
        logger.warning("Sentinel not in DB yet — skipping check")
        return True
    basic = _fetch_basic(500, client)
    if not basic:
        logger.error("Sentinel: could not fetch project ID 500")
        return False
    live_reg = basic.get("projRegNo", "")
    if live_reg != sentinel_reg:
        logger.error("Sentinel reg no mismatch", expected=sentinel_reg, got=live_reg)
        return False
    logger.info("Sentinel check passed", reg=sentinel_reg)
    return True


def run(config: dict, run_id: int, mode: str) -> dict:  # noqa: C901
    logger  = CrawlerLogger(config["id"], run_id)
    site_id = config["id"]
    counts  = dict(projects_found=0, projects_new=0, projects_updated=0,
                   projects_skipped=0, documents_uploaded=0, error_count=0)

    item_limit   = settings.CRAWL_ITEM_LIMIT or 0
    max_id       = _MAX_PROJECT_ID
    if settings.MAX_PAGES:
        max_id = min(max_id, settings.MAX_PAGES * 50)
    machine_name, machine_ip = get_machine_context()

    _timeout = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=5.0)
    session  = httpx.Client(
        timeout=_timeout,
        follow_redirects=True,
        verify=get_legacy_ssl_context(),
    )

    if not _sentinel_check(config, run_id, logger, session):
        insert_crawl_error(run_id, site_id, "SENTINEL_FAILED", "Sentinel check failed")
        session.close()
        return counts

    checkpoint = load_checkpoint(site_id, mode) or {}
    resume_from_id = int(checkpoint.get("last_page", 0))
    if item_limit and resume_from_id == 0:
        # Dry-run/debug mode: skip the long empty prefix and start near the known live range.
        resume_from_id = 499
    items_processed = 0

    for proj_id in range(resume_from_id + 1, max_id + 1):
        if item_limit and items_processed >= item_limit:
            logger.info(f"Item limit {item_limit} reached — stopping")
            break

        basic = _fetch_basic(proj_id, session)
        if basic is None:
            continue

        counts["projects_found"] += 1
        reg_no     = basic["projRegNo"]
        key        = generate_project_key(reg_no)
        detail_url = f"{BASE_URL}/#/home-p/registered-project-details/{proj_id}"
        logger.set_project(key=key, reg_no=reg_no, url=detail_url, page=proj_id)

        if mode == "daily_light" and get_project_by_key(key):
            logger.info("Skipping — already in DB (daily_light)", step="skip")
            counts["projects_skipped"] += 1
            logger.clear_project()
            random_delay(*config.get("rate_limit_delay", (1, 2)))
            continue

        try:
            detail_data = _fetch_details(proj_id, session, logger)
            qpr_data = _fetch_qpr_details(proj_id, session)
            promoter_profile = {}
            promoter_id = basic.get("promoterId")
            if promoter_id not in (None, "", "0"):
                try:
                    promoter_profile = _fetch_promoter_profile(int(promoter_id), session)
                except (TypeError, ValueError):
                    promoter_profile = {}
            doc_data    = _fetch_docs(proj_id, session) if mode != "daily_light" else {}

            data = _extract_fields(basic, detail_data, qpr_data, promoter_profile)
            data.update({
                "key": key, "state": config["state"],
                "project_state": STATE, "domain": DOMAIN,
                "config_id": config["config_id"], "url": detail_url,
                "is_live": True, "machine_name": machine_name,
                "crawl_machine_ip": machine_ip,
            })
            data["data"] = merge_data_sections(
                {"govt_type": "state", "is_processed": False, "proj_reg_id": proj_id},
                {"source_api": "alldatabyprojectid", "raw_basic": basic},
                {"source_api": "getproject-details",  "raw_detail": detail_data},
                {"source_api": "get-project-details-for-qpr", "raw_qpr": qpr_data},
                {"source_api": "promoter-profile", "raw_promoter": promoter_profile},
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
            items_processed += 1
            if action == "new":       counts["projects_new"] += 1
            elif action == "updated": counts["projects_updated"] += 1
            else:                     counts["projects_skipped"] += 1
            logger.info(f"DB: {action}", step="db_upsert")

            if mode != "daily_light":
                doc_links = _collect_all_docs(basic, doc_data)
                logger.info(f"Processing {len(doc_links)} documents", step="documents")
                uploaded_docs: list[dict] = []
                doc_name_counts: dict[str, int] = {}
                for doc in doc_links:
                    selected = select_document_for_download(
                        config["state"], doc, doc_name_counts, domain=DOMAIN)
                    if selected:
                        result = _handle_document(key, selected, run_id, site_id, logger, session)
                        uploaded_docs.append(result or {"link": doc.get("url"), "type": doc.get("label", "document")})
                        if result:
                            counts["documents_uploaded"] += 1
                    else:
                        uploaded_docs.append({"link": doc.get("url"), "type": doc.get("label", "document")})
                if uploaded_docs:
                    upsert_project({
                        "key": db_dict["key"], "url": db_dict["url"],
                        "state": db_dict["state"], "domain": db_dict["domain"],
                        "project_registration_no": db_dict["project_registration_no"],
                        "uploaded_documents": uploaded_docs,
                        "document_urls": build_document_urls(uploaded_docs),
                    })

            if proj_id % 100 == 0:
                save_checkpoint(site_id, mode, proj_id, key, run_id)
            random_delay(*config.get("rate_limit_delay", (1, 2)))

        except Exception as exc:
            logger.exception("Project processing failed", exc, step="project_loop", proj_id=proj_id)
            insert_crawl_error(run_id, site_id, "PROJECT_ERROR", str(exc),
                               project_key=key, url=detail_url)
            counts["error_count"] += 1
        finally:
            logger.clear_project()

    session.close()
    reset_checkpoint(site_id, mode)
    logger.info(f"Gujarat RERA complete: {counts}")
    return counts
