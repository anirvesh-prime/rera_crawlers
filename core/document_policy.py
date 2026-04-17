from __future__ import annotations

import copy
import re
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import unquote, urlparse

from core.project_normalizer import clean_string


STATE_DOC_DICT: dict[str, list[str]] = {
    "karnataka": [
        "Rera Registration Certificate", "Commencement Certificate", "Approved Building", "Plotting Plan",
        "Layout Plan", "Area Development Plan", "Project Specification", "Building Plan",
        "Brochure", "Completion Details", "Layout Plan", "Project Specifications", "BUILDING LICENCE",
        "Section Plan", "Specification", "Approved Section Of Building", "Infrastructure Plan", "Annexure 67",
        "Approval letter", "Sectional Drawing", "Floor Plans", "Engineer", "ARCHITECT", "Form 2", "Form 3",
        "demarcation drawing", "Annexure 80", "COM CER", "A67", "A80", "Occupation Certificate",
        "Occupancy Certificate",
    ],
    "tamil nadu": [
        "Rera Registration Certificate", "Promoter Details", "Project Details", "Approval Details",
        "Carpet Area", "status of the project", "Approved Plan", "Structural Stability Certificate",
        "Structural Design Calculation", "Drawing for Structural Stability", "Design for Structural Stability",
        "Form C", "Work Progress", "Engineer", "ARCHITECT", "Occupation Certificate",
        "Occupancy Certificate",
    ],
    "kerala": [
        "Brochures", "RERA Certificate", "Registration Certificate", "RERA Registration", "Sanctioned Plan",
        "Chart", "project schedule", "List and details of common amenities", "Other Plans",
        "Layout approval plan", "Brochure", "Development certificate", "Prospectus Issued",
        "Detailed Technical specifications", "Location details", "Commencement Certificate", "Site Plan",
        "Site map", "technical specifications", "Engineer", "ARCHITECT", "Form No 2", "Form No 3",
        "Occupation Certificate", "Occupancy Certificate",
    ],
    "andhra pradesh": [
        "Brochure", "Layout Plan", "Latest Project", "Site", "Building Photo", "APRERA CERTIFICATE",
        "Detailed site plan showing the measurements", "Latitude and Longitude",
        "the plan and proceedings issued by the competent Authority for approval of plans", "Approved plan",
        "list of amenities proposed in the site", "technical specifications", "Approved plan",
        "Structural Stability Certificate", "Detailed technical specifications of the construction", "Form 1",
        "Form 2", "Engineer", "ARCHITECT", "Full address of the proposed development project",
        "Topo plan drawn to a scale", "Occupation Certificate", "Occupancy Certificate",
    ],
    "telangana": [
        "Registration Certificate", "Commencement Certificate", "Building Permit", "Proceedings",
        "Building Plan", "Layout Plan", "layout approval", "Engineer", "ARCHITECT",
        "Occupation Certificate", "Occupancy Certificate",
    ],
    "maharashtra": [
        "Rera Registration Certificate", "Layout Approval", "Building Plan Approval", "Commencement Certificate",
        "NA Order for plotted development", "Layout Approval", "Building Plan Approval", "Engineer", "Architect",
        "Occupation Certificate", "Occupancy Certificate",
    ],
    "goa": [
        "Registration Certificate", "Sanctioned Building Plan", "Land documents", "Location",
        "Area Development Plan", "Brochure of Current Project", "Project Related Documents",
        "Engineer", "ARCHITECT", "Occupation Certificate", "Occupancy Certificate",
    ],
    "gujarat": [
        "Rera Registration Certificate", "Approved Layout Plan", "Land documents", "Location",
        "Approved Building", "Plotting Plan", "Brochure", "Project Specification",
        "Area development plan", "Approved Section Plan", "Infrastructure Plan",
        "Project Specifications", "ARCHITECT", "ENGINEER", "Occupation Certificate",
        "Occupancy Certificate",
    ],
    "rajasthan": [
        "RERA Registration Certificate", "Gantt Chart", "Approved Site Plan", "Building Plan",
        "Structural drawings", "Building Sanction Plan", "layout approval", "Engineer", "ARCHITECT",
        "Building Plan Approval", "Location with Demarcation", "Approved Site Plan", "Electrical",
        "Form R1", "Form R2", "Occupation Certificate", "Occupancy Certificate",
    ],
    "punjab": [
        "Rera Registration Certificate", "Approved Layout Plan", "Approved Project Site", "Location Map",
        "Sanctioned Building Plan", "Advertisement and Brochure", "Prospectus", "Engineer", "ARCHITECT",
        "Occupation Certificate", "Occupancy Certificate",
    ],
    "delhi": [
        "Registration Certificate", "Extension certificate", "Sanction Plan", "Project approval",
        "Commencement Certificate", "project lay out plan", "Engineer", "ARCHITECT", "Approved Building",
        "Plotting plan", "Prospectus of the Project", "Brochure of project", "Occupation Certificate",
        "Occupancy Certificate",
    ],
    "haryana": [
        "LAYOUT PLAN", "COMMERCIAL SITES", "APPROVED SITE PLAN", "APPROVAL LETTER", "ZONING PLAN",
        "Rera Registration Certificate", "Construction specifications", "DEMARCATION PLAN",
        "CERTIFICATE OF THE REGISTERED ENGINEER", "CERTIFICATE OF THE REGISTERED ARCHITECT",
        "Occupation Certificate", "Occupancy Certificate",
    ],
    "uttar pradesh": [
        "Rera Registration Certificate", "Commencement Certificate", "Other Plan", "Development Work Plan",
        "ARCHITECT", "ENGINEER", "Project Specifications", "Approved Layout", "Development Works",
        "Floor Plan", "Occupation Certificate", "Occupancy Certificate",
    ],
    "west bengal": [
        "Rera Registration Certificate", "Floor plans", "Gantt Charts and Project Schedule",
        "Plan of Development", "proposed facilities", "Master Plan", "Project Extension Ceritificate",
        "Sanction Plan", "Facilities", "Specification", "Occupation Certificate",
        "Occupancy Certificate",
    ],
    "assam": [
        "RERA REGISTRATION CERTIFICATE", "CARPET AREA", " PARKING AREA DETAILS", "PLAN OF DEVELOPMENT WORKS",
        "APPROVED DRAWING", "Project Specifications", "CERTIFICATE OF THE REGISTERED ARCHITECT",
        "CERTIFICATE OF THE REGISTERED ENGINEER",
        "ARCHITECT, STRUCTURAL ENGINEER AND CONTRACTOR DETAILS", "Occupation Certificate",
        "Occupancy Certificate",
    ],
    "jharkhand": [
        "Brochure", "Gant chart", "RERA Certificate", "Registration Certificate", "RERA Registration",
        "Engineer", "ARCHITECT", "Occupation Certificate", "Occupancy Certificate",
    ],
    "odisha": [
        "RERA Registration Certificate", "Approved layout", "Building plan", "Layout plan",
        "approval letter", "Site Plan", "Engineer", "ARCHITECT", "Occupation Certificate",
        "Occupancy Certificate",
    ],
    "chhattisgarh": [
        "Registration Certificate", "Layout Plan", "Project Specifications", "Engineer Certificate",
        "Architect certificate", "Development team details", "Development work plan",
        "Common area facilities", "Sanctioned Building Plan", "Engg Certificate",
        "Occupation Certificate", "Occupancy Certificate", "Completion Certificate",
    ],
    "bihar": [
        "Registration Certificate", "Layout Plan", "Brochure", "building Plan", "Layout Plan",
        "Development Plan", "Engineer", "ARCHITECT", "Occupation Certificate",
        "Occupancy Certificate",
    ],
    "madhya pradesh": [
        "Project Specifications", "Building Plan", "Layout Plan", "Engineer", "ARCHITECT",
        "Development work plan", "Development team details", "Brochure", "Common Area facilities",
        "Occupation Certificate", "Occupancy Certificate",
    ],
}

RERA_DOMAIN_TO_STATE: dict[str, str] = {
    "rera.kerala.gov.in": "kerala",
    "rera.rajasthan.gov.in": "rajasthan",
    "rera.odisha.gov.in": "odisha",
}


def normalize_doc_name(text: Any) -> str:
    if text in (None, ""):
        return ""
    normalized = str(text).lower()
    normalized = re.sub(r"\buploaded\b", " ", normalized)
    normalized = re.sub(r"\bapproved\b", " ", normalized)
    normalized = " ".join(normalized.split())
    return "".join(char for char in normalized if char.isalnum())


def _url_filename(url: str | None) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    return unquote(PurePosixPath(parsed.path).name)


def _resolve_state(state: str | None, *, domain: str | None = None) -> str:
    if domain:
        mapped = RERA_DOMAIN_TO_STATE.get(domain.lower().strip())
        if mapped:
            return mapped
    return (clean_string(state) or "").lower()


def state_has_document_policy(state: str | None, *, domain: str | None = None) -> bool:
    resolved_state = _resolve_state(state, domain=domain)
    return resolved_state in STATE_DOC_DICT


def decide_download_rera(state: str | None, doc_name: Any, *, domain: str | None = None) -> tuple[bool, str | None]:
    resolved_state = _resolve_state(state, domain=domain)
    allowed_doc_names = STATE_DOC_DICT.get(resolved_state, [])
    normalized_doc_name = normalize_doc_name(doc_name)
    if not normalized_doc_name:
        return False, None

    for name in allowed_doc_names:
        if normalize_doc_name(name) in normalized_doc_name:
            return True, name
    return False, None


def rename_document_category(
    original_name: str | None,
    matched_name: str | None,
    counters: dict[str, int],
) -> str:
    original = clean_string(original_name) or ""
    renamed = clean_string(matched_name) or clean_string(original_name) or "document"

    if "architect" in renamed.lower() and "certificate" not in renamed.lower():
        renamed += " Certificate"
    if "engineer" in renamed.lower() and "certificate" not in renamed.lower():
        renamed += " Certificate"

    if renamed in original:
        parts = original.split()
        if parts:
            try:
                int(parts[-1])
                return original
            except ValueError:
                pass

    counters[renamed] = counters.get(renamed, 0) + 1
    return f"{renamed} {counters[renamed]}"


def select_document_for_download(
    state: str | None,
    doc: dict[str, Any],
    counters: dict[str, int],
    *,
    domain: str | None = None,
) -> dict[str, Any] | None:
    label = clean_string(doc.get("type") or doc.get("label"))
    allowed, matched_name = decide_download_rera(state, label, domain=domain)

    if not allowed:
        file_name = _url_filename(clean_string(doc.get("url") or doc.get("link") or doc.get("source_url")))
        allowed, matched_name = decide_download_rera(state, file_name, domain=domain)

    if not allowed or not matched_name:
        return None

    selected = copy.deepcopy(doc)
    renamed_label = rename_document_category(label or _url_filename(selected.get("url")), matched_name, counters)
    selected["label"] = renamed_label
    selected["type"] = renamed_label
    selected.setdefault("source_label", label or matched_name)
    selected.setdefault("matched_category", matched_name)
    return selected
