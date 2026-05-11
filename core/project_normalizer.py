from __future__ import annotations

import copy
import hashlib
import re
import socket
from datetime import datetime, timezone

UTC = timezone.utc
from typing import Any
from urllib.parse import parse_qs, urlparse

from core.crawler_base import generate_project_key
from core.project_schema import (
    ARRAY_FIELDS,
    BOOLEAN_FIELDS,
    DATETIME_FIELDS,
    FLOAT_FIELDS,
    INTEGER_FIELDS,
    JSONB_FIELDS,
    PROJECT_COLUMN_SET,
    REQUIRED_PROJECT_FIELDS,
    TEXT_FIELDS,
)

_JSON_FIELD_ALLOWED_KEYS: dict[str, set[str]] = {
    "project_location_raw": {
        "city", "district", "exact_location", "has_same_data", "house_no_building_name",
        "latitude", "locality", "longitude", "pin_code", "plot_no", "post_office",
        "processed_latitude", "processed_longitude", "raw_address", "state",
        "survey_resurvey_number", "taluk", "village",
    },
    "promoter_address_raw": {
        "building_name", "city", "correspondence_address", "district",
        "house_no_building_name", "landmark", "locality", "pin_code", "plot_no", "raw_address",
        "registered_address", "state", "street_name", "taluk", "village",
    },
    "promoter_contact_details": {"email", "mobile no", "phone", "telephone_no", "website"},
    "bank_details": {
        "IFSC", "account_name", "account_no", "account_type", "address", "bank_name",
        "branch", "district", "email", "phone", "pin_code", "scan_copy_of_cheque",
        "state", "telephone_no", "updated",
    },
    "data": {
        "START_PAGE", "actual_start_date", "agent_type", "arrived_date", "complete_html_url",
        "completion_month", "completion_year", "construction_area_1", "construction_area_alter",
        "construction_area_unit", "construction_unit", "construction_units", "data_cert",
        "detail_url", "district", "district_promo", "doc_decoded", "email", "end_date",
        "estimated_construction_cost", "flats", "form_c", "govt_type", "has_same_data",
        "house_no_building_name_promo", "is_processed", "land_area_unit", "land_area_units",
        "lat", "latitude", "link", "link_download", "links_download", "locality_promo",
        "long", "longitude", "names", "no_of_plots", "no_of_units", "phone",
        "pin_code_promo", "project_district", "project_id", "project_location", "promo_id",
        "promo_type", "promoter_type", "promoter_url", "qp_url", "raw_address", "rc",
        "regis_cert", "state_promo", "status", "taluk_promo", "temp", "temp_promoter",
        "total_completion_percentage", "total_unit", "type", "type_of_units",
        "unbuilt_area", "village_promo",
    },
    "project_cost_detail": {
        "cost_of_land", "estimated_construction_cost", "estimated_project_cost",
        "fund_from_allottees", "fund_from_bank", "fund_from_promoter", "total_project_cost",
    },
    "building_details": {
        "amount_paid", "balcony_area", "block_name", "booking_detail", "booking_status",
        "carpet_area", "flat_name", "flat_type", "floor_no", "max_flat_value",
        "min_flat_value", "no_of_plots", "no_of_units", "open_area", "total_area", "updated",
    },
    "members_details": {"email", "has_same_data", "name", "phone", "photo", "position", "raw_address"},
    "professional_information": {
        "address", "effective_date", "email", "has_same_data", "key_real_estate_projects",
        "liscence_no", "mobile", "name", "pan_no", "phone", "registration_no", "role",
        "type", "updated",
    },
    "promoters_details": {
        "GSTIN", "experience_outside_state", "experience_state", "gst_no", "name",
        "objective", "pan", "pan_card", "pan_no", "photo", "promoters_details",
        "reg_no", "registration_certificate", "registration_no", "type_of_firm",
    },
    "construction_progress": {
        "building_name", "date_of_reporting", "dated_on", "has_same_data",
        "progress_percentage", "remarks", "status", "title", "updated",
    },
    "provided_faciltiy": {"description", "facility", "has_same_data", "name", "status"},
    "land_detail": {
        "any_encumbrance", "covered_area", "encumbrance_certificate", "khata_no", "mouza",
        "no_of_plots", "open_area", "plot_area", "plot_no", "registration_place", "ror_doc",
        "sale_deed", "title_holder_name", "total_area",
    },
    "land_area_details": {"construction_area", "construction_area_unit", "land_area", "land_area_unit"},
    "status_update": {
        "amenity_detail", "booked_detail", "booking_details", "building_detail",
        "building_details", "construction_progress", "date_of_reporting", "end_date",
        "gallery_url", "plot_detail", "progress_detail", "proposed_timeline", "qpr_docs",
        "qpr_url", "quarter", "ref_no", "sold_apartment", "start_date", "submitted_on",
        "updated", "updated_date", "year",
    },
    "authorised_signatory_details": {
        "email", "name", "official_address", "pan_no", "permanent_address", "phone",
        "photo", "present_address", "raw_address", "role",
    },
    "co_promoter_details": {
        "comm_address", "email", "land_share", "mobile", "name", "pan_no", "phone",
        "photo", "present_address", "raw_data", "role", "survey_no",
    },
    "complaints_litigation_details": {"count"},
    "proposed_timeline": {"proposed_end_date", "status", "title"},
}

_JSON_FIELD_KEY_ALIASES: dict[str, dict[str, str]] = {
    "project_location_raw": {
        "pincode": "pin_code",
        "address": "raw_address",
        "survey_no": "survey_resurvey_number",
    },
    "promoter_address_raw": {
        "pincode": "pin_code",
        "address": "raw_address",
        "address_text": "raw_address",
    },
    "promoter_contact_details": {
        "E-mail": "email",
        "Email": "email",
        "Mobile": "phone",
        "mobile": "phone",
        "Telephone": "telephone_no",
    },
    "bank_details": {
        "ifsc": "IFSC",
        "ifsc_code": "IFSC",
        "ifsccode": "IFSC",
    },
}

_STATE_JSON_FIELD_ALLOWED_KEYS: dict[str, dict[str, set[str]]] = {
    "bihar": {
        "project_location_raw": {
            "plot_no", "district", "latitude", "longitude", "raw_address",
            "taluk", "village", "city", "pin_code",
            "state", "processed_latitude", "processed_longitude",
        },
        "promoter_address_raw": {"raw_address"},
        "data": {"link", "type", "govt_type", "land_area_unit", "construction_area_unit"},
    },
    "kerala": {
        "data": {
            "govt_type", "land_area_unit", "construction_area_unit", "source_url",
            # Interim fields used to carry raw structured data through normalization
            # for post-normalization legacy shaping. Removed from final output.
            "_raw_building", "_raw_facilities",
        },
    },
    "maharashtra": {
        "project_location_raw": {"state", "taluk", "plot_no", "village", "district", "locality", "pin_code", "latitude", "longitude", "street_name"},
        "data": {
            "START_PAGE", "agent_type", "project_id", "state_promo", "taluk_promo", "village_promo",
            "district_promo", "land_area_unit", "locality_promo", "pin_code_promo",
            "project_district", "construction_area_unit", "construction_area_alter",
            "estimated_construction_cost", "house_no_building_name_promo",
        },
    },
    "puducherry": {
        # The Pondicherry detail page uses raw_address and structured address fields
        "promoter_address_raw": {"raw_address", "registered_address", "correspondence_address"},
        "promoter_contact_details": {"email", "phone"},
    },
    "rajasthan": {
        "project_location_raw": {"state", "taluk", "village", "locality", "pin_code", "raw_address", "house_no_building_name"},
        "data": {"temp", "type", "no_of_plots", "is_processed", "land_area_unit", "construction_area_unit"},
    },
}

_STATE_JSON_SINGLETON_OBJECT_FIELDS: dict[str, set[str]] = {
    "puducherry": {"promoter_contact_details"},
}


def get_machine_context() -> tuple[str | None, str | None]:
    machine_name = socket.gethostname()
    try:
        machine_ip = socket.gethostbyname(machine_name)
    except OSError:
        machine_ip = None
    return machine_name, machine_ip


def normalize_state_key(value: Any) -> str:
    text = clean_string(value) or ""
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return re.sub(r"\s+", " ", text)


def parse_datetime(value: Any) -> datetime | None:
    """
    Parse a date/datetime value from any format seen on Indian government RERA
    sites into a timezone-aware UTC datetime.

    Handles:
    - ISO 8601 and common numeric formats (YYYY-MM-DD, DD/MM/YYYY, etc.)
    - Abbreviated and full month names ("30 Jun 2025", "30 June 2025")
    - 2-digit years ("12 Oct 24", "30/06/25")
    - Dash-month variants ("12-Oct-2024", "12-Oct-24")
    - Comma variants ("30 Jun, 2025")
    - Weekday + full timestamp ("Wed Mar 25 16:15:36 IST 2026")
    - Embedded timezone abbreviations (IST, UTC, GMT, etc.) — stripped before parsing
    """
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)

    text = clean_string(value)
    if not text:
        return None

    # Strip embedded timezone abbreviations (IST, UTC, GMT, PST, …) so formats
    # like "Wed Mar 25 16:15:36 IST 2026" or "30 Jun 2025 IST" parse cleanly.
    # Only strip standalone uppercase 2-5 char tokens that look like tz codes.
    cleaned = re.sub(r"\b[A-Z]{2,5}\b(?=\s|$)", "", text).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)

    # Try ISO 8601 first (covers YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, etc.)
    iso_candidate = cleaned.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_candidate)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        pass

    formats = (
        # ── Full-year numeric (most specific first) ────────────────────────────
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%d-%m-%Y",
        # ── DD Mon YYYY (long-form month names) ───────────────────────────────
        "%d %b %Y %H:%M:%S",     # "30 Jun 2025 10:00:00"
        "%d %B %Y %H:%M:%S",     # "30 June 2025 10:00:00"
        "%d %b %Y",              # "30 Jun 2025"
        "%d %B %Y",              # "30 June 2025"
        "%d %b, %Y",             # "30 Jun, 2025"
        "%d %B, %Y",             # "30 June, 2025"
        # ── DD-Mon-YYYY (dash separator) ──────────────────────────────────────
        "%d-%b-%Y",              # "30-Jun-2025"
        "%d-%B-%Y",              # "30-June-2025"
        # ── Mon DD, YYYY ──────────────────────────────────────────────────────
        "%b %d, %Y",             # "Jun 30, 2025"
        "%B %d, %Y",             # "June 30, 2025"
        # ── 2-digit year variants (common in legacy government exports) ────────
        "%d/%m/%y",              # "30/06/25"
        "%d-%m-%y",              # "30-06-25"
        "%d %b %y",              # "12 Oct 24"  ← the original reported case
        "%d %B %y",              # "12 October 24"
        "%d %b, %y",             # "12 Oct, 24"
        "%d-%b-%y",              # "12-Oct-24"
        "%d-%B-%y",              # "12-October-24"
        # ── Weekday prefix (Pondicherry, some legacy APIs) ────────────────────
        "%a %b %d %H:%M:%S %Y",  # "Wed Mar 25 16:15:36 2026" (tz already stripped)
        "%a %B %d %H:%M:%S %Y",  # "Wed March 25 16:15:36 2026"
    )
    for fmt in formats:
        try:
            return datetime.strptime(cleaned, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def parse_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    text = clean_string(value)
    if not text:
        return None
    text = text.replace(",", "")
    match = re.search(r"-?\d+", text)
    if not match:
        return None
    try:
        return int(match.group(0))
    except ValueError:
        return None


def parse_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    text = clean_string(value)
    if not text:
        return None
    text = text.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def parse_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = clean_string(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"true", "yes", "y", "1"}:
        return True
    if lowered in {"false", "no", "n", "0"}:
        return False
    return None


def clean_json(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, raw_val in value.items():
            cleaned = clean_json(raw_val)
            if cleaned in (None, "", [], {}):
                continue
            result[str(key)] = cleaned
        return result
    if isinstance(value, list):
        result = [clean_json(item) for item in value]
        return [item for item in result if item not in (None, "", [], {})]
    if isinstance(value, tuple):
        return clean_json(list(value))
    if isinstance(value, set):
        return clean_json(list(value))
    if isinstance(value, str):
        return clean_string(value)
    return value


def normalize_array(values: Any) -> list[Any] | None:
    if values in (None, ""):
        return None
    items = values if isinstance(values, (list, tuple, set)) else [values]
    result: list[Any] = []
    seen: set[str] = set()
    for item in items:
        cleaned = clean_json(item)
        if cleaned in (None, "", [], {}):
            continue
        marker = repr(cleaned)
        if marker in seen:
            continue
        seen.add(marker)
        result.append(cleaned)
    return result or None


def normalize_document_records(documents: Any) -> list[dict[str, Any]] | None:
    entries = normalize_array(documents)
    if not entries:
        return None

    normalized: list[dict[str, Any]] = []
    for entry in entries:
        if isinstance(entry, dict):
            item = copy.deepcopy(entry)
        else:
            item = {"link": clean_string(entry)}

        doc_type = clean_string(
            item.get("type")
            or item.get("label")
            or item.get("document_type")
            or item.get("name")
        )
        link = clean_string(item.get("link") or item.get("source_url") or item.get("url"))
        s3_link = clean_string(item.get("s3_link"))
        filename = clean_string(item.get("filename"))

        updated = item.get("updated")

        cleaned_item: dict[str, Any] = {}
        if doc_type:
            cleaned_item["type"] = doc_type
        if link:
            cleaned_item["link"] = link
        if s3_link:
            cleaned_item["s3_link"] = s3_link
        if filename:
            cleaned_item["filename"] = filename
        if updated is not None:
            cleaned_item["updated"] = bool(updated)

        # Keep entries that have at least a document type — links are added
        # in a later step (document-upload enrichment via data_extractors.py).
        # Requiring a link here would discard valid records for crawlers (e.g.
        # AP RERA) whose document links are JavaScript callbacks resolved later.
        if doc_type:
            normalized.append(cleaned_item)
    return normalized or None


def _allowed_json_keys(field_name: str, state_key: str | None) -> set[str] | None:
    if state_key:
        state_fields = _STATE_JSON_FIELD_ALLOWED_KEYS.get(state_key, {})
        if field_name in state_fields:
            return state_fields[field_name]
    return _JSON_FIELD_ALLOWED_KEYS.get(field_name)


def _field_aliases(field_name: str) -> dict[str, str]:
    return _JSON_FIELD_KEY_ALIASES.get(field_name, {})


def _should_flatten_singleton_json(field_name: str, state_key: str | None) -> bool:
    if not state_key:
        return False
    return field_name in _STATE_JSON_SINGLETON_OBJECT_FIELDS.get(state_key, set())


def _normalize_structured_json_item(field_name: str, item: Any, state_key: str | None = None) -> Any:
    if not isinstance(item, dict):
        return item

    aliases = _field_aliases(field_name)
    allowed = _allowed_json_keys(field_name, state_key)
    normalized_item: dict[str, Any] = {}

    for key, raw_value in item.items():
        mapped_key = aliases.get(str(key), str(key))
        if allowed is not None and mapped_key not in allowed:
            continue
        normalized_item[mapped_key] = raw_value

    return clean_json(normalized_item)


def normalize_structured_json(field_name: str, value: Any, *, state_key: str | None = None) -> Any:
    cleaned = clean_json(value)
    if cleaned in (None, "", [], {}):
        return None

    if (
        field_name not in _JSON_FIELD_ALLOWED_KEYS
        and field_name not in _JSON_FIELD_KEY_ALIASES
        and (not state_key or field_name not in _STATE_JSON_FIELD_ALLOWED_KEYS.get(state_key, {}))
    ):
        return cleaned

    if isinstance(cleaned, dict):
        return _normalize_structured_json_item(field_name, cleaned, state_key=state_key)

    if isinstance(cleaned, list):
        result = [_normalize_structured_json_item(field_name, item, state_key=state_key) for item in cleaned]
        filtered = [item for item in result if item not in (None, "", [], {})]
        if _should_flatten_singleton_json(field_name, state_key) and len(filtered) == 1 and isinstance(filtered[0], dict):
            return filtered[0]
        return filtered or None

    return cleaned


def build_document_urls(documents: Any) -> list[dict[str, Any]] | None:
    if not documents:
        return None

    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str | None]] = set()
    items = documents if isinstance(documents, list) else [documents]
    for doc in items:
        if not isinstance(doc, dict):
            continue
        s3_link = clean_string(doc.get("s3_link"))
        doc_type = clean_string(doc.get("type") or doc.get("label"))
        if not s3_link:
            continue
        marker = (s3_link, doc_type)
        if marker in seen:
            continue
        seen.add(marker)
        entry: dict[str, Any] = {"link": s3_link}
        if doc_type:
            entry["type"] = doc_type
        result.append(entry)
    return result or None


def document_identity_url(doc: dict[str, Any]) -> str | None:
    return clean_string(doc.get("identity_url") or doc.get("source_url") or doc.get("url"))


_KNOWN_EXTENSIONS: frozenset[str] = frozenset({
    ".pdf", ".xlsx", ".xls", ".doc", ".docx",
    ".jpg", ".jpeg", ".png", ".gif", ".zip",
})


def build_document_filename(doc: dict[str, Any], default_ext: str = ".pdf") -> str:
    label = clean_string(doc.get("label") or doc.get("type")) or "document"
    slug = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_") or "document"

    url = clean_string(doc.get("source_url") or doc.get("url")) or ""
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    suffix = None
    for key in ("fileId", "fileid", "DOC_ID", "doc_id", "id"):
        values = query.get(key)
        if values and values[0]:
            suffix = re.sub(r"[^a-z0-9]+", "_", values[0].lower()).strip("_")
            break

    if not suffix and url:
        suffix = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]

    # Infer file extension from URL path (e.g. .xlsx, .docx) when available;
    # fall back to default_ext (typically .pdf) for opaque or extensionless URLs.
    from pathlib import PurePosixPath
    url_path_ext = PurePosixPath(parsed.path).suffix.lower() if parsed.path else ""
    ext = url_path_ext if url_path_ext in _KNOWN_EXTENSIONS else ((default_ext or ".pdf").strip() or ".pdf")
    if not ext.startswith("."):
        ext = f".{ext}"

    if suffix:
        return f"{slug}_{suffix}{ext}"
    return f"{slug}{ext}"


def document_result_entry(
    doc: dict[str, Any],
    s3_url: str,
    file_name: str | None = None,
    **_: Any,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "type": doc.get("type") or doc.get("label") or "document",
        "link": doc.get("source_url") or doc.get("url"),
        "s3_link": s3_url,
        "updated": True,
    }
    dated_on = doc.get("dated_on")
    if dated_on:
        entry["dated_on"] = dated_on
    return clean_json(entry) or entry


def merge_data_sections(*sections: Any) -> dict[str, Any] | None:
    merged: dict[str, Any] = {}
    for section in sections:
        cleaned = clean_json(section)
        if not cleaned:
            continue
        if isinstance(cleaned, dict):
            for key, value in cleaned.items():
                if key not in merged:
                    merged[key] = value
                elif isinstance(merged[key], dict) and isinstance(value, dict):
                    merged[key] = {**merged[key], **value}
                else:
                    merged[key] = value
        else:
            merged.setdefault("raw_sections", []).append(cleaned)
    return merged or None


def normalize_project_payload(
    payload: dict[str, Any],
    config: dict[str, Any],
    *,
    machine_name: str | None = None,
    machine_ip: str | None = None,
) -> dict[str, Any]:
    data = copy.deepcopy(payload)
    raw_snapshot = data.pop("data", None)
    state_key = normalize_state_key(config.get("state") or data.get("state"))

    normalized: dict[str, Any] = {}
    unmapped_fields: dict[str, Any] = {}

    for key, value in data.items():
        if key not in PROJECT_COLUMN_SET:
            unmapped_fields[key] = value
            continue

        cleaned: Any
        if key in TEXT_FIELDS:
            cleaned = clean_string(value)
        elif key in DATETIME_FIELDS:
            cleaned = parse_datetime(value)
        elif key in INTEGER_FIELDS:
            cleaned = parse_int(value)
        elif key in FLOAT_FIELDS:
            cleaned = parse_float(value)
        elif key in BOOLEAN_FIELDS:
            cleaned = parse_bool(value)
        elif key in JSONB_FIELDS:
            if key in {"uploaded_documents", "document_urls"}:
                cleaned = normalize_document_records(value)
            else:
                cleaned = normalize_structured_json(key, value, state_key=state_key)
        elif key in ARRAY_FIELDS:
            cleaned = normalize_array(value)
        else:
            cleaned = clean_json(value)

        if cleaned in (None, "", [], {}):
            continue
        normalized[key] = cleaned

    if config.get("state"):
        normalized.setdefault("state", config["state"])
        normalized.setdefault("project_state", normalized.get("project_state") or config["state"])
    if config.get("domain"):
        normalized.setdefault("domain", config["domain"])
    if config.get("config_id") is not None:
        normalized.setdefault("config_id", config["config_id"])
    if config.get("listing_url"):
        normalized.setdefault("url", config["listing_url"])
    if machine_name:
        normalized.setdefault("machine_name", machine_name)
    if machine_ip:
        normalized.setdefault("crawl_machine_ip", machine_ip)

    normalized.setdefault("is_duplicate", False)

    reg_no = normalized.get("project_registration_no")
    if reg_no and not normalized.get("key"):
        normalized["key"] = generate_project_key(reg_no)

    merged_raw = merge_data_sections(raw_snapshot)
    if unmapped_fields:
        merged_raw = merge_data_sections(merged_raw, {"unmapped_fields": unmapped_fields})
    if merged_raw:
        cleaned_data = normalize_structured_json("data", merged_raw, state_key=state_key)
        if cleaned_data:
            normalized["data"] = cleaned_data

    if normalized.get("uploaded_documents"):
        normalized["uploaded_documents"] = normalize_document_records(normalized["uploaded_documents"])

    document_urls = normalized.get("document_urls")
    if document_urls:
        normalized["document_urls"] = build_document_urls(document_urls)
    elif normalized.get("uploaded_documents"):
        derived_urls = build_document_urls(normalized["uploaded_documents"])
        if derived_urls:
            normalized["document_urls"] = derived_urls

    # Compute is_live from the project's finish date.
    # A project is live if its end date is still in the future.
    # If no finish date is available we keep whatever the crawler set
    # (all crawlers default to True for projects visible on the listing).
    _finish_date: datetime | None = (
        normalized.get("estimated_finish_date")
        or normalized.get("actual_finish_date")
    )
    if _finish_date is not None:
        _now = datetime.now(UTC)
        normalized["is_live"] = _finish_date > _now

    missing = [field for field in REQUIRED_PROJECT_FIELDS if not normalized.get(field)]
    if missing:
        raise ValueError(f"Missing required project fields after normalization: {', '.join(sorted(missing))}")

    return normalized
