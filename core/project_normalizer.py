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


def get_machine_context() -> tuple[str | None, str | None]:
    machine_name = socket.gethostname()
    try:
        machine_ip = socket.gethostbyname(machine_name)
    except OSError:
        machine_ip = None
    return machine_name, machine_ip


def clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return re.sub(r"\s+", " ", text)


def parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)

    text = clean_string(value)
    if not text:
        return None

    iso_candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_candidate)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        pass

    formats = (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%d %b %Y",
        "%d %B %Y",
        "%d %b %Y %H:%M:%S",
        "%d %B %Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%a %b %d %H:%M:%S %Y",
    )
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=UTC)
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

        cleaned_item: dict[str, Any] = {}
        if doc_type:
            cleaned_item["type"] = doc_type
        if link:
            cleaned_item["link"] = link
        if s3_link:
            cleaned_item["s3_link"] = s3_link

        if doc_type and (link or s3_link):
            normalized.append(cleaned_item)
    return normalized or None


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

    ext = (default_ext or ".pdf").strip() or ".pdf"
    if not ext.startswith("."):
        ext = f".{ext}"

    if suffix:
        return f"{slug}_{suffix}{ext}"
    return f"{slug}{ext}"


def document_result_entry(doc: dict[str, Any], s3_url: str, file_name: str) -> dict[str, Any]:
    entry = {
        "type": doc.get("type") or doc.get("label") or "document",
        "link": doc.get("source_url") or doc.get("url"),
        "s3_link": s3_url,
    }
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
                cleaned = clean_json(value)
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
    if reg_no and not normalized.get("key") and config.get("state_code"):
        normalized["key"] = generate_project_key(config["state_code"], reg_no)

    merged_raw = merge_data_sections(raw_snapshot)
    if unmapped_fields:
        merged_raw = merge_data_sections(merged_raw, {"unmapped_fields": unmapped_fields})
    if merged_raw:
        normalized["data"] = merged_raw

    if normalized.get("uploaded_documents"):
        normalized["uploaded_documents"] = normalize_document_records(normalized["uploaded_documents"])

    document_urls = normalized.get("document_urls")
    if document_urls:
        normalized["document_urls"] = build_document_urls(document_urls)
    elif normalized.get("uploaded_documents"):
        derived_urls = build_document_urls(normalized["uploaded_documents"])
        if derived_urls:
            normalized["document_urls"] = derived_urls

    missing = [field for field in REQUIRED_PROJECT_FIELDS if not normalized.get(field)]
    if missing:
        raise ValueError(f"Missing required project fields after normalization: {', '.join(sorted(missing))}")

    return normalized
