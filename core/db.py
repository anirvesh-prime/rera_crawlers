from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone

UTC = timezone.utc
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
from psycopg.types.json import Jsonb

from core.config import settings
from core.project_schema import JSONB_FIELDS, REQUIRED_PROJECT_FIELDS

log = logging.getLogger(__name__)

# ── Per-process persistent connection ────────────────────────────────────────
# ProcessPoolExecutor gives each crawler its own OS process, so this module-
# level variable is private to that process — no cross-process sharing occurs.
_conn: "psycopg.Connection | None" = None
_schema_ensured: bool = False

# ── Comparison constants (mirrors production DataComparator) ──────────────────

# Fields never compared for business-level changes
_COMPARE_IGNORE: frozenset[str] = frozenset({
    "retrieved_on", "config_id", "data", "domain", "crawl_machine_ip",
    "machine_name", "is_processed", "is_duplicate", "last_updated",
    "updated_fields", "old_updates", "iw_picked", "iw_processed",
    "last_crawled_date", "doc_ocr_url", "has_same_data", "url", "key",
})

# Values treated as empty/null (mirrors constants.none_list)
_NONE_SCALARS: frozenset = frozenset({
    None, "", "None", "none", "null", "NA", "'", '"', "[]", "{}",
})

# Max entries kept in old_updates history
_MAX_UPDATES_HISTORY = 5

# Days difference required before a date field is considered changed
_DATE_THRESHOLD_DAYS = 31


def _is_none_equiv(value: Any) -> bool:
    """True if value is empty / None-equivalent (production constants.none_list)."""
    if value is None:
        return True
    if isinstance(value, (str,)) and value in _NONE_SCALARS:
        return True
    if isinstance(value, (list, dict, set, tuple)) and len(value) == 0:
        return True
    if value in ([None], ["None"], [{}]):
        return True
    return False


def _normalize_str(value: Any) -> str:
    """Lowercase + strip + remove spaces — production normalize_string."""
    return str(value).lower().strip().replace(" ", "")


def _dicts_differ(old: Any, new: Any) -> bool:
    """True if two dicts are meaningfully different (null-preserving)."""
    if _is_none_equiv(old) and _is_none_equiv(new):
        return False
    if _is_none_equiv(old):
        return True
    if not isinstance(old, dict) or not isinstance(new, dict):
        return True
    for k, new_v in new.items():
        old_v = old.get(k)
        if _is_none_equiv(new_v) and not _is_none_equiv(old_v):
            continue  # null-preserve — not a diff
        if _field_differs(k, old_v, new_v):
            return True
    return False


def _field_differs(column: str, old_val: Any, new_val: Any) -> bool:
    """Type-aware comparison — mirrors DataComparator.check_updates branching."""
    # Numeric
    if isinstance(new_val, (int, float)) or isinstance(old_val, (int, float)):
        try:
            old_n = float(old_val) if old_val is not None else 0.0
            new_n = float(new_val) if new_val is not None else 0.0
            if old_n == 0:
                return False  # skip uninitialized
            return old_n != new_n
        except (ValueError, TypeError):
            return False

    # Datetime with threshold
    if isinstance(new_val, datetime) and isinstance(old_val, datetime):
        old_dt = old_val.replace(tzinfo=UTC) if old_val.tzinfo is None else old_val
        new_dt = new_val.replace(tzinfo=UTC) if new_val.tzinfo is None else new_val
        return abs((new_dt.date() - old_dt.date()).days) > _DATE_THRESHOLD_DAYS

    # List — count new items absent from old list
    if isinstance(new_val, list):
        if _is_none_equiv(old_val):
            return True
        if not isinstance(old_val, list):
            return True
        new_count = 0
        for ni in new_val:
            found = any(
                (not _dicts_differ(oi, ni) if isinstance(ni, dict) and isinstance(oi, dict) else ni == oi)
                for oi in old_val
            )
            if not found:
                new_count += 1
        return new_count > 0

    # Dict
    if isinstance(new_val, dict):
        return _dicts_differ(old_val, new_val)

    # String / bool
    return _normalize_str(old_val) != _normalize_str(new_val)

_SCHEMA_DDL = [
    """
    CREATE TABLE IF NOT EXISTS rera_projects (
        key TEXT NOT NULL PRIMARY KEY,
        project_name TEXT,
        project_type TEXT,
        promoter_name TEXT,
        project_registration_no TEXT,
        status_of_the_project TEXT,
        acknowledgement_no TEXT,
        project_pin_code TEXT,
        project_city TEXT,
        project_state TEXT,
        project_location_raw JSONB,
        promoter_address_raw JSONB,
        promoter_contact_details JSONB,
        submitted_date TIMESTAMPTZ,
        last_modified TIMESTAMPTZ,
        estimated_commencement_date TIMESTAMPTZ,
        actual_commencement_date TIMESTAMPTZ,
        estimated_finish_date TIMESTAMPTZ,
        actual_finish_date TIMESTAMPTZ,
        approved_on_date TIMESTAMPTZ,
        past_experience_of_promoter INTEGER,
        bank_details JSONB,
        land_area DOUBLE PRECISION,
        construction_area DOUBLE PRECISION,
        total_floor_area_under_commercial_or_other_uses DOUBLE PRECISION,
        total_floor_area_under_residential DOUBLE PRECISION,
        project_cost_detail JSONB,
        number_of_residential_units INTEGER,
        number_of_commercial_units INTEGER,
        building_details JSONB,
        complaints_litigation_details JSONB,
        uploaded_documents JSONB,
        authorised_signatory_details JSONB,
        co_promoter_details JSONB,
        project_description TEXT,
        provided_faciltiy JSONB,
        professional_information JSONB,
        development_agreement_detail JSONB,
        construction_progress JSONB,
        land_detail JSONB,
        document_urls JSONB,
        members_details JSONB,
        retrieved_on TIMESTAMPTZ DEFAULT now(),
        config_id INTEGER,
        data JSONB,
        promoters_details JSONB,
        domain TEXT,
        state TEXT,
        crawl_machine_ip TEXT,
        machine_name TEXT,
        is_updated BOOLEAN DEFAULT false,
        is_duplicate BOOLEAN DEFAULT false,
        url TEXT NOT NULL,
        last_updated TIMESTAMPTZ,
        updated_fields TEXT[],
        project_images TEXT[],
        detail_images TEXT[],
        lister_images TEXT[],
        images TEXT,
        old_updates JSONB DEFAULT '[]'::jsonb,
        status_update JSONB,
        iw_part_processed BOOLEAN,
        iw_processed BOOLEAN DEFAULT false,
        last_crawled_date TIMESTAMPTZ DEFAULT now(),
        land_area_details JSONB,
        doc_ocr_url TEXT[],
        proposed_timeline JSONB,
        checked_updates BOOLEAN DEFAULT false,
        checked_updates_date TIMESTAMPTZ,
        rera_housing_found BOOLEAN DEFAULT false,
        is_live BOOLEAN DEFAULT false,
        alternative_rera_ids TEXT[]
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS crawl_runs (
        id SERIAL PRIMARY KEY,
        site_id TEXT NOT NULL,
        run_type TEXT NOT NULL,
        started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at TIMESTAMPTZ,
        status TEXT NOT NULL,
        projects_found INTEGER DEFAULT 0,
        projects_new INTEGER DEFAULT 0,
        projects_updated INTEGER DEFAULT 0,
        projects_skipped INTEGER DEFAULT 0,
        documents_uploaded INTEGER DEFAULT 0,
        error_count INTEGER DEFAULT 0,
        sentinel_passed BOOLEAN,
        notes TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS crawl_errors (
        id SERIAL PRIMARY KEY,
        run_id INTEGER REFERENCES crawl_runs(id),
        site_id TEXT NOT NULL,
        project_key TEXT,
        error_type TEXT NOT NULL,
        error_message TEXT,
        url TEXT,
        occurred_at TIMESTAMPTZ DEFAULT now(),
        raw_data JSONB
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS crawl_checkpoints (
        site_id TEXT PRIMARY KEY,
        run_type TEXT NOT NULL,
        last_page INTEGER,
        last_project_key TEXT,
        last_run_id INTEGER REFERENCES crawl_runs(id),
        updated_at TIMESTAMPTZ DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS project_documents (
        id SERIAL PRIMARY KEY,
        project_key TEXT NOT NULL,
        document_type TEXT,
        original_url TEXT,
        s3_key TEXT NOT NULL,
        s3_bucket TEXT NOT NULL,
        file_name TEXT,
        md5_checksum TEXT NOT NULL,
        file_size_bytes INTEGER,
        uploaded_at TIMESTAMPTZ DEFAULT now(),
        last_verified TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS crawl_logs (
        id SERIAL PRIMARY KEY,
        run_id INTEGER REFERENCES crawl_runs(id),
        site_id TEXT,
        level TEXT NOT NULL,
        message TEXT NOT NULL,
        project_key TEXT,
        registration_no TEXT,
        step TEXT,
        traceback TEXT,
        extra JSONB,
        logged_at TIMESTAMPTZ DEFAULT now()
    )
    """,
]


def get_connection() -> psycopg.Connection:
    """Return the process-level persistent connection, creating it if needed.

    Opening a new TCP connection + TLS handshake + auth on every DB call is
    the single biggest overhead when crawlers log frequently.  Reusing one
    connection per process eliminates that cost entirely while staying safe
    because ProcessPoolExecutor gives each crawler its own address space.
    """
    global _conn, _schema_ensured
    if _conn is None or _conn.closed:
        _conn = psycopg.connect(settings.postgres_dsn, row_factory=dict_row)
        _schema_ensured = False
    if not _schema_ensured:
        ensure_schema(_conn)
        _schema_ensured = True
    return _conn


def ensure_schema(conn: psycopg.Connection):
    for statement in _SCHEMA_DDL:
        conn.execute(statement)
    conn.commit()


def _db_value(value: Any, column: str = "") -> Any:
    """Wrap value for psycopg. Only JSONB columns get the Jsonb wrapper;
    TEXT[] columns (updated_fields, doc_ocr_url, etc.) are passed as plain lists."""
    if isinstance(value, dict):
        return Jsonb(value)
    if isinstance(value, list):
        if column in JSONB_FIELDS:
            return Jsonb(value)
        return value  # TEXT[] — psycopg handles natively
    return value


def _missing_required_project_fields(data: Mapping[str, Any]) -> list[str]:
    missing: list[str] = []
    for field in sorted(REQUIRED_PROJECT_FIELDS):
        value = data.get(field)
        if isinstance(value, str):
            if not value.strip():
                missing.append(field)
            continue
        if _is_none_equiv(value):
            missing.append(field)
    return missing


# crawl_runs

def insert_crawl_run(site_id: str, run_type: str) -> int:
    with get_connection() as conn:
        row = conn.execute(
            """
            INSERT INTO crawl_runs (site_id, run_type, status, started_at)
            VALUES (%s, %s, 'running', now())
            RETURNING id
            """,
            (site_id, run_type),
        ).fetchone()
        conn.commit()
        return row["id"]


def update_crawl_run(
    run_id: int,
    status: str,
    counts: dict | None = None,
    sentinel_passed: bool | None = None,
    notes: str | None = None,
):
    counts = counts or {}
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE crawl_runs SET
                status = %s,
                finished_at = now(),
                projects_found = COALESCE(%s, projects_found),
                projects_new = COALESCE(%s, projects_new),
                projects_updated = COALESCE(%s, projects_updated),
                projects_skipped = COALESCE(%s, projects_skipped),
                documents_uploaded = COALESCE(%s, documents_uploaded),
                error_count = COALESCE(%s, error_count),
                sentinel_passed = COALESCE(%s, sentinel_passed),
                notes = COALESCE(%s, notes)
            WHERE id = %s
            """,
            (
                status,
                counts.get("projects_found"),
                counts.get("projects_new"),
                counts.get("projects_updated"),
                counts.get("projects_skipped"),
                counts.get("documents_uploaded"),
                counts.get("error_count"),
                sentinel_passed,
                notes,
                run_id,
            ),
        )
        conn.commit()


# crawl_errors

def insert_crawl_error(
    run_id: int,
    site_id: str,
    error_type: str,
    error_message: str,
    project_key: str | None = None,
    url: str | None = None,
    raw_data: dict | None = None,
):
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO crawl_errors (run_id, site_id, project_key, error_type, error_message, url, raw_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (run_id, site_id, project_key, error_type, error_message, url, Jsonb(raw_data) if raw_data else None),
        )
        conn.commit()


# crawl_checkpoints

def get_checkpoint(site_id: str, run_type: str) -> dict | None:
    with get_connection() as conn:
        return conn.execute(
            "SELECT * FROM crawl_checkpoints WHERE site_id = %s AND run_type = %s",
            (site_id, run_type),
        ).fetchone()


def set_checkpoint(site_id: str, run_type: str, last_page: int, last_project_key: str | None, run_id: int):
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO crawl_checkpoints (site_id, run_type, last_page, last_project_key, last_run_id, updated_at)
            VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (site_id) DO UPDATE SET
                run_type = EXCLUDED.run_type,
                last_page = EXCLUDED.last_page,
                last_project_key = EXCLUDED.last_project_key,
                last_run_id = EXCLUDED.last_run_id,
                updated_at = now()
            """,
            (site_id, run_type, last_page, last_project_key, run_id),
        )
        conn.commit()


def insert_log(
    run_id: int | None,
    site_id: str,
    level: str,
    message: str,
    project_key: str | None = None,
    registration_no: str | None = None,
    step: str | None = None,
    traceback: str | None = None,
    extra: dict | None = None,
):
    try:
        with get_connection() as conn:
            conn.execute(
                """INSERT INTO crawl_logs
                   (run_id, site_id, level, message, project_key, registration_no, step, traceback, extra)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (run_id, site_id, level, message,
                 project_key, registration_no, step, traceback,
                 json.dumps(extra or {})),
            )
            conn.commit()
    except Exception:
        pass  # never let logging break the crawler


def clear_checkpoint(site_id: str, run_type: str):
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM crawl_checkpoints WHERE site_id = %s AND run_type = %s",
            (site_id, run_type),
        )
        conn.commit()


# projects

def get_project_by_key(key: str) -> dict | None:
    with get_connection() as conn:
        return conn.execute("SELECT * FROM rera_projects WHERE key = %s", (key,)).fetchone()


def upsert_project(data: dict[str, Any]) -> str:
    """
    Insert or update a project using production DataComparator logic:
    - New:     key not in DB → full insert
    - Updated: key exists, meaningful field changes found →
               write only changed fields + bookkeeping columns
    - Skipped: key exists, nothing changed →
               write updated_fields=NULL + last_crawled_date only

    Runs entirely inside one transaction with SELECT … FOR UPDATE so that
    parallel crawler processes cannot race on the same project key.
    """
    key = data["key"]
    conn = get_connection()

    with conn.transaction():
        existing = conn.execute(
            "SELECT * FROM rera_projects WHERE key = %s FOR UPDATE", (key,)
        ).fetchone()

        if existing is None:
            _insert_project(data, conn)
            return "new"

        item = dict(data)          # working copy (null-preservation writes back here)
        updated_fields: list[str] = []

        for column, new_val in list(item.items()):
            if column in _COMPARE_IGNORE:
                continue
            if column not in existing:
                continue

            old_val = existing[column]

            # Null-preservation: new crawl missed a value → keep DB value
            if _is_none_equiv(new_val) and not _is_none_equiv(old_val):
                item[column] = old_val
                continue

            if _is_none_equiv(new_val):
                continue  # both empty — not a change

            if _field_differs(column, old_val, new_val):
                updated_fields.append(column)

        if not updated_fields:
            _touch_project(key, conn)
            return "skipped"

        # Build old_updates history entry (stores old values for changed fields)
        old_updates: list[dict] = _parse_old_updates(existing.get("old_updates"))
        new_entry: dict = {"updated_on": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f%z")}
        for field in updated_fields:
            if field in ("old_updates", "last_updated", "updated_fields"):
                continue
            old_v = existing.get(field)
            if not _is_none_equiv(old_v):
                new_entry[field] = str(old_v)
        if len(new_entry) > 1:
            old_updates.append(new_entry)

        # Keep most recent MAX_UPDATES_HISTORY entries
        old_updates = sorted(
            [u for u in old_updates if isinstance(u, dict) and "updated_on" in u and len(u) > 1],
            key=lambda x: x.get("updated_on", ""),
            reverse=True,
        )[:_MAX_UPDATES_HISTORY]

        _update_project_fields(key, item, updated_fields, old_updates, conn)
        return "updated"


def _parse_old_updates(raw: Any) -> list[dict]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            result = json.loads(raw)
            return result if isinstance(result, list) else []
        except Exception:
            return []
    return []


def _insert_project(data: dict[str, Any], conn: psycopg.Connection):
    """Execute INSERT within the caller's transaction — no commit here."""
    missing = _missing_required_project_fields(data)
    if missing:
        raise ValueError(
            f"Refusing to insert project without required fields: {', '.join(missing)}"
        )

    columns = list(data.keys())
    query = SQL("INSERT INTO rera_projects ({fields}) VALUES ({values})").format(
        fields=SQL(", ").join(Identifier(c) for c in columns),
        values=SQL(", ").join(SQL("%s") for _ in columns),
    )
    conn.execute(query, [_db_value(data[c], c) for c in columns])


def _touch_project(key: str, conn: psycopg.Connection):
    """No meaningful change — only refresh last_crawled_date, clear updated_fields.
    Executes within the caller's transaction — no commit here."""
    conn.execute(
        "UPDATE rera_projects SET last_crawled_date = now(), updated_fields = NULL WHERE key = %s",
        (key,),
    )


def _update_project_fields(
    key: str,
    item: dict[str, Any],
    updated_fields: list[str],
    old_updates: list[dict],
    conn: psycopg.Connection,
):
    """Write only changed business fields + bookkeeping columns.
    Executes within the caller's transaction — no commit here."""
    _BOOKKEEPING = ["updated_fields", "last_updated", "last_crawled_date",
                    "old_updates", "config_id", "is_updated"]
    # Deduplicate preserving order: changed fields first, then bookkeeping
    seen: set[str] = set()
    all_columns: list[str] = []
    for c in updated_fields + _BOOKKEEPING:
        if c not in seen:
            seen.add(c)
            all_columns.append(c)

    # Build the values dict for all columns we'll write
    write: dict[str, Any] = dict(item)
    write["updated_fields"]  = updated_fields
    write["is_updated"]      = True
    write["last_updated"]    = datetime.now(UTC)
    write["last_crawled_date"] = datetime.now(UTC)
    write["old_updates"]     = old_updates

    # Only include columns that exist in write dict
    all_columns = [c for c in all_columns if c in write]

    assignments = SQL(", ").join(
        SQL("{} = %s").format(Identifier(c)) for c in all_columns
    )
    query = SQL("UPDATE rera_projects SET {assignments} WHERE key = %s").format(
        assignments=assignments,
    )
    values = [_db_value(write[c], c) for c in all_columns]
    values.append(key)
    conn.execute(query, values)


def bulk_insert_logs(entries: list[dict]) -> None:
    """Batch-insert buffered log entries in a single round-trip.

    Called by the buffered DbLogHandler — never raises so that a DB hiccup
    cannot kill the crawler.  Uses a nested transaction (savepoint) so it
    is safe to call even when the persistent connection already has an open
    transaction (e.g. inside upsert_project).
    """
    if not entries:
        return
    try:
        conn = get_connection()
        with conn.transaction():
            conn.executemany(
                """
                INSERT INTO crawl_logs
                    (run_id, site_id, level, message, project_key,
                     registration_no, step, traceback, extra)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        e.get("run_id"),
                        e.get("site_id"),
                        e.get("level"),
                        e.get("message"),
                        e.get("project_key"),
                        e.get("registration_no"),
                        e.get("step"),
                        e.get("traceback"),
                        json.dumps(e.get("extra") or {}),
                    )
                    for e in entries
                ],
            )
    except Exception:
        pass  # never let logging break the crawler


# project_documents

def get_document(project_key: str, original_url: str) -> dict | None:
    with get_connection() as conn:
        return conn.execute(
            "SELECT * FROM project_documents WHERE project_key = %s AND original_url = %s",
            (project_key, original_url),
        ).fetchone()


def upsert_document(
    project_key: str,
    document_type: str,
    original_url: str,
    s3_key: str,
    s3_bucket: str,
    file_name: str,
    md5_checksum: str,
    file_size_bytes: int,
) -> str:
    existing = get_document(project_key, original_url)
    with get_connection() as conn:
        if existing is None:
            conn.execute(
                """
                INSERT INTO project_documents
                    (project_key, document_type, original_url, s3_key, s3_bucket, file_name, md5_checksum, file_size_bytes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    project_key,
                    document_type,
                    original_url,
                    s3_key,
                    s3_bucket,
                    file_name,
                    md5_checksum,
                    file_size_bytes,
                ),
            )
            conn.commit()
            return "uploaded"

        if existing["md5_checksum"] != md5_checksum:
            conn.execute(
                """
                UPDATE project_documents
                SET s3_key = %s, md5_checksum = %s, file_size_bytes = %s, uploaded_at = now()
                WHERE id = %s
                """,
                (s3_key, md5_checksum, file_size_bytes, existing["id"]),
            )
            conn.commit()
            return "updated"

        conn.execute("UPDATE project_documents SET last_verified = now() WHERE id = %s", (existing["id"],))
        conn.commit()
        return "skipped"
