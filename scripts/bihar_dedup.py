"""Bihar duplicate-row consolidation.

Backstory:
    Prod historically used ``siphash24(project_name + project_registration_no +
    promoter_name)`` as the ``rera_projects.key``. The unified crawler used
    ``siphash24(project_registration_no)`` alone. The mismatch left every Bihar
    project as two rows in prod: one prod-formula row (the survivor) and one
    legacy reg-only row (the loser). Non-tech edits landed on either side, so
    neither set can be blindly dropped.

Merge policy (per user direction):
    * For each column, if only one row has a value, use that value.
    * If both rows have values and they differ, take the value from the row with
      the later ``last_updated`` (fallback chain:
      ``last_updated → last_crawled_date → retrieved_on``; final tie → prefer
      the surviving prod-key row).
    * For array / JSONB-array columns (``uploaded_documents``, ``document_urls``,
      ``old_updates``, ``updated_fields``, ``alternative_rera_ids``, image
      arrays, ``doc_ocr_url``) — union both rows, dedupe.
    * ``last_updated``, ``last_crawled_date`` → ``MAX``; ``retrieved_on`` → ``MIN``.
    * Document rows in ``rera_project_documents`` belonging to the legacy
      (loser) reg-only key are deleted outright; the surviving prod-key row
      keeps its own documents.

Default mode is ``--dry-run``: reads only, writes three CSV reports.
``--apply`` performs the merges inside a per-cluster transaction.
"""
from __future__ import annotations

import os
import sys

# Must be set before any siphash24 call — keeps generate_project_key deterministic
# across processes. Mirrors the convention used by run_karnataka_ack_crawl.py.
if os.environ.get("PYTHONHASHSEED") != "0":
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = "0"
    os.execvpe(sys.executable, [sys.executable, *sys.argv], env)

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from psycopg.sql import SQL, Identifier

from core.crawler_base import generate_project_key
from core.db import _db_value, get_connection
from core.project_schema import ARRAY_FIELDS, PROJECT_COLUMNS

UTC = timezone.utc
_LEGACY_DT = datetime(1970, 1, 1, tzinfo=UTC)

# Per-field collection rules
_DOC_COLLECTION_DEDUP_KEYS = {
    "uploaded_documents": ("original_url", "s3_key", "link"),
    "document_urls": ("link", "url", "s3_link"),
}
_HISTORY_COLLECTIONS = {"old_updates"}
_SET_COLLECTIONS = set(ARRAY_FIELDS)

# Columns that store S3 URLs (or document/image references). Skipped from the
# merge when --skip-docs is passed, on the assumption that the operator will
# purge the legacy S3 prefix and let a recrawl repopulate these on the prod
# row. Merging them would otherwise inject soon-to-be-broken legacy URLs.
_SKIP_DOC_FIELDS = frozenset({
    "uploaded_documents", "document_urls", "doc_ocr_url",
    "project_images", "detail_images", "lister_images", "images",
})

# Fields the merge handles with bespoke rules (skipped from the generic loop)
_SPECIAL_FIELDS = (
    {"key", "retrieved_on", "last_updated", "last_crawled_date",
     "is_duplicate", "is_updated"}
    | _DOC_COLLECTION_DEDUP_KEYS.keys()
    | _HISTORY_COLLECTIONS
    | _SET_COLLECTIONS
)

_NONE_SCALARS = {None, "", "None", "none", "null", "NA", "[]", "{}"}


def _is_none(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (list, dict, set, tuple)):
        return len(value) == 0
    if isinstance(value, str):
        return value in _NONE_SCALARS
    return False


def _row_ts(row: dict) -> datetime:
    """Tiebreak timestamp: last_updated → last_crawled_date → retrieved_on."""
    for col in ("last_updated", "last_crawled_date", "retrieved_on"):
        ts = row.get(col)
        if ts is not None:
            return ts if ts.tzinfo else ts.replace(tzinfo=UTC)
    return _LEGACY_DT


def _stable_dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _equal(a: Any, b: Any) -> bool:
    if _is_none(a) and _is_none(b):
        return True
    if _is_none(a) or _is_none(b):
        return False
    if isinstance(a, (dict, list)) or isinstance(b, (dict, list)):
        return _stable_dump(a) == _stable_dump(b)
    return a == b


def _coerce_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []



# ── Row classification ────────────────────────────────────────────────────────


def classify_row(row: dict) -> tuple[str, str | None]:
    """Return ('prod' | 'legacy' | 'anomaly', reason).

    A row is 'prod' if its key matches ``siphash24(name + reg + promoter)``
    computed from the row's own current values, 'legacy' if it matches
    ``siphash24(reg)``, else 'anomaly' (e.g. manually-renamed prod row whose
    key no longer reproduces from current fields).
    """
    reg_no = (row.get("project_registration_no") or "").strip()
    if not reg_no:
        return ("anomaly", "missing_reg_no")
    name = row.get("project_name") or ""
    prom = row.get("promoter_name") or ""
    if name and prom:
        prod_k = generate_project_key(name + reg_no + prom)
        if row["key"] == prod_k:
            return ("prod", None)
    legacy_k = generate_project_key(reg_no)
    if row["key"] == legacy_k:
        return ("legacy", None)
    return ("anomaly", "key_unmatched")


# ── Merge planning ────────────────────────────────────────────────────────────


def _pick_latest(rows: list[dict], col: str) -> tuple[Any, str]:
    """Last-write-wins over rows for a single scalar/JSONB column.

    Returns (chosen_value, source_label). source_label identifies which row
    contributed the value: 'winner', 'loser:<key>', 'equal', 'neither'.
    """
    present = [(r, r.get(col)) for r in rows if not _is_none(r.get(col))]
    if not present:
        return (None, "neither")
    if len(present) == 1:
        r, v = present[0]
        return (v, "winner" if r is rows[0] else f"loser:{r['key']}")
    first = present[0][1]
    if all(_equal(first, v) for _, v in present):
        return (first, "equal")
    present.sort(key=lambda rv: _row_ts(rv[0]), reverse=True)
    top_row, top_val = present[0]
    return (top_val, "winner-latest" if top_row is rows[0] else f"loser-latest:{top_row['key']}")


def _union_collection(rows: list[dict], col: str, dedup_keys: tuple[str, ...] | None) -> list | None:
    """Union items across rows for a JSONB-array column, deduped.

    ``dedup_keys``: ordered candidate dict keys for collapse. The first present
    key with a non-empty value is used as the dedup marker; falls back to a
    stable JSON dump of the whole item.
    """
    seen: set[str] = set()
    out: list = []
    for r in rows:
        for item in _coerce_list(r.get(col)):
            marker: str | None = None
            if dedup_keys and isinstance(item, dict):
                for k in dedup_keys:
                    v = item.get(k)
                    if v:
                        marker = f"{k}:{v}"
                        break
            if marker is None:
                marker = _stable_dump(item)
            if marker in seen:
                continue
            seen.add(marker)
            out.append(item)
    return out or None


def _union_set(rows: list[dict], col: str) -> list | None:
    seen: set = set()
    out: list = []
    for r in rows:
        for item in _coerce_list(r.get(col)):
            if item in seen:
                continue
            seen.add(item)
            out.append(item)
    return out or None


def build_merge_plan(
    winner: dict,
    losers: list[dict],
    skip_docs: bool = False,
) -> tuple[dict, list[dict]]:
    """Compute the merged row and a list of per-field diff records.

    ``winner`` is the surviving prod-key row; ``losers`` are the rows that will
    be deleted. The merged dict is a shallow override of the winner's columns
    (so unchanged columns are not rewritten). When ``skip_docs`` is true,
    columns in ``_SKIP_DOC_FIELDS`` are not touched on the winner — useful
    when the post-merge plan is to purge S3 and recrawl those columns.
    """
    rows = [winner, *losers]
    merged: dict = {}
    diffs: list[dict] = []

    skip = _SPECIAL_FIELDS | (_SKIP_DOC_FIELDS if skip_docs else set())

    for col in PROJECT_COLUMNS:
        if col in skip:
            continue
        chosen, source = _pick_latest(rows, col)
        if source in ("neither", "equal"):
            continue
        merged[col] = chosen
        if source != "winner":
            for loser in losers:
                diffs.append({
                    "field": col,
                    "source": source,
                    "winner_value": _stable_dump(winner.get(col)),
                    "loser_key": loser["key"],
                    "loser_value": _stable_dump(loser.get(col)),
                    "chosen_value": _stable_dump(chosen),
                })

    # Bespoke handlers
    for col, dedup_keys in _DOC_COLLECTION_DEDUP_KEYS.items():
        if skip_docs and col in _SKIP_DOC_FIELDS:
            continue
        unioned = _union_collection(rows, col, dedup_keys)
        if unioned is not None and not _equal(unioned, winner.get(col)):
            merged[col] = unioned
    for col in _HISTORY_COLLECTIONS:
        unioned = _union_collection(rows, col, None)
        if unioned is not None and not _equal(unioned, winner.get(col)):
            merged[col] = unioned
    for col in _SET_COLLECTIONS:
        if skip_docs and col in _SKIP_DOC_FIELDS:
            continue
        unioned = _union_set(rows, col)
        if unioned is not None and not _equal(unioned, winner.get(col)):
            merged[col] = unioned

    # Bookkeeping timestamps
    last_updated_vals = [r.get("last_updated") for r in rows if r.get("last_updated")]
    if last_updated_vals:
        merged["last_updated"] = max(last_updated_vals)
    last_crawled_vals = [r.get("last_crawled_date") for r in rows if r.get("last_crawled_date")]
    if last_crawled_vals:
        merged["last_crawled_date"] = max(last_crawled_vals)
    retrieved_vals = [r.get("retrieved_on") for r in rows if r.get("retrieved_on")]
    if retrieved_vals:
        merged["retrieved_on"] = min(retrieved_vals)

    merged["is_duplicate"] = False  # cluster collapsed
    merged["is_updated"] = any(r.get("is_updated") for r in rows)

    return merged, diffs


# ── Document cleanup ──────────────────────────────────────────────────────────


def build_doc_plan(
    winner_key: str,
    loser_keys: list[str],
    docs_by_key: dict[str, list[dict]],
) -> tuple[list[int], list[int]]:
    """Plan document cleanup for the cluster.

    Returns (ids_to_repoint, ids_to_delete). Per user direction, the legacy
    (loser) reg-only document rows are simply deleted rather than re-pointed to
    the winner; the winner keeps its own documents untouched. ``repoint`` is
    therefore always empty (kept for report/apply-path compatibility).
    """
    repoint: list[int] = []
    delete: list[int] = []
    for lk in loser_keys:
        for d in docs_by_key.get(lk, []):
            delete.append(d["id"])
    return repoint, delete



# ── Cluster planning ──────────────────────────────────────────────────────────


def plan_cluster(
    reg_no: str,
    rows: list[dict],
    docs_by_key: dict[str, list[dict]],
    skip_docs: bool = False,
) -> dict:
    """Decide the action for a single registration-number cluster.

    Result dict keys:
        status            one of: 'already_clean', 'will_merge', 'rekey_only',
                          'anomaly_no_winner', 'anomaly_multiple_winners',
                          'anomaly_classification'
        winner_key        surviving key (or candidate prod-key for rekey case)
        loser_keys        keys that will be deleted
        merged            column dict to UPDATE on the winner (apply mode)
        diffs             per-field diff records
        docs_repoint      doc.id list to repoint to winner
        docs_delete       doc.id list to delete as collisions
        anomaly_reasons   list of per-row classification reasons (anomalies)
    """
    classified = [(r, *classify_row(r)) for r in rows]
    prod_rows  = [r for r, k, _ in classified if k == "prod"]
    legacy_rows = [r for r, k, _ in classified if k == "legacy"]
    anomalies  = [(r, why) for r, k, why in classified if k == "anomaly"]

    out: dict = {
        "reg_no": reg_no, "status": "", "winner_key": None,
        "loser_keys": [], "merged": {}, "diffs": [],
        "docs_repoint": [], "docs_delete": [],
        "anomaly_reasons": [{"key": r["key"], "reason": why} for r, why in anomalies],
    }

    if len(prod_rows) > 1:
        out["status"] = "anomaly_multiple_winners"
        out["winner_key"] = prod_rows[0]["key"]
        out["loser_keys"] = [r["key"] for r in prod_rows[1:] + legacy_rows]
        return out

    if not prod_rows and not legacy_rows:
        out["status"] = "anomaly_classification"
        return out

    if not prod_rows and legacy_rows:
        # Singleton (or multi-legacy) on the wrong key — rekey to prod formula.
        # We can only compute the prod-key from the row's own name/promoter.
        candidate = legacy_rows[0]
        name = candidate.get("project_name") or ""
        prom = candidate.get("promoter_name") or ""
        if not name or not prom:
            out["status"] = "anomaly_classification"
            out["anomaly_reasons"].append(
                {"key": candidate["key"], "reason": "rekey_blocked_missing_name_or_promoter"}
            )
            return out
        new_key = generate_project_key(name + reg_no + prom)
        out["status"] = "rekey_only"
        out["winner_key"] = new_key
        out["loser_keys"] = [r["key"] for r in legacy_rows]
        return out

    winner = prod_rows[0]
    if not legacy_rows:
        out["status"] = "already_clean"
        out["winner_key"] = winner["key"]
        return out

    merged, diffs = build_merge_plan(winner, legacy_rows, skip_docs=skip_docs)
    loser_keys = [r["key"] for r in legacy_rows]
    if skip_docs:
        repoint, delete = [], []
    else:
        repoint, delete = build_doc_plan(winner["key"], loser_keys, docs_by_key)

    out.update({
        "status": "will_merge",
        "winner_key": winner["key"],
        "loser_keys": loser_keys,
        "merged": merged,
        "diffs": diffs,
        "docs_repoint": repoint,
        "docs_delete": delete,
    })
    return out



# ── Apply (writes to DB) ──────────────────────────────────────────────────────


def apply_plan(conn, plan: dict) -> None:
    """Execute one cluster plan inside a single transaction."""
    status = plan["status"]
    if status not in ("will_merge", "rekey_only"):
        return  # already_clean and anomalies are no-ops at apply time

    with conn.transaction():
        if status == "rekey_only":
            legacy_key = plan["loser_keys"][0]
            new_key = plan["winner_key"]
            conn.execute(
                "UPDATE rera_projects SET key = %s WHERE key = %s",
                (new_key, legacy_key),
            )
            conn.execute(
                "UPDATE rera_project_documents SET project_key = %s WHERE project_key = %s",
                (new_key, legacy_key),
            )
            return

        # will_merge
        winner_key = plan["winner_key"]
        merged = plan["merged"]
        if merged:
            cols = list(merged.keys())
            assignments = SQL(", ").join(
                SQL("{} = %s").format(Identifier(c)) for c in cols
            )
            sql = SQL("UPDATE rera_projects SET {a} WHERE key = %s").format(a=assignments)
            values = [_db_value(merged[c], c) for c in cols]
            values.append(winner_key)
            conn.execute(sql, values)

        for doc_id in plan["docs_delete"]:
            conn.execute("DELETE FROM rera_project_documents WHERE id = %s", (doc_id,))
        for doc_id in plan["docs_repoint"]:
            conn.execute(
                "UPDATE rera_project_documents SET project_key = %s WHERE id = %s",
                (winner_key, doc_id),
            )
        for loser_key in plan["loser_keys"]:
            conn.execute("DELETE FROM rera_projects WHERE key = %s", (loser_key,))


# ── DB reads ──────────────────────────────────────────────────────────────────


def fetch_bihar_projects(conn, reg_no: str | None = None) -> list[dict]:
    """Fetch all Bihar project rows, optionally filtered to one registration number.

    The ``reg_no`` filter is matched after trimming; both DB and supplied values
    are compared with surrounding whitespace stripped so a paste with stray
    spaces still matches.
    """
    if reg_no:
        return conn.execute(
            "SELECT * FROM rera_projects "
            "WHERE (LOWER(state) = 'bihar' OR LOWER(domain) LIKE %s) "
            "  AND TRIM(project_registration_no) = TRIM(%s)",
            ("%bihar%", reg_no),
        ).fetchall()
    return conn.execute(
        "SELECT * FROM rera_projects WHERE LOWER(state) = 'bihar' OR LOWER(domain) LIKE %s",
        ("%bihar%",),
    ).fetchall()


def fetch_reg_no_for_key(conn, key: str) -> str | None:
    row = conn.execute(
        "SELECT project_registration_no FROM rera_projects WHERE key = %s",
        (key,),
    ).fetchone()
    if row is None:
        return None
    return (row.get("project_registration_no") or "").strip() or None


def fetch_bihar_documents(conn, keys: list[str]) -> dict[str, list[dict]]:
    if not keys:
        return {}
    rows = conn.execute(
        "SELECT id, project_key, original_url FROM rera_project_documents "
        "WHERE project_key = ANY(%s)",
        (keys,),
    ).fetchall()
    grouped: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        grouped[r["project_key"]].append(r)
    return grouped



# ── Report writers ────────────────────────────────────────────────────────────


def write_reports(plans: list[dict], output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "clusters":   output_dir / "bihar_dedup_clusters.csv",
        "field_diffs": output_dir / "bihar_dedup_field_diffs.csv",
        "anomalies":  output_dir / "bihar_dedup_anomalies.csv",
    }

    with paths["clusters"].open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow([
            "reg_no", "status", "winner_key", "loser_keys",
            "n_field_diffs", "n_docs_repoint", "n_docs_delete",
            "n_anomaly_reasons",
        ])
        for p in plans:
            w.writerow([
                p["reg_no"], p["status"], p["winner_key"] or "",
                ";".join(p["loser_keys"]),
                len(p["diffs"]), len(p["docs_repoint"]), len(p["docs_delete"]),
                len(p["anomaly_reasons"]),
            ])

    with paths["field_diffs"].open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow([
            "reg_no", "winner_key", "loser_key", "field", "source",
            "winner_value", "loser_value", "chosen_value",
        ])
        for p in plans:
            for d in p["diffs"]:
                w.writerow([
                    p["reg_no"], p["winner_key"], d["loser_key"], d["field"],
                    d["source"], d["winner_value"], d["loser_value"], d["chosen_value"],
                ])

    with paths["anomalies"].open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["reg_no", "status", "key", "reason"])
        for p in plans:
            for a in p["anomaly_reasons"]:
                w.writerow([p["reg_no"], p["status"], a["key"], a["reason"]])

    return paths


def summarize(plans: list[dict]) -> dict[str, int]:
    summary: dict[str, int] = defaultdict(int)
    for p in plans:
        summary[p["status"]] += 1
        summary["field_diffs_total"] += len(p["diffs"])
        summary["docs_repoint_total"] += len(p["docs_repoint"])
        summary["docs_delete_total"] += len(p["docs_delete"])
        summary["anomaly_reasons_total"] += len(p["anomaly_reasons"])
    return dict(summary)



# ── Entrypoint ────────────────────────────────────────────────────────────────


def build_plans(
    conn,
    reg_no: str | None = None,
    skip_docs: bool = False,
) -> list[dict]:
    projects = fetch_bihar_projects(conn, reg_no=reg_no)
    by_reg: dict[str, list[dict]] = defaultdict(list)
    for r in projects:
        reg_no = (r.get("project_registration_no") or "").strip()
        by_reg[reg_no].append(r)

    all_keys = [r["key"] for r in projects]
    docs_by_key = fetch_bihar_documents(conn, all_keys)

    plans: list[dict] = []
    for reg_no, rows in sorted(by_reg.items()):
        if not reg_no:
            for row in rows:
                plans.append({
                    "reg_no": "", "status": "anomaly_classification",
                    "winner_key": None, "loser_keys": [], "merged": {},
                    "diffs": [], "docs_repoint": [], "docs_delete": [],
                    "anomaly_reasons": [{"key": row["key"], "reason": "missing_reg_no"}],
                })
            continue
        plans.append(plan_cluster(reg_no, rows, docs_by_key, skip_docs=skip_docs))
    return plans


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Consolidate Bihar duplicate rows in rera_projects.")
    parser.add_argument("--apply", action="store_true",
                        help="Execute merges. Without this flag the script is read-only.")
    parser.add_argument("--output-dir", default="bihar_dedup_reports",
                        help="Directory for CSV reports (default: ./bihar_dedup_reports).")
    parser.add_argument("--limit", type=int, default=0,
                        help="Optional cap on cluster count (0 = no limit).")
    parser.add_argument("--skip-docs", action="store_true",
                        help="Skip merging document/image URL columns "
                             "(uploaded_documents, document_urls, doc_ocr_url, "
                             "project_images, detail_images, lister_images, images) "
                             "and skip all rera_project_documents repoint/delete ops. "
                             "Use when the post-merge plan is to purge the legacy "
                             "S3 prefix and recrawl the docs from scratch.")
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--reg-no", default=None,
                        help="Process only the cluster matching this project_registration_no "
                             "(safe way to validate the merge on one project before a full run).")
    target.add_argument("--key", default=None,
                        help="Process only the cluster containing this rera_projects.key. "
                             "The reg-no is resolved from the row, then the cluster is rebuilt.")
    args = parser.parse_args(argv)

    conn = get_connection()

    target_reg_no: str | None = args.reg_no.strip() if args.reg_no else None
    if args.key:
        target_reg_no = fetch_reg_no_for_key(conn, args.key.strip())
        if not target_reg_no:
            print(f"No rera_projects row found with key={args.key!r}", file=sys.stderr)
            return 2
        print(f"Resolved key={args.key} → project_registration_no={target_reg_no!r}")

    plans = build_plans(conn, reg_no=target_reg_no, skip_docs=args.skip_docs)
    if target_reg_no and not plans:
        print(f"No Bihar rows found with project_registration_no={target_reg_no!r}", file=sys.stderr)
        return 2
    if args.limit:
        plans = plans[: args.limit]

    paths = write_reports(plans, Path(args.output_dir))
    summary = summarize(plans)

    print("Bihar dedup plan summary")
    print("-" * 40)
    if args.skip_docs:
        print("  mode                           skip-docs (doc/image fields untouched,")
        print("                                  rera_project_documents not modified)")
    for k in sorted(summary):
        print(f"  {k:30s} {summary[k]}")
    print("-" * 40)
    for label, path in paths.items():
        print(f"  report.{label:11s} {path}")

    if not args.apply:
        print("\nDry run only — no changes applied. Re-run with --apply to execute.")
        return 0

    print(f"\nApplying {summary.get('will_merge', 0)} merges "
          f"and {summary.get('rekey_only', 0)} rekeys...")
    applied = 0
    for plan in plans:
        if plan["status"] in ("will_merge", "rekey_only"):
            apply_plan(conn, plan)
            applied += 1
    print(f"Applied {applied} cluster plans.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
