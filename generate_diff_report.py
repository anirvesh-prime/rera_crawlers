#!/usr/bin/env python3
"""
generate_diff_report.py

Compares dry_run_outputs/<state>.json against state_projects_sample/<state>.json
and prints a structured field-level diff report for all states.
"""
from __future__ import annotations
import json
from pathlib import Path

DRY_DIR    = Path("dry_run_outputs")
SAMPLE_DIR = Path("state_projects_sample")

# Fields to skip in diff (internal/meta fields not meaningful for comparison)
SKIP_FIELDS = {"key", "_doc_links", "_project_images", "_canonical_url", "run_id",
               "config_id", "crawl_id", "uploaded_documents", "document_urls"}

# Fields where we only check presence/absence (not value) — URLs, S3 links etc.
PRESENCE_ONLY = {"cert_url", "preview_pdf_url", "print_preview_url", "source_url"}


def _norm(v):
    """Normalize a value for comparison: strip whitespace from strings."""
    if isinstance(v, str):
        return v.strip()
    return v


def _is_empty(v):
    return v in (None, "", [], {})


def _diff_values(field, sample_val, dry_val):
    """Return a human-readable diff string, or None if equivalent."""
    sv = _norm(sample_val)
    dv = _norm(dry_val)

    if field in PRESENCE_ONLY:
        s_present = not _is_empty(sv)
        d_present = not _is_empty(dv)
        if s_present == d_present:
            return None
        return f"  [{field}]  sample={'present' if s_present else 'absent'}  dry={'present' if d_present else 'absent'}"

    if sv == dv:
        return None
    if _is_empty(sv) and _is_empty(dv):
        return None

    # For complex types show truncated JSON
    def _fmt(x):
        s = json.dumps(x, ensure_ascii=False, default=str)
        return s if len(s) <= 120 else s[:117] + "…"

    return f"  [{field}]\n    SAMPLE: {_fmt(sv)}\n    DRY:    {_fmt(dv)}"


def compare_state(state_key: str) -> dict:
    dry_path    = DRY_DIR / f"{state_key}.json"
    sample_path = SAMPLE_DIR / f"{state_key}.json"

    result = {"state": state_key, "status": "", "diffs": [], "notes": []}

    if not sample_path.exists():
        result["status"] = "NO_SAMPLE"
        return result

    sample = json.loads(sample_path.read_text())

    if not dry_path.exists():
        result["status"] = "NO_DRY_OUTPUT"
        result["notes"].append("Dry run produced no output file")
        return result

    dry_raw = json.loads(dry_path.read_text())
    dry = dry_raw[0] if isinstance(dry_raw, list) else dry_raw

    # Check if it looks like a "no projects captured" stub
    if dry.get("note") == "no projects captured":
        result["status"] = "CAPTURED_NOTHING"
        result["notes"].append(dry.get("note", ""))
        return result

    result["status"] = "COMPARED"

    all_fields = sorted(set(sample) | set(dry))
    for field in all_fields:
        if field in SKIP_FIELDS:
            continue
        sample_val = sample.get(field)
        dry_val    = dry.get(field)
        diff = _diff_values(field, sample_val, dry_val)
        if diff:
            result["diffs"].append(diff)

    return result


def main():
    states = sorted(
        p.stem for p in SAMPLE_DIR.glob("*.json")
    )

    print("=" * 70)
    print("  DRY RUN vs. SAMPLE — FIELD DIFF REPORT")
    print("=" * 70)

    summary_ok, summary_diffs, summary_fail = [], [], []

    for state in states:
        r = compare_state(state)
        status = r["status"]

        if status in ("NO_DRY_OUTPUT", "CAPTURED_NOTHING"):
            summary_fail.append(state)
        elif r["diffs"]:
            summary_diffs.append(state)
        else:
            summary_ok.append(state)

        print(f"\n{'─'*70}")
        print(f"  STATE: {state.upper().replace('_',' ')}  [{status}]")
        if r["notes"]:
            for n in r["notes"]:
                print(f"  NOTE: {n}")
        if r["diffs"]:
            print(f"  {len(r['diffs'])} field(s) differ:")
            for d in r["diffs"]:
                print(d)
        elif status == "COMPARED":
            print("  ✓  All compared fields match sample.")

    print(f"\n{'='*70}")
    print("  SUMMARY")
    print(f"{'='*70}")
    print(f"  ✓  Clean match      ({len(summary_ok)}):  {', '.join(summary_ok) or '—'}")
    print(f"  ⚠  Has diffs        ({len(summary_diffs)}):  {', '.join(summary_diffs) or '—'}")
    print(f"  ✗  Failed/no output ({len(summary_fail)}):  {', '.join(summary_fail) or '—'}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
