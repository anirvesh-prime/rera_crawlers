#!/usr/bin/env python3
"""RERA Crawlers monitoring dashboard.

Usage:
    python dashboard.py                   # http://127.0.0.1:8080
    python dashboard.py --host 0.0.0.0   # bind to all interfaces (direct access)
    python dashboard.py --port 9090       # custom port

On your local machine (SSH tunnel):
    ssh -L 8080:localhost:8080 user@server
    # then open http://localhost:8080
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, render_template_string

load_dotenv()

# ── psycopg v3 (already in requirements.txt) ──────────────────────────────────
try:
    import psycopg
    from psycopg.rows import dict_row as _dict_row
    _HAS_DB = True
except ImportError:
    _HAS_DB = False

# ── Sites list ────────────────────────────────────────────────────────────────
from sites_config import SITES  # noqa: E402

_SITES = [{"id": s["id"], "name": s["name"]} for s in SITES]
_SITE_IDS = {s["id"] for s in SITES}
_LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))

app = Flask(__name__)


# ── Message helpers ───────────────────────────────────────────────────────────

# Logger prepends "[site_id] [step] [reg=X] [key=X]" to every message.
# We strip those bracket-prefixes so the dashboard shows the clean reason.
_PREFIX_RE = re.compile(r"^(?:\[[^\]]*\]\s*)+")

def _clean_msg(msg: str) -> str:
    """Strip leading [tag] prefixes added by CrawlerLogger from a log message."""
    return _PREFIX_RE.sub("", msg).strip() if msg else msg


# ── DB helpers ────────────────────────────────────────────────────────────────

def _db_conn():
    if not _HAS_DB:
        return None
    try:
        return psycopg.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "rera_crawlers"),
            user=os.getenv("POSTGRES_USER", ""),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            connect_timeout=5,
            row_factory=_dict_row,
        )
    except Exception:
        return None


def _fetch_db():
    conn = _db_conn()
    if conn is None:
        return None
    try:
        with conn:
            cur = conn.cursor()

            # 1. Latest run per site
            cur.execute("""
                SELECT DISTINCT ON (site_id)
                    id, site_id, run_type, started_at, finished_at, status,
                    projects_found, projects_new, projects_updated, projects_skipped,
                    documents_uploaded, error_count, sentinel_passed
                FROM crawl_runs
                ORDER BY site_id, started_at DESC
            """)
            latest_runs = {r["site_id"]: dict(r) for r in cur.fetchall()}

            if not latest_runs:
                return {
                    "latest_runs": {}, "sentinel_data": {},
                    "errors_by_site": {}, "orch_info": {}, "source": "database",
                }

            # 2. Most-recent orchestrator window (±4 h around the latest started_at)
            max_start = max(r["started_at"] for r in latest_runs.values())
            window_start = max_start - timedelta(hours=4)
            cur.execute(
                "SELECT id FROM crawl_runs WHERE started_at >= %s",
                (window_start,),
            )
            recent_ids = [r["id"] for r in cur.fetchall()]

            # 3. Sentinel log entries for that window
            sentinel_data: dict = {}
            if recent_ids:
                cur.execute(
                    """
                    SELECT cl.site_id, cl.extra, cl.message, cr.sentinel_passed
                    FROM crawl_logs cl
                    JOIN crawl_runs cr ON cl.run_id = cr.id
                    WHERE cl.step = 'sentinel' AND cl.run_id = ANY(%s)
                    ORDER BY cl.logged_at DESC
                    """,
                    (recent_ids,),
                )
                for row in cur.fetchall():
                    sid = row["site_id"]
                    if sid in sentinel_data:
                        continue
                    extra = row.get("extra") or {}
                    sentinel_data[sid] = {
                        "passed": row["sentinel_passed"],
                        "covered": extra.get("covered"),
                        "expected": extra.get("expected"),
                        "missing_fields": extra.get("missing_fields") or [],
                        "coverage_ratio": extra.get("coverage_ratio"),
                        "message": row.get("message") or "",
                    }
            # Backfill sentinel_passed from crawl_runs for sites with no log entry
            for sid, run in latest_runs.items():
                if sid not in sentinel_data and run.get("sentinel_passed") is not None:
                    sentinel_data[sid] = {
                        "passed": run["sentinel_passed"],
                        "covered": None, "expected": None,
                        "missing_fields": [], "coverage_ratio": None, "message": "",
                    }

            # 4. Latest error per site with step + extra context
            errors_by_site: dict = {}
            if recent_ids:
                cur.execute(
                    """
                    SELECT DISTINCT ON (site_id) site_id, message, step, extra, traceback
                    FROM crawl_logs
                    WHERE level = 'ERROR' AND run_id = ANY(%s)
                    ORDER BY site_id, logged_at DESC
                    """,
                    (recent_ids,),
                )
                for row in cur.fetchall():
                    sid = row["site_id"]
                    errors_by_site[sid] = {
                        "message": _clean_msg(row["message"] or ""),
                        "step": row.get("step") or "",
                        "extra": row.get("extra") or {},
                        "traceback": row.get("traceback") or "",
                    }

            # 4b. Fallback: for sites whose error_count > 0 but have no crawl_logs
            #     ERROR row (e.g. process crashed before the buffer could flush),
            #     pull the detail from crawl_errors — which is committed immediately.
            need_error = [
                sid for sid, r in latest_runs.items()
                if (r.get("error_count") or 0) > 0 and sid not in errors_by_site
            ]
            if need_error and recent_ids:
                cur.execute(
                    """
                    SELECT DISTINCT ON (site_id)
                        site_id, error_message, error_type, raw_data
                    FROM crawl_errors
                    WHERE site_id = ANY(%s) AND run_id = ANY(%s)
                    ORDER BY site_id, occurred_at DESC
                    """,
                    (need_error, recent_ids),
                )
                for row in cur.fetchall():
                    sid = row["site_id"]
                    raw = row.get("raw_data") or {}
                    errors_by_site[sid] = {
                        "message": row.get("error_message") or "",
                        "step": row.get("error_type") or "",
                        "extra": {},
                        "traceback": raw.get("traceback") or "",
                    }

            # 5. Compute elapsed_s for each run from started_at / finished_at;
            #    always set the key so the template never hits UndefinedError.
            for run in latest_runs.values():
                run.setdefault("elapsed_s", None)
                if run["elapsed_s"] is None:
                    sa = run.get("started_at")
                    fa = run.get("finished_at")
                    if sa and fa:
                        try:
                            run["elapsed_s"] = (fa - sa).total_seconds()
                        except Exception:
                            pass

            # 6. Orchestrator-level summary (aggregate of the window)
            totals_keys = (
                "projects_found", "projects_new", "projects_updated",
                "projects_skipped", "documents_uploaded", "error_count",
            )
            totals = {k: sum((r.get(k) or 0) for r in latest_runs.values()) for k in totals_keys}
            most_recent = max(latest_runs.values(), key=lambda r: r["started_at"])
            orch_info = {
                "mode": most_recent.get("run_type", "unknown"),
                "started": max_start,
                "totals": totals,
            }

        return {
            "latest_runs": latest_runs,
            "sentinel_data": sentinel_data,
            "errors_by_site": errors_by_site,
            "orch_info": orch_info,
            "source": "database",
        }
    except Exception:
        return None
    finally:
        conn.close()


# ── Log-file fallback ─────────────────────────────────────────────────────────

def _iter_site_jsonl_files():
    """Yield (site_id, Path) for the most-recent .jsonl file per site.

    Handles two layouts:
      New:    logs/{site_id}/{timestamp}.jsonl
      Legacy: logs/{timestamp}_{site_id}.jsonl  (flat root dir)
    """
    seen: set = set()
    # New layout — subdirectories named after site IDs
    if _LOG_DIR.exists():
        for site_dir in _LOG_DIR.iterdir():
            if not site_dir.is_dir() or site_dir.name == "orchestrator":
                continue
            if site_dir.name not in _SITE_IDS:
                continue
            files = sorted(site_dir.glob("*.jsonl"), reverse=True)
            if files:
                yield site_dir.name, files[0]
                seen.add(site_dir.name)
        # Legacy flat layout — root-level .jsonl files named {ts}_{site_id}.jsonl
        for f in sorted(_LOG_DIR.glob("*.jsonl"), reverse=True):
            matched = next(
                (sid for sid in _SITE_IDS if sid not in seen and f"_{sid}" in f.stem),
                None,
            )
            if matched:
                yield matched, f
                seen.add(matched)


def _fetch_logs():
    """Fallback data source: read orchestrator summary JSON + per-site .jsonl files."""
    latest_runs: dict = {}
    sentinel_data: dict = {}
    errors_by_site: dict = {}
    orch_info: dict = {}

    # ── Orchestrator summary: new path (logs/orchestrator/) ──────────────────
    orch_dir = _LOG_DIR / "orchestrator"
    if orch_dir.exists():
        summaries = sorted(orch_dir.glob("*.json"), reverse=True)
        if summaries:
            try:
                data = json.loads(summaries[0].read_text())
                orch_info = {
                    "mode": data.get("mode", "unknown"),
                    "started": data.get("started"),
                    "totals": data.get("totals", {}),
                }
                for site in data.get("sites", []):
                    _add_summary_run(latest_runs, site, data.get("mode", "unknown"), data.get("started"))
            except Exception:
                pass

    # ── Orchestrator summary: legacy path (logs/*_orchestrator_summary.json) ─
    if not orch_info and _LOG_DIR.exists():
        flat = sorted(_LOG_DIR.glob("*orchestrator_summary.json"), reverse=True)
        if flat:
            try:
                data = json.loads(flat[0].read_text())
                orch_info = {
                    "mode": data.get("mode", "unknown"),
                    "started": data.get("started"),
                    "totals": data.get("totals", {}),
                }
                for site in data.get("sites", []):
                    _add_summary_run(latest_runs, site, data.get("mode", "unknown"), data.get("started"))
            except Exception:
                pass

    # ── Per-site .jsonl files: extract sentinel + error entries ──────────────
    for sid, jsonl_path in _iter_site_jsonl_files():
        try:
            lines = jsonl_path.read_text().strip().splitlines()
            for line in reversed(lines):
                if not line.strip():
                    continue
                entry = json.loads(line)
                if entry.get("step") == "sentinel" and sid not in sentinel_data:
                    extra = entry.get("extra") or {}
                    covered = extra.get("covered")
                    expected = extra.get("expected")
                    passed: bool | None = None
                    if covered is not None and expected and expected > 0:
                        passed = (covered / expected) >= 0.80
                    sentinel_data[sid] = {
                        "covered": covered,
                        "expected": expected,
                        "passed": passed,
                        "missing_fields": extra.get("missing_fields") or [],
                        "coverage_ratio": extra.get("coverage_ratio"),
                        "message": entry.get("message") or "",
                    }
                if entry.get("level") == "ERROR" and sid not in errors_by_site:
                    err_extra = entry.get("extra") or {}
                    errors_by_site[sid] = {
                        "message": _clean_msg(entry.get("message", "")),
                        "step": entry.get("step") or "",
                        "extra": err_extra,
                        "traceback": entry.get("traceback") or "",
                    }
                if sid in sentinel_data and sid in errors_by_site:
                    break
        except Exception:
            pass

    return {
        "latest_runs": latest_runs,
        "sentinel_data": sentinel_data,
        "errors_by_site": errors_by_site,
        "orch_info": orch_info,
        "source": "log files",
    }


def _add_summary_run(runs: dict, site: dict, mode: str, started: str | None) -> None:
    """Parse one site entry from an orchestrator summary JSON into a run dict."""
    sid = site.get("site_id")
    if not sid:
        return
    elapsed = site.get("elapsed_s")
    runs[sid] = {
        "site_id": sid,
        "run_type": mode,
        "started_at": started,
        "finished_at": None,
        "elapsed_s": elapsed,
        # A run is only "failed" if it never completed; errors within a completed
        # run keep status="completed" — the dashboard flags them separately.
        "status": "completed",
        "projects_found": site.get("projects_found", 0),
        "projects_new": site.get("projects_new", 0),
        "projects_updated": site.get("projects_updated", 0),
        "projects_skipped": site.get("projects_skipped", 0),
        "documents_uploaded": site.get("documents_uploaded", 0),
        "error_count": site.get("error_count", 0),
        "sentinel_passed": None,
    }


def _get_data() -> dict:
    result = _fetch_db()
    if result is None:
        result = _fetch_logs()
    return result or {}


# ── HTML template ─────────────────────────────────────────────────────────────

_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="60">
  <title>RERA Crawlers Dashboard</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
  <style>
    body { background:#0d1117; color:#e6edf3; font-family:system-ui,sans-serif; }
    .card { background:#161b22; border:1px solid #30363d; border-radius:8px; }
    .card-danger { border-color:#6e2424!important; }
    .table { color:#e6edf3; margin-bottom:0; }
    .table th { background:#21262d; color:#8b949e; font-size:.72rem;
                text-transform:uppercase; letter-spacing:.06em; border-color:#30363d; }
    .table td { border-color:#30363d; vertical-align:middle; padding:.4rem .5rem; }
    .table-hover tbody tr:hover td { background:rgba(255,255,255,.04); }
    .badge { font-size:.68rem; font-weight:600; padding:3px 7px; border-radius:4px; }
    .bg-pass  { background:#1f6335!important; color:#3fb950; }
    .bg-fail  { background:#4a1212!important; color:#f85149; }
    .bg-warn  { background:#5c3800!important; color:#d29922; }
    .bg-run   { background:#3d2e00!important; color:#d29922; }
    .bg-done  { background:#1f6335!important; color:#3fb950; }
    .bg-none  { background:#21262d!important; color:#8b949e; }
    .row-err  td { background:rgba(248,81,73,.07)!important; }
    .row-fail td { background:rgba(248,81,73,.13)!important; }
    .row-warn td { background:rgba(210,153,34,.05)!important; }
    .sec-title { color:#58a6ff; font-size:.8rem; font-weight:700;
                 text-transform:uppercase; letter-spacing:.1em; }
    .bar-wrap { width:60px; height:5px; background:#30363d;
                border-radius:3px; display:inline-block; vertical-align:middle; }
    .bar-fill { height:5px; border-radius:3px; }
    .err-msg  { font-size:.68rem; color:#f85149; opacity:.9; margin-top:3px; line-height:1.3; word-break:break-word; }
    .err-step { font-size:.62rem; color:#8b949e; font-family:monospace;
                background:#21262d; border-radius:3px; padding:1px 4px; margin-bottom:2px; display:inline-block; }
    .stat-box { border-right:1px solid #30363d; padding:0 1.4rem; }
    .stat-box:last-child { border-right:none; }
    .stat-val { font-size:1.35rem; font-weight:700; line-height:1; }
    .stat-lbl { font-size:.68rem; color:#8b949e; text-transform:uppercase; letter-spacing:.05em; }
    .hdr { background:#161b22; border-bottom:1px solid #30363d; padding:12px 24px; margin-bottom:20px; }
    .diag-block { background:#1c1010; border:1px solid #6e2424; border-radius:6px;
                  padding:.6rem .9rem; margin-bottom:.5rem; }
    .diag-site  { font-size:.82rem; font-weight:600; color:#f85149; }
    .diag-meta  { font-size:.7rem; color:#8b949e; margin-top:2px; }
    .diag-msg   { font-size:.75rem; color:#e6edf3; margin-top:4px; word-break:break-word;
                  background:#110d0d; border-left:3px solid #6e2424; padding:4px 8px;
                  border-radius:0 4px 4px 0; font-family:monospace; line-height:1.4; }
    .missing-pill { display:inline-block; font-size:.6rem; background:#3d2300; color:#d29922;
                    border-radius:3px; padding:1px 5px; margin:1px; }
    .sentinel-msg { font-size:.68rem; color:#d29922; margin-top:3px; font-style:italic; }
    .dur { font-size:.75rem; color:#8b949e; white-space:nowrap; }
    a.expand-toggle { font-size:.65rem; color:#58a6ff; text-decoration:none; cursor:pointer; }
  </style>
</head>
<body>
{# ── compute summary counts used throughout the page ──────────────────── #}
{% set sites_with_errors = [] %}
{% for site in sites %}{% set sid = site.id %}
  {% if sid in latest_runs %}{% set r = latest_runs[sid] %}
    {% if (r.error_count or 0) > 0 %}{% if sites_with_errors.append(site) %}{% endif %}{% endif %}
  {% endif %}
{% endfor %}
{% set n_sites = latest_runs | length %}
{% set n_errs  = sites_with_errors | length %}
{% set n_ok    = n_sites - n_errs %}

<div class="hdr d-flex justify-content-between align-items-center">
  <div>
    <span style="font-size:1.1rem;font-weight:700;">🏗️ RERA Crawlers Dashboard</span>
    <span class="ms-3" style="font-size:.8rem;color:#8b949e;">
      Source: <span class="text-warning">{{ data_source }}</span> &nbsp;·&nbsp; auto-refresh 60s
    </span>
    {% if n_sites %}
    <span class="ms-3">
      <span class="badge bg-done">{{ n_ok }} OK</span>
      {% if n_errs %}<span class="badge bg-fail ms-1">{{ n_errs }} w/ errors</span>{% endif %}
    </span>
    {% endif %}
  </div>
  <div style="font-size:.8rem;color:#8b949e;">
    Refreshed {{ now.strftime('%Y-%m-%d %H:%M:%S') }} UTC
  </div>
</div>

<div class="container-fluid px-4">

  {# ── Orchestrator run summary ──────────────────────────────────────────── #}
  {% if orch_info %}
  <div class="card p-3 mb-3">
    <div class="sec-title mb-3">⚙️ Most Recent Orchestrator Run</div>
    <div class="d-flex flex-wrap align-items-center gap-0">
      <div class="stat-box">
        <div class="stat-val">{{ (orch_info.mode or 'unknown') | replace('_',' ') | title }}</div>
        <div class="stat-lbl">Mode</div>
      </div>
      <div class="stat-box ps-3">
        <div class="stat-val" style="font-size:1rem;">
          {% if orch_info.started %}
            {% if orch_info.started is string %}{{ orch_info.started[:16] | replace('T',' ') }} UTC
            {% else %}{{ orch_info.started.strftime('%Y-%m-%d %H:%M') }} UTC{% endif %}
          {% else %}—{% endif %}
        </div>
        <div class="stat-lbl">Started</div>
      </div>
      {% set t = orch_info.totals %}
      {% if t %}
      <div class="stat-box ps-3">
        <div class="stat-val">{{ t.get('projects_found','—') }}</div>
        <div class="stat-lbl">Found</div>
      </div>
      <div class="stat-box ps-3">
        <div class="stat-val text-success">+{{ t.get('projects_new',0) }}</div>
        <div class="stat-lbl">New</div>
      </div>
      <div class="stat-box ps-3">
        <div class="stat-val" style="color:#58a6ff;">~{{ t.get('projects_updated',0) }}</div>
        <div class="stat-lbl">Updated</div>
      </div>
      <div class="stat-box ps-3">
        <div class="stat-val" style="color:#8b949e;">{{ t.get('projects_skipped',0) }}</div>
        <div class="stat-lbl">Skipped</div>
      </div>
      <div class="stat-box ps-3">
        <div class="stat-val" style="color:#8b949e;">{{ t.get('documents_uploaded',0) }}</div>
        <div class="stat-lbl">Docs</div>
      </div>
      <div class="stat-box ps-3">
        <div class="stat-val {% if t.get('error_count',0) > 0 %}text-danger{% endif %}">
          {{ t.get('error_count',0) }}</div>
        <div class="stat-lbl">Total Errors</div>
      </div>
      {% set total_elapsed = t.get('elapsed_s') %}
      {% if total_elapsed %}
      <div class="stat-box ps-3">
        <div class="stat-val" style="font-size:1rem;color:#8b949e;">
          {{ '%dm %ds' | format((total_elapsed // 60)|int, (total_elapsed % 60)|int) }}</div>
        <div class="stat-lbl">Total Duration</div>
      </div>
      {% endif %}
      {% endif %}
    </div>
  </div>
  {% endif %}

  {# ── Failures & Diagnostics ────────────────────────────────────────────── #}
  {% if sites_with_errors %}
  <div class="card card-danger p-3 mb-3">
    <div class="sec-title mb-3" style="color:#f85149;">🚨 Failures &amp; Diagnostics ({{ sites_with_errors|length }} site{{ 's' if sites_with_errors|length != 1 }})</div>
    {% for site in sites_with_errors %}{% set sid = site.id %}{% set r = latest_runs[sid] %}
    <div class="diag-block">
      <div class="d-flex justify-content-between align-items-start flex-wrap gap-1">
        <div>
          <span class="diag-site">{{ site.name }}</span>
          <span class="ms-2" style="font-size:.7rem;color:#8b949e;">
            {{ (r.run_type or '') | replace('_',' ') | upper }}
            {% if r.started_at %}&nbsp;·&nbsp;
              {% if r.started_at is string %}{{ r.started_at[:16]|replace('T',' ') }}
              {% else %}{{ r.started_at.strftime('%Y-%m-%d %H:%M') }}{% endif %} UTC
            {% endif %}
          </span>
        </div>
        <div>
          {% set errs = r.error_count or 0 %}
          <span class="badge bg-fail">{{ errs }} error{{ 's' if errs != 1 }}</span>
          {% set st = (r.status or '')|lower %}
          {% if st == 'running' %}<span class="badge bg-run ms-1">⟳ running</span>{% endif %}
        </div>
      </div>
      <div class="diag-meta mt-1">
        Found: {{ r.projects_found or 0 }} &nbsp;·&nbsp;
        New: {{ r.projects_new or 0 }} &nbsp;·&nbsp;
        Updated: {{ r.projects_updated or 0 }} &nbsp;·&nbsp;
        Skipped: {{ r.projects_skipped or 0 }} &nbsp;·&nbsp;
        Docs: {{ r.documents_uploaded or 0 }}
        {% set r_elapsed = r.get('elapsed_s') %}{% if r_elapsed %}&nbsp;·&nbsp; Duration: {{ '%dm %ds'|format((r_elapsed//60)|int, (r_elapsed%60)|int) }}{% endif %}
      </div>
      {% if sid in errors_by_site %}{% set e = errors_by_site[sid] %}
      <div class="mt-2">
        {% if e.step %}<span class="err-step">step: {{ e.step }}</span><br>{% endif %}
        <div class="diag-msg">{{ e.message }}</div>
        {% if e.traceback %}
        <details style="margin-top:4px;">
          <summary style="font-size:.65rem;color:#8b949e;cursor:pointer;">▶ traceback</summary>
          <pre style="font-size:.62rem;color:#8b949e;background:#0d1117;padding:6px;border-radius:4px;overflow-x:auto;margin-top:4px;white-space:pre-wrap;">{{ e.traceback[:1200] }}{% if e.traceback|length > 1200 %}\n… (truncated){% endif %}</pre>
        </details>
        {% endif %}
      </div>
      {% endif %}
      {% if sid in sentinel_data %}{% set s = sentinel_data[sid] %}
        {% if s.passed == false %}
        <div class="mt-1" style="font-size:.7rem;color:#d29922;">
          ⚠ Sentinel FAIL
          {% if s.covered is not none and s.expected %}— coverage {{ s.covered }}/{{ s.expected }}
            ({{ ((s.covered/s.expected*100)|int) }}%)
          {% endif %}
          {% if s.missing_fields %}&nbsp;· Missing: {% for f in s.missing_fields %}<span class="missing-pill">{{ f }}</span>{% endfor %}{% endif %}
        </div>
        {% endif %}
      {% endif %}
    </div>
    {% endfor %}
  </div>
  {% endif %}

  <div class="row g-3">

    {# ── Sentinel health — all states always listed ────────────────────────── #}
    <div class="col-12 col-xl-4">
      <div class="card p-3 h-100">
        <div class="sec-title mb-3">🔍 Sentinel Health (Most Recent Run)</div>
        <div class="table-responsive">
        <table class="table table-sm table-hover">
          <thead><tr><th>State</th><th>Status</th><th>Coverage</th><th>Issue</th></tr></thead>
          <tbody>
          {% for site in sites %}{% set sid = site.id %}
          {% if sid in sentinel_data %}{% set s = sentinel_data[sid] %}
            {% set cov = s.covered %}{% set exp = s.expected %}
            {% set pct = ((cov / exp * 100)|int) if (cov is not none and exp and exp > 0) else none %}
            <tr class="{% if s.passed == false %}row-fail{% elif s.passed == true %}{% endif %}">
              <td style="font-size:.8rem;white-space:nowrap;">{{ site.name }}</td>
              <td>
                {% if s.passed == true %}<span class="badge bg-pass">✓ PASS</span>
                {% elif s.passed == false %}<span class="badge bg-fail">✗ FAIL</span>
                {% else %}<span class="badge bg-none">— N/A</span>{% endif %}
              </td>
              <td>
                {% if pct is not none %}
                  <div class="d-flex align-items-center gap-1">
                    <span style="font-size:.72rem;">{{ cov }}/{{ exp }}</span>
                    <div class="bar-wrap"><div class="bar-fill" style="width:{{ [pct,100]|min }}%;background:
                      {% if pct >= 80 %}#3fb950{% elif pct >= 60 %}#d29922{% else %}#f85149{% endif %};"></div></div>
                    <span style="font-size:.72rem;color:#8b949e;">{{ pct }}%</span>
                  </div>
                {% else %}<span style="color:#8b949e;font-size:.8rem;">—</span>{% endif %}
              </td>
              <td>
                {% if s.missing_fields %}
                  {% for f in s.missing_fields[:4] %}<span class="missing-pill">{{ f }}</span>{% endfor %}
                  {% if s.missing_fields|length > 4 %}<span class="missing-pill">+{{ s.missing_fields|length - 4 }} more</span>{% endif %}
                {% elif s.message %}
                  <span class="sentinel-msg">{{ s.message[:60] }}{% if s.message|length > 60 %}…{% endif %}</span>
                {% else %}<span style="color:#8b949e;font-size:.75rem;">—</span>{% endif %}
              </td>
            </tr>
          {% else %}
            <tr style="opacity:.45;">
              <td style="font-size:.8rem;white-space:nowrap;">{{ site.name }}</td>
              <td><span class="badge bg-none">— not checked</span></td>
              <td colspan="2" style="font-size:.75rem;color:#8b949e;">No sentinel log entry</td>
            </tr>
          {% endif %}
          {% endfor %}
          </tbody>
        </table>
        </div>
      </div>
    </div>

    {# ── Per-state latest run ──────────────────────────────────────────────── #}
    <div class="col-12 col-xl-8">
      <div class="card p-3 h-100">
        <div class="sec-title mb-3">📋 Per-State Latest Run</div>
        {% if latest_runs %}
        <div class="table-responsive">
        <table class="table table-sm table-hover">
          <thead>
            <tr>
              <th>State</th><th>Status</th><th>Sentinel</th>
              <th>Found</th><th>New</th><th>Upd</th><th>Skip</th><th>Docs</th>
              <th>Errors / Reason</th><th>Duration</th><th>Last Run</th>
            </tr>
          </thead>
          <tbody>
          {% for site in sites %}{% set sid = site.id %}
          {% if sid in latest_runs %}{% set r = latest_runs[sid] %}
          {% set st = (r.status or 'unknown')|lower %}
          {% set errs = (r.error_count or 0) %}
          {% set is_failed = (st == 'failed') %}
          {% set has_errors = (errs > 0) %}
          <tr class="{% if is_failed %}row-fail{% elif has_errors %}row-err{% elif st == 'running' %}row-warn{% endif %}">
            <td style="font-size:.8rem;white-space:nowrap;font-weight:{% if has_errors or is_failed %}600{% else %}400{% endif %};">
              {{ site.name }}
            </td>
            <td>
              {% if is_failed %}<span class="badge bg-fail">✗ failed</span>
              {% elif st == 'completed' and has_errors %}<span class="badge bg-warn">⚠ done</span>
              {% elif st == 'completed' %}<span class="badge bg-done">✓ done</span>
              {% elif st == 'running' %}<span class="badge bg-run">⟳ running</span>
              {% else %}<span class="badge bg-none">{{ st }}</span>{% endif %}
            </td>
            <td>
              {% if sid in sentinel_data %}{% set s = sentinel_data[sid] %}
                {% if s.passed == true %}<span class="badge bg-pass" title="Sentinel passed">✓</span>
                {% elif s.passed == false %}
                  {% set cov = s.covered %}{% set exp = s.expected %}
                  {% set pct = ((cov / exp * 100)|int) if (cov is not none and exp and exp > 0) else none %}
                  <span class="badge bg-fail" title="{{ s.message or 'Sentinel failed' }}">✗{% if pct is not none %} {{ pct }}%{% endif %}</span>
                {% else %}<span class="badge bg-none" title="Sentinel result unknown">—</span>{% endif %}
              {% else %}<span style="color:#484f58;font-size:.75rem;">—</span>{% endif %}
            </td>
            <td style="font-size:.8rem;">{{ r.projects_found if r.projects_found is not none else '—' }}</td>
            <td class="text-success" style="font-size:.8rem;">{% if r.projects_new %}+{{ r.projects_new }}{% else %}<span style="color:#484f58;">—</span>{% endif %}</td>
            <td style="font-size:.8rem;color:#58a6ff;">{% if r.projects_updated %}~{{ r.projects_updated }}{% else %}<span style="color:#484f58;">—</span>{% endif %}</td>
            <td style="font-size:.8rem;color:#8b949e;">{{ r.projects_skipped or '—' }}</td>
            <td style="font-size:.8rem;color:#8b949e;">{{ r.documents_uploaded or '—' }}</td>
            <td style="max-width:220px;">
              {% if has_errors %}
                <span class="text-danger fw-bold" style="font-size:.8rem;">{{ errs }} error{{ 's' if errs != 1 }}</span>
                {% if sid in errors_by_site %}{% set e = errors_by_site[sid] %}
                  {% if e.step %}<br><span class="err-step">{{ e.step }}</span>{% endif %}
                  <div class="err-msg">{{ e.message[:120] }}{% if e.message|length > 120 %}…{% endif %}</div>
                {% endif %}
              {% else %}<span style="color:#484f58;font-size:.8rem;">—</span>{% endif %}
            </td>
            <td class="dur">
              {% set r_elapsed = r.get('elapsed_s') %}
              {% if r_elapsed is not none and r_elapsed is number %}
                {{ '%dm %ds'|format((r_elapsed//60)|int, (r_elapsed%60)|int) }}
              {% else %}—{% endif %}
            </td>
            <td style="font-size:.75rem;color:#8b949e;white-space:nowrap;">
              {% if r.started_at %}
                {% if r.started_at is string %}{{ r.started_at[:16]|replace('T',' ') }}
                {% else %}{{ r.started_at.strftime('%m-%d %H:%M') }}{% endif %}
              {% else %}—{% endif %}
            </td>
          </tr>
          {% else %}
          <tr style="opacity:.35;">
            <td style="font-size:.8rem;">{{ site.name }}</td>
            <td colspan="10" style="font-size:.75rem;color:#8b949e;">No data yet</td>
          </tr>
          {% endif %}
          {% endfor %}
          </tbody>
        </table>
        </div>
        {% else %}
        <p style="color:#8b949e;font-size:.85rem;">No run data available.</p>
        {% endif %}
      </div>
    </div>

  </div><!-- /.row -->
</div><!-- /.container-fluid -->
</body>
</html>"""


# ── Flask route ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    data = _get_data()
    return render_template_string(
        _TEMPLATE,
        sites=_SITES,
        latest_runs=data.get("latest_runs", {}),
        sentinel_data=data.get("sentinel_data", {}),
        errors_by_site=data.get("errors_by_site", {}),
        orch_info=data.get("orch_info", {}),
        data_source=data.get("source", "unknown"),
        now=datetime.now(timezone.utc),
    )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RERA Crawlers monitoring dashboard")
    parser.add_argument(
        "--host", default="127.0.0.1",
        help="Bind address (use 0.0.0.0 to expose on all interfaces; default: 127.0.0.1)",
    )
    parser.add_argument("--port", type=int, default=8080, help="Port (default: 8080)")
    args = parser.parse_args()
    print(f"Dashboard → http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=False)
