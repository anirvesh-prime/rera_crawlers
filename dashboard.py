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
import os
import re
import signal
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string, request

load_dotenv()

import psycopg
from psycopg.rows import dict_row as _dict_row

from core.config import settings
from sites_config import SITES  # noqa: E402

_SITES = [{"id": s["id"], "name": s["name"], "enabled": bool(s.get("enabled"))} for s in SITES]
_SITE_IDS = {s["id"] for s in _SITES}
_VALID_MODES = {"daily_light", "weekly_deep", "full", "single", "incremental", "listing"}

_PROJECT_ROOT = Path(__file__).resolve().parent
_LOGS_DIR = _PROJECT_ROOT / "logs"

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
    try:
        return psycopg.connect(settings.postgres_dsn, connect_timeout=5, row_factory=_dict_row)
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

            # 2. Use only the latest run per site for all log queries
            recent_ids = [r["id"] for r in latest_runs.values()]

            # 3. Sentinel log entries for that window.
            #
            # A single sentinel check emits several log rows with step='sentinel':
            #   INFO  "Sentinel coverage: 16/18 fields"  → extra has covered/expected
            #   INFO  "Sentinel check passed"             → extra has reg only
            #   ERROR "Sentinel coverage too low …"       → extra has missing_fields/coverage_ratio
            #
            # Reading only the most-recent row per site loses the coverage numbers on
            # passing runs.  Instead, collect ALL sentinel rows for a run and merge:
            #   passed          = no ERROR row exists for that site
            #   covered/expected = from whichever row carries those keys (the coverage INFO row)
            #   missing_fields  = from the ERROR row (if any)
            #   message         = from the ERROR row, or the final INFO row
            sentinel_data: dict = {}
            if recent_ids:
                cur.execute(
                    """
                    SELECT cl.site_id, cl.level, cl.extra, cl.message
                    FROM crawl_logs cl
                    WHERE cl.step = 'sentinel' AND cl.run_id = ANY(%s)
                    ORDER BY cl.logged_at ASC
                    """,
                    (recent_ids,),
                )
                # Accumulator: one bucket per site, filled in as we scan rows
                buckets: dict[str, dict] = {}
                for row in cur.fetchall():
                    sid = row["site_id"]
                    extra = row.get("extra") or {}
                    level = (row.get("level") or "").upper()
                    msg   = _clean_msg(row.get("message") or "")

                    if sid not in buckets:
                        buckets[sid] = {
                            "has_error": False,
                            "covered": None, "expected": None,
                            "missing_fields": [], "coverage_ratio": None,
                            "message": "",
                        }
                    b = buckets[sid]
                    if level == "ERROR":
                        b["has_error"] = True
                        b["message"] = msg  # error message takes priority
                    elif not b["has_error"]:
                        b["message"] = msg  # keep last non-error message
                    # Merge coverage numbers from whichever row has them
                    if extra.get("covered") is not None:
                        b["covered"]  = extra["covered"]
                        b["expected"] = extra.get("expected")
                    if extra.get("missing_fields"):
                        b["missing_fields"] = extra["missing_fields"]
                    if extra.get("coverage_ratio") is not None:
                        b["coverage_ratio"] = extra["coverage_ratio"]

                for sid, b in buckets.items():
                    sentinel_data[sid] = {
                        "passed": not b["has_error"],
                        "covered": b["covered"],
                        "expected": b["expected"],
                        "missing_fields": b["missing_fields"],
                        "coverage_ratio": b["coverage_ratio"],
                        "message": b["message"],
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
                    SELECT DISTINCT ON (site_id) site_id, message, step, extra, traceback, registration_no
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
                        "registration_no": row.get("registration_no") or "",
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

            # 4c. Last-resort fallback: some crawlers bump error_count from
            #     WARNING-level paths (e.g. missing reg_no, detail fetch
            #     fallback) without emitting an ERROR row or a crawl_errors
            #     entry.  Pull the latest WARNING for those sites so the
            #     dashboard at least shows what was counted.
            need_warn = [
                sid for sid, r in latest_runs.items()
                if (r.get("error_count") or 0) > 0 and sid not in errors_by_site
            ]
            if need_warn and recent_ids:
                cur.execute(
                    """
                    SELECT DISTINCT ON (site_id) site_id, message, step, extra, traceback, registration_no
                    FROM crawl_logs
                    WHERE level = 'WARNING' AND site_id = ANY(%s) AND run_id = ANY(%s)
                    ORDER BY site_id, logged_at DESC
                    """,
                    (need_warn, recent_ids),
                )
                for row in cur.fetchall():
                    sid = row["site_id"]
                    errors_by_site[sid] = {
                        "message": _clean_msg(row["message"] or ""),
                        "step": row.get("step") or "",
                        "extra": row.get("extra") or {},
                        "traceback": row.get("traceback") or "",
                        "registration_no": row.get("registration_no") or "",
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
                "started": most_recent["started_at"],
                "totals": totals,
            }

            # 7. Per-site phase timing from timing logs (step='timing').
            #    Builds: { site_id: { phase: elapsed_s } }
            #    Phases emitted by crawlers: 'sentinel', 'search', 'total_run'.
            #    Later rows for the same phase overwrite earlier ones so we always
            #    display the most-recent measurement (relevant when a run retried).
            timing_by_site: dict = {}
            if recent_ids:
                cur.execute(
                    """
                    SELECT site_id, extra
                    FROM crawl_logs
                    WHERE step = 'timing' AND run_id = ANY(%s)
                    ORDER BY site_id, logged_at ASC
                    """,
                    (recent_ids,),
                )
                for row in cur.fetchall():
                    sid = row["site_id"]
                    extra = row.get("extra") or {}
                    phase   = extra.get("phase")
                    elapsed = extra.get("elapsed_s")
                    if phase and elapsed is not None:
                        timing_by_site.setdefault(sid, {})[phase] = elapsed

        return {
            "latest_runs": latest_runs,
            "sentinel_data": sentinel_data,
            "errors_by_site": errors_by_site,
            "orch_info": orch_info,
            "timing_by_site": timing_by_site,
            "source": "database",
        }
    except Exception:
        return None
    finally:
        conn.close()


def _get_data() -> dict:
    return _fetch_db() or {}


# ── HTML template ─────────────────────────────────────────────────────────────

_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
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
    .btn-run { background:#1f6335; border-color:#238636; color:#e6edf3; font-size:.78rem;
               font-weight:600; padding:5px 14px; }
    .btn-run:hover { background:#2ea043; color:#fff; }
    .btn-stop { background:#4a1212; border-color:#6e2424; color:#f85149; font-size:.78rem;
                font-weight:600; padding:5px 14px; }
    .btn-stop:hover { background:#6e2424; color:#fff; }
    .running-row { display:flex; justify-content:space-between; align-items:center;
                    background:#0d1117; border:1px solid #30363d; border-radius:6px;
                    padding:8px 12px; margin-bottom:6px; }
    .running-meta { font-size:.72rem; color:#8b949e; font-family:monospace;
                     word-break:break-all; margin-top:2px; }
    .running-pid { font-size:.82rem; font-weight:600; color:#e6edf3; font-family:monospace; }
    .modal-content { background:#161b22; color:#e6edf3; border:1px solid #30363d; }
    .modal-header, .modal-footer { border-color:#30363d; }
    .modal-title { font-size:.95rem; font-weight:700; color:#58a6ff; }
    .modal .form-label { font-size:.72rem; color:#8b949e; text-transform:uppercase;
                          letter-spacing:.05em; font-weight:600; }
    .modal .form-check-label { font-size:.82rem; color:#e6edf3; }
    .modal .form-control, .modal .form-select { background:#0d1117; color:#e6edf3;
                                                  border-color:#30363d; font-size:.85rem; }
    .modal .form-control:focus, .modal .form-select:focus { background:#0d1117;
                                                              color:#e6edf3; border-color:#58a6ff;
                                                              box-shadow:none; }
    .modal hr { border-color:#30363d; }
    .site-list { max-height:240px; overflow-y:auto; background:#0d1117;
                  border:1px solid #30363d; border-radius:6px; padding:8px 12px; }
    .site-list .form-check { margin-bottom:2px; }
    .run-result { font-size:.72rem; font-family:monospace; background:#0d1117;
                   border:1px solid #30363d; border-radius:4px; padding:8px;
                   margin-top:10px; word-break:break-all; }
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
  <div class="d-flex align-items-center gap-2">
    <button type="button" class="btn btn-run" data-bs-toggle="modal" data-bs-target="#runModal">
      ▶ Run Crawlers
    </button>
    <button type="button" class="btn btn-stop" data-bs-toggle="modal" data-bs-target="#killModal">
      ■ Stop Crawlers
    </button>
    <div class="ms-2" style="font-size:.8rem;color:#8b949e;">
      Refreshed {{ now.strftime('%Y-%m-%d %H:%M:%S') }} UTC
    </div>
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
        {% if e.step %}<span class="err-step">step: {{ e.step }}</span>{% if e.registration_no and e.step == 'detail' %}&nbsp;<span class="err-step" style="color:#f0883e;">reg: {{ e.registration_no }}</span>{% endif %}<br>{% endif %}
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
                  {% if e.step %}<br><span class="err-step">{{ e.step }}</span>{% if e.registration_no and e.step == 'detail' %}&nbsp;<span class="err-step" style="color:#f0883e;">{{ e.registration_no }}</span>{% endif %}{% endif %}
                  <div class="err-msg">{{ e.message[:120] }}{% if e.message|length > 120 %}…{% endif %}</div>
                {% endif %}
              {% else %}<span style="color:#484f58;font-size:.8rem;">—</span>{% endif %}
            </td>
            <td class="dur">
              {% set r_elapsed = r.get('elapsed_s') %}
              {% if r_elapsed is not none and r_elapsed is number %}
                {{ '%dm %ds'|format((r_elapsed//60)|int, (r_elapsed%60)|int) }}
              {% else %}—{% endif %}
              {%- if sid in timing_by_site %}{% set t = timing_by_site[sid] %}
              <div style="font-size:.58rem;color:#484f58;margin-top:2px;line-height:1.5;white-space:nowrap;">
                {%- if t.get('sentinel') is not none %}<span title="sentinel check">🔍 {{ '%.1f'|format(t.sentinel) }}s</span>{% endif %}
                {%- if t.get('search') is not none %} <span title="listing fetch">🔎 {{ '%.1f'|format(t.search) }}s</span>{% endif %}
              </div>
              {%- endif %}
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

{# ── Run Crawlers modal ───────────────────────────────────────────────── #}
<div class="modal fade" id="runModal" tabindex="-1" aria-hidden="true">
  <div class="modal-dialog modal-lg modal-dialog-scrollable">
    <div class="modal-content">
      <div class="modal-header">
        <h5 class="modal-title">▶ Run Crawlers</h5>
        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div class="modal-body">
        <form id="runForm">

          <div class="mb-3">
            <label class="form-label d-block mb-2">Mode</label>
            <div class="form-check form-check-inline">
              <input class="form-check-input" type="radio" name="mode" id="modeDaily" value="daily_light" checked>
              <label class="form-check-label" for="modeDaily">daily_light</label>
            </div>
            <div class="form-check form-check-inline">
              <input class="form-check-input" type="radio" name="mode" id="modeWeekly" value="weekly_deep">
              <label class="form-check-label" for="modeWeekly">weekly_deep</label>
            </div>
          </div>

          <hr>

          <div class="mb-3">
            <div class="d-flex justify-content-between align-items-center mb-2">
              <label class="form-label mb-0">Sites</label>
              <div>
                <button type="button" class="btn btn-sm btn-outline-secondary" id="sitesAll">All enabled</button>
                <button type="button" class="btn btn-sm btn-outline-secondary" id="sitesNone">Clear</button>
              </div>
            </div>
            <div class="form-check mb-2">
              <input class="form-check-input" type="checkbox" id="runAllSites" checked>
              <label class="form-check-label" for="runAllSites">
                Run all enabled sites (default)
              </label>
            </div>
            <div class="site-list" id="siteList">
              {% for site in sites %}
              <div class="form-check">
                <input class="form-check-input site-checkbox" type="checkbox"
                       value="{{ site.id }}" id="site_{{ site.id }}"
                       data-enabled="{{ '1' if site.enabled else '0' }}">
                <label class="form-check-label" for="site_{{ site.id }}">
                  {{ site.name }}
                  <span style="font-size:.65rem;color:#8b949e;">{{ site.id }}</span>
                  {% if not site.enabled %}<span class="badge bg-none ms-1">disabled</span>{% endif %}
                </label>
              </div>
              {% endfor %}
            </div>
          </div>

          <hr>

          <div class="row g-3">
            <div class="col-md-6">
              <div class="form-check mb-2">
                <input class="form-check-input" type="checkbox" id="useItemLimit" checked>
                <label class="form-check-label" for="useItemLimit">
                  Apply item limit
                </label>
              </div>
              <input type="number" class="form-control" id="itemLimit" min="1" value="10">
              <div style="font-size:.68rem;color:#8b949e;margin-top:4px;">
                Caps each crawler to N projects. Uncheck to run unlimited.
              </div>
            </div>
            <div class="col-md-6">
              <label class="form-label">&nbsp;</label>
              <div class="form-check">
                <input class="form-check-input" type="checkbox" id="testLogs" checked>
                <label class="form-check-label" for="testLogs">
                  --test-logs <span style="font-size:.7rem;color:#8b949e;">(skip writes, keep logs)</span>
                </label>
              </div>
            </div>
          </div>

          <div id="runResult" class="run-result" style="display:none;"></div>
        </form>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary btn-sm" data-bs-dismiss="modal">Cancel</button>
        <button type="button" class="btn btn-run" id="runSubmit">▶ Start run</button>
      </div>
    </div>
  </div>
</div>

{# ── Stop Crawlers modal ───────────────────────────────────────────────── #}
<div class="modal fade" id="killModal" tabindex="-1" aria-hidden="true">
  <div class="modal-dialog modal-lg modal-dialog-scrollable">
    <div class="modal-content">
      <div class="modal-header">
        <h5 class="modal-title" style="color:#f85149;">■ Stop Crawlers</h5>
        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div class="modal-body">
        <div class="d-flex justify-content-between align-items-center mb-2">
          <div style="font-size:.8rem;color:#8b949e;">
            Lists live <code style="color:#d29922;">run_crawlers.py</code> orchestrators
            (process-group leaders). Killing a row signals the orchestrator and all of its
            worker processes via <code style="color:#d29922;">killpg</code>.
          </div>
          <button type="button" class="btn btn-sm btn-outline-secondary" id="killRefresh">⟳ Refresh</button>
        </div>

        <div class="form-check mb-2">
          <input class="form-check-input" type="checkbox" id="killForce">
          <label class="form-check-label" for="killForce">
            Force kill (SIGKILL) — skip graceful shutdown
          </label>
        </div>

        <div id="runningList" style="margin-top:10px;">
          <div style="color:#8b949e;font-size:.8rem;">Loading…</div>
        </div>

        <div id="killResult" class="run-result" style="display:none;"></div>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary btn-sm" data-bs-dismiss="modal">Close</button>
        <button type="button" class="btn btn-stop" id="killAll">■ Stop all</button>
      </div>
    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<script>
(function() {
  const runAll = document.getElementById("runAllSites");
  const siteList = document.getElementById("siteList");
  const useItemLimit = document.getElementById("useItemLimit");
  const itemLimit = document.getElementById("itemLimit");
  const resultBox = document.getElementById("runResult");
  const submitBtn = document.getElementById("runSubmit");

  function syncSiteList() {
    siteList.style.opacity = runAll.checked ? "0.5" : "1";
    siteList.querySelectorAll(".site-checkbox").forEach(cb => cb.disabled = runAll.checked);
  }
  runAll.addEventListener("change", syncSiteList);
  syncSiteList();

  useItemLimit.addEventListener("change", () => {
    itemLimit.disabled = !useItemLimit.checked;
  });

  document.getElementById("sitesAll").addEventListener("click", () => {
    runAll.checked = false; syncSiteList();
    siteList.querySelectorAll(".site-checkbox").forEach(cb => {
      cb.checked = cb.dataset.enabled === "1";
    });
  });
  document.getElementById("sitesNone").addEventListener("click", () => {
    runAll.checked = false; syncSiteList();
    siteList.querySelectorAll(".site-checkbox").forEach(cb => cb.checked = false);
  });

  // Auto-refresh every 60s, but skip if a modal is open or a run is in flight.
  setInterval(() => {
    if (document.querySelector(".modal.show")) return;
    if (submitBtn.disabled) return;
    window.location.reload();
  }, 60000);

  submitBtn.addEventListener("click", async () => {
    const mode = document.querySelector("input[name=mode]:checked").value;
    let sites = "all";
    if (!runAll.checked) {
      sites = Array.from(siteList.querySelectorAll(".site-checkbox:checked")).map(cb => cb.value);
      if (sites.length === 0) {
        resultBox.style.display = "block";
        resultBox.style.color = "#f85149";
        resultBox.textContent = "Select at least one site, or check \\"Run all enabled sites\\".";
        return;
      }
    }
    const payload = {
      mode,
      sites,
      item_limit: useItemLimit.checked ? parseInt(itemLimit.value, 10) : null,
      test_logs: document.getElementById("testLogs").checked,
    };

    submitBtn.disabled = true;
    submitBtn.textContent = "Starting…";
    resultBox.style.display = "block";
    resultBox.style.color = "#8b949e";
    resultBox.textContent = "Starting run…";

    try {
      const res = await fetch("/api/run", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok) {
        resultBox.style.color = "#f85149";
        resultBox.textContent = "Error: " + (data.error || res.statusText);
      } else {
        resultBox.style.color = "#3fb950";
        resultBox.innerHTML = "✓ Started (PID " + data.pid + ")<br>" +
                              "Log: " + data.logfile + "<br>" +
                              "<span style=\\"color:#8b949e;\\">" + data.cmd + "</span>";
      }
    } catch (err) {
      resultBox.style.color = "#f85149";
      resultBox.textContent = "Request failed: " + err;
    } finally {
      submitBtn.disabled = false;
      submitBtn.textContent = "▶ Start run";
    }
  });

  // ── Stop Crawlers modal ──────────────────────────────────────────────
  const killModalEl = document.getElementById("killModal");
  const runningList = document.getElementById("runningList");
  const killResult = document.getElementById("killResult");
  const killForce = document.getElementById("killForce");
  const killAllBtn = document.getElementById("killAll");
  const killRefreshBtn = document.getElementById("killRefresh");
  let killPollTimer = null;

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, c => ({
      "&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;","'":"&#39;"
    })[c]);
  }

  async function loadRunning() {
    try {
      const res = await fetch("/api/running");
      const data = await res.json();
      const rows = data.running || [];
      if (rows.length === 0) {
        runningList.innerHTML = '<div style="color:#8b949e;font-size:.8rem;">No running crawlers.</div>';
        killAllBtn.disabled = true;
        return;
      }
      killAllBtn.disabled = false;
      runningList.innerHTML = rows.map(r => `
        <div class="running-row">
          <div style="flex:1;min-width:0;">
            <div class="running-pid">PID ${r.pid} <span style="color:#8b949e;font-weight:400;">· elapsed ${escapeHtml(r.etime)}</span></div>
            <div class="running-meta">${escapeHtml(r.cmd)}</div>
          </div>
          <button type="button" class="btn btn-stop btn-sm ms-2" data-pid="${r.pid}">Stop</button>
        </div>
      `).join("");
      runningList.querySelectorAll("button[data-pid]").forEach(btn => {
        btn.addEventListener("click", () => killOne(parseInt(btn.dataset.pid, 10), btn));
      });
    } catch (err) {
      runningList.innerHTML = '<div style="color:#f85149;font-size:.8rem;">Failed to load: ' + escapeHtml(err) + '</div>';
    }
  }

  async function postKill(payload, statusEl) {
    statusEl.style.display = "block";
    statusEl.style.color = "#8b949e";
    statusEl.textContent = "Sending signal…";
    try {
      const res = await fetch("/api/kill", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok) {
        statusEl.style.color = "#f85149";
        statusEl.textContent = "Error: " + (data.error || res.statusText);
        return;
      }
      const killedPids = (data.killed || []).map(k => k.pid).join(", ") || "none";
      const errParts = (data.errors || []).map(e => `${e.pid}: ${e.error}`);
      statusEl.style.color = "#3fb950";
      statusEl.innerHTML =
        "Sent " + (data.signal || "signal") + " to PG of: " + escapeHtml(killedPids) +
        (errParts.length ? "<br><span style=\\"color:#f85149;\\">errors: " + escapeHtml(errParts.join("; ")) + "</span>" : "") +
        (data.message ? "<br><span style=\\"color:#8b949e;\\">" + escapeHtml(data.message) + "</span>" : "");
      setTimeout(loadRunning, 600);
    } catch (err) {
      statusEl.style.color = "#f85149";
      statusEl.textContent = "Request failed: " + err;
    }
  }

  function killOne(pid, btn) {
    btn.disabled = true; btn.textContent = "…";
    postKill({pid, force: killForce.checked}, killResult);
  }

  killAllBtn.addEventListener("click", () => {
    postKill({all: true, force: killForce.checked}, killResult);
  });
  killRefreshBtn.addEventListener("click", loadRunning);

  killModalEl.addEventListener("show.bs.modal", () => {
    killResult.style.display = "none";
    loadRunning();
    killPollTimer = setInterval(loadRunning, 5000);
  });
  killModalEl.addEventListener("hidden.bs.modal", () => {
    if (killPollTimer) { clearInterval(killPollTimer); killPollTimer = null; }
  });
})();
</script>
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
        timing_by_site=data.get("timing_by_site", {}),
        data_source=data.get("source", "unknown"),
        now=datetime.now(timezone.utc),
    )


@app.route("/api/run", methods=["POST"])
def api_run():
    """Kick off run_crawlers.py as a detached background process.

    Accepts JSON: { mode, sites: [] | "all", item_limit: null|int, test_logs: bool }.
    Returns: { pid, logfile, cmd } on success.
    """
    payload = request.get_json(silent=True) or {}

    mode = str(payload.get("mode", "daily_light"))
    if mode not in _VALID_MODES:
        return jsonify({"error": f"Invalid mode: {mode}"}), 400

    sites_arg = payload.get("sites", "all")
    selected_sites: list[str] = []
    if sites_arg != "all":
        if not isinstance(sites_arg, list):
            return jsonify({"error": "sites must be a list of site ids or \"all\""}), 400
        for sid in sites_arg:
            if not isinstance(sid, str) or sid not in _SITE_IDS:
                return jsonify({"error": f"Unknown site id: {sid}"}), 400
            selected_sites.append(sid)
        if not selected_sites:
            return jsonify({"error": "No sites selected"}), 400

    item_limit = payload.get("item_limit")
    if item_limit is not None:
        try:
            item_limit = int(item_limit)
        except (TypeError, ValueError):
            return jsonify({"error": "item_limit must be an integer"}), 400
        if item_limit <= 0:
            return jsonify({"error": "item_limit must be greater than 0"}), 400

    test_logs = bool(payload.get("test_logs", False))

    cmd: list[str] = [sys.executable, "run_crawlers.py", "--mode", mode]
    for sid in selected_sites:
        cmd.extend(["--site", sid])
    if item_limit is not None:
        cmd.extend(["--item-limit", str(item_limit)])
    if test_logs:
        cmd.append("--test-logs")

    _LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logfile = _LOGS_DIR / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    env = os.environ.copy()
    env["PYTHONHASHSEED"] = "0"
    env["PYTHONUNBUFFERED"] = "1"

    try:
        log_fh = open(logfile, "ab", buffering=0)
        proc = subprocess.Popen(
            cmd,
            cwd=str(_PROJECT_ROOT),
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            env=env,
            start_new_session=True,
            close_fds=True,
        )
        log_fh.close()
    except Exception as exc:
        return jsonify({"error": f"Failed to start: {exc}"}), 500

    return jsonify({
        "pid": proc.pid,
        "logfile": str(logfile.relative_to(_PROJECT_ROOT)),
        "cmd": " ".join(cmd),
    })


def _list_running_crawlers() -> list[dict]:
    """Return one entry per live run_crawlers.py orchestrator (process-group leader).

    Excludes worker processes spawned by ProcessPoolExecutor, which share the
    orchestrator's pgid but have multiprocessing helper cmdlines that do not
    contain 'run_crawlers.py'.
    """
    try:
        out = subprocess.check_output(
            ["ps", "-eo", "pid=,pgid=,etime=,args="],
            stderr=subprocess.DEVNULL, text=True, timeout=5,
        )
    except Exception:
        return []
    rows: list[dict] = []
    for line in out.splitlines():
        if "run_crawlers.py" not in line:
            continue
        parts = line.strip().split(None, 3)
        if len(parts) < 4:
            continue
        try:
            pid = int(parts[0])
            pgid = int(parts[1])
        except ValueError:
            continue
        # Only show process-group leaders (the orchestrator itself).
        if pid != pgid:
            continue
        rows.append({
            "pid": pid,
            "pgid": pgid,
            "etime": parts[2],
            "cmd": parts[3],
        })
    return rows


@app.route("/api/running")
def api_running():
    return jsonify({"running": _list_running_crawlers()})


@app.route("/api/kill", methods=["POST"])
def api_kill():
    """Send SIGTERM (or SIGKILL with force=true) to a run_crawlers.py orchestrator.

    Accepts JSON: { pid: int } or { all: true }. Optional: { force: true }.
    Only PIDs returned by /api/running can be targeted, so this endpoint cannot
    be used to signal arbitrary processes.
    """
    payload = request.get_json(silent=True) or {}
    force = bool(payload.get("force"))
    sig = signal.SIGKILL if force else signal.SIGTERM

    running = _list_running_crawlers()
    running_pids = {r["pid"] for r in running}

    if payload.get("all"):
        targets = sorted(running_pids)
    else:
        try:
            pid = int(payload.get("pid"))
        except (TypeError, ValueError):
            return jsonify({"error": "pid must be an integer, or pass {\"all\": true}"}), 400
        if pid not in running_pids:
            return jsonify({"error": f"pid {pid} is not a tracked crawler orchestrator"}), 400
        targets = [pid]

    if not targets:
        return jsonify({"killed": [], "errors": [], "signal": sig.name,
                        "message": "No running crawlers"})

    killed: list[dict] = []
    errors: list[dict] = []
    for pid in targets:
        try:
            pgid = os.getpgid(pid)
            os.killpg(pgid, sig)
            killed.append({"pid": pid, "pgid": pgid})
        except ProcessLookupError:
            errors.append({"pid": pid, "error": "not found"})
        except PermissionError:
            errors.append({"pid": pid, "error": "permission denied"})
        except Exception as exc:
            errors.append({"pid": pid, "error": str(exc)})

    return jsonify({"killed": killed, "errors": errors, "signal": sig.name})


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
