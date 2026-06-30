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
import shlex
import signal
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string, request

load_dotenv()

import psycopg
from psycopg.rows import dict_row as _dict_row

from core.config import settings
from core.repair_state import list_repair_attempts, reset_repair_attempt
from sites_config import SITES  # noqa: E402

_SITES = [{"id": s["id"], "name": s["name"], "enabled": bool(s.get("enabled"))} for s in SITES]
_SITE_IDS = {s["id"] for s in _SITES}
_ENABLED_SITE_IDS = {s["id"] for s in _SITES if s["enabled"]}
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
                    "errors_by_site": {}, "repair_by_site": list_repair_attempts(),
                    "orch_info": {}, "source": "database",
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
            "repair_by_site": list_repair_attempts(),
            "orch_info": orch_info,
            "timing_by_site": timing_by_site,
            "source": "database",
        }
    except Exception:
        return None
    finally:
        conn.close()


def _get_data() -> dict:
    return _fetch_db() or {"repair_by_site": list_repair_attempts()}


# ── Process probe helpers ────────────────────────────────────────────────────

def _site_ids_from_crawler_cmd(cmd: str) -> list[str]:
    """Infer affected site ids from a live run_crawlers.py command line."""
    try:
        tokens = shlex.split(cmd)
    except ValueError:
        tokens = cmd.split()

    selected: list[str] = []
    i = 0
    while i < len(tokens):
        token = tokens[i]
        value = None
        if token in {"--site", "--sites"} and i + 1 < len(tokens):
            value = tokens[i + 1]
            i += 1
        elif token.startswith("--site="):
            value = token.split("=", 1)[1]
        elif token.startswith("--sites="):
            value = token.split("=", 1)[1]

        if value:
            for site_id in value.split(","):
                site_id = site_id.strip()
                if site_id in _SITE_IDS and site_id not in selected:
                    selected.append(site_id)
        i += 1

    if selected:
        return selected
    # No explicit --site means the orchestrator is running all enabled sites.
    return [site["id"] for site in _SITES if site["id"] in _ENABLED_SITE_IDS]


def _running_sites_from_processes() -> dict[str, list[dict]]:
    """Map site_id -> live run_crawlers.py processes from the OS process table."""
    by_site: dict[str, list[dict]] = {}
    for proc in _list_running_crawlers():
        for site_id in _site_ids_from_crawler_cmd(proc.get("cmd") or ""):
            by_site.setdefault(site_id, []).append(proc)
    return by_site


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
    .btn-reset { background:#21262d; border:1px solid #8b949e; color:#e6edf3; font-size:.68rem;
                 font-weight:600; padding:3px 8px; border-radius:4px; }
    .btn-reset:hover { background:#30363d; color:#fff; }
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
    .test-log { background:#0d1117; color:#e6edf3; border:1px solid #30363d;
                 border-radius:4px; padding:10px; font-family:Menlo,Consolas,monospace;
                 font-size:.72rem; line-height:1.35; height:55vh; overflow-y:auto;
                 white-space:pre-wrap; word-break:break-word; margin:0; }
    .test-log .lvl-ERROR { color:#f85149; }
    .test-log .lvl-WARNING { color:#d29922; }
    .test-log .lvl-DEBUG { color:#6e7681; }
    .test-log .fld-hdr  { color:#7ee787; font-weight:600; }
    .test-log .fld-name { color:#79c0ff; }
    .test-log .fld-sep  { color:#6e7681; }
    .test-log .fld-val  { color:#7ee787; }
    .live-log { background:#020409; color:#e6edf3; border:1px solid #30363d;
                 border-radius:4px; padding:10px; font-family:Menlo,Consolas,monospace;
                 font-size:.72rem; line-height:1.35; height:62vh; overflow-y:auto;
                 white-space:pre-wrap; word-break:break-word; margin:0; }
    .live-terminal-open { cursor:pointer; border:0; }
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
      Source: <span class="text-warning">{{ data_source }}</span>
      &nbsp;·&nbsp; process probe: <span class="text-warning">{{ running_process_count }}</span>
      &nbsp;·&nbsp; auto-refresh 60s
    </span>
    {% if n_sites %}
    <span class="ms-3">
      <span class="badge bg-done">{{ n_ok }} OK</span>
      {% if n_errs %}<span class="badge bg-fail ms-1">{{ n_errs }} w/ errors</span>{% endif %}
    </span>
    {% endif %}
  </div>
  <div class="d-flex align-items-center gap-2">
    <button type="button" class="btn btn-run" data-bs-toggle="modal" data-bs-target="#testModal">
      🧪 Test Crawler
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
    {% set is_proc_running = sid in process_state %}
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
          {% if is_proc_running %}<button type="button" class="badge bg-run ms-1 live-terminal-open" data-pid="{{ process_state[sid][0].pid }}">⟳ running</button>{% endif %}
          {% if sid in repair_by_site %}{% set repair = repair_by_site[sid] %}
            <span class="badge bg-warn ms-1" title="{{ repair.reason or '' }}">repair {{ repair.status }}</span>
            <button type="button" class="btn-reset ms-1 repair-reset" data-site="{{ sid }}">Reset repair</button>
          {% endif %}
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
          {% set db_st = (r.status or 'unknown')|lower %}
          {% set is_proc_running = sid in process_state %}
          {% set running_proc = process_state[sid][0] if is_proc_running else none %}
          {% set errs = (r.error_count or 0) %}
          {% set is_failed = (db_st == 'failed') %}
          {% set has_errors = (errs > 0) %}
          <tr class="{% if is_failed and not is_proc_running %}row-fail{% elif has_errors %}row-err{% elif is_proc_running %}row-warn{% endif %}">
            <td style="font-size:.8rem;white-space:nowrap;font-weight:{% if has_errors or is_failed %}600{% else %}400{% endif %};">
              {{ site.name }}
            </td>
            <td>
              {% if is_proc_running %}<button type="button" class="badge bg-run live-terminal-open" data-pid="{{ running_proc.pid }}" title="Open live terminal for PID {{ running_proc.pid }}">⟳ running</button>
              {% elif is_failed %}<span class="badge bg-fail">✗ failed</span>
              {% elif db_st == 'completed' and has_errors %}<span class="badge bg-warn">⚠ done</span>
              {% elif db_st == 'completed' %}<span class="badge bg-done">✓ done</span>
              {% elif db_st == 'running' %}<span class="badge bg-none" title="DB row says running, but no crawler process was found">stopped?</span>
              {% else %}<span class="badge bg-none">{{ db_st }}</span>{% endif %}
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
                {% if sid in repair_by_site %}{% set repair = repair_by_site[sid] %}
                  <br><span class="err-step" title="{{ repair.reason or '' }}">repair: {{ repair.status }}</span>
                  <button type="button" class="btn-reset repair-reset" data-site="{{ sid }}" style="margin-left:4px;">Reset</button>
                {% endif %}
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
              {% if is_proc_running %}
                <div style="font-size:.58rem;color:#d29922;margin-top:2px;" title="{{ running_proc.cmd }}">pid {{ running_proc.pid }} · {{ running_proc.etime }}</div>
              {% endif %}
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

{# ── Test Crawler modal ───────────────────────────────────────────────── #}
<div class="modal fade" id="testModal" tabindex="-1" aria-hidden="true">
  <div class="modal-dialog modal-xl modal-dialog-scrollable">
    <div class="modal-content">
      <div class="modal-header">
        <h5 class="modal-title">🧪 Test Crawler <span style="font-size:.7rem;color:#8b949e;font-weight:400;">— verbose, single-site, no DB / S3 writes</span></h5>
        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div class="modal-body">
        <form id="testForm" class="mb-2">
          <div class="row g-3 align-items-end">
            <div class="col-md-5">
              <label class="form-label">Site</label>
              <select class="form-select" id="testSite">
                {% for site in sites %}
                <option value="{{ site.id }}" {% if not site.enabled %}data-disabled="1"{% endif %}>
                  {{ site.name }}{% if not site.enabled %} (disabled){% endif %}
                </option>
                {% endfor %}
              </select>
            </div>
            <div class="col-md-3">
              <label class="form-label">Mode</label>
              <select class="form-select" id="testMode">
                <option value="daily_light" selected>daily_light</option>
                <option value="weekly_deep">weekly_deep</option>
              </select>
            </div>
            <div class="col-md-2">
              <label class="form-label">Item limit</label>
              <input type="number" class="form-control" id="testItemLimit" min="1" value="3">
            </div>
            <div class="col-md-2 d-grid">
              <button type="button" class="btn btn-run" id="testStart">▶ Start</button>
            </div>
          </div>
        </form>

        <div class="d-flex justify-content-between align-items-center mb-1" style="font-size:.72rem;color:#8b949e;">
          <div id="testStatus">Idle</div>
          <div>
            <label style="margin-right:8px;"><input type="checkbox" id="testAutoScroll" checked> auto-scroll</label>
            <button type="button" class="btn btn-sm btn-outline-secondary" id="testClear">Clear</button>
            <button type="button" class="btn btn-sm btn-stop" id="testStop" disabled>■ Stop</button>
          </div>
        </div>

        <pre id="testLog" class="test-log"></pre>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary btn-sm" data-bs-dismiss="modal">Close</button>
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

{# ── Live Terminal modal ───────────────────────────────────────────────── #}
<div class="modal fade" id="liveTerminalModal" tabindex="-1" aria-hidden="true">
  <div class="modal-dialog modal-xl modal-dialog-scrollable">
    <div class="modal-content">
      <div class="modal-header">
        <h5 class="modal-title">Live Terminal <span id="liveTerminalTitle" style="font-size:.7rem;color:#8b949e;font-weight:400;"></span></h5>
        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div class="modal-body">
        <div class="d-flex justify-content-between align-items-center mb-1" style="font-size:.72rem;color:#8b949e;">
          <div id="liveTerminalStatus">Idle</div>
          <div>
            <label style="margin-right:8px;"><input type="checkbox" id="liveTerminalAutoScroll" checked> auto-scroll</label>
            <button type="button" class="btn btn-sm btn-outline-secondary" id="liveTerminalClear">Clear</button>
          </div>
        </div>
        <pre id="liveTerminalLog" class="live-log"></pre>
      </div>
    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<script>
(function() {
  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, c => ({
      "&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;","'":"&#39;"
    })[c]);
  }

  // ── Test Crawler modal ───────────────────────────────────────────────
  const testModalEl   = document.getElementById("testModal");
  const testSite      = document.getElementById("testSite");
  const testMode      = document.getElementById("testMode");
  const testItemLimit = document.getElementById("testItemLimit");
  const testStartBtn  = document.getElementById("testStart");
  const testStopBtn   = document.getElementById("testStop");
  const testClearBtn  = document.getElementById("testClear");
  const testStatus    = document.getElementById("testStatus");
  const testLog       = document.getElementById("testLog");
  const testAutoScroll = document.getElementById("testAutoScroll");

  let testJobId = null;
  let testOffset = 0;
  let testPollTimer = null;
  let testIsRunning = false;

  // Auto-refresh page every 60s unless a modal is open or a test is in flight.
  setInterval(() => {
    if (document.querySelector(".modal.show")) return;
    if (testIsRunning) return;
    window.location.reload();
  }, 60000);

  document.querySelectorAll(".repair-reset").forEach(btn => {
    btn.addEventListener("click", async () => {
      const site = btn.dataset.site;
      btn.disabled = true;
      btn.textContent = "Resetting...";
      try {
        const res = await fetch("/api/repair/reset", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({site}),
        });
        const data = await res.json();
        if (!res.ok) {
          btn.textContent = data.error || "Error";
          btn.style.color = "#f85149";
          return;
        }
        btn.textContent = data.reset ? "Reset" : "No attempt";
        setTimeout(() => window.location.reload(), 500);
      } catch (err) {
        btn.textContent = "Error";
        btn.style.color = "#f85149";
      }
    });
  });

  // ── Live terminal modal for normal crawler runs ───────────────────────
  const liveTerminalEl = document.getElementById("liveTerminalModal");
  const liveTerminalModal = new bootstrap.Modal(liveTerminalEl);
  const liveTerminalTitle = document.getElementById("liveTerminalTitle");
  const liveTerminalStatus = document.getElementById("liveTerminalStatus");
  const liveTerminalLog = document.getElementById("liveTerminalLog");
  const liveTerminalAutoScroll = document.getElementById("liveTerminalAutoScroll");
  const liveTerminalClear = document.getElementById("liveTerminalClear");
  let livePid = null;
  let liveOffset = 0;
  let livePollTimer = null;

  function setLiveStatus(text, color) {
    liveTerminalStatus.textContent = text;
    liveTerminalStatus.style.color = color || "#8b949e";
  }

  function appendLiveChunk(chunk) {
    if (!chunk) return;
    liveTerminalLog.appendChild(document.createTextNode(chunk));
    if (liveTerminalAutoScroll.checked) {
      liveTerminalLog.scrollTop = liveTerminalLog.scrollHeight;
    }
  }

  async function pollLiveTerminal() {
    if (!livePid) return;
    try {
      const res = await fetch("/api/live-terminal?pid=" + encodeURIComponent(livePid) +
                              "&offset=" + liveOffset);
      const data = await res.json();
      if (!res.ok) {
        setLiveStatus(data.error || "Live terminal unavailable", "#f85149");
        if (data.hint) appendLiveChunk("\\n" + data.hint + "\\n");
        stopLivePolling();
        return;
      }
      if (data.chunk) {
        appendLiveChunk(data.chunk);
        liveOffset = data.offset;
      }
      setLiveStatus(data.running ? "Streaming PID " + livePid : "Process finished", data.running ? "#d29922" : "#3fb950");
      if (!data.running) stopLivePolling();
    } catch (err) {
      setLiveStatus("Live terminal poll failed: " + err, "#f85149");
      stopLivePolling();
    }
  }

  function startLivePolling() {
    if (livePollTimer) clearInterval(livePollTimer);
    livePollTimer = setInterval(pollLiveTerminal, 800);
  }

  function stopLivePolling() {
    if (livePollTimer) { clearInterval(livePollTimer); livePollTimer = null; }
  }

  document.querySelectorAll(".live-terminal-open").forEach(btn => {
    btn.addEventListener("click", () => {
      livePid = btn.dataset.pid;
      liveOffset = 0;
      liveTerminalLog.textContent = "";
      liveTerminalTitle.textContent = "PID " + livePid;
      setLiveStatus("Opening stream...", "#8b949e");
      liveTerminalModal.show();
      pollLiveTerminal();
      startLivePolling();
    });
  });

  liveTerminalClear.addEventListener("click", () => {
    liveTerminalLog.textContent = "";
  });

  liveTerminalEl.addEventListener("hidden.bs.modal", () => {
    stopLivePolling();
  });

  function setTestStatus(text, color) {
    testStatus.textContent = text;
    testStatus.style.color = color || "#8b949e";
  }

  function appendLogChunk(chunk) {
    if (!chunk) return;
    // Tab-separated formatter: ts \\t file:line \\t LEVEL \\t msg
    const lines = chunk.split("\\n");
    const frag = document.createDocumentFragment();
    for (const raw of lines) {
      if (raw === "" && raw === lines[lines.length - 1]) continue;
      const span = document.createElement("span");
      const lvl = raw.match(/\\t(ERROR|WARNING|DEBUG)\\t/);
      if (lvl) {
        span.className = "lvl-" + lvl[1];
        span.textContent = raw + "\\n";
        frag.appendChild(span);
        continue;
      }
      // Field-dump rows from core/db.py:_log_extracted_fields are INFO and
      // come in two shapes — colorize so they pop out of the stream:
      //   ...\\tINFO\\t─── extracted project REG (key=K) ───
      //   ...\\tINFO\\t  field_name | value
      const tabIdx = raw.indexOf("\\tINFO\\t");
      if (tabIdx >= 0) {
        const prefix = raw.slice(0, tabIdx + 6);
        const msg    = raw.slice(tabIdx + 6);
        if (msg.startsWith("─── extracted project")) {
          span.appendChild(document.createTextNode(prefix));
          const hdr = document.createElement("span");
          hdr.className = "fld-hdr";
          hdr.textContent = msg;
          span.appendChild(hdr);
          span.appendChild(document.createTextNode("\\n"));
          frag.appendChild(span);
          continue;
        }
        const fm = msg.match(/^  ([^|]+?) \\| ([\\s\\S]*)$/);
        if (fm) {
          span.appendChild(document.createTextNode(prefix + "  "));
          const n = document.createElement("span");
          n.className = "fld-name";
          n.textContent = fm[1];
          span.appendChild(n);
          const s = document.createElement("span");
          s.className = "fld-sep";
          s.textContent = " | ";
          span.appendChild(s);
          const v = document.createElement("span");
          v.className = "fld-val";
          v.textContent = fm[2];
          span.appendChild(v);
          span.appendChild(document.createTextNode("\\n"));
          frag.appendChild(span);
          continue;
        }
      }
      span.textContent = raw + "\\n";
      frag.appendChild(span);
    }
    testLog.appendChild(frag);
    if (testAutoScroll.checked) testLog.scrollTop = testLog.scrollHeight;
  }

  async function pollTestLog() {
    if (!testJobId) return;
    try {
      const res = await fetch("/api/test/log?job_id=" + encodeURIComponent(testJobId) +
                              "&offset=" + testOffset);
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setTestStatus("Log fetch error: " + (data.error || res.statusText), "#f85149");
        return;
      }
      const data = await res.json();
      if (data.chunk) {
        appendLogChunk(data.chunk);
        testOffset = data.offset;
      }
      if (!data.running) {
        stopPolling();
        const exit = (data.exit_code !== null && data.exit_code !== undefined)
                      ? " (exit " + data.exit_code + ")" : "";
        setTestStatus("Finished" + exit, data.exit_code === 0 ? "#3fb950" : "#d29922");
        testStartBtn.disabled = false;
        testStopBtn.disabled = true;
        testIsRunning = false;
      }
    } catch (err) {
      setTestStatus("Poll failed: " + err, "#f85149");
    }
  }

  function startPolling() {
    if (testPollTimer) clearInterval(testPollTimer);
    testPollTimer = setInterval(pollTestLog, 800);
  }

  function stopPolling() {
    if (testPollTimer) { clearInterval(testPollTimer); testPollTimer = null; }
  }

  testStartBtn.addEventListener("click", async () => {
    const payload = {
      site: testSite.value,
      mode: testMode.value,
      item_limit: parseInt(testItemLimit.value, 10) || null,
    };
    testStartBtn.disabled = true;
    testStartBtn.textContent = "Starting…";
    setTestStatus("Starting…", "#8b949e");
    testLog.textContent = "";
    testOffset = 0;
    try {
      const res = await fetch("/api/test/start", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok) {
        setTestStatus("Start failed: " + (data.error || res.statusText), "#f85149");
        testStartBtn.disabled = false;
        testStartBtn.textContent = "▶ Start";
        return;
      }
      testJobId = data.job_id;
      testIsRunning = true;
      testStopBtn.disabled = false;
      setTestStatus("Running (PID " + data.pid + ") · " + escapeHtml(data.cmd), "#d29922");
      startPolling();
    } catch (err) {
      setTestStatus("Request failed: " + err, "#f85149");
      testStartBtn.disabled = false;
    } finally {
      testStartBtn.textContent = "▶ Start";
    }
  });

  testStopBtn.addEventListener("click", async () => {
    if (!testJobId) return;
    testStopBtn.disabled = true;
    setTestStatus("Stopping…", "#d29922");
    try {
      await fetch("/api/test/stop", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({job_id: testJobId}),
      });
    } catch (err) {
      setTestStatus("Stop failed: " + err, "#f85149");
    }
  });

  testClearBtn.addEventListener("click", () => { testLog.textContent = ""; });

  testModalEl.addEventListener("hidden.bs.modal", () => {
    // Keep the background process alive but stop polling while the
    // modal is hidden so we don't waste cycles.  Polling resumes on reopen.
    stopPolling();
  });
  testModalEl.addEventListener("shown.bs.modal", () => {
    if (testJobId && testIsRunning) startPolling();
  });

  // ── Stop Crawlers modal ──────────────────────────────────────────────
  const killModalEl = document.getElementById("killModal");
  const runningList = document.getElementById("runningList");
  const killResult = document.getElementById("killResult");
  const killForce = document.getElementById("killForce");
  const killAllBtn = document.getElementById("killAll");
  const killRefreshBtn = document.getElementById("killRefresh");
  let killPollTimer = null;

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
    process_state = _running_sites_from_processes()
    running_process_count = len({p["pid"] for procs in process_state.values() for p in procs})
    return render_template_string(
        _TEMPLATE,
        sites=_SITES,
        latest_runs=data.get("latest_runs", {}),
        sentinel_data=data.get("sentinel_data", {}),
        errors_by_site=data.get("errors_by_site", {}),
        repair_by_site=data.get("repair_by_site", {}),
        orch_info=data.get("orch_info", {}),
        timing_by_site=data.get("timing_by_site", {}),
        process_state=process_state,
        running_process_count=running_process_count,
        data_source=data.get("source", "unknown"),
        now=datetime.now(timezone.utc),
    )


# ── Test Crawler (single-site verbose tester) ────────────────────────────────
# Only one tester runs at a time process-wide.  State is held in memory because
# the dashboard is a single-process Flask app; if/when it is replicated this
# will need to move to a shared store (Redis, disk, etc.).
_TESTER_LOCK = threading.Lock()
_TESTER_JOBS: dict[str, dict] = {}
_CURRENT_TESTER_JOB: str | None = None


def _current_tester_job() -> dict | None:
    job_id = _CURRENT_TESTER_JOB
    return _TESTER_JOBS.get(job_id) if job_id else None


@app.route("/api/test/start", methods=["POST"])
def api_test_start():
    """Launch a single-site verbose crawler test (no DB / S3 writes).

    Accepts JSON: { site: <id>, mode: <mode>, item_limit: null|int }.
    Returns: { job_id, pid, logfile, cmd } on success.

    Only one tester runs at a time.  Starting a new one while another is
    still alive returns 409.
    """
    global _CURRENT_TESTER_JOB
    payload = request.get_json(silent=True) or {}

    site_id = str(payload.get("site", ""))
    if site_id not in _SITE_IDS:
        return jsonify({"error": f"Unknown site id: {site_id!r}"}), 400

    mode = str(payload.get("mode", "daily_light"))
    if mode not in _VALID_MODES:
        return jsonify({"error": f"Invalid mode: {mode}"}), 400

    item_limit = payload.get("item_limit")
    if item_limit is not None:
        try:
            item_limit = int(item_limit)
        except (TypeError, ValueError):
            return jsonify({"error": "item_limit must be an integer"}), 400
        if item_limit <= 0:
            return jsonify({"error": "item_limit must be greater than 0"}), 400

    with _TESTER_LOCK:
        active = _current_tester_job()
        if active and active["proc"].poll() is None:
            return jsonify({"error": "Another tester is already running",
                            "job_id": active["job_id"], "pid": active["proc"].pid}), 409

        cmd: list[str] = [
            sys.executable, "-u", "run_crawlers.py",
            "--tester", "--site", site_id, "--mode", mode,
        ]
        if item_limit is not None:
            cmd.extend(["--item-limit", str(item_limit)])

        _LOGS_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        logfile = _LOGS_DIR / f"tester_{site_id}_{ts}.log"

        env = os.environ.copy()
        env["PYTHONHASHSEED"] = "0"
        env["PYTHONUNBUFFERED"] = "1"
        env["CRAWLER_TESTER"] = "true"

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

        job_id = f"tester-{ts}-{proc.pid}"
        _TESTER_JOBS[job_id] = {
            "job_id":  job_id,
            "site_id": site_id,
            "mode":    mode,
            "cmd":     " ".join(cmd),
            "logfile": logfile,
            "proc":    proc,
            "started": datetime.now(timezone.utc),
        }
        _CURRENT_TESTER_JOB = job_id

    return jsonify({
        "job_id":  job_id,
        "pid":     proc.pid,
        "logfile": str(logfile.relative_to(_PROJECT_ROOT)),
        "cmd":     " ".join(cmd),
    })


@app.route("/api/test/log")
def api_test_log():
    """Return the tester log content starting at byte ``offset``.

    Query params: job_id (required), offset (default 0).
    Returns: { chunk, offset, running, exit_code }.
    """
    job_id = request.args.get("job_id", "")
    job = _TESTER_JOBS.get(job_id)
    if not job:
        return jsonify({"error": f"Unknown job_id: {job_id!r}"}), 404
    try:
        offset = int(request.args.get("offset", "0"))
    except ValueError:
        return jsonify({"error": "offset must be an integer"}), 400

    logfile: Path = job["logfile"]
    chunk = ""
    new_offset = offset
    try:
        if logfile.exists():
            with logfile.open("rb") as fh:
                fh.seek(offset)
                data = fh.read()
                new_offset = offset + len(data)
                chunk = data.decode("utf-8", errors="replace")
    except Exception as exc:
        return jsonify({"error": f"Failed to read log: {exc}"}), 500

    proc: subprocess.Popen = job["proc"]
    exit_code = proc.poll()
    running = exit_code is None

    return jsonify({
        "chunk":     chunk,
        "offset":    new_offset,
        "running":   running,
        "exit_code": exit_code,
    })


@app.route("/api/test/stop", methods=["POST"])
def api_test_stop():
    """Terminate a tester process by job_id (or the current tester if omitted)."""
    payload = request.get_json(silent=True) or {}
    job_id = payload.get("job_id") or _CURRENT_TESTER_JOB
    job = _TESTER_JOBS.get(job_id) if job_id else None
    if not job:
        return jsonify({"error": "No such tester job"}), 404

    proc: subprocess.Popen = job["proc"]
    if proc.poll() is not None:
        return jsonify({"stopped": False, "message": "Already finished",
                        "exit_code": proc.returncode})

    force = bool(payload.get("force"))
    sig = signal.SIGKILL if force else signal.SIGTERM
    try:
        pgid = os.getpgid(proc.pid)
        os.killpg(pgid, sig)
    except ProcessLookupError:
        return jsonify({"stopped": False, "message": "process gone"})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"stopped": True, "signal": sig.name, "pid": proc.pid})


@app.route("/api/repair/reset", methods=["POST"])
def api_repair_reset():
    payload = request.get_json(silent=True) or {}
    site_id = str(payload.get("site", ""))
    if site_id not in _SITE_IDS:
        return jsonify({"error": f"Unknown site id: {site_id!r}"}), 400
    return jsonify({"site": site_id, "reset": reset_repair_attempt(site_id)})


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
            "live_log": str((_LOGS_DIR / "live" / f"crawler_{pid}.log").relative_to(_PROJECT_ROOT)),
            "live_log_exists": (_LOGS_DIR / "live" / f"crawler_{pid}.log").exists(),
        })
    return rows


@app.route("/api/running")
def api_running():
    return jsonify({"running": _list_running_crawlers()})


@app.route("/api/live-terminal")
def api_live_terminal():
    """Stream captured stdout/stderr for a running crawler process.

    This endpoint reads logs/live/crawler_<pid>.log, created by run_crawlers.py
    via stdout/stderr teeing. It does not read crawl_logs and does not use DB
    log rows. Processes started before that capture feature was added will not
    have a streamable terminal file.
    """
    try:
        pid = int(request.args.get("pid", ""))
    except (TypeError, ValueError):
        return jsonify({"error": "pid must be an integer"}), 400
    try:
        offset = int(request.args.get("offset", "0"))
    except ValueError:
        return jsonify({"error": "offset must be an integer"}), 400
    if offset < 0:
        offset = 0

    running = _list_running_crawlers()
    running_pids = {r["pid"] for r in running}
    live_path = (_LOGS_DIR / "live" / f"crawler_{pid}.log").resolve()
    live_root = (_LOGS_DIR / "live").resolve()
    try:
        live_path.relative_to(live_root)
    except ValueError:
        return jsonify({"error": "invalid live log path"}), 400

    if not live_path.exists():
        return jsonify({
            "error": "No live terminal capture for this process",
            "hint": (
                "This process was likely started before live terminal capture was added. "
                "Restart the crawler with the current run_crawlers.py and click the running badge again."
            ),
            "running": pid in running_pids,
            "offset": offset,
        }), 404

    chunk = ""
    new_offset = offset
    try:
        with live_path.open("rb") as fh:
            fh.seek(offset)
            data = fh.read()
            new_offset = offset + len(data)
            chunk = data.decode("utf-8", errors="replace")
    except Exception as exc:
        return jsonify({"error": f"Failed to read live terminal log: {exc}"}), 500

    return jsonify({
        "pid": pid,
        "chunk": chunk,
        "offset": new_offset,
        "running": pid in running_pids,
    })


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
