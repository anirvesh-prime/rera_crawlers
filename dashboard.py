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
import shlex
import subprocess
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string, request

load_dotenv()

from core.config import settings
from core.dashboard_state import COUNT_KEYS, orchestrator_state_path
from core.repair_state import list_repair_attempts, reset_repair_attempt
from scripts.crawler_container import LABEL_PREFIX, ROLE_LABEL, start_detached
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


# ── Direct dashboard probes ───────────────────────────────────────────────────

def _parse_dt(value: object) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _read_json(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _read_latest_runs_from_files() -> dict[str, dict]:
    latest: dict[str, dict] = {}
    state_dir = _LOGS_DIR / "dashboard" / "sites"
    for path in state_dir.glob("*.json"):
        row = _read_json(path)
        site_id = row.get("site_id") or path.stem
        if site_id not in _SITE_IDS:
            continue
        for key in COUNT_KEYS:
            row.setdefault(key, 0)
        row.setdefault("elapsed_s", None)
        latest[site_id] = row
    return latest


def _read_latest_runs_from_db() -> dict[str, dict]:
    try:
        import psycopg
        from psycopg.rows import dict_row

        # Dashboard reads must not call core.db.get_connection(): that path
        # runs ensure_schema() and can block behind crawler schema locks.
        with psycopg.connect(
            settings.postgres_dsn,
            row_factory=dict_row,
            connect_timeout=3,
        ) as conn:
            conn.execute("SET LOCAL statement_timeout = '5000ms'")
            rows = conn.execute(
                """
                SELECT DISTINCT ON (site_id)
                    id AS run_id,
                    site_id,
                    run_type,
                    status,
                    started_at,
                    finished_at,
                    projects_found,
                    projects_new,
                    projects_updated,
                    projects_skipped,
                    documents_uploaded,
                    error_count,
                    sentinel_passed
                FROM crawl_runs
                ORDER BY site_id, started_at DESC NULLS LAST, id DESC
                """
            ).fetchall()
    except Exception:
        return {}
    latest: dict[str, dict] = {}
    for row in rows:
        site_id = row.get("site_id")
        if site_id not in _SITE_IDS:
            continue
        data = dict(row)
        for key in COUNT_KEYS:
            data.setdefault(key, 0)
        data.setdefault("elapsed_s", None)
        latest[site_id] = data
    return latest


def _entry_matches_run(entry: dict, run: dict) -> bool:
    run_id = run.get("run_id")
    if isinstance(run_id, int) and run_id > 0:
        return entry.get("run_id") == run_id
    started = _parse_dt(run.get("started_at"))
    ts = _parse_dt(entry.get("timestamp"))
    if started and ts:
        return ts >= started - timedelta(minutes=1)
    return True


def _log_entries_for_run(site_id: str, run: dict) -> list[dict]:
    log_dir = _LOGS_DIR / site_id
    files = sorted(log_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    entries: list[dict] = []
    for path in files[:5]:
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            continue
        for line in lines:
            try:
                entry = json.loads(line)
            except Exception:
                continue
            if isinstance(entry, dict) and _entry_matches_run(entry, run):
                entries.append(entry)
    entries.sort(key=lambda e: e.get("timestamp") or "")
    return entries


def _build_sentinel_data(latest_runs: dict[str, dict], logs_by_site: dict[str, list[dict]]) -> dict:
    sentinel_data: dict = {}
    for sid, entries in logs_by_site.items():
        bucket = {
            "has_error": False,
            "covered": None,
            "expected": None,
            "missing_fields": [],
            "coverage_ratio": None,
            "message": "",
        }
        seen = False
        for entry in entries:
            if entry.get("step") != "sentinel":
                continue
            seen = True
            extra = entry.get("extra") or {}
            level = (entry.get("level") or "").upper()
            msg = _clean_msg(entry.get("message") or "")
            if level == "ERROR":
                bucket["has_error"] = True
                bucket["message"] = msg
            elif not bucket["has_error"]:
                bucket["message"] = msg
            if extra.get("covered") is not None:
                bucket["covered"] = extra["covered"]
                bucket["expected"] = extra.get("expected")
            if extra.get("missing_fields"):
                bucket["missing_fields"] = extra["missing_fields"]
            if extra.get("coverage_ratio") is not None:
                bucket["coverage_ratio"] = extra["coverage_ratio"]
        if seen:
            sentinel_data[sid] = {
                "passed": not bucket["has_error"],
                "covered": bucket["covered"],
                "expected": bucket["expected"],
                "missing_fields": bucket["missing_fields"],
                "coverage_ratio": bucket["coverage_ratio"],
                "message": bucket["message"],
            }
    for sid, run in latest_runs.items():
        if sid not in sentinel_data and run.get("sentinel_passed") is not None:
            sentinel_data[sid] = {
                "passed": run["sentinel_passed"],
                "covered": None,
                "expected": None,
                "missing_fields": [],
                "coverage_ratio": None,
                "message": "",
            }
    return sentinel_data


def _build_errors_by_site(latest_runs: dict[str, dict], logs_by_site: dict[str, list[dict]]) -> dict:
    errors: dict = {}
    for sid, run in latest_runs.items():
        entries = logs_by_site.get(sid, [])
        chosen = next((e for e in reversed(entries) if (e.get("level") or "").upper() == "ERROR"), None)
        if chosen is None and (run.get("error_count") or 0) > 0:
            chosen = next((e for e in reversed(entries) if (e.get("level") or "").upper() == "WARNING"), None)
        if chosen is not None:
            errors[sid] = {
                "message": _clean_msg(chosen.get("message") or ""),
                "step": chosen.get("step") or "",
                "extra": chosen.get("extra") or {},
                "traceback": chosen.get("traceback") or "",
                "registration_no": chosen.get("registration_no") or "",
            }
        elif run.get("error_message"):
            errors[sid] = {
                "message": run.get("error_message") or "",
                "step": "run",
                "extra": {},
                "traceback": run.get("traceback") or "",
                "registration_no": "",
            }
    return errors


def _build_timing_by_site(logs_by_site: dict[str, list[dict]]) -> dict:
    timing: dict = {}
    for sid, entries in logs_by_site.items():
        for entry in entries:
            if entry.get("step") != "timing":
                continue
            extra = entry.get("extra") or {}
            phase = extra.get("phase")
            elapsed = extra.get("elapsed_s")
            if phase and elapsed is not None:
                timing.setdefault(sid, {})[phase] = elapsed
    return timing


def _build_orchestrator_info(latest_runs: dict[str, dict]) -> dict:
    orch = _read_json(orchestrator_state_path())
    if orch:
        return orch
    if not latest_runs:
        return {}
    totals = {key: sum((run.get(key) or 0) for run in latest_runs.values()) for key in COUNT_KEYS}
    most_recent = max(
        latest_runs.values(),
        key=lambda r: _parse_dt(r.get("started_at")) or datetime.min.replace(tzinfo=timezone.utc),
    )
    return {
        "mode": most_recent.get("run_type", "unknown"),
        "started": most_recent.get("started_at"),
        "totals": totals,
    }


def _fetch_direct_probe_data() -> dict:
    latest_runs = _read_latest_runs_from_db()
    logs_by_site = {
        site_id: _log_entries_for_run(site_id, run)
        for site_id, run in latest_runs.items()
    }
    return {
        "latest_runs": latest_runs,
        "sentinel_data": _build_sentinel_data(latest_runs, logs_by_site),
        "errors_by_site": _build_errors_by_site(latest_runs, logs_by_site),
        "repair_by_site": list_repair_attempts(),
        "orch_info": _build_orchestrator_info(latest_runs),
        "timing_by_site": _build_timing_by_site(logs_by_site),
        "source": "db+logs",
    }


def _get_data() -> dict:
    return _fetch_direct_probe_data()


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


def _running_sites_from_processes(latest_runs: dict[str, dict] | None = None) -> dict[str, list[dict]]:
    """Map site_id -> live crawler containers."""
    by_site: dict[str, list[dict]] = {}
    running_sites: dict[str, datetime | None] = {}
    if latest_runs is not None:
        running_sites = {
            site_id: _parse_dt(row.get("started_at"))
            for site_id, row in latest_runs.items()
            if str(row.get("status") or "").lower() == "running"
        }
    for proc in _list_running_crawlers():
        site_ids = _site_ids_from_crawler_cmd(proc.get("cmd") or "")
        if latest_runs is not None:
            proc_started = _parse_dt(proc.get("started_at"))
            site_ids = [
                site_id
                for site_id in site_ids
                if site_id in running_sites
                and (
                    proc_started is None
                    or running_sites[site_id] is None
                    or running_sites[site_id] >= proc_started - timedelta(minutes=2)
                )
            ]
        for site_id in site_ids:
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
      &nbsp;·&nbsp; container probe: <span class="text-warning">{{ running_process_count }}</span>
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
          {% if is_proc_running %}<button type="button" class="badge bg-run ms-1 live-terminal-open" data-container="{{ process_state[sid][0].container }}">⟳ running</button>{% endif %}
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
              {% if is_proc_running %}<button type="button" class="badge bg-run live-terminal-open" data-container="{{ running_proc.container }}" title="Open live terminal for container {{ running_proc.container }}">⟳ running</button>
              {% elif is_failed %}<span class="badge bg-fail">✗ failed</span>
              {% elif db_st == 'completed' and has_errors %}<span class="badge bg-warn">⚠ done</span>
              {% elif db_st == 'completed' %}<span class="badge bg-done">✓ done</span>
              {% elif db_st == 'running' %}<span class="badge bg-none" title="Local state says running, but no crawler container was found">stopped?</span>
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
                <div style="font-size:.58rem;color:#d29922;margin-top:2px;" title="{{ running_proc.cmd }}">{{ running_proc.container }} · {{ running_proc.etime }}</div>
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
            Lists live Docker crawler containers. Stopping a row stops the whole
            container, including Python workers, ChromeDriver, and Chrome.
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
  let liveContainer = null;
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
    if (!liveContainer) return;
    try {
      const res = await fetch("/api/live-terminal?container=" + encodeURIComponent(liveContainer) +
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
      setLiveStatus(data.running ? "Streaming container " + liveContainer : "Container finished", data.running ? "#d29922" : "#3fb950");
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
      liveContainer = btn.dataset.container;
      liveOffset = 0;
      liveTerminalLog.textContent = "";
      liveTerminalTitle.textContent = "Container " + liveContainer;
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
      setTestStatus("Running (container " + data.container + ") · " + escapeHtml(data.cmd), "#d29922");
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
            <div class="running-pid">${escapeHtml(r.container)} <span style="color:#8b949e;font-weight:400;">· elapsed ${escapeHtml(r.etime)}</span></div>
            <div class="running-meta">${escapeHtml(r.cmd)}</div>
          </div>
          <button type="button" class="btn btn-stop btn-sm ms-2" data-container="${escapeHtml(r.container)}">Stop</button>
        </div>
      `).join("");
      runningList.querySelectorAll("button[data-container]").forEach(btn => {
        btn.addEventListener("click", () => killOne(btn.dataset.container, btn));
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
      const killedContainers = (data.killed || []).map(k => k.container).join(", ") || "none";
      const errParts = (data.errors || []).map(e => `${e.container}: ${e.error}`);
      statusEl.style.color = "#3fb950";
      statusEl.innerHTML =
        "Sent " + (data.signal || "signal") + " to container(s): " + escapeHtml(killedContainers) +
        (errParts.length ? "<br><span style=\\"color:#f85149;\\">errors: " + escapeHtml(errParts.join("; ")) + "</span>" : "") +
        (data.message ? "<br><span style=\\"color:#8b949e;\\">" + escapeHtml(data.message) + "</span>" : "");
      setTimeout(loadRunning, 600);
    } catch (err) {
      statusEl.style.color = "#f85149";
      statusEl.textContent = "Request failed: " + err;
    }
  }

  function killOne(container, btn) {
    btn.disabled = true; btn.textContent = "…";
    postKill({container, force: killForce.checked}, killResult);
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
    try:
        data = _get_data()
    except Exception as exc:
        app.logger.exception("Dashboard data probe failed")
        data = {
            "latest_runs": {},
            "sentinel_data": {},
            "errors_by_site": {
                "__dashboard__": {
                    "message": f"Dashboard data probe failed: {exc}",
                    "step": "dashboard",
                    "extra": {},
                    "traceback": "",
                    "registration_no": "",
                }
            },
            "repair_by_site": {},
            "orch_info": {},
            "timing_by_site": {},
            "source": "error",
        }
    latest_runs = data.get("latest_runs", {})
    try:
        process_state = _running_sites_from_processes(latest_runs)
    except Exception:
        app.logger.exception("Dashboard process probe failed")
        process_state = {}
    running_process_count = len({p["container"] for procs in process_state.values() for p in procs})
    return render_template_string(
        _TEMPLATE,
        sites=_SITES,
        latest_runs=latest_runs,
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
    Returns: { job_id, container, container_id, cmd } on success.

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
        if active and _container_is_running(active["container_id"]):
            return jsonify({"error": "Another tester is already running",
                            "job_id": active["job_id"], "container": active["container"]}), 409

        cmd: list[str] = [
            "--tester", "--site", site_id, "--mode", mode,
        ]
        if item_limit is not None:
            cmd.extend(["--item-limit", str(item_limit)])

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        name = f"rera-crawler-tester-{site_id}-{ts}".replace("_", "-")

        try:
            started = start_detached(
                cmd,
                name=name,
            )
        except Exception as exc:
            return jsonify({"error": f"Failed to start: {exc}"}), 500

        container_id = started["container_id"]
        container = started["container"]
        job_id = f"tester-{ts}-{container}"
        _TESTER_JOBS[job_id] = {
            "job_id":       job_id,
            "site_id":      site_id,
            "mode":         mode,
            "cmd":          " ".join(cmd),
            "docker_cmd":   started["cmd"],
            "container_id": container_id,
            "container":    container,
            "started":      datetime.now(timezone.utc),
        }
        _CURRENT_TESTER_JOB = job_id

    return jsonify({
        "job_id":       job_id,
        "container":    container,
        "container_id": container_id,
        "cmd":          " ".join(cmd),
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

    try:
        chunk, new_offset = _docker_logs_since_offset(job["container_id"], offset)
    except Exception as exc:
        return jsonify({"error": f"Failed to read log: {exc}"}), 500

    running = _container_is_running(job["container_id"])
    exit_code = _container_exit_code(job["container_id"])

    return jsonify({
        "chunk":     chunk,
        "offset":    new_offset,
        "running":   running,
        "exit_code": exit_code,
    })


@app.route("/api/test/stop", methods=["POST"])
def api_test_stop():
    """Terminate a tester container by job_id (or the current tester if omitted)."""
    payload = request.get_json(silent=True) or {}
    job_id = payload.get("job_id") or _CURRENT_TESTER_JOB
    job = _TESTER_JOBS.get(job_id) if job_id else None
    if not job:
        return jsonify({"error": "No such tester job"}), 404

    if not _container_is_running(job["container_id"]):
        return jsonify({"stopped": False, "message": "Already finished",
                        "exit_code": _container_exit_code(job["container_id"])})

    force = bool(payload.get("force"))
    try:
        _stop_container(job["container_id"], force=force)
    except ProcessLookupError:
        return jsonify({"stopped": False, "message": "container gone"})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({
        "stopped": True,
        "signal": "SIGKILL" if force else "SIGTERM",
        "container": job["container"],
    })


@app.route("/api/repair/reset", methods=["POST"])
def api_repair_reset():
    payload = request.get_json(silent=True) or {}
    site_id = str(payload.get("site", ""))
    if site_id not in _SITE_IDS:
        return jsonify({"error": f"Unknown site id: {site_id!r}"}), 400
    return jsonify({"site": site_id, "reset": reset_repair_attempt(site_id)})


def _docker_json(args: list[str]) -> object:
    out = subprocess.check_output(["docker", *args], stderr=subprocess.DEVNULL, text=True, timeout=10)
    return json.loads(out) if out.strip() else None


def _container_is_running(container_id: str) -> bool:
    try:
        state = _docker_json(["inspect", container_id, "--format", "{{json .State}}"])
    except Exception:
        return False
    return bool(isinstance(state, dict) and state.get("Running"))


def _container_exit_code(container_id: str) -> int | None:
    try:
        state = _docker_json(["inspect", container_id, "--format", "{{json .State}}"])
    except Exception:
        return None
    if not isinstance(state, dict):
        return None
    value = state.get("ExitCode")
    return value if isinstance(value, int) else None


def _docker_logs_since_offset(container_id: str, offset: int) -> tuple[str, int]:
    if offset < 0:
        offset = 0
    out = subprocess.check_output(
        ["docker", "logs", container_id],
        stderr=subprocess.STDOUT,
        timeout=10,
    )
    if offset > len(out):
        offset = 0
    chunk_bytes = out[offset:]
    return chunk_bytes.decode("utf-8", errors="replace"), len(out)


def _stop_container(container_id: str, *, force: bool = False) -> None:
    cmd = ["docker", "kill", container_id] if force else ["docker", "stop", "--time", "20", container_id]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)
    if result.returncode != 0:
        msg = (result.stderr or result.stdout or "docker stop failed").strip()
        if "No such container" in msg:
            raise ProcessLookupError(msg)
        raise RuntimeError(msg)


def _docker_started_at_to_dt(value: str) -> datetime | None:
    if not value:
        return None
    try:
        # Docker emits nanoseconds; Python accepts at most microseconds.
        if "." in value:
            head, tail = value.split(".", 1)
            frac, _, zone = tail.partition("Z")
            value = f"{head}.{frac[:6]}+00:00" if zone == "" else value
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _elapsed(started_at: str) -> str:
    started = _docker_started_at_to_dt(started_at)
    if started is None:
        return "unknown"
    seconds = max(0, int((datetime.now(timezone.utc) - started).total_seconds()))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _list_running_crawlers() -> list[dict]:
    """Return one entry per live crawler Docker container."""
    try:
        out = subprocess.check_output(
            ["docker", "ps", "--filter", f"label={ROLE_LABEL}=crawler", "--format", "{{.ID}}"],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=10,
        )
    except Exception:
        return []
    ids = [line.strip() for line in out.splitlines() if line.strip()]
    if not ids:
        return []
    try:
        containers = _docker_json(["inspect", *ids])
    except Exception:
        return []
    if not isinstance(containers, list):
        return []
    rows: list[dict] = []
    for item in containers:
        if not isinstance(item, dict):
            continue
        state = item.get("State") or {}
        if not state.get("Running"):
            continue
        labels = ((item.get("Config") or {}).get("Labels") or {})
        container_id = str(item.get("Id") or "")
        short_id = container_id[:12]
        name = str(item.get("Name") or "").lstrip("/")
        cmd = labels.get(f"{LABEL_PREFIX}.cmd") or " ".join((item.get("Config") or {}).get("Cmd") or [])
        started_at = str(state.get("StartedAt") or "")
        rows.append({
            "container": short_id,
            "container_id": container_id,
            "name": name,
            "etime": _elapsed(started_at),
            "started_at": started_at,
            "cmd": cmd,
            "mode": labels.get(f"{LABEL_PREFIX}.mode", ""),
            "sites": labels.get(f"{LABEL_PREFIX}.sites", ""),
            "tester": labels.get(f"{LABEL_PREFIX}.tester", "false") == "true",
        })
    return rows


@app.route("/api/running")
def api_running():
    return jsonify({"running": _list_running_crawlers()})


@app.route("/api/live-terminal")
def api_live_terminal():
    """Stream stdout/stderr for a running crawler container."""
    container = request.args.get("container") or request.args.get("pid") or ""
    if not container:
        return jsonify({"error": "container is required"}), 400
    try:
        offset = int(request.args.get("offset", "0"))
    except ValueError:
        return jsonify({"error": "offset must be an integer"}), 400
    if offset < 0:
        offset = 0

    try:
        chunk, new_offset = _docker_logs_since_offset(container, offset)
    except Exception as exc:
        return jsonify({"error": f"Failed to read container logs: {exc}"}), 500

    return jsonify({
        "container": container,
        "chunk": chunk,
        "offset": new_offset,
        "running": _container_is_running(container),
    })


@app.route("/api/kill", methods=["POST"])
def api_kill():
    """Stop tracked crawler containers."""
    payload = request.get_json(silent=True) or {}
    force = bool(payload.get("force"))

    running = _list_running_crawlers()
    running_by_short = {r["container"]: r for r in running}
    running_by_id = {r["container_id"]: r for r in running}

    if payload.get("all"):
        targets = [r["container_id"] for r in running]
    else:
        container = str(payload.get("container") or payload.get("pid") or "")
        if not container:
            return jsonify({"error": "container is required, or pass {\"all\": true}"}), 400
        matched = running_by_id.get(container) or running_by_short.get(container)
        if not matched:
            return jsonify({"error": f"container {container} is not a tracked crawler"}), 400
        targets = [matched["container_id"]]

    if not targets:
        return jsonify({"killed": [], "errors": [], "signal": "SIGKILL" if force else "SIGTERM",
                        "message": "No running crawlers"})

    killed: list[dict] = []
    errors: list[dict] = []
    for container_id in targets:
        try:
            _stop_container(container_id, force=force)
            killed.append({"container": container_id[:12], "container_id": container_id})
        except ProcessLookupError:
            errors.append({"container": container_id[:12], "error": "not found"})
        except PermissionError:
            errors.append({"container": container_id[:12], "error": "permission denied"})
        except Exception as exc:
            errors.append({"container": container_id[:12], "error": str(exc)})

    return jsonify({"killed": killed, "errors": errors, "signal": "SIGKILL" if force else "SIGTERM"})


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
