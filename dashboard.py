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
                    SELECT cl.site_id, cl.extra, cr.sentinel_passed
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
                    }
            # Backfill sentinel_passed from crawl_runs for sites with no log entry
            for sid, run in latest_runs.items():
                if sid not in sentinel_data and run.get("sentinel_passed") is not None:
                    sentinel_data[sid] = {
                        "passed": run["sentinel_passed"],
                        "covered": None, "expected": None,
                    }

            # 4. Latest error message per site (within the same window)
            errors_by_site: dict = {}
            if recent_ids:
                cur.execute(
                    """
                    SELECT DISTINCT ON (site_id) site_id, message
                    FROM crawl_logs
                    WHERE level = 'ERROR' AND run_id = ANY(%s)
                    ORDER BY site_id, logged_at DESC
                    """,
                    (recent_ids,),
                )
                for row in cur.fetchall():
                    errors_by_site[row["site_id"]] = row["message"]

            # 5. Orchestrator-level summary (aggregate of the window)
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
                    sentinel_data[sid] = {"covered": covered, "expected": expected, "passed": passed}
                if entry.get("level") == "ERROR" and sid not in errors_by_site:
                    errors_by_site[sid] = entry.get("message", "")
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
    runs[sid] = {
        "site_id": sid,
        "run_type": mode,
        "started_at": started,
        "finished_at": None,
        "status": "failed" if site.get("error_count", 0) > 0 else "completed",
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
    .table { color:#e6edf3; margin-bottom:0; }
    .table th { background:#21262d; color:#8b949e; font-size:.75rem;
                text-transform:uppercase; letter-spacing:.06em; border-color:#30363d; }
    .table td { border-color:#30363d; vertical-align:middle; }
    .table-hover tbody tr:hover td { background:rgba(255,255,255,.03); }
    .badge { font-size:.7rem; font-weight:600; padding:3px 8px; border-radius:4px; }
    .bg-pass { background:#1f6335!important; color:#3fb950; }
    .bg-fail { background:#4a1212!important; color:#f85149; }
    .bg-run  { background:#3d2e00!important; color:#d29922; }
    .bg-done { background:#1f6335!important; color:#3fb950; }
    .bg-none { background:#21262d!important; color:#8b949e; }
    .sec-title { color:#58a6ff; font-size:.8rem; font-weight:700;
                 text-transform:uppercase; letter-spacing:.1em; }
    .bar-wrap { width:70px; height:5px; background:#30363d;
                border-radius:3px; display:inline-block; vertical-align:middle; }
    .bar-fill { height:5px; border-radius:3px; }
    .err-msg  { font-size:.7rem; color:#f85149; opacity:.85; margin-top:2px; }
    .stat-box { border-right:1px solid #30363d; padding:0 1.5rem; }
    .stat-box:last-child { border-right:none; }
    .stat-val { font-size:1.4rem; font-weight:700; line-height:1; }
    .stat-lbl { font-size:.7rem; color:#8b949e; text-transform:uppercase; letter-spacing:.05em; }
    .hdr { background:#161b22; border-bottom:1px solid #30363d; padding:12px 24px; margin-bottom:24px; }
  </style>
</head>
<body>
<div class="hdr d-flex justify-content-between align-items-center">
  <div>
    <span style="font-size:1.1rem;font-weight:700;">🏗️ RERA Crawlers Dashboard</span>
    <span class="ms-3" style="font-size:.8rem;color:#8b949e;">
      Source: <span class="text-warning">{{ data_source }}</span> &nbsp;·&nbsp; auto-refresh 60s
    </span>
  </div>
  <div style="font-size:.8rem;color:#8b949e;">
    Refreshed {{ now.strftime('%Y-%m-%d %H:%M:%S') }} UTC
  </div>
</div>

<div class="container-fluid px-4">

  {# ── Orchestrator run summary ──────────────────────────────────────────── #}
  {% if orch_info %}
  <div class="card p-3 mb-4">
    <div class="sec-title mb-3">Most Recent Orchestrator Run</div>
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
        <div class="stat-val {% if t.get('error_count',0) > 0 %}text-danger{% endif %}">
          {{ t.get('error_count',0) }}</div>
        <div class="stat-lbl">Errors</div>
      </div>
      {% endif %}
    </div>
  </div>
  {% endif %}

  <div class="row g-4">

    {# ── Sentinel health ───────────────────────────────────────────────────── #}
    <div class="col-12 col-xl-4">
      <div class="card p-3 h-100">
        <div class="sec-title mb-3">🔍 Sentinel Health (Most Recent Run)</div>
        {% if sentinel_data %}
        <div class="table-responsive">
        <table class="table table-sm table-hover">
          <thead><tr><th>State</th><th>Status</th><th>Coverage</th></tr></thead>
          <tbody>
          {% for site in sites %}{% set sid = site.id %}{% if sid in sentinel_data %}
          {% set s = sentinel_data[sid] %}
          {% set cov = s.covered %}{% set exp = s.expected %}
          {% set pct = ((cov / exp * 100)|int) if (cov is not none and exp and exp > 0) else none %}
          <tr>
            <td style="font-size:.82rem;">{{ site.name }}</td>
            <td>
              {% if s.passed == true %}<span class="badge bg-pass">✓ PASS</span>
              {% elif s.passed == false %}<span class="badge bg-fail">✗ FAIL</span>
              {% else %}<span class="badge bg-none">— N/A</span>{% endif %}
            </td>
            <td>
              {% if pct is not none %}
                <span class="me-1" style="font-size:.78rem;">{{ cov }}/{{ exp }}</span>
                <div class="bar-wrap"><div class="bar-fill" style="width:{{ pct }}%;background:
                  {% if pct >= 80 %}#3fb950{% elif pct >= 60 %}#d29922{% else %}#f85149{% endif %};"></div></div>
                <span class="ms-1" style="font-size:.78rem;color:#8b949e;">{{ pct }}%</span>
              {% else %}<span style="color:#8b949e;">—</span>{% endif %}
            </td>
          </tr>
          {% endif %}{% endfor %}
          </tbody>
        </table>
        </div>
        {% else %}
        <p style="color:#8b949e;font-size:.85rem;">No sentinel data for the most recent run.</p>
        {% endif %}
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
            <tr><th>State</th><th>Status</th><th>Found</th><th>New</th>
                <th>Updated</th><th>Docs</th><th>Errors</th><th>Last Run</th></tr>
          </thead>
          <tbody>
          {% for site in sites %}{% set sid = site.id %}
          {% if sid in latest_runs %}{% set r = latest_runs[sid] %}
          {% set st = (r.status or 'unknown')|lower %}
          <tr>
            <td style="font-size:.82rem;white-space:nowrap;">{{ site.name }}</td>
            <td>
              {% if st == 'completed' %}<span class="badge bg-done">✓ done</span>
              {% elif st == 'running' %}<span class="badge bg-run">⟳ running</span>
              {% elif st == 'failed' %}<span class="badge bg-fail">✗ failed</span>
              {% else %}<span class="badge bg-none">{{ st }}</span>{% endif %}
            </td>
            <td style="font-size:.82rem;">{{ r.projects_found if r.projects_found is not none else '—' }}</td>
            <td class="text-success" style="font-size:.82rem;">{% if r.projects_new %}+{{ r.projects_new }}{% else %}—{% endif %}</td>
            <td style="font-size:.82rem;color:#58a6ff;">{% if r.projects_updated %}~{{ r.projects_updated }}{% else %}—{% endif %}</td>
            <td style="font-size:.82rem;color:#8b949e;">{{ r.documents_uploaded if r.documents_uploaded else '—' }}</td>
            <td>
              {% set errs = (r.error_count or 0) %}
              {% if errs > 0 %}
                <span class="text-danger fw-bold" style="font-size:.82rem;">{{ errs }}</span>
                {% if sid in errors_by_site %}
                <div class="err-msg">{{ errors_by_site[sid][:90] }}{% if errors_by_site[sid]|length > 90 %}…{% endif %}</div>
                {% endif %}
              {% else %}<span style="color:#8b949e;font-size:.82rem;">0</span>{% endif %}
            </td>
            <td style="font-size:.78rem;color:#8b949e;white-space:nowrap;">
              {% if r.started_at %}
                {% if r.started_at is string %}{{ r.started_at[:16]|replace('T',' ') }}
                {% else %}{{ r.started_at.strftime('%m-%d %H:%M') }}{% endif %}
              {% else %}—{% endif %}
            </td>
          </tr>
          {% else %}
          <tr style="opacity:.4;">
            <td style="font-size:.82rem;">{{ site.name }}</td>
            <td colspan="7" style="font-size:.78rem;color:#8b949e;">No data yet</td>
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
