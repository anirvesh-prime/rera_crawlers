from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.config import settings


COUNT_KEYS = (
    "projects_found",
    "projects_new",
    "projects_updated",
    "projects_skipped",
    "documents_uploaded",
    "error_count",
)


def _dashboard_dir() -> Path:
    return Path(settings.LOG_DIR) / "dashboard"


def _sites_dir() -> Path:
    return _dashboard_dir() / "sites"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, sort_keys=True, default=str)
            fh.write("\n")
        os.replace(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def site_state_path(site_id: str) -> Path:
    return _sites_dir() / f"{site_id}.json"


def orchestrator_state_path() -> Path:
    return _dashboard_dir() / "orchestrator.json"


def normalise_counts(counts: dict[str, Any] | None = None) -> dict[str, Any]:
    counts = counts or {}
    return {key: counts.get(key, 0) for key in COUNT_KEYS}


def write_site_run_state(
    *,
    site_id: str,
    run_type: str,
    status: str,
    counts: dict[str, Any] | None = None,
    run_id: int | None = None,
    started_at: str | datetime | None = None,
    finished_at: str | datetime | None = None,
    elapsed_s: float | None = None,
    sentinel_passed: bool | None = None,
    error_message: str | None = None,
    traceback: str | None = None,
) -> None:
    if not settings.DASHBOARD_LOCAL_STATE:
        return
    data: dict[str, Any] = {
        "site_id": site_id,
        "run_type": run_type,
        "status": status,
        "run_id": run_id,
        "started_at": started_at or _utc_now(),
        "finished_at": finished_at,
        "elapsed_s": elapsed_s,
        "sentinel_passed": sentinel_passed,
        "updated_at": _utc_now(),
        **normalise_counts(counts),
    }
    if error_message:
        data["error_message"] = error_message
    if traceback:
        data["traceback"] = traceback
    _atomic_write_json(site_state_path(site_id), data)


def write_orchestrator_state(
    *,
    mode: str,
    status: str,
    sites: list[str],
    started_at: str | datetime,
    totals: dict[str, Any] | None = None,
    finished_at: str | datetime | None = None,
) -> None:
    if not settings.DASHBOARD_LOCAL_STATE:
        return
    _atomic_write_json(
        orchestrator_state_path(),
        {
            "mode": mode,
            "status": status,
            "sites": sites,
            "started": started_at,
            "finished_at": finished_at,
            "totals": normalise_counts(totals),
            "elapsed_s": (totals or {}).get("elapsed_s"),
            "updated_at": _utc_now(),
        },
    )
