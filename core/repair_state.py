from __future__ import annotations

import json
import os
import tempfile
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fcntl

from core.config import settings

_LOCK = threading.RLock()


def _state_path() -> Path:
    return Path(settings.LOG_DIR) / "crawler_repair_attempts.json"


def _lock_path() -> Path:
    return Path(settings.LOG_DIR) / "crawler_repair_attempts.lock"


@contextmanager
def _file_lock():
    path = _lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def _read_state() -> dict[str, dict[str, Any]]:
    path = _state_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _write_state(data: dict[str, dict[str, Any]]) -> None:
    path = _state_path()
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


def list_repair_attempts() -> dict[str, dict[str, Any]]:
    with _LOCK:
        with _file_lock():
            return _read_state()


def create_repair_attempt(
    site_id: str,
    run_id: int | None,
    status: str,
    reason: str,
    codex_command: str,
    prompt: str,
    tester_output: str,
) -> bool:
    """Create the one allowed local repair latch for a crawler."""
    with _LOCK:
        with _file_lock():
            data = _read_state()
            if site_id in data:
                return False
            data[site_id] = {
                "site_id": site_id,
                "run_id": run_id if run_id and run_id > 0 else None,
                "attempted_at": datetime.now(timezone.utc).isoformat(),
                "status": status,
                "reason": reason,
                "codex_command": codex_command,
                "prompt": prompt,
                "tester_output": tester_output,
                "codex_output": "",
                "error_message": "",
            }
            _write_state(data)
            return True


def update_repair_attempt(
    site_id: str,
    status: str,
    codex_output: str | None = None,
    error_message: str | None = None,
) -> None:
    with _LOCK:
        with _file_lock():
            data = _read_state()
            attempt = data.get(site_id)
            if not attempt:
                return
            attempt["status"] = status
            if codex_output is not None:
                attempt["codex_output"] = codex_output
            if error_message is not None:
                attempt["error_message"] = error_message
            _write_state(data)


def reset_repair_attempt(site_id: str) -> bool:
    with _LOCK:
        with _file_lock():
            data = _read_state()
            if site_id not in data:
                return False
            del data[site_id]
            _write_state(data)
            return True
