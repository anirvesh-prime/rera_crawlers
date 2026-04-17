from __future__ import annotations

import json
import logging
import traceback as tb_module
from datetime import datetime, timezone
from pathlib import Path

from core.config import settings


class DbLogHandler(logging.Handler):
    """Writes INFO+ log entries to crawl_logs with full structured context."""

    def __init__(self, run_id: int | None, site_id: str):
        super().__init__(level=logging.INFO)
        self._run_id  = run_id
        self._site_id = site_id

    def emit(self, record: logging.LogRecord) -> None:
        from core.db import insert_log  # late import — avoids circular dependency
        insert_log(
            run_id=getattr(record, "run_id", self._run_id),
            site_id=getattr(record, "site_id", self._site_id),
            level=record.levelname,
            message=record.getMessage(),
            project_key=getattr(record, "project_key", None),
            registration_no=getattr(record, "registration_no", None),
            step=getattr(record, "step", None),
            traceback=getattr(record, "traceback", None),
            extra=getattr(record, "extra", {}),
        )


class JsonLineHandler(logging.Handler):
    """Writes one JSON object per log line to a .jsonl file."""

    def __init__(self, log_path: Path):
        super().__init__()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(log_path, "a", encoding="utf-8")

    def emit(self, record: logging.LogRecord) -> None:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "site_id": getattr(record, "site_id", "orchestrator"),
            "run_id": getattr(record, "run_id", None),
            "level": record.levelname,
            "message": record.getMessage(),
            "project_key": getattr(record, "project_key", None),
            "registration_no": getattr(record, "registration_no", None),
            "step": getattr(record, "step", None),
            "traceback": getattr(record, "traceback", None),
            "extra": getattr(record, "extra", {}),
        }
        self._file.write(json.dumps(entry, default=str) + "\n")
        self._file.flush()

    def close(self):
        self._file.close()
        super().close()


class CrawlerLogger:
    def __init__(self, site_id: str, run_id: int | None = None):
        self.site_id = site_id
        self.run_id  = run_id
        self._logger = logging.getLogger(f"rera.{site_id}.{run_id}")
        self._logger.setLevel(logging.DEBUG)
        self._ctx: dict = {}   # current project context

        if not self._logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(logging.WARNING)   # console: warnings and errors only
            ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            self._logger.addHandler(ch)

            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
            log_path = Path(settings.LOG_DIR) / f"{ts}_{site_id}.jsonl"
            self._logger.addHandler(JsonLineHandler(log_path))
            self._logger.addHandler(DbLogHandler(run_id, site_id))

    # ── Project context ───────────────────────────────────────────────────────

    def set_project(self, *, key: str | None = None, reg_no: str | None = None,
                    url: str | None = None, page: int | None = None):
        """Set per-project context — included in every subsequent log call."""
        self._ctx = {k: v for k, v in {
            "project_key": key, "registration_no": reg_no,
            "url": url, "page": page,
        }.items() if v is not None}

    def clear_project(self):
        """Clear project context after a project is fully processed."""
        self._ctx = {}

    # ── Internal ──────────────────────────────────────────────────────────────

    def _log(self, level: int, message: str, step: str | None = None,
             traceback: str | None = None, extra: dict | None = None):
        url_in_ctx  = self._ctx.get("url", "")
        console_msg = message
        if self._ctx.get("registration_no"):
            console_msg = f"[{self._ctx['registration_no']}] {message}"
        if step:
            console_msg = f"[{step}] {console_msg}"

        self._logger.log(
            level,
            console_msg,
            extra={
                "site_id":         self.site_id,
                "run_id":          self.run_id,
                "project_key":     self._ctx.get("project_key"),
                "registration_no": self._ctx.get("registration_no"),
                "step":            step,
                "traceback":       traceback,
                "extra": {
                    **{k: v for k, v in self._ctx.items()
                       if k not in ("project_key", "registration_no")},
                    **(extra or {}),
                },
            },
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def info(self, message: str, step: str | None = None, **kwargs):
        self._log(logging.INFO, message, step=step, extra=kwargs or None)

    def debug(self, message: str, step: str | None = None, **kwargs):
        self._log(logging.DEBUG, message, step=step, extra=kwargs or None)

    def warning(self, message: str, step: str | None = None, **kwargs):
        self._log(logging.WARNING, message, step=step, extra=kwargs or None)

    def error(self, message: str, step: str | None = None, **kwargs):
        self._log(logging.ERROR, message, step=step, extra=kwargs or None)

    def exception(self, message: str, exc: BaseException, step: str | None = None, **kwargs):
        """Log an error with the full exception traceback."""
        trace = tb_module.format_exc()
        self._log(
            logging.ERROR,
            f"{message} | {type(exc).__name__}: {exc}",
            step=step,
            traceback=trace,
            extra={"error_type": type(exc).__name__, "error_detail": str(exc), **(kwargs or {})},
        )
