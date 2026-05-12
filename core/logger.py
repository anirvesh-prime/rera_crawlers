from __future__ import annotations

import json
import logging
import traceback as tb_module
from datetime import datetime, timezone
from pathlib import Path

from core.config import settings


_FLUSH_SIZE = 25  # flush to DB after this many buffered entries
_DOCUMENT_EVENT_FLUSH_SIZE = 50  # flush document-event buffer after this many entries

# Steps that represent a successful DB write — shown on the console at INFO level
_WRITE_STEPS = frozenset({"db_upsert", "upsert"})


class _WriteStepFilter(logging.Filter):
    """Let WARNING+ through unconditionally; let INFO through only for write steps."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.WARNING:
            return True
        return getattr(record, "step", None) in _WRITE_STEPS


_CONTEXT_ALIASES = {
    "project_key": ("project_key", "key"),
    "registration_no": ("registration_no", "reg_no"),
    "url": ("url",),
    "page": ("page",),
}

STAGE_DOCUMENTS    = "documents"


class DbLogHandler(logging.Handler):
    """Writes INFO+ log entries to crawl_logs in batches.

    Previously every logger.info() opened a new Postgres connection, executed
    an INSERT, and closed the connection — blocking the crawler on every log
    call.  This handler buffers entries in memory and flushes them as a single
    executemany round-trip when the buffer is full, at the end of each project
    (clear_project), or when the handler is closed (process exit).
    """

    def __init__(self, run_id: int | None, site_id: str):
        super().__init__(level=logging.INFO)
        self._run_id  = run_id
        self._site_id = site_id
        self._buffer: list[dict] = []

    def _make_entry(self, record: logging.LogRecord) -> dict:
        return {
            "run_id":          getattr(record, "run_id", self._run_id),
            "site_id":         getattr(record, "site_id", self._site_id),
            "level":           record.levelname,
            "message":         record.getMessage(),
            "project_key":     getattr(record, "project_key", None),
            "registration_no": getattr(record, "registration_no", None),
            "step":            getattr(record, "step", None),
            "traceback":       getattr(record, "traceback", None),
            "extra":           getattr(record, "extra", {}),
        }

    def emit(self, record: logging.LogRecord) -> None:
        self._buffer.append(self._make_entry(record))
        # Flush immediately for ERROR+ so that failure details are always
        # committed to the DB even when the process crashes shortly after.
        if len(self._buffer) >= _FLUSH_SIZE or record.levelno >= logging.ERROR:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        entries, self._buffer = self._buffer, []
        from core.db import bulk_insert_logs  # late import — avoids circular dependency
        bulk_insert_logs(entries)

    def close(self) -> None:
        """Flush remaining buffered entries before the handler is torn down.
        The logging module calls close() on all handlers at process exit via
        logging.shutdown() (registered as an atexit hook), so buffered entries
        are never silently dropped on a clean exit."""
        self.flush()
        super().close()


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
    """Structured logger for RERA crawlers.

    DB logging (``crawl_logs`` + ``crawl_document_events``) is always active.
    Local ``.jsonl`` file logging is written **only** when ``LOG_LOCAL=true``
    is set in the environment / ``.env`` file.
    """

    def __init__(self, site_id: str, run_id: int | None = None):
        self.site_id = site_id
        self.run_id  = run_id
        self._logger = logging.getLogger(f"rera.{site_id}.{run_id}")
        self._logger.setLevel(logging.DEBUG)

        if not self._logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)      # filter controls what actually appears
            ch.addFilter(_WriteStepFilter())  # WARNING+ always; INFO only for write steps
            ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            self._logger.addHandler(ch)

            # Local .jsonl file — opt-in only (LOG_LOCAL=true in env/.env)
            if settings.LOG_LOCAL:
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
                log_path = Path(settings.LOG_DIR) / site_id / f"{ts}.jsonl"
                self._logger.addHandler(JsonLineHandler(log_path))

            self._logger.addHandler(DbLogHandler(run_id, site_id))

        if not hasattr(self._logger, "_crawler_logger_state"):
            self._logger._crawler_logger_state = {
                "ctx": {},
                "touched_keys": [],
                "touched_key_set": set(),
                "key_summary_logged": False,
            }
        self._state = self._logger._crawler_logger_state

        # Internal buffer for document-event rows
        self._document_event_buffer: list[dict] = []

    def _register_touched_key(self, project_key: str | None) -> None:
        if not project_key:
            return
        key_set = self._state["touched_key_set"]
        if project_key in key_set:
            return
        key_set.add(project_key)
        self._state["touched_keys"].append(project_key)

    def _extract_context(self, extra: dict | None) -> tuple[dict, dict]:
        remaining = dict(extra or {})
        context: dict = {}
        for canonical, aliases in _CONTEXT_ALIASES.items():
            for alias in aliases:
                if alias not in remaining:
                    continue
                value = remaining.pop(alias)
                if value is not None:
                    context[canonical] = value
                    break
        return context, remaining

    # ── Project context ───────────────────────────────────────────────────────

    def set_project(self, *, key: str | None = None, reg_no: str | None = None,
                    url: str | None = None, page: int | None = None):
        """Set per-project context — included in every subsequent log call."""
        self._state["ctx"] = {k: v for k, v in {
            "project_key": key, "registration_no": reg_no,
            "url": url, "page": page,
        }.items() if v is not None}
        self._register_touched_key(self._state["ctx"].get("project_key"))

    def clear_project(self):
        """Clear project context and flush buffered DB log entries.
        Called after every project is fully processed, ensuring logs reach
        the database in near-real-time rather than only at process exit."""
        self._state["ctx"] = {}
        self._flush_db()

    def _flush_db(self) -> None:
        for handler in self._logger.handlers:
            if isinstance(handler, DbLogHandler):
                handler.flush()
        self._flush_document_event_buffer()

    def _flush_document_event_buffer(self) -> None:
        """Flush buffered document-event rows to ``crawl_document_events``."""
        if not self._document_event_buffer:
            return
        entries, self._document_event_buffer = self._document_event_buffer, []
        from core.db import bulk_insert_document_events  # late import — avoids circular dep
        bulk_insert_document_events(entries)

    def _buffer_document_event(self, entry: dict) -> None:
        """Add a document-event row to the buffer; auto-flush when it grows large."""
        self._document_event_buffer.append(entry)
        if len(self._document_event_buffer) >= _DOCUMENT_EVENT_FLUSH_SIZE:
            self._flush_document_event_buffer()

    def close(self) -> None:
        """Flush all buffered entries and close file handles.
        Call this explicitly at the end of a crawler run for a clean shutdown.
        The logging atexit hook also calls this on normal process exit."""
        self._flush_document_event_buffer()
        for handler in list(self._logger.handlers):
            handler.flush()
            handler.close()
            self._logger.removeHandler(handler)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _log(self, level: int, message: str, step: str | None = None,
             traceback: str | None = None, extra: dict | None = None):
        ctx_updates, extra_payload = self._extract_context(extra)
        if ctx_updates:
            self._state["ctx"] = {**self._state["ctx"], **ctx_updates}
        ctx = dict(self._state["ctx"])
        self._register_touched_key(ctx.get("project_key"))

        console_msg = message
        if ctx.get("project_key"):
            console_msg = f"[key={ctx['project_key']}] {console_msg}"
        if ctx.get("registration_no"):
            console_msg = f"[reg={ctx['registration_no']}] {console_msg}"
        if step:
            console_msg = f"[{step}] {console_msg}"
        console_msg = f"[{self.site_id}] {console_msg}"

        self._logger.log(
            level,
            console_msg,
            extra={
                "site_id":         self.site_id,
                "run_id":          self.run_id,
                "project_key":     ctx.get("project_key"),
                "registration_no": ctx.get("registration_no"),
                "step":            step,
                "traceback":       traceback,
                "extra": {
                    **{k: v for k, v in ctx.items()
                       if k not in ("project_key", "registration_no")},
                    **extra_payload,
                },
            },
        )

    def log_run_key_summary(self, limit: int = 10, step: str = "done") -> None:
        if self._state["key_summary_logged"]:
            return
        self._state["key_summary_logged"] = True

        touched_keys = list(self._state["touched_keys"])
        shown_keys = touched_keys[:limit]
        remaining = max(0, len(touched_keys) - len(shown_keys))
        previous_ctx = dict(self._state["ctx"])
        self._state["ctx"] = {}
        try:
            if shown_keys:
                self.info(
                    f"Run key summary ({len(shown_keys)}/{len(touched_keys)} shown): {', '.join(shown_keys)}",
                    step=step,
                )
            else:
                self.info("Run key summary (0/0 shown): no project keys logged", step=step)
            self.info(f"Remaining keys not shown: {remaining}", step=step)
        finally:
            self._state["ctx"] = previous_ctx

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

    def log_document(
        self,
        doc_type: str,
        original_url: str,
        status: str,
        s3_key: str | None = None,
        file_size_bytes: int | None = None,
        **kwargs,
    ) -> None:
        """Record a document download / S3-upload event → ``crawl_document_events``.

        Args:
            doc_type:        Document label / type (e.g. ``"registration_certificate"``).
            original_url:    Source URL before S3 upload.
            status:          ``"uploaded"`` | ``"updated"`` | ``"skipped"`` | ``"failed"``.
            s3_key:          S3 object key (set on success).
            file_size_bytes: Downloaded file size (set on success).
        """
        project_key = self._state["ctx"].get("project_key")

        self._log(
            logging.INFO,
            f"Document [{doc_type}]: {status}",
            step=STAGE_DOCUMENTS,
            extra={"doc_type": doc_type, "url": original_url, "status": status,
                   **(kwargs or {})},
        )
        self._buffer_document_event({
            "run_id":          self.run_id,
            "site_id":         self.site_id,
            "project_key":     project_key,
            "document_type":   doc_type,
            "original_url":    original_url,
            "s3_key":          s3_key,
            "file_size_bytes": file_size_bytes,
            "status":          status,
            "extra":           kwargs or {},
        })
