from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from core.config import settings


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
        self.run_id = run_id
        self._logger = logging.getLogger(f"rera.{site_id}")
        self._logger.setLevel(logging.DEBUG)

        if not self._logger.handlers:
            # Console handler
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            self._logger.addHandler(ch)

            # JSON file handler
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
            log_path = Path(settings.LOG_DIR) / f"{ts}_{site_id}.jsonl"
            self._logger.addHandler(JsonLineHandler(log_path))

    def _log(self, level: int, message: str, extra: dict | None = None):
        self._logger.log(
            level,
            message,
            extra={"site_id": self.site_id, "run_id": self.run_id, "extra": extra or {}},
        )

    def info(self, message: str, **kwargs):
        self._log(logging.INFO, message, kwargs)

    def debug(self, message: str, **kwargs):
        self._log(logging.DEBUG, message, kwargs)

    def warning(self, message: str, **kwargs):
        self._log(logging.WARNING, message, kwargs)

    def error(self, message: str, **kwargs):
        self._log(logging.ERROR, message, kwargs)
