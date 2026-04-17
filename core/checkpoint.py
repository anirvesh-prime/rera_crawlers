from __future__ import annotations

from core.db import get_checkpoint, set_checkpoint, clear_checkpoint


def load_checkpoint(site_id: str, run_type: str) -> dict | None:
    """Returns checkpoint dict with last_page and last_project_key, or None."""
    return get_checkpoint(site_id, run_type)


def save_checkpoint(site_id: str, run_type: str, last_page: int,
                    last_project_key: str | None, run_id: int) -> None:
    set_checkpoint(site_id, run_type, last_page, last_project_key, run_id)


def reset_checkpoint(site_id: str, run_type: str) -> None:
    clear_checkpoint(site_id, run_type)
