from __future__ import annotations

from collections.abc import MutableMapping


DEEP_CRAWL_MODES = {"weekly_deep", "full"}


def is_deep_crawl(mode: str) -> bool:
    return mode in DEEP_CRAWL_MODES


def checkpoint_for_mode(checkpoint: dict | None, mode: str) -> dict:
    """Deep crawls must start from the listing head and process every row."""
    if is_deep_crawl(mode):
        return {}
    return checkpoint or {}


def count_project_upsert(
    counts: MutableMapping[str, int],
    action: str,
    mode: str,
) -> None:
    """Record DB upsert results without reporting unchanged deep refreshes as skips."""
    if action == "new":
        counts["projects_new"] += 1
    elif action == "updated" or is_deep_crawl(mode):
        counts["projects_updated"] += 1
    else:
        counts["projects_skipped"] += 1
