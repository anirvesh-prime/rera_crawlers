"""
Tests for two Tamil Nadu RERA crawler fixes:

  1. weekly_deep must NEVER skip rows via last_project_key — even with a stale
     checkpoint in the DB.  (Bug: interrupted weekly_deep left a checkpoint that
     caused the next run to silently skip thousands of projects.)

  2. Safety guard: if last_project_key is not found anywhere in a listing (e.g. the
     listing was reordered or the project was removed), the key must be cleared after
     that listing so subsequent listings are NOT silently skipped wholesale.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from core.crawler_base import generate_project_key
from sites import tamil_nadu_rera

# ── Shared constants ──────────────────────────────────────────────────────────

_CONFIG = {
    "id": "tamil_nadu_rera",
    "state": "tamil_nadu",
    "config_id": 14374,
    "domain": "rera.tn.gov.in",
    "state_code": "TN",
}

_BUILDING_URL = "https://rera.tn.gov.in/registered-building/tn"
_LAYOUT_URL   = "https://rera.tn.gov.in/registered-layout/tn"

_REG_A = "TN/01/BLG/0001/2024"
_REG_B = "TN/01/BLG/0002/2024"
_REG_C = "TN/01/BLG/0003/2024"
_REG_LAYOUT = "TN/29/LO/9001/2024"

_KEY_A = generate_project_key(_REG_A)
_KEY_B = generate_project_key(_REG_B)
_KEY_C = generate_project_key(_REG_C)


def _row(reg_no: str) -> dict:
    return {
        "project_registration_no": reg_no,
        "promoter_url": None,
        "detail_url": f"https://rera.tn.gov.in/view/{reg_no}",
        "uploaded_documents": [],
    }


# ── Shared patch factory ──────────────────────────────────────────────────────

def _make_patches(
    checkpoint: dict,
    listings: list[tuple[str, str, list[dict]]],
    item_limit: int = 0,
) -> list:
    """
    Build the minimal set of mock.patch.object calls needed to exercise run()
    without any network, DB, or S3 I/O.

    ``listings`` is the sequence of (base_url, year, rows) tuples that the
    fake _iter_listing_rows generator will yield, in order.
    """

    def fake_iter_listings(logger):
        for tup in listings:
            yield tup

    def fake_build_record(row, pd, pj, cfg, rid):
        return {"project_registration_no": row["project_registration_no"]}

    def fake_normalize(payload, config, machine_name, machine_ip):
        return payload

    return [
        mock.patch.object(tamil_nadu_rera, "_sentinel_check", return_value=True),
        mock.patch.object(tamil_nadu_rera, "load_checkpoint", return_value=checkpoint),
        mock.patch.object(tamil_nadu_rera, "save_checkpoint"),
        mock.patch.object(tamil_nadu_rera, "reset_checkpoint"),
        mock.patch.object(tamil_nadu_rera, "_iter_listing_rows", side_effect=fake_iter_listings),
        mock.patch.object(tamil_nadu_rera, "_parse_promoter_page", return_value={}),
        mock.patch.object(tamil_nadu_rera, "_parse_project_page", return_value={}),
        mock.patch.object(tamil_nadu_rera, "_build_project_record", side_effect=fake_build_record),
        mock.patch.object(tamil_nadu_rera, "normalize_project_payload", side_effect=fake_normalize),
        mock.patch.object(tamil_nadu_rera, "ProjectRecord"),
        mock.patch.object(tamil_nadu_rera, "upsert_project", return_value="updated"),
        mock.patch.object(tamil_nadu_rera, "get_project_by_key", return_value=None),
        mock.patch.object(tamil_nadu_rera, "insert_crawl_error"),
        mock.patch.object(tamil_nadu_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
        mock.patch.object(tamil_nadu_rera, "random_delay"),
        mock.patch.object(tamil_nadu_rera, "select_document_for_download", return_value=None),
        mock.patch.object(settings, "CRAWL_ITEM_LIMIT", item_limit),
    ]


# ── Test suite 1: checkpoint skip behaviour ───────────────────────────────────

class TestWeeklyDeepIgnoresCheckpoint(unittest.TestCase):
    """weekly_deep must never skip rows via last_project_key."""

    def _run(self, mode: str, checkpoint: dict, rows_building: list) -> dict:
        patches = _make_patches(
            checkpoint=checkpoint,
            listings=[
                (_BUILDING_URL, "2026", rows_building),
                (_LAYOUT_URL,   "2026", []),
            ],
        )
        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            return tamil_nadu_rera.run(_CONFIG, run_id=1, mode=mode)

    def test_weekly_deep_processes_all_rows_despite_stale_checkpoint(self):
        """Stale last_project_key must be completely ignored in weekly_deep."""
        stale = {"last_page": 0, "last_project_key": _KEY_B}
        counts = self._run("weekly_deep", stale, [_row(_REG_A), _row(_REG_B), _row(_REG_C)])

        self.assertEqual(counts["projects_skipped"], 0,
                         "weekly_deep must NOT skip any row via last_project_key")
        self.assertEqual(counts["projects_found"], 3)
        self.assertEqual(counts["projects_updated"], 3)

    def test_full_mode_processes_all_rows_despite_stale_checkpoint(self):
        """full mode also ignores last_project_key."""
        stale = {"last_page": 0, "last_project_key": _KEY_A}
        counts = self._run("full", stale, [_row(_REG_A), _row(_REG_B)])

        self.assertEqual(counts["projects_skipped"], 0)
        self.assertEqual(counts["projects_found"], 2)
        self.assertEqual(counts["projects_updated"], 2)

    def test_incremental_still_skips_up_to_checkpoint(self):
        """incremental (non-deep) must still honour last_project_key for resumption."""
        # Checkpoint at REG_A → REG_A skipped, REG_B and REG_C processed
        checkpoint = {"last_page": 0, "last_project_key": _KEY_A}
        counts = self._run("incremental", checkpoint,
                           [_row(_REG_A), _row(_REG_B), _row(_REG_C)])

        self.assertEqual(counts["projects_skipped"], 1)
        self.assertEqual(counts["projects_found"], 3)
        self.assertEqual(counts["projects_updated"], 2)

    def test_incremental_skips_including_checkpoint_row_itself(self):
        """The checkpoint row is skipped (it was already processed in the prior run)."""
        checkpoint = {"last_page": 0, "last_project_key": _KEY_C}
        counts = self._run("incremental", checkpoint,
                           [_row(_REG_A), _row(_REG_B), _row(_REG_C)])

        # All 3 are at or before the checkpoint → all skipped
        self.assertEqual(counts["projects_skipped"], 3)
        self.assertEqual(counts["projects_updated"], 0)


# ── Test suite 2: safety guard ────────────────────────────────────────────────

class TestCheckpointSafetyGuard(unittest.TestCase):
    """If last_project_key is not found in a listing, it must be cleared."""

    def test_safety_guard_clears_key_so_next_listing_is_not_skipped(self):
        """
        last_project_key not present in building listing → safety guard clears it
        → layout listing rows are processed normally instead of being skipped.
        """
        checkpoint = {"last_page": 0, "last_project_key": "key_that_does_not_exist"}

        patches = _make_patches(
            checkpoint=checkpoint,
            listings=[
                (_BUILDING_URL, "2026", [_row(_REG_A), _row(_REG_B)]),
                (_LAYOUT_URL,   "2026", [_row(_REG_LAYOUT)]),
            ],
        )
        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            counts = tamil_nadu_rera.run(_CONFIG, run_id=2, mode="incremental")

        self.assertEqual(counts["projects_found"], 3)
        self.assertEqual(counts["projects_skipped"], 2,
                         "building rows skipped while searching for missing checkpoint key")
        self.assertEqual(counts["projects_updated"], 1,
                         "layout row must be processed after safety guard clears the key")

    def test_safety_guard_does_not_fire_when_key_is_found(self):
        """When the checkpoint key IS found, subsequent listings still work correctly."""
        checkpoint = {"last_page": 0, "last_project_key": _KEY_A}

        patches = _make_patches(
            checkpoint=checkpoint,
            listings=[
                (_BUILDING_URL, "2026", [_row(_REG_A), _row(_REG_B)]),
                (_LAYOUT_URL,   "2026", [_row(_REG_LAYOUT)]),
            ],
        )
        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            counts = tamil_nadu_rera.run(_CONFIG, run_id=3, mode="incremental")

        # REG_A skipped (checkpoint), REG_B processed, REG_LAYOUT processed
        self.assertEqual(counts["projects_skipped"], 1)
        self.assertEqual(counts["projects_updated"], 2)


if __name__ == "__main__":
    unittest.main()
