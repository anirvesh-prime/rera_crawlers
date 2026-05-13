"""
Tests for two Tamil Nadu RERA crawler fixes:

  1. weekly_deep must NEVER skip rows via last_project_key — even with a stale
     checkpoint in the DB.  (Bug: interrupted weekly_deep left a checkpoint that
     caused the next run to silently skip thousands of projects.)

  2. CMS year-specific building pages (/cms/reg_projects_tamilnadu/Building/<YYYY>.php)
     must be included in the crawl queue for weekly_deep / full / incremental modes,
     but NOT for daily_light / single modes.
     (Bug: run() only used the master listing — 165 rows — and missed ~2 934 archived
     building projects from 2017-2025.)

  3. Safety guard: if last_project_key is not found anywhere in a listing (e.g. the
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

_FAKE_CMS_URL = (
    "https://rera.tn.gov.in/cms/reg_projects_tamilnadu/Building/2024.php"
)

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
    rows_by_url_substr: dict[str, list[dict]],
    cms_urls: list[str] | None = None,
    item_limit: int = 0,
) -> list:
    """
    Build the minimal set of mock.patch.object calls needed to exercise run()
    without any network, DB, or S3 I/O.

    rows_by_url_substr maps a URL substring → list of row dicts that
    _parse_year_listing should return for that URL.
    """
    if cms_urls is None:
        cms_urls = [_FAKE_CMS_URL]

    def fake_parse_listing(url, logger):
        for substr, rows in rows_by_url_substr.items():
            if substr in url:
                return rows
        return []

    def fake_build_record(row, pd, pj, cfg, rid):
        return {"project_registration_no": row["project_registration_no"]}

    def fake_normalize(payload, config, machine_name, machine_ip):
        return payload

    return [
        mock.patch.object(tamil_nadu_rera, "_sentinel_check", return_value=True),
        mock.patch.object(tamil_nadu_rera, "load_checkpoint", return_value=checkpoint),
        mock.patch.object(tamil_nadu_rera, "save_checkpoint"),
        mock.patch.object(tamil_nadu_rera, "reset_checkpoint"),
        mock.patch.object(tamil_nadu_rera, "_discover_urls_from_cms", return_value=cms_urls),
        mock.patch.object(tamil_nadu_rera, "_parse_year_listing", side_effect=fake_parse_listing),
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
            rows_by_url_substr={
                "registered-building": rows_building,
                "registered-layout": [],
                "2024.php": [],
            },
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
            rows_by_url_substr={
                "registered-building": [_row(_REG_A), _row(_REG_B)],
                "2024.php": [],
                "registered-layout": [_row(_REG_LAYOUT)],
            },
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
            rows_by_url_substr={
                "registered-building": [_row(_REG_A), _row(_REG_B)],
                "2024.php": [],
                "registered-layout": [_row(_REG_LAYOUT)],
            },
        )
        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            counts = tamil_nadu_rera.run(_CONFIG, run_id=3, mode="incremental")

        # REG_A skipped (checkpoint), REG_B processed, REG_LAYOUT processed
        self.assertEqual(counts["projects_skipped"], 1)
        self.assertEqual(counts["projects_updated"], 2)


# ── Test suite 3: CMS year pages in the crawl queue ──────────────────────────

class TestCMSYearPagesInQueue(unittest.TestCase):
    """CMS building year pages must be queued in the right modes."""

    _CMS_2025 = "https://rera.tn.gov.in/cms/reg_projects_tamilnadu/Building/2025.php"
    _CMS_2024 = "https://rera.tn.gov.in/cms/reg_projects_tamilnadu/Building/2024.php"

    def _fetched_urls(self, mode: str, cms_urls: list[str]) -> list[str]:
        captured: list[str] = []

        def fake_parse(url, logger):
            captured.append(url)
            return []

        patches = [
            mock.patch.object(tamil_nadu_rera, "_sentinel_check", return_value=True),
            mock.patch.object(tamil_nadu_rera, "load_checkpoint", return_value={}),
            mock.patch.object(tamil_nadu_rera, "save_checkpoint"),
            mock.patch.object(tamil_nadu_rera, "reset_checkpoint"),
            mock.patch.object(tamil_nadu_rera, "_discover_urls_from_cms", return_value=cms_urls),
            mock.patch.object(tamil_nadu_rera, "_parse_year_listing", side_effect=fake_parse),
            mock.patch.object(tamil_nadu_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(tamil_nadu_rera, "random_delay"),
            mock.patch.object(settings, "CRAWL_ITEM_LIMIT", 0),
        ]
        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            tamil_nadu_rera.run(_CONFIG, run_id=10, mode=mode)

        return captured

    def test_weekly_deep_fetches_cms_year_pages(self):
        urls = self._fetched_urls("weekly_deep", [self._CMS_2025, self._CMS_2024])
        self.assertIn("https://rera.tn.gov.in/registered-building/tn", urls)
        self.assertIn(self._CMS_2025, urls)
        self.assertIn(self._CMS_2024, urls)
        self.assertIn("https://rera.tn.gov.in/registered-layout/tn", urls)
        self.assertEqual(len(urls), 4)  # master_building + 2 CMS + master_layout

    def test_full_fetches_cms_year_pages(self):
        urls = self._fetched_urls("full", [self._CMS_2025])
        self.assertIn(self._CMS_2025, urls)
        self.assertEqual(len(urls), 3)  # master_building + 1 CMS + master_layout

    def test_incremental_fetches_cms_year_pages(self):
        urls = self._fetched_urls("incremental", [self._CMS_2024])
        self.assertIn(self._CMS_2024, urls)
        self.assertEqual(len(urls), 3)

    def test_daily_light_does_not_fetch_cms_year_pages(self):
        urls = self._fetched_urls("daily_light", [self._CMS_2025])
        self.assertNotIn(self._CMS_2025, urls,
                         "daily_light must not crawl CMS year pages")
        self.assertEqual(len(urls), 2,
                         "daily_light must only fetch master building + master layout")

    def test_single_does_not_fetch_cms_year_pages(self):
        urls = self._fetched_urls("single", [self._CMS_2025])
        self.assertNotIn(self._CMS_2025, urls)
        self.assertEqual(len(urls), 2)

    def test_cms_fallback_used_when_discover_returns_empty(self):
        """When _discover_urls_from_cms returns [], known-years fallback kicks in."""
        urls = self._fetched_urls("weekly_deep", [])
        self.assertIn("https://rera.tn.gov.in/registered-building/tn", urls)
        self.assertIn("https://rera.tn.gov.in/registered-layout/tn", urls)
        self.assertGreater(len(urls), 2,
                           "fallback year pages must be appended when CMS index unreachable")

    def test_cms_urls_not_duplicated(self):
        """Duplicate URLs returned by _discover_urls_from_cms must appear only once."""
        urls = self._fetched_urls("weekly_deep", [self._CMS_2025, self._CMS_2025])
        self.assertEqual(urls.count(self._CMS_2025), 1,
                         "each CMS URL must appear exactly once in the queue")


if __name__ == "__main__":
    unittest.main()
