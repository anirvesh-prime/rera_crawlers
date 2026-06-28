"""
Targeted-crawl tests for West Bengal RERA (single-listing pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — every other
     row from the DataTables listing is filtered out before detail/upsert.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import west_bengal_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class WestBengalTargetedCrawlTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_target = settings.TARGET_REG_NO
        self._orig_limit = settings.CRAWL_ITEM_LIMIT
        self._orig_scrape = settings.SCRAPE_DETAILS
        settings.CRAWL_ITEM_LIMIT = 0
        settings.SCRAPE_DETAILS = False

    def tearDown(self) -> None:
        settings.TARGET_REG_NO = self._orig_target
        settings.CRAWL_ITEM_LIMIT = self._orig_limit
        settings.SCRAPE_DETAILS = self._orig_scrape

    def _rows(self) -> list[dict]:
        return [
            {"project_registration_no": "WBRERA/P/SOUTH/2023/001", "project_name": "Alpha"},
            {"project_registration_no": "WBRERA/P/SOUTH/2023/002", "project_name": "Beta"},
            {"project_registration_no": "WBRERA/P/SOUTH/2023/003", "project_name": "Gamma"},
        ]

    def _run_with_target(self, target: str):
        settings.TARGET_REG_NO = target
        processed_regs: list[str] = []

        def upsert_side_effect(payload: dict) -> str:
            processed_regs.append(payload.get("project_registration_no"))
            return "new"

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(west_bengal_rera, "_sentinel_check", sentinel),
            mock.patch.object(west_bengal_rera, "_fetch_all_listing_rows",
                              return_value=self._rows()),
            mock.patch.object(west_bengal_rera, "load_checkpoint", return_value={}),
            mock.patch.object(west_bengal_rera, "reset_checkpoint"),
            mock.patch.object(west_bengal_rera, "update_crawl_run_progress"),
            mock.patch.object(west_bengal_rera, "get_project_by_key", return_value=None),
            mock.patch.object(west_bengal_rera, "upsert_project",
                              side_effect=upsert_side_effect),
            mock.patch.object(west_bengal_rera, "insert_crawl_error"),
            mock.patch.object(west_bengal_rera, "get_machine_context",
                              return_value=("host", "127.0.0.1")),
            mock.patch.object(west_bengal_rera, "merge_data_sections", return_value={}),
            mock.patch.object(west_bengal_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(west_bengal_rera, "_quit_driver"),
            mock.patch.object(
                west_bengal_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = west_bengal_rera.run(
                {"id": "west_bengal_rera", "state": "West Bengal", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_listing(self):
        counts, processed_regs, sentinel = self._run_with_target("WBRERA/P/SOUTH/2023/002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["WBRERA/P/SOUTH/2023/002"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("wbrera/p/south/2023/002")
        self.assertEqual(processed_regs, ["WBRERA/P/SOUTH/2023/002"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(west_bengal_rera, "_sentinel_check", sentinel), \
                mock.patch.object(west_bengal_rera, "load_checkpoint", return_value={}), \
                mock.patch.object(west_bengal_rera, "get_machine_context",
                                  return_value=("host", "127.0.0.1")), \
                mock.patch.object(west_bengal_rera, "_quit_driver"):
            counts = west_bengal_rera.run(
                {"id": "west_bengal_rera", "state": "West Bengal", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])


if __name__ == "__main__":
    unittest.main()
