"""
Targeted-crawl tests for Tripura RERA (two-phase listing pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — both the primary
     POST /search listing and the supplementary listing are filtered.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import tripura_rera


class TripuraTargetedCrawlTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_target = settings.TARGET_REG_NO
        self._orig_limit = settings.CRAWL_ITEM_LIMIT
        self._orig_max_pages = settings.MAX_PAGES
        settings.CRAWL_ITEM_LIMIT = 0
        settings.MAX_PAGES = None

    def tearDown(self) -> None:
        settings.TARGET_REG_NO = self._orig_target
        settings.CRAWL_ITEM_LIMIT = self._orig_limit
        settings.MAX_PAGES = self._orig_max_pages

    def _cards(self) -> list[dict]:
        return [
            {"project_registration_no": "PRTR012300001", "project_name": "Alpha"},
            {"project_registration_no": "PRTR012300002", "project_name": "Beta"},
            {"project_registration_no": "PRTR012300003", "project_name": "Gamma"},
        ]

    def _run_with_target(self, target: str, *, search_rows=None, supp_rows=None):
        settings.TARGET_REG_NO = target
        processed_regs: list[str] = []

        def process_side_effect(row, config, run_id, site_id, client, logger,
                                machine_name, machine_ip, counts, done_regs,
                                items_processed, item_limit):
            processed_regs.append(row["project_registration_no"])
            counts["projects_new"] += 1
            return items_processed + 1

        search_rows = self._cards() if search_rows is None else search_rows
        supp_rows = [] if supp_rows is None else supp_rows
        fake_resp = mock.MagicMock()
        fake_resp.text = "<html></html>"

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(tripura_rera, "_sentinel_check", sentinel),
            mock.patch.object(tripura_rera, "_post", return_value=fake_resp),
            mock.patch.object(tripura_rera, "_get", return_value=fake_resp),
            mock.patch.object(tripura_rera, "_parse_search_rows",
                              return_value=(search_rows, len(search_rows))),
            mock.patch.object(tripura_rera, "_parse_listing_rows", return_value=supp_rows),
            mock.patch.object(tripura_rera, "_process_row", side_effect=process_side_effect),
            mock.patch.object(tripura_rera, "load_checkpoint", return_value={}),
            mock.patch.object(tripura_rera, "save_checkpoint"),
            mock.patch.object(tripura_rera, "reset_checkpoint"),
            mock.patch.object(tripura_rera, "random_delay"),
            mock.patch.object(tripura_rera, "update_crawl_run_progress"),
            mock.patch.object(tripura_rera, "insert_crawl_error"),
            mock.patch.object(tripura_rera, "get_machine_context",
                              return_value=("host", "127.0.0.1")),
            mock.patch.object(tripura_rera, "_quit_driver"),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = tripura_rera.run(
                {"id": "tripura_rera", "state": "Tripura", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_listing(self):
        counts, processed_regs, sentinel = self._run_with_target("PRTR012300002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["PRTR012300002"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_filters_supplementary_listing(self):
        counts, processed_regs, _ = self._run_with_target(
            "PRTR012300009",
            search_rows=[],
            supp_rows=[
                {"project_registration_no": "PRTR012300001"},
                {"project_registration_no": "PRTR012300009"},
            ],
        )
        self.assertEqual(processed_regs, ["PRTR012300009"])
        self.assertEqual(counts["projects_found"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("prtr012300002")
        self.assertEqual(processed_regs, ["PRTR012300002"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(tripura_rera, "_sentinel_check", sentinel), \
                mock.patch.object(tripura_rera, "load_checkpoint", return_value={}), \
                mock.patch.object(tripura_rera, "get_machine_context",
                                  return_value=("host", "127.0.0.1")), \
                mock.patch.object(tripura_rera, "insert_crawl_error"), \
                mock.patch.object(tripura_rera, "_quit_driver"):
            counts = tripura_rera.run(
                {"id": "tripura_rera", "state": "Tripura", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])


if __name__ == "__main__":
    unittest.main()
