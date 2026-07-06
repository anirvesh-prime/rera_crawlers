"""
Targeted-crawl tests for Kerala RERA (collected-cards + detail-pool pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — every other
     collected listing card is filtered out before the detail pool runs.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import kerala_rera


class KeralaTargetedCrawlTests(unittest.TestCase):
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

    def _cards_with_page(self) -> list[tuple[int, dict]]:
        return [
            (1, {"cert_no_from_card": "K-RERA/PRJ/KKD/001/2023", "project_name": "Alpha"}),
            (1, {"cert_no_from_card": "K-RERA/PRJ/KKD/002/2023", "project_name": "Beta"}),
            (1, {"cert_no_from_card": "K-RERA/PRJ/KKD/003/2023", "project_name": "Gamma"}),
        ]

    @staticmethod
    def _serial_process_details(items, worker, n_workers, on_result):
        for idx, item in enumerate(items):
            try:
                deltas = worker(idx, item)
            except Exception as exc:  # pragma: no cover - defensive
                on_result(idx, None, exc)
            else:
                on_result(idx, deltas, None)

    def _run_with_target(self, target: str):
        settings.TARGET_REG_NO = target
        processed_regs: list[str] = []

        def card_side_effect(page_num, card, config, run_id, site_id, mode,
                             machine_name, machine_ip, logger):
            processed_regs.append(card["cert_no_from_card"])
            return {"projects_skipped": 0, "projects_new": 1, "projects_updated": 0,
                    "documents_uploaded": 0, "error_count": 0}

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(kerala_rera, "_sentinel_check", sentinel),
            mock.patch.object(kerala_rera, "_get_explore_page",
                              return_value=mock.MagicMock()),
            mock.patch.object(kerala_rera, "_get_total_pages", return_value=1),
            mock.patch.object(kerala_rera, "_collect_listing_cards",
                              return_value=(self._cards_with_page(), 0)),
            mock.patch.object(kerala_rera, "_process_card", side_effect=card_side_effect),
            mock.patch.object(kerala_rera, "process_details",
                              side_effect=self._serial_process_details),
            mock.patch.object(kerala_rera, "get_detail_workers", return_value=1),
            mock.patch.object(kerala_rera, "load_checkpoint", return_value=None),
            mock.patch.object(kerala_rera, "save_checkpoint"),
            mock.patch.object(kerala_rera, "reset_checkpoint"),
            mock.patch.object(kerala_rera, "random_delay"),
            mock.patch.object(kerala_rera, "update_crawl_run_progress"),
            mock.patch.object(kerala_rera, "insert_crawl_error"),
            mock.patch.object(kerala_rera, "get_machine_context",
                              return_value=("host", "127.0.0.1")),
            mock.patch.object(kerala_rera, "_quit_driver"),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = kerala_rera.run(
                {"id": "kerala_rera", "state": "kerala", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_cards(self):
        counts, processed_regs, sentinel = self._run_with_target("K-RERA/PRJ/KKD/002/2023")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["K-RERA/PRJ/KKD/002/2023"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("k-rera/prj/kkd/002/2023")
        self.assertEqual(processed_regs, ["K-RERA/PRJ/KKD/002/2023"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(kerala_rera, "_sentinel_check", sentinel), \
                mock.patch.object(kerala_rera, "_quit_driver"):
            counts = kerala_rera.run(
                {"id": "kerala_rera", "state": "kerala", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])

    def test_collect_listing_cards_stops_at_item_limit(self):
        progress = mock.MagicMock()
        with mock.patch.object(kerala_rera, "_parse_explore_cards",
                               return_value=[c for _, c in self._cards_with_page()]), \
                mock.patch.object(kerala_rera, "save_checkpoint"):
            cards, errors = kerala_rera._collect_listing_cards(
                1,
                1,
                mock.MagicMock(),
                mock.MagicMock(),
                0,
                0,
                "kerala_rera",
                123,
                "weekly_deep",
                item_limit=1,
                on_progress=progress,
            )

        self.assertEqual(errors, 0)
        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0][1]["cert_no_from_card"], "K-RERA/PRJ/KKD/001/2023")
        progress.assert_called_with(1)


if __name__ == "__main__":
    unittest.main()
