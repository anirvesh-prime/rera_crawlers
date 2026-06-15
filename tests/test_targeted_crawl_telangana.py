"""
Targeted-crawl tests for Telangana RERA (detail-only reg-no pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — Telangana's
     listing exposes no reg-no, so non-target projects are skipped after the
     PrintPreview detail fetch.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import telangana_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class TelanganaTargetedCrawlTests(unittest.TestCase):
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

    def _rows(self) -> list[dict]:
        return [
            {"print_preview_url": "p1", "project_name": "Alpha", "promoter_name": "P1",
             "data_cert": "c1", "_reg": "P02400001"},
            {"print_preview_url": "p2", "project_name": "Beta", "promoter_name": "P2",
             "data_cert": "c2", "_reg": "P02400002"},
            {"print_preview_url": "p3", "project_name": "Gamma", "promoter_name": "P3",
             "data_cert": "c3", "_reg": "P02400003"},
        ]

    def _run_with_target(self, target: str):
        settings.TARGET_REG_NO = target
        processed_regs: list[str] = []

        def upsert_side_effect(payload: dict) -> str:
            processed_regs.append(payload.get("project_registration_no"))
            return "new"

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(telangana_rera, "_sentinel_check", sentinel),
            mock.patch.object(telangana_rera, "_session", return_value=mock.MagicMock()),
            mock.patch.object(telangana_rera, "page_adapter", return_value=mock.MagicMock()),
            mock.patch.object(telangana_rera, "_submit_search", return_value=True),
            mock.patch.object(telangana_rera, "_get_total_pages", return_value=1),
            mock.patch.object(telangana_rera, "_goto_next_page", return_value=False),
            mock.patch.object(telangana_rera, "_parse_listing_rows", return_value=self._rows()),
            mock.patch.object(telangana_rera, "_fetch_print_preview_html",
                              return_value="<html></html>"),
            mock.patch.object(
                telangana_rera, "_scrape_print_preview",
                side_effect=lambda soup, row: {
                    "project_registration_no": row["_reg"],
                    "project_name": row["project_name"],
                    "promoter_name": row["promoter_name"],
                },
            ),
            mock.patch.object(telangana_rera, "_compute_doc_decoded", return_value="decoded"),
            mock.patch.object(telangana_rera, "_build_uploaded_documents", return_value=[]),
            mock.patch.object(telangana_rera, "load_checkpoint", return_value=None),
            mock.patch.object(telangana_rera, "save_checkpoint"),
            mock.patch.object(telangana_rera, "reset_checkpoint"),
            mock.patch.object(telangana_rera, "random_delay"),
            mock.patch.object(telangana_rera, "update_crawl_run_progress"),
            mock.patch.object(telangana_rera, "get_project_by_key", return_value=None),
            mock.patch.object(telangana_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(telangana_rera, "insert_crawl_error"),
            mock.patch.object(telangana_rera, "get_machine_context",
                              return_value=("host", "127.0.0.1")),
            mock.patch.object(telangana_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(telangana_rera, "_quit_driver"),
            mock.patch.object(
                telangana_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = telangana_rera.run(
                {"id": "telangana_rera", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_detail(self):
        counts, processed_regs, sentinel = self._run_with_target("P02400002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["P02400002"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("p02400002")
        self.assertEqual(processed_regs, ["P02400002"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(telangana_rera, "_sentinel_check", sentinel), \
                mock.patch.object(telangana_rera, "_quit_driver"):
            counts = telangana_rera.run(
                {"id": "telangana_rera", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])


if __name__ == "__main__":
    unittest.main()
