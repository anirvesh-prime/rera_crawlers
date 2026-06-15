"""
Targeted-crawl tests for Gujarat RERA (bulk-enumeration stub pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — the bulk
     enumeration stub list is filtered before the detail-page scrape loop.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import gujarat_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class GujaratTargetedCrawlTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_target = settings.TARGET_REG_NO
        self._orig_limit = settings.CRAWL_ITEM_LIMIT
        settings.CRAWL_ITEM_LIMIT = 0

    def tearDown(self) -> None:
        settings.TARGET_REG_NO = self._orig_target
        settings.CRAWL_ITEM_LIMIT = self._orig_limit

    def _stubs(self) -> list[dict]:
        return [
            {"proj_id": 1, "project_registration_no": "PR/GJ/AHMEDABAD/0001"},
            {"proj_id": 2, "project_registration_no": "PR/GJ/AHMEDABAD/0002"},
        ]

    def _run_with_target(self, target: str):
        settings.TARGET_REG_NO = target
        processed_regs: list[str] = []

        def upsert_side_effect(payload: dict) -> str:
            processed_regs.append(payload.get("project_registration_no"))
            return "new"

        page = mock.MagicMock()
        page.content.return_value = "<html></html>"

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(gujarat_rera, "_sentinel_check", sentinel),
            mock.patch.object(gujarat_rera, "_session", mock.MagicMock()),
            mock.patch.object(gujarat_rera, "page_adapter", return_value=page),
            mock.patch.object(gujarat_rera, "_fetch_listing_stubs", return_value=self._stubs()),
            mock.patch.object(gujarat_rera, "_extract_label_values", return_value={"a": "b"}),
            mock.patch.object(gujarat_rera, "_extract_html_fields", return_value={}),
            mock.patch.object(gujarat_rera, "_parse_overview_card", return_value={}),
            mock.patch.object(gujarat_rera, "_parse_promoter_card", return_value={}),
            mock.patch.object(gujarat_rera, "_parse_partners", return_value={}),
            mock.patch.object(gujarat_rera, "_parse_professionals", return_value={}),
            mock.patch.object(gujarat_rera, "_parse_facilities", return_value={}),
            mock.patch.object(gujarat_rera, "_parse_flat_table", return_value=[]),
            mock.patch.object(gujarat_rera, "_fetch_document_tokens", return_value=[]),
            mock.patch.object(gujarat_rera, "load_checkpoint", return_value={}),
            mock.patch.object(gujarat_rera, "save_checkpoint"),
            mock.patch.object(gujarat_rera, "reset_checkpoint"),
            mock.patch.object(gujarat_rera, "random_delay"),
            mock.patch.object(gujarat_rera, "update_crawl_run_progress"),
            mock.patch.object(gujarat_rera, "get_project_by_key", return_value=None),
            mock.patch.object(gujarat_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(gujarat_rera, "insert_crawl_error"),
            mock.patch.object(gujarat_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(gujarat_rera, "merge_data_sections", return_value={}),
            mock.patch.object(gujarat_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(gujarat_rera, "_quit_driver"),
            mock.patch.object(
                gujarat_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = gujarat_rera.run(
                {"id": "gujarat_rera", "state": "Gujarat", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_listing(self):
        counts, processed_regs, sentinel = self._run_with_target("PR/GJ/AHMEDABAD/0002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["PR/GJ/AHMEDABAD/0002"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("pr/gj/ahmedabad/0002")
        self.assertEqual(processed_regs, ["PR/GJ/AHMEDABAD/0002"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(gujarat_rera, "_sentinel_check", sentinel), \
                mock.patch.object(gujarat_rera, "get_machine_context",
                                  return_value=("host", "127.0.0.1")), \
                mock.patch.object(gujarat_rera, "_quit_driver"):
            counts = gujarat_rera.run(
                {"id": "gujarat_rera", "state": "Gujarat", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])


if __name__ == "__main__":
    unittest.main()
