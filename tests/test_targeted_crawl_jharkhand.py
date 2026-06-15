"""
Targeted-crawl tests for Jharkhand RERA (server-paginated listing pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — every other
     row on each listing page is filtered out, and the page walk stops once
     all targets are found.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import jharkhand_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class JharkhandTargetedCrawlTests(unittest.TestCase):
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
            {"project_registration_no": "JHARERA0001", "project_name": "Alpha",
             "address": "Ranchi", "detail_url": ""},
            {"project_registration_no": "JHARERA0002", "project_name": "Beta",
             "address": "Dhanbad", "detail_url": ""},
        ]

    def _run_with_target(self, target: str):
        settings.TARGET_REG_NO = target
        processed_regs: list[str] = []

        def upsert_side_effect(payload: dict) -> str:
            processed_regs.append(payload.get("project_registration_no"))
            return "new"

        fake_resp = mock.MagicMock()
        fake_resp.text = "<html></html>"

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(jharkhand_rera, "_sentinel_check", sentinel),
            mock.patch.object(jharkhand_rera, "safe_get", return_value=fake_resp),
            mock.patch.object(jharkhand_rera, "_parse_listing_rows", return_value=self._rows()),
            mock.patch.object(jharkhand_rera, "_has_next_page", return_value=False),
            mock.patch.object(jharkhand_rera, "reset_checkpoint"),
            mock.patch.object(jharkhand_rera, "random_delay"),
            mock.patch.object(jharkhand_rera, "update_crawl_run_progress"),
            mock.patch.object(jharkhand_rera, "get_project_by_key", return_value=None),
            mock.patch.object(jharkhand_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(jharkhand_rera, "insert_crawl_error"),
            mock.patch.object(jharkhand_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(jharkhand_rera, "merge_data_sections", return_value={}),
            mock.patch.object(jharkhand_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(jharkhand_rera, "_quit_driver"),
            mock.patch.object(
                jharkhand_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = jharkhand_rera.run(
                {"id": "jharkhand_rera", "state": "jharkhand", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_listing(self):
        counts, processed_regs, sentinel = self._run_with_target("JHARERA0002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["JHARERA0002"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("jharera0002")
        self.assertEqual(processed_regs, ["JHARERA0002"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(jharkhand_rera, "_sentinel_check", sentinel), \
                mock.patch.object(jharkhand_rera, "get_machine_context",
                                  return_value=("host", "127.0.0.1")), \
                mock.patch.object(jharkhand_rera, "_quit_driver"):
            counts = jharkhand_rera.run(
                {"id": "jharkhand_rera", "state": "jharkhand", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])


if __name__ == "__main__":
    unittest.main()
