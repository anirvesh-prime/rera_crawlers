"""
Targeted-crawl tests for Odisha RERA (Selenium-page paginated pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — every other
     card on each listing page is filtered out, and the page walk stops once
     all targets are found.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import odisha_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class OdishaTargetedCrawlTests(unittest.TestCase):
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
            {"project_registration_no": "RERA/01/2020/00001", "project_name": "Alpha"},
            {"project_registration_no": "RERA/01/2020/00002", "project_name": "Beta"},
            {"project_registration_no": "RERA/01/2020/00003", "project_name": "Gamma"},
        ]

    def _run_with_target(self, target: str):
        settings.TARGET_REG_NO = target
        processed_regs: list[str] = []

        def upsert_side_effect(payload: dict) -> str:
            processed_regs.append(payload.get("project_registration_no"))
            return "new"

        page = mock.MagicMock()
        page.url = "https://rera.odisha.gov.in/project-list"
        page.content.return_value = "<html></html>"

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(odisha_rera, "_session", return_value=mock.MagicMock()),
            mock.patch.object(odisha_rera, "page_adapter", return_value=page),
            mock.patch.object(odisha_rera, "_sentinel_check", sentinel),
            mock.patch.object(odisha_rera, "BeautifulSoup", return_value=mock.MagicMock()),
            mock.patch.object(odisha_rera, "_wait_for_listing_cards", return_value=True),
            mock.patch.object(odisha_rera, "_dismiss_modal"),
            mock.patch.object(odisha_rera, "_scroll_full"),
            mock.patch.object(odisha_rera, "_wait_for_loaders"),
            mock.patch.object(odisha_rera, "_parse_page_cards", return_value=self._cards()),
            mock.patch.object(odisha_rera, "_open_detail_page", return_value=True),
            mock.patch.object(
                odisha_rera, "_parse_overview",
                return_value={"_doc_links": []},
            ),
            mock.patch.object(odisha_rera, "_parse_promoter_tab", return_value={}),
            mock.patch.object(odisha_rera, "_parse_booking_status_cards", return_value=[]),
            mock.patch.object(odisha_rera, "_parse_timeline_table", return_value=[]),
            mock.patch.object(odisha_rera, "_extract_doc_links", return_value=[]),
            mock.patch.object(odisha_rera, "load_checkpoint", return_value={}),
            mock.patch.object(odisha_rera, "save_checkpoint"),
            mock.patch.object(odisha_rera, "reset_checkpoint"),
            mock.patch.object(odisha_rera, "random_delay"),
            mock.patch.object(odisha_rera, "update_crawl_run_progress"),
            mock.patch.object(odisha_rera, "get_project_by_key", return_value=None),
            mock.patch.object(odisha_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(odisha_rera, "insert_crawl_error"),
            mock.patch.object(odisha_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(odisha_rera, "merge_data_sections", return_value={}),
            mock.patch.object(odisha_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(odisha_rera, "build_document_urls", return_value=[]),
            mock.patch.object(
                odisha_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = odisha_rera.run(
                {"id": "odisha_rera", "state": "odisha", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_cards(self):
        counts, processed_regs, sentinel = self._run_with_target("RERA/01/2020/00002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["RERA/01/2020/00002"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("rera/01/2020/00002")
        self.assertEqual(processed_regs, ["RERA/01/2020/00002"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(odisha_rera, "_session", return_value=mock.MagicMock()), \
                mock.patch.object(odisha_rera, "page_adapter", return_value=mock.MagicMock()), \
                mock.patch.object(odisha_rera, "_sentinel_check", sentinel):
            counts = odisha_rera.run(
                {"id": "odisha_rera", "state": "odisha", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])


if __name__ == "__main__":
    unittest.main()
