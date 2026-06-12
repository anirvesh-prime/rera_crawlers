"""
Targeted-crawl tests for Pondicherry RERA (single-listing pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — every other
     card in the parsed listing is filtered out before detail fetch.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import pondicherry_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class PondicherryTargetedCrawlTests(unittest.TestCase):
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
            {
                "project_name": "Alpha", "project_registration_no": "PY/01/2020/001",
                "promoter_name": "P1", "promoter_type": "Individual",
                "project_type": "Residential", "listing_status": "APPROVED",
                "detail_url": "",
            },
            {
                "project_name": "Beta", "project_registration_no": "PY/01/2020/002",
                "promoter_name": "P2", "promoter_type": "Individual",
                "project_type": "Residential", "listing_status": "APPROVED",
                "detail_url": "",
            },
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
            mock.patch.object(pondicherry_rera, "_sentinel_check", sentinel),
            mock.patch.object(pondicherry_rera, "_get_listing", return_value=fake_resp),
            mock.patch.object(pondicherry_rera, "_parse_listing_cards", return_value=self._cards()),
            mock.patch.object(pondicherry_rera, "load_checkpoint", return_value={}),
            mock.patch.object(pondicherry_rera, "save_checkpoint"),
            mock.patch.object(pondicherry_rera, "reset_checkpoint"),
            mock.patch.object(pondicherry_rera, "random_delay"),
            mock.patch.object(pondicherry_rera, "update_crawl_run_progress"),
            mock.patch.object(pondicherry_rera, "get_project_by_key", return_value=None),
            mock.patch.object(pondicherry_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(pondicherry_rera, "insert_crawl_error"),
            mock.patch.object(pondicherry_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(pondicherry_rera, "merge_data_sections", return_value={}),
            mock.patch.object(pondicherry_rera, "build_document_urls", return_value=[]),
            mock.patch.object(pondicherry_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(
                pondicherry_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = pondicherry_rera.run(
                {"id": "pondicherry_rera", "state": "puducherry", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_listing(self):
        counts, processed_regs, sentinel = self._run_with_target("PY/01/2020/002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["PY/01/2020/002"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("py/01/2020/002")
        self.assertEqual(processed_regs, ["PY/01/2020/002"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(pondicherry_rera, "_sentinel_check", sentinel):
            counts = pondicherry_rera.run(
                {"id": "pondicherry_rera", "state": "puducherry", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])


if __name__ == "__main__":
    unittest.main()
