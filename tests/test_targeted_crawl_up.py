"""
Targeted-crawl tests for Uttar Pradesh RERA (district-iteration pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — every other
     project in the district listing is filtered out before detail fetch.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import uttar_pradesh_rera


class UttarPradeshTargetedCrawlTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_target = settings.TARGET_REG_NO
        self._orig_limit = settings.CRAWL_ITEM_LIMIT
        settings.CRAWL_ITEM_LIMIT = 0

    def tearDown(self) -> None:
        settings.TARGET_REG_NO = self._orig_target
        settings.CRAWL_ITEM_LIMIT = self._orig_limit

    def _stubs(self) -> list[dict]:
        return [
            {
                "reg_no": "UPRERAPRJ1001",
                "project_name": "Project One",
                "promoter_name": "Promoter One",
                "district": "Agra",
                "project_type": "Residential",
                "row_index": 0,
            },
            {
                "reg_no": "UPRERAPRJ1002",
                "project_name": "Project Two",
                "promoter_name": "Promoter Two",
                "district": "Agra",
                "project_type": "Residential",
                "row_index": 1,
            },
        ]

    def _run_with_target(self, target: str):
        settings.TARGET_REG_NO = target
        processed_keys: list[str] = []

        def upsert_side_effect(payload: dict) -> str:
            processed_keys.append(payload["key"])
            return "new"

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(uttar_pradesh_rera, "_UP_DISTRICTS", ["Agra"]),
            mock.patch.object(uttar_pradesh_rera, "_sentinel_check", sentinel),
            mock.patch.object(uttar_pradesh_rera, "load_checkpoint", return_value={}),
            mock.patch.object(uttar_pradesh_rera, "save_checkpoint"),
            mock.patch.object(uttar_pradesh_rera, "reset_checkpoint"),
            mock.patch.object(uttar_pradesh_rera, "random_delay"),
            mock.patch.object(uttar_pradesh_rera, "_fetch_district_listing", return_value=self._stubs()),
            mock.patch.object(
                uttar_pradesh_rera, "_fetch_detail_html",
                return_value=("<html>detail</html>", "https://example.com/d"),
            ),
            mock.patch.object(
                uttar_pradesh_rera, "_parse_detail_page",
                side_effect=lambda html, reg_no, district: {
                    "project_registration_no": reg_no,
                    "project_city": district.upper(),
                },
            ),
            mock.patch.object(uttar_pradesh_rera, "_fetch_full_detail_html", return_value=("", "")),
            mock.patch.object(uttar_pradesh_rera, "_extract_documents", return_value=[]),
            mock.patch.object(uttar_pradesh_rera, "get_project_by_key", return_value=None),
            mock.patch.object(uttar_pradesh_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(uttar_pradesh_rera, "insert_crawl_error"),
            mock.patch.object(uttar_pradesh_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(uttar_pradesh_rera, "merge_data_sections", return_value={}),
            mock.patch.object(
                uttar_pradesh_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = uttar_pradesh_rera.run(
                {"id": "uttar_pradesh_rera", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_keys, sentinel

    def test_targeted_run_skips_sentinel_and_filters_listing(self):
        counts, processed_keys, sentinel = self._run_with_target("UPRERAPRJ1002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(
            processed_keys,
            [uttar_pradesh_rera.generate_project_key("UPRERAPRJ1002")],
        )
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        counts, processed_keys, _ = self._run_with_target("upreraprj1002")
        self.assertEqual(
            processed_keys,
            [uttar_pradesh_rera.generate_project_key("UPRERAPRJ1002")],
        )
        self.assertEqual(counts["projects_new"], 1)

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=True)
        with mock.patch.object(uttar_pradesh_rera, "_sentinel_check", sentinel), \
                mock.patch.object(uttar_pradesh_rera, "load_checkpoint", return_value={}), \
                mock.patch.object(uttar_pradesh_rera, "_UP_DISTRICTS", []):
            uttar_pradesh_rera.run(
                {"id": "uttar_pradesh_rera", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()


if __name__ == "__main__":
    unittest.main()
