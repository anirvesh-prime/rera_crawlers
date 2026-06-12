"""
Targeted-crawl tests for Chhattisgarh RERA (single-listing pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — every other
     stub in the listing map data is filtered out before detail fetch.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import chhattisgarh_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class ChhattisgarhTargetedCrawlTests(unittest.TestCase):
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

    def _stubs(self) -> list[dict]:
        return [
            {
                "project_registration_no": "PCGRERA0001",
                "project_name": "Alpha",
                "detail_url": "https://example.com/d1",
                "my_id": "1",
            },
            {
                "project_registration_no": "PCGRERA0002",
                "project_name": "Beta",
                "detail_url": "https://example.com/d2",
                "my_id": "2",
            },
        ]

    def _run_with_target(self, target: str):
        settings.TARGET_REG_NO = target
        processed_keys: list[str] = []

        def upsert_side_effect(payload: dict) -> str:
            processed_keys.append(payload["key"])
            return "new"

        fake_resp = mock.MagicMock()
        fake_resp.text = "<html></html>"

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(chhattisgarh_rera, "_sentinel_check", sentinel),
            mock.patch.object(chhattisgarh_rera, "_get", return_value=fake_resp),
            mock.patch.object(chhattisgarh_rera, "_parse_listing_map_data", return_value=self._stubs()),
            mock.patch.object(chhattisgarh_rera, "load_checkpoint", return_value={}),
            mock.patch.object(chhattisgarh_rera, "save_checkpoint"),
            mock.patch.object(chhattisgarh_rera, "reset_checkpoint"),
            mock.patch.object(chhattisgarh_rera, "random_delay"),
            mock.patch.object(chhattisgarh_rera, "update_crawl_run_progress"),
            mock.patch.object(chhattisgarh_rera, "get_project_by_key", return_value=None),
            mock.patch.object(chhattisgarh_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(chhattisgarh_rera, "insert_crawl_error"),
            mock.patch.object(chhattisgarh_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(chhattisgarh_rera, "merge_data_sections", return_value={}),
            mock.patch.object(chhattisgarh_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(
                chhattisgarh_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = chhattisgarh_rera.run(
                {"id": "chhattisgarh_rera", "state": "Chhattisgarh", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_keys, sentinel

    def test_targeted_run_skips_sentinel_and_filters_listing(self):
        counts, processed_keys, sentinel = self._run_with_target("PCGRERA0002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(
            processed_keys,
            [chhattisgarh_rera.generate_project_key("PCGRERA0002")],
        )
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        counts, processed_keys, _ = self._run_with_target("pcgrera0002")
        self.assertEqual(
            processed_keys,
            [chhattisgarh_rera.generate_project_key("PCGRERA0002")],
        )
        self.assertEqual(counts["projects_new"], 1)

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(chhattisgarh_rera, "_sentinel_check", sentinel):
            counts = chhattisgarh_rera.run(
                {"id": "chhattisgarh_rera", "state": "Chhattisgarh", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])


if __name__ == "__main__":
    unittest.main()
