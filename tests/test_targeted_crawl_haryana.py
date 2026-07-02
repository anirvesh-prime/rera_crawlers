"""
Targeted-crawl tests for Haryana RERA (multi-listing-page stub pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — every other
     stub aggregated from the listing pages is filtered out before detail fetch.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import haryana_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class HaryanaTargetedCrawlTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_target = settings.TARGET_REG_NO
        self._orig_limit = settings.CRAWL_ITEM_LIMIT
        settings.CRAWL_ITEM_LIMIT = 0

    def tearDown(self) -> None:
        settings.TARGET_REG_NO = self._orig_target
        settings.CRAWL_ITEM_LIMIT = self._orig_limit

    def _stubs(self) -> list[dict]:
        return [
            {"project_registration_no": "RC/REP/HARERA/GGM/001", "detail_url": "https://x/1"},
            {"project_registration_no": "RC/REP/HARERA/GGM/002", "detail_url": "https://x/2"},
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
            mock.patch.object(haryana_rera, "_sentinel_check", sentinel),
            mock.patch.object(haryana_rera, "_fetch_listing", return_value=self._stubs()),
            mock.patch.object(haryana_rera, "safe_get", return_value=fake_resp),
            mock.patch.object(haryana_rera, "_parse_detail_page", return_value={}),
            mock.patch.object(
                haryana_rera, "_merge_stub_and_detail",
                side_effect=lambda stub, detail, config_id: {
                    "project_registration_no": stub["project_registration_no"],
                    "key": haryana_rera.generate_project_key(stub["project_registration_no"]),
                    "url": stub.get("detail_url"),
                    "state": "haryana",
                    "domain": "haryana",
                },
            ),
            mock.patch.object(haryana_rera, "load_checkpoint", return_value={}),
            mock.patch.object(haryana_rera, "save_checkpoint"),
            mock.patch.object(haryana_rera, "reset_checkpoint"),
            mock.patch.object(haryana_rera, "random_delay"),
            mock.patch.object(haryana_rera, "update_crawl_run_progress"),
            mock.patch.object(haryana_rera, "get_project_by_key", return_value=None),
            mock.patch.object(haryana_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(haryana_rera, "insert_crawl_error"),
            mock.patch.object(haryana_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(haryana_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(haryana_rera, "_quit_driver"),
            mock.patch.object(
                haryana_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = haryana_rera.run(
                {"id": "haryana_rera", "state": "haryana", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_listing(self):
        counts, processed_regs, sentinel = self._run_with_target("RC/REP/HARERA/GGM/002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["RC/REP/HARERA/GGM/002"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("rc/rep/harera/ggm/002")
        self.assertEqual(processed_regs, ["RC/REP/HARERA/GGM/002"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(haryana_rera, "_sentinel_check", sentinel), \
                mock.patch.object(haryana_rera, "_quit_driver"):
            counts = haryana_rera.run(
                {"id": "haryana_rera", "state": "haryana", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])

    def test_item_limit_does_not_reduce_projects_found(self):
        settings.TARGET_REG_NO = ""
        settings.CRAWL_ITEM_LIMIT = 1
        processed_regs: list[str] = []

        def upsert_side_effect(payload: dict) -> str:
            processed_regs.append(payload.get("project_registration_no"))
            return "updated"

        fake_resp = mock.MagicMock()
        fake_resp.text = "<html></html>"

        patches = [
            mock.patch.object(haryana_rera, "_sentinel_check", return_value=True),
            mock.patch.object(haryana_rera, "_fetch_listing", return_value=self._stubs()),
            mock.patch.object(haryana_rera, "safe_get", return_value=fake_resp),
            mock.patch.object(haryana_rera, "_parse_detail_page", return_value={}),
            mock.patch.object(
                haryana_rera, "_merge_stub_and_detail",
                side_effect=lambda stub, detail, config_id: {
                    "project_registration_no": stub["project_registration_no"],
                    "key": haryana_rera.generate_project_key(stub["project_registration_no"]),
                    "url": stub.get("detail_url"),
                    "state": "haryana",
                    "domain": "haryana",
                },
            ),
            mock.patch.object(haryana_rera, "load_checkpoint", return_value={}),
            mock.patch.object(haryana_rera, "save_checkpoint"),
            mock.patch.object(haryana_rera, "reset_checkpoint"),
            mock.patch.object(haryana_rera, "random_delay"),
            mock.patch.object(haryana_rera, "update_crawl_run_progress"),
            mock.patch.object(haryana_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(haryana_rera, "insert_crawl_error"),
            mock.patch.object(haryana_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(haryana_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(haryana_rera, "_quit_driver"),
            mock.patch.object(
                haryana_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = haryana_rera.run(
                {"id": "haryana_rera", "state": "haryana", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )

        self.assertEqual(counts["projects_found"], 2)
        self.assertEqual(processed_regs, ["RC/REP/HARERA/GGM/001"])
        self.assertEqual(counts["projects_updated"], 1)


if __name__ == "__main__":
    unittest.main()
