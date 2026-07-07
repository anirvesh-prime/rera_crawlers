"""
Targeted-crawl tests for Himachal Pradesh RERA (AJAX listing pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped (it lives inside the client block), and
  2. only the requested registration number(s) are processed — every other
     reg-no in the listing qs_map is filtered out before detail fetch.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import himachal_pradesh_rera as hp


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class HimachalPradeshTargetedCrawlTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_target = settings.TARGET_REG_NO
        self._orig_limit = settings.CRAWL_ITEM_LIMIT
        settings.CRAWL_ITEM_LIMIT = 0

    def tearDown(self) -> None:
        settings.TARGET_REG_NO = self._orig_target
        settings.CRAWL_ITEM_LIMIT = self._orig_limit

    def _qs_map(self) -> dict[str, str]:
        return {
            "RERAHP01210001": "qs1",
            "RERAHP01210002": "qs2",
            "HPRERA2024ABC001": "qs3",
        }

    def _run_with_target(self, target: str):
        settings.TARGET_REG_NO = target
        processed_regs: list[str] = []

        def upsert_side_effect(payload: dict) -> str:
            processed_regs.append(payload.get("project_registration_no"))
            return "new"

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(hp, "_sentinel_check", sentinel),
            mock.patch.object(hp, "_session", mock.MagicMock()),
            mock.patch.object(hp, "_ClientAdapter", mock.MagicMock()),
            mock.patch.object(hp, "_fetch_listing", return_value=([], self._qs_map())),
            mock.patch.object(hp, "_fetch_section", return_value=None),
            mock.patch.object(hp, "load_checkpoint", return_value={}),
            mock.patch.object(hp, "save_checkpoint"),
            mock.patch.object(hp, "reset_checkpoint"),
            mock.patch.object(hp, "random_delay"),
            mock.patch.object(hp, "update_crawl_run_progress"),
            mock.patch.object(hp, "get_project_by_key", return_value=None),
            mock.patch.object(hp, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(hp, "insert_crawl_error"),
            mock.patch.object(hp, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(hp, "merge_data_sections", return_value={}),
            mock.patch.object(hp, "ProjectRecord", _FakeRecord),
            mock.patch.object(hp, "_quit_driver"),
            mock.patch.object(
                hp, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = hp.run(
                {"id": "himachal_pradesh_rera", "state": "Himachal Pradesh", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_listing(self):
        counts, processed_regs, sentinel = self._run_with_target("RERAHP01210002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["RERAHP01210002"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("rerahp01210002")
        self.assertEqual(processed_regs, ["RERAHP01210002"])

    def test_targeted_run_accepts_hprera_prefix(self):
        _, processed_regs, _ = self._run_with_target("hprera2024abc001")
        self.assertEqual(processed_regs, ["HPRERA2024ABC001"])

    def test_listing_extracts_both_hp_registration_prefixes(self):
        class _Resp:
            text = """
                <div id="reg-Projects">
                  <a title="View Application" data-qs="qs-old">RERAHPABC12345678</a>
                  <a title="View Application" data-qs="qs-new">HPRERA2024ABC001</a>
                  <a title="View Application" data-qs="qs-other">NOT-A-REG</a>
                </div>
                <script>var markers = [];</script>
            """

            def raise_for_status(self):
                return None

        class _Client:
            def get(self, *args, **kwargs):
                return _Resp()

        logger = mock.MagicMock()
        with mock.patch.object(hp, "_get_form_data", return_value=[]):
            _, qs_map = hp._fetch_listing(_Client(), logger)

        self.assertEqual(
            qs_map,
            {
                "RERAHPABC12345678": "qs-old",
                "HPRERA2024ABC001": "qs-new",
            },
        )

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        patches = [
            mock.patch.object(hp, "_sentinel_check", sentinel),
            mock.patch.object(hp, "_session", mock.MagicMock()),
            mock.patch.object(hp, "_ClientAdapter", mock.MagicMock()),
            mock.patch.object(hp, "_fetch_listing", return_value=([], {})),
            mock.patch.object(hp, "load_checkpoint", return_value={}),
            mock.patch.object(hp, "reset_checkpoint"),
            mock.patch.object(hp, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(hp, "_quit_driver"),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = hp.run(
                {"id": "himachal_pradesh_rera", "state": "Himachal Pradesh", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])


if __name__ == "__main__":
    unittest.main()
