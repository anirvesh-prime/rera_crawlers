"""
Targeted-crawl tests for Tamil Nadu RERA (paginated year-listing pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — every other
     row in each year listing is filtered out, and the year walk stops once
     all targets are found.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import tamil_nadu_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class TamilNaduTargetedCrawlTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_target = settings.TARGET_REG_NO
        self._orig_limit = settings.CRAWL_ITEM_LIMIT
        settings.CRAWL_ITEM_LIMIT = 0

    def tearDown(self) -> None:
        settings.TARGET_REG_NO = self._orig_target
        settings.CRAWL_ITEM_LIMIT = self._orig_limit

    def _rows(self) -> list[dict]:
        return [
            {"project_registration_no": "TN/01/Building/0001/2020", "promoter_name": "Alpha"},
            {"project_registration_no": "TN/01/Building/0002/2020", "promoter_name": "Beta"},
            {"project_registration_no": "TN/01/Building/0003/2020", "promoter_name": "Gamma"},
        ]

    def _run_with_target(self, target: str):
        settings.TARGET_REG_NO = target
        processed_regs: list[str] = []

        def upsert_side_effect(payload: dict) -> str:
            processed_regs.append(payload.get("project_registration_no"))
            return "new"

        def build_record(row, promoter_data, project_data, config_id, run_id):
            return {"project_registration_no": row["project_registration_no"]}

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(tamil_nadu_rera, "_sentinel_check", sentinel),
            mock.patch.object(tamil_nadu_rera, "_discover_urls_from_cms", return_value=[]),
            mock.patch.object(tamil_nadu_rera, "_parse_year_listing", return_value=self._rows()),
            mock.patch.object(tamil_nadu_rera, "_build_project_record", side_effect=build_record),
            mock.patch.object(tamil_nadu_rera, "load_checkpoint", return_value={}),
            mock.patch.object(tamil_nadu_rera, "save_checkpoint"),
            mock.patch.object(tamil_nadu_rera, "reset_checkpoint"),
            mock.patch.object(tamil_nadu_rera, "random_delay"),
            mock.patch.object(tamil_nadu_rera, "update_crawl_run_progress"),
            mock.patch.object(tamil_nadu_rera, "get_project_by_key", return_value=None),
            mock.patch.object(tamil_nadu_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(tamil_nadu_rera, "insert_crawl_error"),
            mock.patch.object(tamil_nadu_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(tamil_nadu_rera, "build_document_urls", return_value=[]),
            mock.patch.object(tamil_nadu_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(
                tamil_nadu_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = tamil_nadu_rera.run(
                {"id": "tamil_nadu_rera", "state": "Tamil Nadu", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_listing(self):
        counts, processed_regs, sentinel = self._run_with_target("TN/01/Building/0002/2020")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["TN/01/Building/0002/2020"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("tn/01/building/0002/2020")
        self.assertEqual(processed_regs, ["TN/01/Building/0002/2020"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(tamil_nadu_rera, "_sentinel_check", sentinel):
            counts = tamil_nadu_rera.run(
                {"id": "tamil_nadu_rera", "state": "Tamil Nadu", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])


if __name__ == "__main__":
    unittest.main()
