"""
Targeted-crawl tests for Assam RERA (single-listing pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — every other
     stub in the listing is filtered out before detail fetch.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import assam_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class AssamTargetedCrawlTests(unittest.TestCase):
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
                "project_registration_no": "RERA/ASM/PR/0001",
                "project_name": "Alpha",
                "detail_url": "https://example.com/d1",
                "internal_id": "1",
            },
            {
                "project_registration_no": "RERA/ASM/PR/0002",
                "project_name": "Beta",
                "detail_url": "https://example.com/d2",
                "internal_id": "2",
            },
        ]

    def _payload_for(self, stub):
        reg_no = stub["project_registration_no"]
        return {
            "project_registration_no": reg_no,
            "key": assam_rera.generate_project_key(reg_no),
            "url": "https://example.com/d",
            "state": "Assam",
            "domain": assam_rera.DOMAIN,
        }

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
            mock.patch.object(assam_rera, "_sentinel_check", sentinel),
            mock.patch.object(assam_rera, "_fetch_listing", return_value=self._stubs()),
            mock.patch.object(assam_rera, "safe_get", return_value=fake_resp),
            mock.patch.object(assam_rera, "_parse_detail_page", return_value={}),
            mock.patch.object(assam_rera, "_parse_form_a", return_value={}),
            mock.patch.object(
                assam_rera, "_build_payload",
                side_effect=lambda stub, detail, form_a, config_id: self._payload_for(stub),
            ),
            mock.patch.object(assam_rera, "load_checkpoint", return_value={}),
            mock.patch.object(assam_rera, "save_checkpoint"),
            mock.patch.object(assam_rera, "reset_checkpoint"),
            mock.patch.object(assam_rera, "random_delay"),
            mock.patch.object(assam_rera, "update_crawl_run_progress"),
            mock.patch.object(assam_rera, "get_project_by_key", return_value=None),
            mock.patch.object(assam_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(assam_rera, "insert_crawl_error"),
            mock.patch.object(assam_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(assam_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(
                assam_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = assam_rera.run(
                {"id": "assam_rera", "state": "Assam", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_keys, sentinel

    def test_targeted_run_skips_sentinel_and_filters_listing(self):
        counts, processed_keys, sentinel = self._run_with_target("RERA/ASM/PR/0002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(
            processed_keys,
            [assam_rera.generate_project_key("RERA/ASM/PR/0002")],
        )
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        counts, processed_keys, _ = self._run_with_target("rera/asm/pr/0002")
        self.assertEqual(
            processed_keys,
            [assam_rera.generate_project_key("RERA/ASM/PR/0002")],
        )
        self.assertEqual(counts["projects_new"], 1)

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(assam_rera, "_sentinel_check", sentinel):
            counts = assam_rera.run(
                {"id": "assam_rera", "state": "Assam", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])


if __name__ == "__main__":
    unittest.main()
