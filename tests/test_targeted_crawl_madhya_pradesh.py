"""
Targeted-crawl tests for Madhya Pradesh RERA (detail-only reg-no pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — the reg-no is
     only available on the detail page, so non-target projects are skipped
     after the detail fetch.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import madhya_pradesh_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class MadhyaPradeshTargetedCrawlTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_target = settings.TARGET_REG_NO
        self._orig_limit = settings.CRAWL_ITEM_LIMIT
        settings.CRAWL_ITEM_LIMIT = 0

    def tearDown(self) -> None:
        settings.TARGET_REG_NO = self._orig_target
        settings.CRAWL_ITEM_LIMIT = self._orig_limit

    def _stubs(self) -> list[dict]:
        return [
            {"detail_url": "u1", "project_name": "Alpha", "promoter_name": "P1",
             "status": "Registered", "_reg": "P-GWA-23-0001"},
            {"detail_url": "u2", "project_name": "Beta", "promoter_name": "P2",
             "status": "Registered", "_reg": "P-GWA-23-0002"},
            {"detail_url": "u3", "project_name": "Gamma", "promoter_name": "P3",
             "status": "Registered", "_reg": "P-GWA-23-0003"},
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
            mock.patch.object(madhya_pradesh_rera, "_sentinel_check", sentinel),
            mock.patch.object(madhya_pradesh_rera, "_fetch_listing", return_value=self._stubs()),
            mock.patch.object(madhya_pradesh_rera, "_get", return_value=fake_resp),
            mock.patch.object(
                madhya_pradesh_rera, "_parse_detail",
                side_effect=lambda url, stub, logger: {"project_registration_no": stub["_reg"]},
            ),
            mock.patch.object(madhya_pradesh_rera, "_parse_promoter", return_value={}),
            mock.patch.object(madhya_pradesh_rera, "_parse_consultants", return_value=[]),
            mock.patch.object(madhya_pradesh_rera, "_parse_building_details", return_value={}),
            mock.patch.object(madhya_pradesh_rera, "_parse_unit_counts", return_value=(0, 0)),
            mock.patch.object(madhya_pradesh_rera, "_parse_documents", return_value=[]),
            mock.patch.object(madhya_pradesh_rera, "_parse_qpr", return_value=[]),
            mock.patch.object(madhya_pradesh_rera, "_parse_images", return_value=[]),
            mock.patch.object(madhya_pradesh_rera, "merge_data_sections", return_value={}),
            mock.patch.object(madhya_pradesh_rera, "load_checkpoint", return_value={}),
            mock.patch.object(madhya_pradesh_rera, "save_checkpoint"),
            mock.patch.object(madhya_pradesh_rera, "reset_checkpoint"),
            mock.patch.object(madhya_pradesh_rera, "random_delay"),
            mock.patch.object(madhya_pradesh_rera, "update_crawl_run_progress"),
            mock.patch.object(madhya_pradesh_rera, "get_project_by_key", return_value=None),
            mock.patch.object(madhya_pradesh_rera, "upsert_project",
                              side_effect=upsert_side_effect),
            mock.patch.object(madhya_pradesh_rera, "insert_crawl_error"),
            mock.patch.object(madhya_pradesh_rera, "get_machine_context",
                              return_value=("host", "127.0.0.1")),
            mock.patch.object(madhya_pradesh_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(madhya_pradesh_rera, "_quit_driver"),
            mock.patch.object(
                madhya_pradesh_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = madhya_pradesh_rera.run(
                {"id": "madhya_pradesh_rera", "state": "Madhya Pradesh", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_detail(self):
        counts, processed_regs, sentinel = self._run_with_target("P-GWA-23-0002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["P-GWA-23-0002"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("p-gwa-23-0002")
        self.assertEqual(processed_regs, ["P-GWA-23-0002"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(madhya_pradesh_rera, "_sentinel_check", sentinel), \
                mock.patch.object(madhya_pradesh_rera, "_quit_driver"):
            counts = madhya_pradesh_rera.run(
                {"id": "madhya_pradesh_rera", "state": "Madhya Pradesh", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])

    def test_parse_listing_html_extracts_project_stubs(self):
        html = """
        <table id="example"><tbody>
          <tr>
            <td>1.</td><td>Alpha Project</td><td>Alpha Promoter</td>
            <td>Bhopal&nbsp; - &nbsp;Bhopal</td><td>In Progress</td>
            <td><a href="view_project_details.php?id=abc">View</a></td>
          </tr>
        </tbody></table>
        """

        stubs = madhya_pradesh_rera._parse_listing_html(html)

        self.assertEqual(stubs, [{
            "project_name": "Alpha Project",
            "promoter_name": "Alpha Promoter",
            "district": "Bhopal",
            "area": "Bhopal",
            "status": "In Progress",
            "detail_url": "https://www.rera.mp.gov.in/view_project_details.php?id=abc",
        }])

    def test_fetch_listing_falls_back_to_selenium_when_direct_fetch_has_no_rows(self):
        class _Resp:
            def __init__(self, text: str):
                self.text = text

            def __bool__(self) -> bool:
                return True

        listing_html = """
        <table id="example"><tbody>
          <tr>
            <td>1.</td><td>Beta Project</td><td>Beta Promoter</td>
            <td>Indore&nbsp; - &nbsp;Other</td><td>Approved</td>
            <td><a href="https://www.rera.mp.gov.in/view_project_details.php?id=xyz">View</a></td>
          </tr>
        </tbody></table>
        """
        with mock.patch.object(madhya_pradesh_rera, "safe_get", return_value=_Resp("<html></html>")) as safe_get, \
                mock.patch.object(madhya_pradesh_rera, "_get", return_value=_Resp(listing_html)) as selenium_get:
            stubs = madhya_pradesh_rera._fetch_listing(None)

        safe_get.assert_called_once()
        selenium_get.assert_called_once()
        self.assertEqual(len(stubs), 1)
        self.assertEqual(stubs[0]["project_name"], "Beta Project")


if __name__ == "__main__":
    unittest.main()
