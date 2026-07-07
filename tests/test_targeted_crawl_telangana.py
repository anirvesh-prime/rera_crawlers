"""
Targeted-crawl tests for Telangana RERA (detail-only reg-no pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — Telangana's
     listing exposes no reg-no, so non-target projects are skipped after the
     PrintPreview detail fetch.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
import base64
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import telangana_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class _FakeNextButton:
    def __init__(self, page: "_FakePaginationPage"):
        self.page = page

    def click(self):
        self.page.current_page += 1


class _FakePaginationPage:
    def __init__(self):
        self.current_page = 1
        self.queries: list[str] = []
        self.wait_arg = None

    def content(self) -> str:
        return (
            "<html><body>"
            f"<input type='hidden' id='CurrentPage' value='{self.current_page}' />"
            "<input type='hidden' id='TotalPages' value='2' />"
            "</body></html>"
        )

    def query_selector(self, selector: str):
        self.queries.append(selector)
        if "text-matches" in selector:
            raise AssertionError("Playwright text-matches selector should not be used")
        if selector == "#btnNext:not([disabled])":
            return _FakeNextButton(self)
        return None

    def wait_for_load_state(self, *args, **kwargs):
        return None

    def wait_for_function(self, script, *, arg=None, timeout=0, **kwargs):
        self.wait_arg = arg
        if arg is None:
            raise AssertionError("current page must be passed as keyword arg")
        return None


class TelanganaTargetedCrawlTests(unittest.TestCase):
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
        telangana_rera._SESSION = None

    def _rows(self) -> list[dict]:
        return [
            {"print_preview_url": "p1", "project_name": "Alpha", "promoter_name": "P1",
             "data_cert": "c1", "_reg": "P02400001"},
            {"print_preview_url": "p2", "project_name": "Beta", "promoter_name": "P2",
             "data_cert": "c2", "_reg": "P02400002"},
            {"print_preview_url": "p3", "project_name": "Gamma", "promoter_name": "P3",
             "data_cert": "c3", "_reg": "P02400003"},
        ]

    def _run_with_target(self, target: str):
        settings.TARGET_REG_NO = target
        processed_projects: list[str] = []
        payloads: list[dict] = []

        def upsert_side_effect(payload: dict) -> str:
            payloads.append(payload)
            processed_projects.append(payload.get("project_name"))
            return "new"

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(telangana_rera, "_sentinel_check", sentinel),
            mock.patch.object(telangana_rera, "_session", return_value=mock.MagicMock()),
            mock.patch.object(telangana_rera, "page_adapter", return_value=mock.MagicMock()),
            mock.patch.object(telangana_rera, "_submit_search", return_value=True),
            mock.patch.object(telangana_rera, "_get_total_pages", return_value=1),
            mock.patch.object(telangana_rera, "_goto_next_page", return_value=False),
            mock.patch.object(telangana_rera, "_parse_listing_rows", return_value=self._rows()),
            mock.patch.object(telangana_rera, "_fetch_print_preview_html",
                              return_value="<html></html>"),
            mock.patch.object(
                telangana_rera, "_scrape_print_preview",
                side_effect=lambda soup, row: {
                    "plan_approval_number": row["_reg"],
                    "project_name": row["project_name"],
                    "promoter_name": row["promoter_name"],
                },
            ),
            mock.patch.object(telangana_rera, "_compute_doc_decoded", return_value="decoded"),
            mock.patch.object(telangana_rera, "_build_uploaded_documents", return_value=[]),
            mock.patch.object(telangana_rera, "load_checkpoint", return_value=None),
            mock.patch.object(telangana_rera, "save_checkpoint"),
            mock.patch.object(telangana_rera, "reset_checkpoint"),
            mock.patch.object(telangana_rera, "random_delay"),
            mock.patch.object(telangana_rera, "update_crawl_run_progress"),
            mock.patch.object(telangana_rera, "get_project_by_key", return_value=None),
            mock.patch.object(telangana_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(telangana_rera, "insert_crawl_error"),
            mock.patch.object(telangana_rera, "get_machine_context",
                              return_value=("host", "127.0.0.1")),
            mock.patch.object(telangana_rera, "_quit_driver"),
            mock.patch.object(
                telangana_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = telangana_rera.run(
                {"id": "telangana_rera", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_projects, payloads, sentinel

    def test_targeted_run_skips_sentinel_and_filters_detail(self):
        counts, processed_projects, payloads, sentinel = self._run_with_target("P02400002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_projects, ["Beta"])
        self.assertNotIn("project_registration_no", payloads[0])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_projects, payloads, _ = self._run_with_target("p02400002")
        self.assertEqual(processed_projects, ["Beta"])
        self.assertNotIn("project_registration_no", payloads[0])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(telangana_rera, "_sentinel_check", sentinel), \
                mock.patch.object(telangana_rera, "_quit_driver"):
            counts = telangana_rera.run(
                {"id": "telangana_rera", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])

    def test_session_allows_captcha_images(self):
        telangana_rera._SESSION = None
        fake_session = mock.MagicMock()
        with mock.patch.object(
            telangana_rera, "SeleniumSession", return_value=fake_session,
        ) as selenium_session:
            self.assertIs(telangana_rera._session(), fake_session)
        selenium_session.assert_called_once_with(
            ignore_certificate_errors=True,
            block_images=False,
        )

    def test_goto_next_page_uses_selenium_safe_next_selector(self):
        page = _FakePaginationPage()

        self.assertTrue(telangana_rera._goto_next_page(page, fast=True))

        self.assertEqual(page.current_page, 2)
        self.assertEqual(page.queries, ["#btnNext:not([disabled])"])
        self.assertEqual(page.wait_arg, 1)

    def test_doc_decoded_uses_legacy_query_string_through_character_d(self):
        def enc(query: str) -> str:
            return base64.b64encode(query.encode("utf-8")).decode("ascii").rstrip("=")

        raw_cert = enc(
            "ProjectID=4&Division=1&UserID=20287&RoleID=1&AppID=5&"
            "Action=SEARCH&CharacterD=07&ExtAppID=999&IsAbyence=1"
        )

        self.assertEqual(
            telangana_rera._compute_doc_decoded(raw_cert),
            "ProjectID=4&Division=1&UserID=20287&RoleID=1&AppID=5&"
            "Action=SEARCH&CharacterD=07",
        )

    def test_radha_meadows_legacy_key_reproduction(self):
        raw_cert = (
            "UHJvamVjdElEPTU1MDY0JkRpdmlzaW9uPTEmVXNlcklEPTE3MTU2NCZSb2xlSUQ9MSZ"
            "BcHBJRD03MDM0MiZBY3Rpb249U0VBUkNIJkNoYXJhY3RlckQ9OTkmRXh0QXBwSUQ9Jk"
            "lzQWJ5ZW5jZT0w"
        )
        doc_decoded = telangana_rera._compute_doc_decoded(raw_cert)
        key_input = (
            "RADHA MEADOWS"
            "STABLE VENTURES LLP"
            "telangana"
            f"{doc_decoded}"
        )

        self.assertEqual(
            doc_decoded,
            "ProjectID=55064&Division=1&UserID=171564&RoleID=1&"
            "AppID=70342&Action=SEARCH&CharacterD=99",
        )
        self.assertEqual(telangana_rera.generate_project_key(key_input), "13046654106607487026")

    def test_listing_parser_reads_sixth_cell_data_cert(self):
        raw_cert = base64.b64encode(
            b"ProjectID=4&Division=1&UserID=20287&RoleID=1&AppID=5&"
            b"Action=SEARCH&CharacterD=07&ExtAppID=&IsAbyence=0"
        ).decode("ascii").rstrip("=")
        html = f"""
        <table>
          <thead>
            <tr>
              <th>Project Name</th><th>Promoter Name</th><th>A</th>
              <th>B</th><th>C</th><th>Certificate</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>SKV S ANANDA VILAS</td>
              <td>KACHAM RAJESHWAR</td>
              <td></td><td></td>
              <td><a href="/PrintPreview/PrintPreview?q=abc">View</a></td>
              <td><a data-qstr="{raw_cert}">View Certificate</a></td>
            </tr>
          </tbody>
        </table>
        """

        rows = telangana_rera._parse_listing_rows(html)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["data_cert"], raw_cert)
        self.assertEqual(rows[0]["project_id"], "4")
        self.assertEqual(rows[0]["user_id"], "20287")
        self.assertEqual(
            telangana_rera._compute_doc_decoded(rows[0]["data_cert"]),
            "ProjectID=4&Division=1&UserID=20287&RoleID=1&AppID=5&"
            "Action=SEARCH&CharacterD=07",
        )
        self.assertIsNotNone(rows[0]["cert_url"])
        self.assertIsNotNone(rows[0]["preview_pdf_url"])


if __name__ == "__main__":
    unittest.main()
