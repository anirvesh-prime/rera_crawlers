"""
Targeted-crawl tests for Rajasthan RERA (Selenium single-listing pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — every other
     row in the parsed listing is filtered out before detail fetch.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import rajasthan_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class RajasthanTargetedCrawlTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_target = settings.TARGET_REG_NO
        self._orig_limit = settings.CRAWL_ITEM_LIMIT
        self._orig_max_pages = settings.MAX_PAGES
        self._orig_skip_documents = settings.SKIP_DOCUMENTS
        settings.CRAWL_ITEM_LIMIT = 0
        settings.MAX_PAGES = None

    def tearDown(self) -> None:
        settings.TARGET_REG_NO = self._orig_target
        settings.CRAWL_ITEM_LIMIT = self._orig_limit
        settings.MAX_PAGES = self._orig_max_pages
        settings.SKIP_DOCUMENTS = self._orig_skip_documents

    def _rows(self) -> list[dict]:
        return [
            {"reg_no": "RAJ/P/2020/0001", "project_name": "Alpha"},
            {"reg_no": "RAJ/P/2020/0002", "project_name": "Beta"},
            {"reg_no": "RAJ/P/2020/0003", "project_name": "Gamma"},
        ]

    def _listing_result(self, rows: list[dict] | None = None, skipped: int = 0):
        rows = self._rows() if rows is None else rows
        return rows, len(self._rows()), skipped

    def _run_with_target(self, target: str):
        settings.TARGET_REG_NO = target
        processed_regs: list[str] = []

        def upsert_side_effect(payload: dict) -> str:
            processed_regs.append(payload.get("project_registration_no"))
            return "new"

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(rajasthan_rera, "_session", return_value=mock.MagicMock()),
            mock.patch.object(rajasthan_rera, "page_adapter", return_value=mock.MagicMock()),
            mock.patch.object(rajasthan_rera, "_sentinel_check", sentinel),
            mock.patch.object(rajasthan_rera, "_scrape_project_list", return_value=self._listing_result()),
            mock.patch.object(rajasthan_rera, "_navigate_to_project_detail", return_value=""),
            mock.patch.object(rajasthan_rera, "random_delay"),
            mock.patch.object(rajasthan_rera, "update_crawl_run_progress"),
            mock.patch.object(rajasthan_rera, "get_project_by_key", return_value=None),
            mock.patch.object(rajasthan_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(rajasthan_rera, "insert_crawl_error"),
            mock.patch.object(rajasthan_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(rajasthan_rera, "merge_data_sections", return_value={}),
            mock.patch.object(rajasthan_rera, "build_document_urls", return_value=[]),
            mock.patch.object(rajasthan_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(
                rajasthan_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = rajasthan_rera.run(
                {"id": "rajasthan_rera", "state": "Rajasthan", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_listing(self):
        counts, processed_regs, sentinel = self._run_with_target("RAJ/P/2020/0002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["RAJ/P/2020/0002"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("raj/p/2020/0002")
        self.assertEqual(processed_regs, ["RAJ/P/2020/0002"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(rajasthan_rera, "_sentinel_check", sentinel):
            counts = rajasthan_rera.run(
                {"id": "rajasthan_rera", "state": "Rajasthan", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])

    def test_skip_documents_bypasses_rajasthan_document_processing(self):
        settings.TARGET_REG_NO = "RAJ/P/2020/0002"
        settings.SKIP_DOCUMENTS = True
        upsert_payloads: list[dict] = []

        def upsert_side_effect(payload: dict) -> str:
            upsert_payloads.append(dict(payload))
            return "new"

        select_doc = mock.MagicMock(return_value={"label": "RERA Registration Certificate 1", "url": "https://example.test/cert.pdf"})
        handle_doc = mock.MagicMock()
        detail_fields = {
            "project_registration_no": "RAJ/P/2020/0002",
            "project_name": "Beta",
        }
        doc_links = [
            {"label": "RERA Registration Certificate", "url": "https://example.test/cert.pdf"}
        ]

        patches = [
            mock.patch.object(rajasthan_rera, "_session", return_value=mock.MagicMock()),
            mock.patch.object(rajasthan_rera, "page_adapter", return_value=mock.MagicMock()),
            mock.patch.object(rajasthan_rera, "_sentinel_check"),
            mock.patch.object(rajasthan_rera, "_scrape_project_list", return_value=self._listing_result()),
            mock.patch.object(rajasthan_rera, "_navigate_to_project_detail", return_value="https://example.test/detail"),
            mock.patch.object(rajasthan_rera, "_scrape_detail_html_via_browser", return_value=(detail_fields, doc_links)),
            mock.patch.object(rajasthan_rera, "random_delay"),
            mock.patch.object(rajasthan_rera, "update_crawl_run_progress"),
            mock.patch.object(rajasthan_rera, "get_project_by_key", return_value=None),
            mock.patch.object(rajasthan_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(rajasthan_rera, "insert_crawl_error"),
            mock.patch.object(rajasthan_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(rajasthan_rera, "merge_data_sections", return_value={}),
            mock.patch.object(rajasthan_rera, "build_document_urls", return_value=[]),
            mock.patch.object(rajasthan_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(
                rajasthan_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
            mock.patch.object(rajasthan_rera, "select_document_for_download", select_doc),
            mock.patch.object(rajasthan_rera, "_handle_document", handle_doc),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = rajasthan_rera.run(
                {"id": "rajasthan_rera", "state": "Rajasthan", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )

        select_doc.assert_not_called()
        handle_doc.assert_not_called()
        self.assertEqual(counts["documents_uploaded"], 0)
        self.assertEqual(len(upsert_payloads), 1)
        self.assertNotIn("uploaded_documents", upsert_payloads[0])
        self.assertNotIn("document_urls", upsert_payloads[0])

    def test_daily_light_returns_before_detail_when_all_rows_exist(self):
        settings.TARGET_REG_NO = ""
        navigate = mock.MagicMock()
        rows = []
        patches = [
            mock.patch.object(rajasthan_rera, "_session", return_value=mock.MagicMock()),
            mock.patch.object(rajasthan_rera, "page_adapter", return_value=mock.MagicMock()),
            mock.patch.object(rajasthan_rera, "_sentinel_check"),
            mock.patch.object(
                rajasthan_rera,
                "_scrape_project_list",
                return_value=self._listing_result(rows=rows, skipped=3),
            ),
            mock.patch.object(rajasthan_rera, "_navigate_to_project_detail", navigate),
            mock.patch.object(rajasthan_rera, "update_crawl_run_progress"),
            mock.patch.object(rajasthan_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = rajasthan_rera.run(
                {"id": "rajasthan_rera", "state": "Rajasthan", "config_id": 1},
                run_id=123,
                mode="daily_light",
            )

        navigate.assert_not_called()
        self.assertEqual(counts["projects_found"], 3)
        self.assertEqual(counts["projects_skipped"], 3)
        self.assertEqual(counts["projects_new"], 0)

    def test_listing_progress_reports_normalized_reg_no(self):
        progress: list[tuple[int, int, int, str | None, str | None]] = []
        page = mock.MagicMock()
        page.locator.return_value.count.return_value = 0
        logger = mock.MagicMock()
        raw_rows = [{"reg_no": "RAJ/P/2024/3058\nApproved on 01/01/2024"}]

        with mock.patch.object(rajasthan_rera, "_session", return_value=mock.MagicMock()), \
             mock.patch.object(rajasthan_rera, "page_adapter", return_value=page), \
             mock.patch.object(rajasthan_rera, "_install_getprojects_tracker"), \
             mock.patch.object(rajasthan_rera, "_reset_getprojects_tracker"), \
             mock.patch.object(rajasthan_rera, "_wait_for_getprojects_request", return_value=True), \
             mock.patch.object(rajasthan_rera, "_wait_for_listing_table", return_value=True), \
             mock.patch.object(rajasthan_rera, "_set_listing_page_size_to_max"), \
             mock.patch.object(rajasthan_rera, "_extract_rj_table_rows", return_value=raw_rows), \
             mock.patch.object(rajasthan_rera, "get_project_by_key", return_value={"key": "existing"}):
            projects, checked, skipped = rajasthan_rera._scrape_project_list(
                logger,
                check_existing=True,
                on_progress=lambda checked_rows, skipped_rows, candidates, reg_no, raw_reg_no:
                    progress.append((checked_rows, skipped_rows, candidates, reg_no, raw_reg_no)),
            )

        self.assertEqual(projects, [])
        self.assertEqual(checked, 1)
        self.assertEqual(skipped, 1)
        self.assertEqual(
            progress[-1],
            (
                1,
                1,
                0,
                "RAJ/P/2024/3058",
                "RAJ/P/2024/3058\nApproved on 01/01/2024",
            ),
        )

    def test_bare_registration_no_removes_rajasthan_listing_suffixes(self):
        self.assertEqual(
            rajasthan_rera._bare_registration_no("RAJ/P/2024/3058\nApproved on 01/01/2024"),
            "RAJ/P/2024/3058",
        )
        self.assertEqual(
            rajasthan_rera._bare_registration_no("RAJ/P/2024/3058 (01/01/2024)"),
            "RAJ/P/2024/3058",
        )


if __name__ == "__main__":
    unittest.main()
