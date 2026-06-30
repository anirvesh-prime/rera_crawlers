"""
Targeted-crawl tests for Bihar RERA (Selenium-paginated pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — every other
     row on each listing page is filtered out during the Selenium walk, and
     the walk stops once all targets are found.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack, contextmanager
from unittest import mock

from core.config import settings
from sites import bihar_rera


class _FakePopup:
    def __init__(self, url: str):
        self.url = url

    def wait_for_load_state(self, *a, **k):
        pass

    def content(self) -> str:
        return "<html></html>"

    def close(self):
        pass


class _PopupInfo:
    def __init__(self, popup):
        self.value = popup


class _FakeContext:
    def __init__(self):
        self._n = 0

    @contextmanager
    def expect_page(self, timeout=None):
        popup = _FakePopup(f"{bihar_rera.FILANPRINT}?id={self._n}")
        self._n += 1
        yield _PopupInfo(popup)

    @property
    def popup_count(self) -> int:
        return self._n


class _FakePage:
    def __init__(self, link_texts):
        self._link_texts = link_texts
        self.context = _FakeContext()

    def goto(self, *a, **k):
        pass

    def content(self) -> str:
        return "<html></html>"

    def eval_on_selector_all(self, selector, js):
        # innerText query → project link labels; everything else (popup click,
        # pager click) returns False (the pager False stops pagination).
        if "innerText" in js:
            return list(self._link_texts)
        return False

    def wait_for_load_state(self, *a, **k):
        pass

    def wait_for_function(self, *a, **k):
        pass

    def close(self):
        pass


class BiharTargetedCrawlTests(unittest.TestCase):
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

    def _rows(self) -> list[dict]:
        return [
            {"project_registration_no": "BRERA0001", "project_name": "Alpha",
             "promoter_name": "P1", "submitted_date": "2024-01-01",
             "project_location_raw": {"address": "A"}},
            {"project_registration_no": "BRERA0002", "project_name": "Beta",
             "promoter_name": "P2", "submitted_date": "2024-01-02",
             "project_location_raw": {"address": "B"}},
            {"project_registration_no": "BRERA0003", "project_name": "Gamma",
             "promoter_name": "P3", "submitted_date": "2024-01-03",
             "project_location_raw": {"address": "C"}},
        ]

    def _run_with_target(self, target: str):
        settings.TARGET_REG_NO = target
        processed_regs: list[str] = []

        def inline_side_effect(raw, detail_url, detail_html, current_page,
                               config, run_id, site_id, mode,
                               machine_name, machine_ip, logger):
            processed_regs.append(raw["project_registration_no"])
            return ({"projects_new": 1, "projects_updated": 0,
                     "projects_skipped": 0, "error_count": 0}, None)

        rows = self._rows()
        link_texts = [r["project_name"] for r in rows]

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(bihar_rera, "_sentinel_check", sentinel),
            mock.patch.object(bihar_rera, "_session", return_value=mock.MagicMock()),
            mock.patch.object(bihar_rera, "page_adapter", return_value=_FakePage(link_texts)),
            mock.patch.object(bihar_rera, "_parse_page_rows", return_value=rows),
            mock.patch.object(bihar_rera, "_process_bihar_inline", side_effect=inline_side_effect),
            mock.patch.object(bihar_rera, "update_crawl_run_progress"),
            mock.patch.object(bihar_rera, "insert_crawl_error"),
            mock.patch.object(bihar_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = bihar_rera.run(
                {"id": "bihar_rera", "state": "Bihar", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_rows(self):
        counts, processed_regs, sentinel = self._run_with_target("BRERA0002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["BRERA0002"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("brera0002")
        self.assertEqual(processed_regs, ["BRERA0002"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(bihar_rera, "_sentinel_check", sentinel):
            counts = bihar_rera.run(
                {"id": "bihar_rera", "state": "Bihar", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])

    def test_daily_light_skips_existing_listing_row_before_detail_navigation(self):
        settings.TARGET_REG_NO = ""
        rows = self._rows()[:2]
        fake_page = _FakePage([r["project_name"] for r in rows])
        processed_regs: list[str] = []

        def existing_by_reg(reg_no, **_kwargs):
            if reg_no == "BRERA0001":
                return {"key": "existing-key", "project_registration_no": reg_no}
            return None

        def inline_side_effect(raw, detail_url, detail_html, current_page,
                               config, run_id, site_id, mode,
                               machine_name, machine_ip, logger):
            processed_regs.append(raw["project_registration_no"])
            return ({"projects_new": 1, "projects_updated": 0,
                     "projects_skipped": 0, "error_count": 0}, None)

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(bihar_rera, "_sentinel_check", sentinel),
            mock.patch.object(bihar_rera, "_session", return_value=mock.MagicMock()),
            mock.patch.object(bihar_rera, "page_adapter", return_value=fake_page),
            mock.patch.object(bihar_rera, "_parse_page_rows", return_value=rows),
            mock.patch.object(
                bihar_rera,
                "get_project_by_registration_no",
                side_effect=existing_by_reg,
            ),
            mock.patch.object(bihar_rera, "get_project_by_key", return_value=None),
            mock.patch.object(bihar_rera, "_process_bihar_inline", side_effect=inline_side_effect),
            mock.patch.object(bihar_rera, "update_crawl_run_progress"),
            mock.patch.object(bihar_rera, "insert_crawl_error"),
            mock.patch.object(bihar_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = bihar_rera.run(
                {"id": "bihar_rera", "state": "Bihar", "config_id": 1},
                run_id=123,
                mode="daily_light",
            )

        sentinel.assert_not_called()
        self.assertEqual(fake_page.context.popup_count, 1)
        self.assertEqual(processed_regs, ["BRERA0002"])
        self.assertEqual(counts["projects_found"], 2)
        self.assertEqual(counts["projects_skipped"], 1)
        self.assertEqual(counts["projects_new"], 1)


if __name__ == "__main__":
    unittest.main()
