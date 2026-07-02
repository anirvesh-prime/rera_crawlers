"""
Targeted-crawl tests for Goa RERA (Selenium listing-card pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — every other
     card in the listing is filtered out before detail fetch.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from core.config import settings
from sites import goa_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class _FakeListingPage:
    def __init__(self, pages: dict[int, str]):
        self.pages = pages
        self.start_from = 0
        self.requested_offsets: list[int] = []

    def goto(self, *args, **kwargs):
        return None

    def wait_for_load_state(self, *args, **kwargs):
        return None

    def wait_for_timeout(self, *args, **kwargs):
        return None

    def fill(self, *args, **kwargs):
        return None

    def evaluate(self, script, *args):
        if args and "startFrom" in script:
            self.start_from = int(args[0])
            self.requested_offsets.append(self.start_from)
            return True
        if "form.submit" in script:
            return True
        return None

    def content(self):
        return self.pages.get(self.start_from, "<html><body></body></html>")


def _listing_html(*regs: str) -> str:
    cards = []
    for reg in regs:
        cards.append(
            f"""
            <div class="no_pad_lft">
              <h1>Project: {reg}</h1>
              <span class="reg">RERA Registration No. : {reg}</span>
              <table><tr><td>Promoter {reg}</td><td>Company</td></tr></table>
              <a href="viewProjectDetailPage?projectID={reg}">View</a>
            </div>
            """
        )
    return "<html><body>" + "\n".join(cards) + "</body></html>"


class GoaTargetedCrawlTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_target = settings.TARGET_REG_NO
        self._orig_limit = settings.CRAWL_ITEM_LIMIT
        settings.CRAWL_ITEM_LIMIT = 0

    def tearDown(self) -> None:
        settings.TARGET_REG_NO = self._orig_target
        settings.CRAWL_ITEM_LIMIT = self._orig_limit

    def _cards(self) -> list[dict]:
        return [
            {
                "project_name": "Alpha",
                "project_registration_no": "PRGO01210001",
                "promoter_name": "Promoter One",
                "promoter_type": "Individual",
                "detail_url": None,
            },
            {
                "project_name": "Beta",
                "project_registration_no": "PRGO01210002",
                "promoter_name": "Promoter Two",
                "promoter_type": "Company",
                "detail_url": None,
            },
        ]

    def _run_with_target(self, target: str):
        settings.TARGET_REG_NO = target
        processed_regs: list[str] = []

        def upsert_side_effect(payload: dict) -> str:
            processed_regs.append(payload.get("project_registration_no"))
            return "new"

        sentinel = mock.MagicMock(return_value=True)
        patches = [
            mock.patch.object(goa_rera, "_sentinel_check", sentinel),
            mock.patch.object(goa_rera, "_fetch_project_listing", return_value=self._cards()),
            mock.patch.object(goa_rera, "load_checkpoint", return_value={}),
            mock.patch.object(goa_rera, "save_checkpoint"),
            mock.patch.object(goa_rera, "reset_checkpoint"),
            mock.patch.object(goa_rera, "random_delay"),
            mock.patch.object(goa_rera, "update_crawl_run_progress"),
            mock.patch.object(goa_rera, "get_project_by_key", return_value=None),
            mock.patch.object(goa_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(goa_rera, "insert_crawl_error"),
            mock.patch.object(goa_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(goa_rera, "merge_data_sections", return_value={}),
            mock.patch.object(goa_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(goa_rera, "_quit_driver"),
            mock.patch.object(
                goa_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = goa_rera.run(
                {"id": "goa_rera", "state": "Goa", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_listing(self):
        counts, processed_regs, sentinel = self._run_with_target("PRGO01210002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["PRGO01210002"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("prgo01210002")
        self.assertEqual(processed_regs, ["PRGO01210002"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(goa_rera, "_sentinel_check", sentinel), \
                mock.patch.object(goa_rera, "load_checkpoint", return_value={}), \
                mock.patch.object(goa_rera, "_quit_driver"):
            counts = goa_rera.run(
                {"id": "goa_rera", "state": "Goa", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])

    def test_listing_paginates_without_next_link(self):
        original_max_pages = settings.MAX_PAGES
        settings.MAX_PAGES = 0
        page = _FakeListingPage({
            0: _listing_html("PRGO00000001", "PRGO00000002"),
            2: _listing_html("PRGO00000003", "PRGO00000004"),
            4: _listing_html("PRGO00000005"),
        })
        logger = mock.MagicMock()

        try:
            with mock.patch.object(goa_rera, "_session", return_value=object()), \
                    mock.patch.object(goa_rera, "page_adapter", return_value=page), \
                    mock.patch.object(goa_rera, "_wait_for_captcha_selector", return_value="#captcha"), \
                    mock.patch.object(
                        goa_rera,
                        "_captcha_data_url_from_page",
                        return_value="data:image/png;base64," + ("x" * 100),
                    ), \
                    mock.patch("core.captcha_solver.captcha_to_text", return_value="1234"):
                cards = goa_rera._fetch_project_listing(
                    {"id": "goa_rera"},
                    run_id=123,
                    logger=logger,
                )
        finally:
            settings.MAX_PAGES = original_max_pages

        self.assertEqual(
            [c["project_registration_no"] for c in cards],
            [
                "PRGO00000001",
                "PRGO00000002",
                "PRGO00000003",
                "PRGO00000004",
                "PRGO00000005",
            ],
        )
        self.assertEqual(page.requested_offsets, [0, 2, 4, 5])


if __name__ == "__main__":
    unittest.main()
