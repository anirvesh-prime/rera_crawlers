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
    def __init__(self, pages: dict[int, str], content_failures: dict[int, int] | None = None):
        self.pages = pages
        self.start_from = 0
        self.requested_offsets: list[int] = []
        self.content_failures = dict(content_failures or {})
        self.waited_for_pages: list[int] = []
        self.previous_start_from: int | None = None

    def goto(self, *args, **kwargs):
        return None

    def wait_for_load_state(self, *args, **kwargs):
        return None

    def go_back(self, *args, **kwargs):
        if self.previous_start_from is not None:
            self.start_from = self.previous_start_from

    def wait_for_timeout(self, *args, **kwargs):
        return None

    def wait_for_function(self, script, *, arg=None, timeout=0, **kwargs):
        if "ul.pagination li.active a" in script:
            self.waited_for_pages.append(int(arg))
            soup = goa_rera.BeautifulSoup(self.content(), "lxml")
            active = soup.select_one("ul.pagination li.active a")
            if not active or active.get_text(strip=True) != str(arg):
                raise TimeoutError(f"active page is not {arg}")
        return None

    def fill(self, *args, **kwargs):
        return None

    def click(self, selector, *, timeout=0):
        match = goa_rera.re.search(r"pagging\((\d+)\)", selector)
        if not match:
            raise AssertionError(f"unexpected click selector: {selector}")
        self.previous_start_from = self.start_from
        self.start_from = int(match.group(1))
        self.requested_offsets.append(self.start_from)

    def evaluate(self, script, *args):
        if args and "startFrom" in script:
            self.start_from = int(args[0])
            self.requested_offsets.append(self.start_from)
            return True
        if "document.readyState" in script and "pagination_links" in script:
            soup = goa_rera.BeautifulSoup(self.content(), "lxml")
            active = soup.select_one("ul.pagination li.active a")
            links = []
            for anchor in soup.select("ul.pagination li a"):
                parent = anchor.find_parent("li")
                links.append({
                    "text": anchor.get_text(" ", strip=True),
                    "href": anchor.get("href") or "",
                    "className": " ".join(parent.get("class", [])) if parent else "",
                })
            return {
                "url": "https://rera.goa.gov.in/reraApp/search",
                "ready_state": "complete",
                "active_page_text": active.get_text(strip=True) if active else None,
                "pagination_links": links,
                "card_count": len(soup.select("div.no_pad_lft")),
                "captcha_present": bool(soup.select_one('input[name="captcha"]')),
                "title": "Fake Goa",
                "body_preview": soup.get_text(" ", strip=True)[:500],
            }
        if "pagging" in script:
            raise AssertionError("pagination should click the anchor, not call pagging directly")
        if "form.submit" in script:
            return True
        return None

    def content(self):
        failures_left = self.content_failures.get(self.start_from, 0)
        if failures_left:
            self.content_failures[self.start_from] = failures_left - 1
            raise RuntimeError("simulated mid-navigation content failure")
        return self.pages.get(self.start_from, "<html><body></body></html>")


def _listing_html(
    *regs: str,
    active_page: int = 1,
    page_links: tuple[tuple[str, int | None], ...] = (),
) -> str:
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
    links = []
    for text, offset in page_links:
        href = "javascript:void()" if offset is None else f"javascript:pagging({offset})"
        css_class = ' class="active"' if text.isdigit() and int(text) == active_page else ""
        links.append(f'<li{css_class}><a href="{href}">{text}</a></li>')
    links_html = "\n".join(links)
    pagination = f'<ul class="pagination">{links_html}</ul>' if links_html else ""
    return "<html><body>" + "\n".join(cards) + pagination + "</body></html>"


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

    def test_listing_mode_returns_after_listing_without_detail_or_db_work(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=True)
        with mock.patch.object(goa_rera, "_sentinel_check", sentinel), \
                mock.patch.object(goa_rera, "_fetch_project_listing", return_value=self._cards()), \
                mock.patch.object(goa_rera, "load_checkpoint") as load_checkpoint, \
                mock.patch.object(goa_rera, "update_crawl_run_progress"), \
                mock.patch.object(goa_rera, "get_project_by_key") as get_project_by_key, \
                mock.patch.object(goa_rera, "upsert_project") as upsert_project, \
                mock.patch.object(goa_rera, "_parse_detail_page") as parse_detail, \
                mock.patch.object(goa_rera, "_quit_driver"):
            counts = goa_rera.run(
                {"id": "goa_rera", "state": "Goa", "config_id": 1},
                run_id=123,
                mode="listing",
            )

        sentinel.assert_not_called()
        load_checkpoint.assert_not_called()
        get_project_by_key.assert_not_called()
        upsert_project.assert_not_called()
        parse_detail.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(counts["projects_found"], 2)
        self.assertEqual(counts["projects_new"], 0)
        self.assertEqual(counts["projects_skipped"], 0)

    def test_listing_paginates_with_pagging_offsets(self):
        original_max_pages = settings.MAX_PAGES
        settings.MAX_PAGES = 0
        page = _FakeListingPage({
            0: _listing_html(
                "PRGO00000001",
                "PRGO00000002",
                active_page=1,
                page_links=(("«", 0), ("1", None), ("2", 10), ("3", 20), ("»", None)),
            ),
            10: _listing_html(
                "PRGO00000003",
                "PRGO00000004",
                active_page=2,
                page_links=(("«", 0), ("1", 0), ("2", None), ("3", 20), ("4", 30), ("»", None)),
            ),
            20: _listing_html(
                "PRGO00000005",
                active_page=3,
                page_links=(("«", 10), ("2", 10), ("3", None), ("4", 30), ("5", 40), ("»", None)),
            ),
            30: _listing_html(
                "PRGO00000006",
                active_page=4,
                page_links=(("«", 20), ("3", 20), ("4", None), ("5", 40), ("6", 50), ("»", None)),
            ),
            40: _listing_html(
                "PRGO00000007",
                active_page=5,
                page_links=(("«", 30), ("3", 20), ("4", 30), ("5", None), ("6", 50), ("7", 60), ("»", None)),
            ),
            50: _listing_html("PRGO00000008", active_page=6, page_links=(("«", 40), ("6", None), ("»", None))),
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
                    mock.patch("core.captcha_solver.captcha_to_text", return_value="123456"):
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
                "PRGO00000006",
                "PRGO00000007",
                "PRGO00000008",
            ],
        )
        self.assertEqual(page.requested_offsets, [0, 10, 20, 30, 40, 50])
        self.assertEqual(page.waited_for_pages, [2, 3, 4, 5, 6])

    def test_listing_content_read_retries_transient_navigation_failures(self):
        page = _FakeListingPage(
            {280: _listing_html("PRGO00000280", active_page=29)},
            content_failures={280: 5},
        )
        page.start_from = 280
        logger = mock.MagicMock()

        html = goa_rera._read_goa_page_content(page, 280, logger)

        self.assertIn("PRGO00000280", html)
        logger.warning.assert_not_called()

    def test_pagination_retry_resubmits_same_next_page_without_history(self):
        page = _FakeListingPage(
            {
                470: _listing_html(
                    "PRGO00000470",
                    active_page=48,
                    page_links=(("47", 460), ("48", None), ("49", 480)),
                ),
                480: _listing_html("PRGO00000480", active_page=49),
            },
            content_failures={480: 3},
        )
        page.start_from = 480
        page.previous_start_from = 470
        logger = mock.MagicMock()

        html = goa_rera._retry_goa_pagination_content(page, 480, 470, logger)

        self.assertIn("PRGO00000480", html)
        self.assertEqual(page.requested_offsets, [480])
        self.assertIn(49, page.waited_for_pages)

    def test_listing_recovers_crashed_pagination_with_fresh_captcha_session(self):
        original_max_pages = settings.MAX_PAGES
        settings.MAX_PAGES = 0
        first_page = _FakeListingPage(
            {
                0: _listing_html(
                    "PRGO00000001",
                    active_page=1,
                    page_links=(("1", None), ("2", 10)),
                ),
                10: _listing_html(
                    "PRGO00000002",
                    active_page=2,
                    page_links=(("1", 0), ("2", None), ("3", 20)),
                ),
                20: _listing_html("PRGO00000003", active_page=3),
            },
            content_failures={20: 100},
        )
        recovered_page = _FakeListingPage({
            20: _listing_html("PRGO00000003", active_page=3),
        })
        logger = mock.MagicMock()

        try:
            with mock.patch.object(goa_rera, "_session", return_value=object()), \
                    mock.patch.object(
                        goa_rera,
                        "page_adapter",
                        side_effect=[first_page, recovered_page],
                    ), \
                    mock.patch.object(goa_rera, "_quit_driver") as quit_driver, \
                    mock.patch.object(goa_rera, "_wait_for_captcha_selector", return_value="#captcha"), \
                    mock.patch.object(
                        goa_rera,
                        "_captcha_data_url_from_page",
                        return_value="data:image/png;base64," + ("x" * 100),
                    ), \
                    mock.patch("core.captcha_solver.captcha_to_text", return_value="123456"):
                cards = goa_rera._fetch_project_listing(
                    {"id": "goa_rera"},
                    run_id=123,
                    logger=logger,
                )
        finally:
            settings.MAX_PAGES = original_max_pages

        self.assertEqual(
            [c["project_registration_no"] for c in cards],
            ["PRGO00000001", "PRGO00000002", "PRGO00000003"],
        )
        self.assertEqual(first_page.requested_offsets, [0, 10, 20, 20, 20])
        self.assertEqual(recovered_page.requested_offsets, [20])
        quit_driver.assert_called_once()
        self.assertTrue(any(
            "Restarting Goa Selenium session at startFrom=20" in call.args[0]
            for call in logger.warning.call_args_list
        ))

    def test_pagination_wait_timeout_logs_dom_diagnostics(self):
        page = _FakeListingPage({
            420: _listing_html(
                "PRGO00000420",
                active_page=42,
                page_links=(("41", 400), ("42", None), ("43", 420), ("44", 430)),
            )
        })
        page.start_from = 420
        logger = mock.MagicMock()

        goa_rera._wait_for_goa_listing_page(page, 420, logger)

        logger.warning.assert_called_once()
        _, kwargs = logger.warning.call_args
        self.assertEqual(kwargs["step"], "timing")
        self.assertEqual(kwargs["start_from"], 420)
        self.assertEqual(kwargs["expected_page"], 43)
        diagnostics = kwargs["diagnostics"]
        self.assertEqual(diagnostics["active_page_text"], "42")
        self.assertEqual(diagnostics["card_count"], 1)
        self.assertFalse(diagnostics["captcha_present"])
        self.assertIn({"text": "43", "href": "javascript:pagging(420)", "className": ""}, diagnostics["pagination_links"])

    def test_next_numeric_pagination_stops_when_active_page_has_no_higher_number(self):
        soup = goa_rera.BeautifulSoup(
            _listing_html(
                "PRGO00000009",
                active_page=25,
                page_links=(("«", 230), ("23", 220), ("24", 230), ("25", None), ("»", None)),
            ),
            "lxml",
        )

        self.assertIsNone(goa_rera._parse_next_numeric_pagination_offset(soup))
        self.assertEqual(
            goa_rera._pagination_summary(soup),
            {
                "active_page": "25",
                "numeric_links": [
                    {"text": "23", "offset": 220},
                    {"text": "24", "offset": 230},
                ],
                "raw_links": [
                    "«->javascript:pagging(230)",
                    "23->javascript:pagging(220)",
                    "24->javascript:pagging(230)",
                    "»->javascript:void()",
                ],
            },
        )


if __name__ == "__main__":
    unittest.main()
