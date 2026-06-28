from __future__ import annotations

import unittest
from unittest.mock import patch

from core.config import settings
from core.crawler_base import (
    SeleniumPageAdapter,
    SeleniumResponse,
    SeleniumSession,
    _find_by_text,
    _glob_to_regex,
    _parse_text_selector,
    _url_matches,
    _xpath_literal,
    get_scaled_delay_range,
    random_delay,
)


class CrawlerBaseOptimizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_delay_scale = settings.CRAWL_DELAY_SCALE

    def tearDown(self) -> None:
        settings.CRAWL_DELAY_SCALE = self.original_delay_scale

    def test_get_scaled_delay_range_applies_global_scale(self):
        settings.CRAWL_DELAY_SCALE = 0.25
        self.assertEqual(get_scaled_delay_range(2.0, 4.0), (0.5, 1.0))

    @patch("core.crawler_base.time.sleep")
    @patch("core.crawler_base.random.uniform", return_value=0.75)
    def test_random_delay_uses_scaled_range(self, mock_uniform, mock_sleep):
        settings.CRAWL_DELAY_SCALE = 0.25

        random_delay(2.0, 4.0)

        mock_uniform.assert_called_once_with(0.5, 1.0)
        mock_sleep.assert_called_once_with(0.75)

    @patch("core.crawler_base.time.sleep")
    @patch("core.crawler_base.random.uniform")
    def test_random_delay_skips_sleep_when_scale_is_zero(self, mock_uniform, mock_sleep):
        settings.CRAWL_DELAY_SCALE = 0.0

        random_delay(2.0, 4.0)

        mock_uniform.assert_not_called()
        mock_sleep.assert_not_called()


class _FakeElement:
    def __init__(self, text: str, displayed: bool = True, raise_on_click: bool = False,
                 tag_name: str = "div", attrs: dict | None = None):
        self.text = text
        self._displayed = displayed
        self._raise_on_click = raise_on_click
        self.tag_name = tag_name
        self._attrs = attrs or {}
        self.clicked = False

    def is_displayed(self) -> bool:
        return self._displayed

    def get_attribute(self, name: str):
        return self._attrs.get(name)

    def click(self) -> None:
        if self._raise_on_click:
            raise RuntimeError("element click intercepted")
        self.clicked = True


class _FakeDriver:
    """Records find_elements(by, selector) calls and returns scripted results."""

    def __init__(self, results: dict):
        # results maps (by, selector) -> list[_FakeElement]; XPath lookups fall
        # back to the single registered XPath entry regardless of the exact
        # query string so tests don't have to mirror the generated XPath.
        self._results = results
        self.calls: list[tuple[str, str]] = []
        self.scripts: list[str] = []

    def execute_script(self, script, *args):
        # scrollIntoView / JS-click helpers invoked by SeleniumPageAdapter.click.
        self.scripts.append(script)
        return None

    def find_elements(self, by, selector):
        self.calls.append((by, selector))
        if (by, selector) in self._results:
            return self._results[(by, selector)]
        for (rby, _rsel), elems in self._results.items():
            if rby == by:
                return elems
        return []


class _FakeSession:
    def __init__(self, driver):
        self._driver = driver

    def driver(self):
        return self._driver


class TextSelectorEngineTests(unittest.TestCase):
    def test_parse_text_selector_substring_vs_exact(self):
        self.assertEqual(_parse_text_selector("text=Promoter Details"),
                         ("Promoter Details", False))
        self.assertEqual(_parse_text_selector('text="Documents"'),
                         ("Documents", True))
        self.assertEqual(_parse_text_selector("text='Booking Status'"),
                         ("Booking Status", True))

    def test_parse_text_selector_ignores_non_text_engine(self):
        self.assertIsNone(_parse_text_selector("div.foo"))
        self.assertIsNone(_parse_text_selector('a:has-text("View")'))

    def test_xpath_literal_quoting(self):
        self.assertEqual(_xpath_literal("Promoter Details"), '"Promoter Details"')
        self.assertEqual(_xpath_literal("say 'hi'"), '''"say 'hi'"''')
        self.assertIn("concat(", _xpath_literal('a"b\'c'))

    def test_find_by_text_uses_xpath_and_orders_innermost_first(self):
        from selenium.webdriver.common.by import By

        outer = _FakeElement("Promoter Details and lots of surrounding text")
        inner = _FakeElement("Promoter Details")
        driver = _FakeDriver({(By.XPATH, "*"): [outer, inner]})

        elems = _find_by_text(driver, "Promoter Details", exact=False)

        self.assertEqual(elems[0], inner)  # shortest text wins
        by, xpath = driver.calls[0]
        self.assertEqual(by, By.XPATH)
        self.assertIn("translate(", xpath)
        self.assertIn("promoter details", xpath)  # lower-cased for matching

    def test_find_by_text_prefers_clickable_anchor_over_wrapper(self):
        from selenium.webdriver.common.by import By

        # Same label text on an <li> wrapper and its inner <a> — the anchor must
        # win so a JS-dispatched click reaches the framework's tab handler.
        li = _FakeElement("Promoter Details", tag_name="li")
        anchor = _FakeElement("Promoter Details", tag_name="a")
        driver = _FakeDriver({(By.XPATH, "*"): [li, anchor]})

        elems = _find_by_text(driver, "Promoter Details", exact=False)

        self.assertEqual(elems[0], anchor)


class AdapterSelectorRoutingTests(unittest.TestCase):
    def _adapter(self, results):
        driver = _FakeDriver(results)
        return SeleniumPageAdapter(_FakeSession(driver)), driver

    def test_find_elements_routes_text_engine_to_xpath(self):
        from selenium.webdriver.common.by import By

        el = _FakeElement("Documents")
        adapter, driver = self._adapter({(By.XPATH, "*"): [el]})

        out = adapter._find_elements("text=Documents")

        self.assertEqual(out, [el])
        self.assertTrue(all(by == By.XPATH for by, _ in driver.calls))

    def test_find_elements_routes_xpath_prefix(self):
        from selenium.webdriver.common.by import By

        el = _FakeElement("x")
        adapter, driver = self._adapter({(By.XPATH, "//div"): [el]})

        out = adapter._find_elements("xpath=//div")

        self.assertEqual(out, [el])
        self.assertEqual(driver.calls[0], (By.XPATH, "//div"))

    def test_find_elements_routes_css_default(self):
        from selenium.webdriver.common.by import By

        el = _FakeElement("x")
        adapter, driver = self._adapter({(By.CSS_SELECTOR, "div.foo"): [el]})

        out = adapter._find_elements("div.foo")

        self.assertEqual(out, [el])
        self.assertEqual(driver.calls[0], (By.CSS_SELECTOR, "div.foo"))

    def test_click_text_selector_clicks_first_displayed(self):
        from selenium.webdriver.common.by import By

        hidden = _FakeElement("Promoter Details", displayed=False)
        visible = _FakeElement("Promoter Details", displayed=True)
        # innermost-first ordering keeps both; the first *displayed* is clicked.
        adapter, _driver = self._adapter({(By.XPATH, "*"): [hidden, visible]})

        adapter.click("text=Promoter Details", timeout=1000)

        self.assertFalse(hidden.clicked)
        self.assertTrue(visible.clicked)

    def test_click_falls_back_to_js_when_native_click_intercepted(self):
        from selenium.webdriver.common.by import By

        el = _FakeElement("Documents", displayed=True, raise_on_click=True)
        adapter, driver = self._adapter({(By.XPATH, "*"): [el]})

        # Native el.click() raises (intercepted); adapter must dispatch a JS
        # click instead of giving up.
        adapter.click("text=Documents", timeout=1000)

        self.assertIn("arguments[0].click();", driver.scripts)


class UrlMatchingTests(unittest.TestCase):
    def test_glob_double_star_spans_path_separators(self):
        rx = _glob_to_regex("**/project-details/**")
        self.assertTrue(rx.match("https://rera.odisha.gov.in/x/project-details/123"))
        self.assertFalse(rx.match("https://rera.odisha.gov.in/project-list"))

    def test_glob_single_star_stays_within_segment(self):
        rx = _glob_to_regex("https://host/*/end")
        self.assertTrue(rx.match("https://host/seg/end"))
        self.assertFalse(rx.match("https://host/a/b/end"))  # * must not span '/'

    def test_url_matches_supports_glob_regex_callable_exact(self):
        import re as _re

        self.assertTrue(_url_matches("**/project-details/**",
                                     "https://h/p/project-details/9"))
        self.assertTrue(_url_matches(_re.compile(r"project-details"),
                                     "https://h/project-details/9"))
        self.assertTrue(_url_matches(lambda u: u.endswith("/9"),
                                     "https://h/9"))
        self.assertTrue(_url_matches("https://h/exact", "https://h/exact"))
        self.assertFalse(_url_matches("https://h/exact", "https://h/other"))


class _UrlSeqDriver:
    """Returns a scripted sequence of current_url values (last value sticks)."""

    def __init__(self, urls):
        self._urls = list(urls)

    @property
    def current_url(self):
        return self._urls.pop(0) if len(self._urls) > 1 else self._urls[0]


class WaitForUrlTests(unittest.TestCase):
    def test_wait_for_url_returns_once_glob_matches(self):
        driver = _UrlSeqDriver([
            "https://h/project-list",
            "https://h/project-details/42",
        ])
        adapter = SeleniumPageAdapter(_FakeSession(driver))
        # Should return without raising once the detail URL appears.
        adapter.wait_for_url("**/project-details/**", timeout=2000)

    def test_wait_for_url_times_out_when_never_matches(self):
        from core.crawler_base import SeleniumTimeout

        driver = _UrlSeqDriver(["https://h/project-list"])
        adapter = SeleniumPageAdapter(_FakeSession(driver))
        with self.assertRaises(SeleniumTimeout):
            adapter.wait_for_url("**/project-details/**", timeout=300)


class SeleniumResponseJsonTests(unittest.TestCase):
    def test_json_parses_text_body(self):
        resp = SeleniumResponse(text='{"status": 200, "result": {"filePath": "/x"}}')
        self.assertEqual(resp.json()["result"]["filePath"], "/x")

    def test_json_parses_content_when_text_empty(self):
        # fetch-backed responses carry the body in .content (bytes), .text="".
        resp = SeleniumResponse(content=b'{"status": 200}')
        self.assertEqual(resp.json()["status"], 200)


class _TimeoutDriver:
    def __init__(self, source: str):
        self.page_source = source
        self.page_load_timeouts: list[float] = []
        self.scripts: list[str] = []

    def set_page_load_timeout(self, timeout):
        self.page_load_timeouts.append(timeout)

    def get(self, url):
        from selenium.common.exceptions import TimeoutException

        raise TimeoutException("renderer timed out")

    def execute_script(self, script):
        self.scripts.append(script)

    def quit(self):
        pass


class SeleniumSessionTimeoutRecoveryTests(unittest.TestCase):
    def test_get_can_return_current_source_after_timeout_when_enabled(self):
        driver = _TimeoutDriver("<html><table class='views-table'></table></html>")
        session = SeleniumSession(page_load_timeout=12.0)
        session._driver = driver

        resp = session.get(
            "https://example.test/list",
            retries=1,
            return_source_on_timeout=True,
        )

        self.assertIsNotNone(resp)
        self.assertIn("views-table", resp.text)
        self.assertEqual(driver.page_load_timeouts, [12.0])
        self.assertEqual(driver.scripts, ["window.stop();"])

    def test_get_preserves_default_retry_failure_on_timeout(self):
        driver = _TimeoutDriver("<html>partial</html>")
        session = SeleniumSession(page_load_timeout=12.0)
        session._driver = driver

        resp = session.get("https://example.test/list", retries=1)

        self.assertIsNone(resp)
        self.assertIsNone(session._driver)


if __name__ == "__main__":
    unittest.main()
