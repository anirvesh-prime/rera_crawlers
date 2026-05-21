"""
Tests for core.sentinel_utils.click_tab_with_retry:
  - succeeds on first attempt
  - succeeds on the second attempt after a transient failure
  - returns False cleanly after exhausting all attempts
  - propagates configured timeouts to page.click() and the settle calls
"""
from __future__ import annotations

import unittest
from unittest import mock

from core.sentinel_utils import click_tab_with_retry


class _FakePage:
    """Minimal stand-in for a Playwright Page tracking calls + scripted clicks."""

    def __init__(self, click_outcomes):
        # click_outcomes is a list of either None (success) or Exception (raise)
        self._click_outcomes = list(click_outcomes)
        self.click_calls: list[dict] = []
        self.wait_for_timeout_calls: list[int] = []
        self.wait_for_load_state_calls: list[dict] = []

    def click(self, selector, *, timeout):
        self.click_calls.append({"selector": selector, "timeout": timeout})
        outcome = self._click_outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome

    def wait_for_timeout(self, ms):
        self.wait_for_timeout_calls.append(ms)

    def wait_for_load_state(self, state, *, timeout):
        self.wait_for_load_state_calls.append({"state": state, "timeout": timeout})


class ClickTabWithRetryTests(unittest.TestCase):

    def test_succeeds_on_first_attempt(self):
        page = _FakePage(click_outcomes=[None])
        logger = mock.MagicMock()

        ok = click_tab_with_retry(
            page, "text=Promoter Details",
            label="Promoter Details", logger=logger,
        )

        self.assertTrue(ok)
        self.assertEqual(len(page.click_calls), 1)
        self.assertEqual(page.click_calls[0]["selector"], "text=Promoter Details")
        # Default timeouts wired through.
        self.assertEqual(page.click_calls[0]["timeout"], 15_000)
        self.assertEqual(page.wait_for_timeout_calls, [6_000])
        self.assertEqual(page.wait_for_load_state_calls,
                         [{"state": "networkidle", "timeout": 15_000}])
        # No warnings on the happy path.
        logger.warning.assert_not_called()

    def test_succeeds_on_second_attempt_after_transient_failure(self):
        page = _FakePage(click_outcomes=[RuntimeError("Timeout 15000ms exceeded"), None])
        logger = mock.MagicMock()

        ok = click_tab_with_retry(
            page, "text=Documents",
            label="Documents", logger=logger, attempts=2,
        )

        self.assertTrue(ok)
        self.assertEqual(len(page.click_calls), 2)
        # First attempt: one warning logged.
        warning_msgs = [c.args[0] for c in logger.warning.call_args_list]
        self.assertEqual(len(warning_msgs), 1)
        self.assertIn("attempt 1/2", warning_msgs[0])
        self.assertIn("Documents", warning_msgs[0])
        # Retry settle happened between attempts (1_000) and post-success settle (6_000).
        self.assertIn(1_000, page.wait_for_timeout_calls)
        self.assertIn(6_000, page.wait_for_timeout_calls)

    def test_gives_up_after_exhausting_attempts(self):
        boom = RuntimeError("Timeout 15000ms exceeded")
        page = _FakePage(click_outcomes=[boom, boom])
        logger = mock.MagicMock()

        ok = click_tab_with_retry(
            page, "text=Documents",
            label="Documents", logger=logger, attempts=2,
        )

        self.assertFalse(ok)
        self.assertEqual(len(page.click_calls), 2)
        # Two per-attempt warnings + one final "giving up" warning.
        warning_msgs = [c.args[0] for c in logger.warning.call_args_list]
        self.assertEqual(len(warning_msgs), 3)
        self.assertIn("attempt 1/2", warning_msgs[0])
        self.assertIn("attempt 2/2", warning_msgs[1])
        self.assertIn("giving up", warning_msgs[2])
        # No networkidle wait was ever attempted because click never succeeded.
        self.assertEqual(page.wait_for_load_state_calls, [])

    def test_custom_timeouts_are_propagated(self):
        page = _FakePage(click_outcomes=[None])

        ok = click_tab_with_retry(
            page, "text=Foo",
            click_timeout_ms=9_000,
            settle_ms=2_500,
            networkidle_timeout_ms=11_000,
        )

        self.assertTrue(ok)
        self.assertEqual(page.click_calls[0]["timeout"], 9_000)
        self.assertEqual(page.wait_for_timeout_calls, [2_500])
        self.assertEqual(page.wait_for_load_state_calls,
                         [{"state": "networkidle", "timeout": 11_000}])

    def test_networkidle_failure_does_not_fail_overall(self):
        """networkidle is best-effort; its timeout must not flip the result."""
        page = _FakePage(click_outcomes=[None])
        # Patch wait_for_load_state to raise — simulates Angular long-poll.
        page.wait_for_load_state = mock.MagicMock(side_effect=RuntimeError("networkidle"))

        ok = click_tab_with_retry(page, "text=Foo")

        self.assertTrue(ok)
        page.wait_for_load_state.assert_called_once()


if __name__ == "__main__":
    unittest.main()
