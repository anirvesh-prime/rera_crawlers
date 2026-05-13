"""
Tests for Pondicherry RERA:
  1. _parse_listing_cards  — HTML parsing unit tests
  2. _sentinel_check       — sentinel behaviour (all branches)
  3. run()                 — empty-listing guard (the occasional-failure fix)
"""
from __future__ import annotations

import json
import os
import unittest
from contextlib import ExitStack
from unittest import mock

from bs4 import BeautifulSoup

from sites import pondicherry_rera
from sites.pondicherry_rera import _parse_listing_cards, _sentinel_check


# ── HTML fixture helpers ──────────────────────────────────────────────────────

def _card_html(
    project_name: str = "TEST PROJECT",
    reg_no: str = "PRPY999999",
    address: str = "123 Test Street",
    promoter: str = "Test Promoter",
    promoter_type: str = "Individual",
    prop_type: str = "Residential",
    status: str = "APPROVED",
    project_id: int = 999,
) -> str:
    return f"""
    <div class="col-md-10 no_pad_lft">
      <h1><span>Project: </span>{project_name}</h1>
      <p>{address}</p>
      <p>Reg No.: {reg_no}</p>
      <table><tbody>
        <tr>
          <td>{promoter}</td><td>{promoter_type}</td>
          <td>{prop_type}</td><td>{status}</td><td></td>
        </tr>
      </tbody></table>
      <a href="viewProjectDetailPage?projectID={project_id}">View</a>
    </div>
    """


def _soup(body: str) -> BeautifulSoup:
    return BeautifulSoup(f"<html><body>{body}</body></html>", "lxml")


# ── _parse_listing_cards ──────────────────────────────────────────────────────

class ParseListingCardsTests(unittest.TestCase):

    def test_extracts_all_fields_from_valid_card(self):
        soup = _soup(_card_html())
        cards = _parse_listing_cards(soup)

        self.assertEqual(len(cards), 1)
        c = cards[0]
        self.assertEqual(c["project_name"], "TEST PROJECT")
        self.assertEqual(c["project_registration_no"], "PRPY999999")
        self.assertEqual(c["promoter_name"], "Test Promoter")
        self.assertEqual(c["promoter_type"], "Individual")
        self.assertEqual(c["project_type"], "Residential")
        self.assertEqual(c["listing_status"], "APPROVED")
        self.assertIn("viewProjectDetailPage?projectID=999", c["detail_url"])

    def test_skips_card_without_registration_number(self):
        html = """
        <div class="col-md-10 no_pad_lft">
          <h1>Some Project</h1>
          <p>Some address here</p>
        </div>
        """
        cards = _parse_listing_cards(_soup(html))
        self.assertEqual(cards, [])

    def test_returns_empty_list_for_blank_page(self):
        cards = _parse_listing_cards(_soup(""))
        self.assertEqual(cards, [])

    def test_multiple_cards_all_extracted(self):
        html = _card_html("Project A", "PRPY111") + _card_html("Project B", "PRPY222")
        cards = _parse_listing_cards(_soup(html))
        self.assertEqual(len(cards), 2)
        regs = {c["project_registration_no"] for c in cards}
        self.assertEqual(regs, {"PRPY111", "PRPY222"})


# ── _sentinel_check ───────────────────────────────────────────────────────────

_BASELINE = {
    "url": "https://prera.py.gov.in/reraAppOffice/viewProjectDetailPage?projectID=567",
    "project_name": "PROPOSED RESIDENTIAL LAYOUT",
    "promoter_name": "TEST PROMOTER",
    "project_type": "Plotted Development",
    "status_of_the_project": "APPROVED",
    "project_location_raw": {"raw_address": "Test Address"},
    "project_cost_detail": {"total_project_cost": "10000000"},
    "uploaded_documents": [{"type": "Declaration (Form B)", "link": "https://x"}],
    "promoters_details": {"name": "TEST PROMOTER"},
    "project_description": "Test description",
    "estimated_commencement_date": "2026-03-19",
    "estimated_finish_date": "2031-03-18",
}


class SentinelCheckTests(unittest.TestCase):

    def _run_sentinel(self, extra_patches: list | None = None):
        """Helper: run _sentinel_check with the sample file patched to _BASELINE."""
        patches = [
            mock.patch("builtins.open", mock.mock_open(read_data=json.dumps(_BASELINE))),
            mock.patch("os.path.join", return_value="/fake/puducherry.json"),
        ]
        if extra_patches:
            patches.extend(extra_patches)
        config = {"id": "pondicherry_rera", "sentinel_registration_no": "PRPY133450"}
        logger = mock.MagicMock()
        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            return _sentinel_check(config, run_id=1, logger=logger)

    def test_passes_when_fresh_data_covers_baseline(self):
        fresh = dict(_BASELINE)  # fresh == baseline → 100 % coverage
        with mock.patch.object(pondicherry_rera, "_parse_detail_page", return_value=fresh):
            result = self._run_sentinel()
        self.assertTrue(result)

    def test_aborts_on_network_error(self):
        with mock.patch.object(
            pondicherry_rera, "_parse_detail_page",
            side_effect=ConnectionError("timeout"),
        ):
            with mock.patch.object(pondicherry_rera, "insert_crawl_error") as mock_err:
                result = self._run_sentinel()
        # A network error means the site is unreachable — sentinel must abort the crawl
        self.assertFalse(result)
        mock_err.assert_called_once()

    def test_aborts_when_page_returns_no_data(self):
        with mock.patch.object(pondicherry_rera, "_parse_detail_page", return_value={}):
            with mock.patch.object(pondicherry_rera, "insert_crawl_error") as mock_err:
                result = self._run_sentinel()
        # Empty response means site is down — sentinel must abort the crawl
        self.assertFalse(result)
        mock_err.assert_called_once()

    def test_skips_when_no_sentinel_reg_configured(self):
        config = {"id": "pondicherry_rera"}  # no sentinel_registration_no
        logger = mock.MagicMock()
        result = _sentinel_check(config, run_id=1, logger=logger)
        self.assertTrue(result)

    def test_fails_below_coverage_threshold(self):
        # Return a bare dict so nearly all baseline fields are missing
        with mock.patch.object(pondicherry_rera, "_parse_detail_page", return_value={"project_name": "X"}):
            with mock.patch.object(pondicherry_rera, "insert_crawl_error") as mock_err:
                result = self._run_sentinel()
        self.assertFalse(result)
        mock_err.assert_called_once()


# ── _get_listing — timeout / retry config ────────────────────────────────────

class GetListingConfigTests(unittest.TestCase):
    """Verify _get_listing calls safe_get with the right timeout and retry knobs."""

    def test_uses_extended_read_timeout_and_more_retries(self):
        import httpx

        captured: dict = {}

        def fake_safe_get(url, *, verify, logger, timeout, retries, delay, **_kw):
            captured["url"]     = url
            captured["timeout"] = timeout
            captured["retries"] = retries
            captured["delay"]   = delay
            return None  # simulate failure; we only care about the call params

        with mock.patch.object(pondicherry_rera, "safe_get", side_effect=fake_safe_get):
            pondicherry_rera._get_listing(logger=mock.MagicMock())

        self.assertEqual(captured["url"], pondicherry_rera.LISTING_URL)
        self.assertIsInstance(captured["timeout"], httpx.Timeout,
                              "listing must use an httpx.Timeout, not a plain float")
        self.assertGreaterEqual(captured["timeout"].read, 120.0,
                                "read timeout must be ≥ 120 s for the slow Pondicherry server")
        self.assertGreaterEqual(captured["retries"], 4,
                                "listing should retry at least 4 times")
        self.assertGreaterEqual(captured["delay"], 5.0,
                                "back-off delay should be ≥ 5 s between retries")


# ── run() — empty-listing guard ───────────────────────────────────────────────

class RunEmptyListingGuardTests(unittest.TestCase):

    def _make_fake_response(self, body: str = "<html></html>"):
        resp = mock.MagicMock()
        resp.text = body
        return resp

    def test_aborts_and_records_error_when_listing_returns_zero_cards(self):
        insert_err = mock.MagicMock()

        patches = [
            mock.patch.object(pondicherry_rera, "_sentinel_check", return_value=True),
            mock.patch.object(pondicherry_rera, "load_checkpoint", return_value={}),
            mock.patch.object(pondicherry_rera, "_get_listing", return_value=self._make_fake_response()),
            mock.patch.object(pondicherry_rera, "_parse_listing_cards", return_value=[]),
            mock.patch.object(pondicherry_rera, "insert_crawl_error", insert_err),
            mock.patch.object(pondicherry_rera, "get_machine_context", return_value=("host", "1.2.3.4")),
        ]

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            counts = pondicherry_rera.run(
                {"id": "pondicherry_rera", "state": "puducherry", "config_id": 1},
                run_id=99,
                mode="weekly_deep",
            )

        self.assertEqual(counts["error_count"], 1)
        self.assertEqual(counts["projects_found"], 0)
        insert_err.assert_called_once()
        call_args = insert_err.call_args
        self.assertIn("listing_empty", call_args.args)

    def test_aborts_when_listing_fetch_itself_fails(self):
        """_get_listing returning None (all retries exhausted) must abort the crawl."""
        insert_err = mock.MagicMock()

        patches = [
            mock.patch.object(pondicherry_rera, "_sentinel_check", return_value=True),
            mock.patch.object(pondicherry_rera, "load_checkpoint", return_value={}),
            mock.patch.object(pondicherry_rera, "_get_listing", return_value=None),
            mock.patch.object(pondicherry_rera, "insert_crawl_error", insert_err),
            mock.patch.object(pondicherry_rera, "get_machine_context", return_value=("host", "1.2.3.4")),
        ]

        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            counts = pondicherry_rera.run(
                {"id": "pondicherry_rera", "state": "puducherry", "config_id": 1},
                run_id=99,
                mode="weekly_deep",
            )

        self.assertEqual(counts["error_count"], 1)
        insert_err.assert_called_once()
        call_args = insert_err.call_args
        self.assertIn("listing_load_failed", call_args.args)


if __name__ == "__main__":
    unittest.main()
