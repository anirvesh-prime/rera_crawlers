"""
Targeted-crawl tests for Maharashtra RERA (server-paginated pattern).

Verifies that when ``settings.TARGET_REG_NO`` is set:
  1. the sentinel health check is skipped, and
  2. only the requested registration number(s) are processed — every other
     card on each listing page is filtered out, and the page walk stops once
     all targets are found.

Fully offline: all network / DB / Selenium calls are mocked.
"""
from __future__ import annotations

import unittest
from contextlib import ExitStack
from unittest import mock

from bs4 import BeautifulSoup

from core.config import settings
from sites import maharashtra_rera


class _FakeRecord:
    def __init__(self, **kwargs):
        self._d = kwargs

    def to_db_dict(self) -> dict:
        return self._d


class MaharashtraTargetedCrawlTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_target = settings.TARGET_REG_NO
        self._orig_limit = settings.CRAWL_ITEM_LIMIT
        self._orig_scrape = settings.SCRAPE_DETAILS
        self._orig_max_pages = settings.MAX_PAGES
        settings.CRAWL_ITEM_LIMIT = 0
        settings.SCRAPE_DETAILS = False
        settings.MAX_PAGES = None

    def tearDown(self) -> None:
        settings.TARGET_REG_NO = self._orig_target
        settings.CRAWL_ITEM_LIMIT = self._orig_limit
        settings.SCRAPE_DETAILS = self._orig_scrape
        settings.MAX_PAGES = self._orig_max_pages

    def _cards(self) -> list[dict]:
        return [
            {"project_registration_no": "P51700000001", "project_name": "Alpha"},
            {"project_registration_no": "P51700000002", "project_name": "Beta"},
            {"project_registration_no": "P51700000003", "project_name": "Gamma"},
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
            mock.patch.object(maharashtra_rera, "_sentinel_check", sentinel),
            mock.patch.object(maharashtra_rera, "safe_get", return_value=fake_resp),
            mock.patch.object(maharashtra_rera, "_get_total_pages", return_value=1),
            mock.patch.object(maharashtra_rera, "_parse_cards", return_value=self._cards()),
            mock.patch.object(maharashtra_rera, "_url_for_page", side_effect=lambda n: f"url?p={n}"),
            mock.patch.object(maharashtra_rera, "load_checkpoint", return_value=None),
            mock.patch.object(maharashtra_rera, "save_checkpoint"),
            mock.patch.object(maharashtra_rera, "reset_checkpoint"),
            mock.patch.object(maharashtra_rera, "random_delay"),
            mock.patch.object(maharashtra_rera, "update_crawl_run_progress"),
            mock.patch.object(maharashtra_rera, "get_project_by_key", return_value=None),
            mock.patch.object(maharashtra_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(maharashtra_rera, "insert_crawl_error"),
            mock.patch.object(maharashtra_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(maharashtra_rera, "merge_data_sections", return_value={}),
            mock.patch.object(maharashtra_rera, "ProjectRecord", _FakeRecord),
            mock.patch.object(
                maharashtra_rera, "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]
        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = maharashtra_rera.run(
                {"id": "maharashtra_rera", "state": "Maharashtra", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )
        return counts, processed_regs, sentinel

    def test_targeted_run_skips_sentinel_and_filters_cards(self):
        counts, processed_regs, sentinel = self._run_with_target("P51700000002")

        sentinel.assert_not_called()
        self.assertTrue(counts["sentinel_passed"])
        self.assertEqual(processed_regs, ["P51700000002"])
        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_new"], 1)

    def test_targeted_run_is_case_insensitive(self):
        _, processed_regs, _ = self._run_with_target("p51700000002")
        self.assertEqual(processed_regs, ["P51700000002"])

    def test_full_run_uses_sentinel(self):
        settings.TARGET_REG_NO = ""
        sentinel = mock.MagicMock(return_value=False)
        with mock.patch.object(maharashtra_rera, "_sentinel_check", sentinel):
            counts = maharashtra_rera.run(
                {"id": "maharashtra_rera", "state": "Maharashtra", "config_id": 1},
                run_id=1,
                mode="weekly_deep",
            )
        sentinel.assert_called_once()
        self.assertFalse(counts["sentinel_passed"])


class MaharashtraBuildingDetailsTests(unittest.TestCase):
    def _parse_building_details(self, html: str) -> list[dict]:
        out: dict = {}
        maharashtra_rera._parse_mh_building_tab(BeautifulSoup(html, "lxml"), out)
        return out.get("building_details") or []

    def test_old_summary_apartment_type_table_feeds_legacy_units(self):
        details = self._parse_building_details("""
            <table>
              <thead><tr>
                <th>#</th><th>Building Name</th><th>Apartment Type</th>
                <th>Carpet Area (in Sqmts)</th><th>Number of Apartment</th>
              </tr></thead>
              <tbody>
                <tr><td>1</td><td>AANAND SHILP APARTMENT</td><td>3BHK</td><td>71.7</td><td>4</td></tr>
                <tr><td>2</td><td>AANAND SHILP APARTMENT</td><td>2BHK</td><td>63.5</td><td>1</td></tr>
              </tbody>
            </table>
        """)
        self.assertEqual(details, [
            {
                "flat_type": "3BHK Apartment",
                "no_of_units": "4",
                "carpet_area": "71.7",
                "block_name": "AANAND SHILP APARTMENT",
            },
            {
                "flat_type": "2BHK Apartment",
                "no_of_units": "1",
                "carpet_area": "63.5",
                "block_name": "AANAND SHILP APARTMENT",
            },
        ])

    def test_new_summary_residential_and_non_residential_columns_split_units(self):
        details = self._parse_building_details("""
            <table>
              <thead><tr>
                <th>#</th><th>Identification of Building/ Wing as per Sanctioned Plan</th>
                <th>Identification of Wing as per Sanctioned Plan</th><th>Floor Type</th>
                <th>Total No. Of Residential Apartments/ Units</th>
                <th>Total No. Of Non-Residential Apartments/ Units</th>
                <th>Total Apartments / Unit (NR+R)</th>
              </tr></thead>
              <tbody>
                <tr><td>1</td><td>Dipti Opal Bay</td><td></td><td>Habitable Floor</td><td>35</td><td>2</td><td>37</td></tr>
                <tr><td>Total</td><td>35</td><td>2</td><td>37</td></tr>
              </tbody>
            </table>
        """)
        self.assertEqual(details, [
            {"flat_type": "Residential Apartment", "no_of_units": "35", "block_name": "Dipti Opal Bay"},
            {"flat_type": "Commercial Unit", "no_of_units": "2", "block_name": "Dipti Opal Bay"},
        ])

    def test_plotted_inventory_table_becomes_residential_plot_units(self):
        details = self._parse_building_details("""
            <table>
              <thead><tr>
                <th>#</th><th>Plot Details</th><th>Total No of Plots</th>
                <th>Permission Issued No of Plots</th><th>Remarks</th><th>View</th>
              </tr></thead>
              <tbody>
                <tr><td>1</td><td>OPEN PLOTS</td><td>67</td><td>67</td><td></td><td></td></tr>
              </tbody>
            </table>
        """)
        self.assertEqual(details, [
            {"flat_type": "Residential OPEN PLOTS", "no_of_units": "67"},
        ])


if __name__ == "__main__":
    unittest.main()
