from __future__ import annotations

import unittest

from sites_config import parse_site_selection, select_sites


class SiteSelectionTests(unittest.TestCase):
    def test_parse_site_selection_supports_repeat_and_comma_syntax(self):
        selected = parse_site_selection([
            "kerala_rera,bihar_rera",
            " kerala_rera ",
            "odisha_rera",
        ])
        self.assertEqual(selected, ["kerala_rera", "bihar_rera", "odisha_rera"])

    def test_select_sites_defaults_to_enabled_only(self):
        catalog = [
            {"id": "prod_site", "enabled": True},
            {"id": "test_site", "enabled": False},
        ]
        sites, unknown, disabled = select_sites([], catalog=catalog)
        self.assertEqual([site["id"] for site in sites], ["prod_site"])
        self.assertEqual(unknown, [])
        self.assertEqual(disabled, [])

    def test_select_sites_allows_explicit_disabled_site_selection(self):
        catalog = [
            {"id": "prod_site", "enabled": True},
            {"id": "test_site", "enabled": False},
        ]
        sites, unknown, disabled = select_sites(["test_site,prod_site"], catalog=catalog)
        self.assertEqual([site["id"] for site in sites], ["test_site", "prod_site"])
        self.assertEqual(unknown, [])
        self.assertEqual(disabled, ["test_site"])

    def test_select_sites_reports_unknown_ids(self):
        catalog = [{"id": "prod_site", "enabled": True}]
        sites, unknown, disabled = select_sites(["missing_site"], catalog=catalog)
        self.assertEqual(sites, [])
        self.assertEqual(unknown, ["missing_site"])
        self.assertEqual(disabled, [])


if __name__ == "__main__":
    unittest.main()
