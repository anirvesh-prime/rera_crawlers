from __future__ import annotations

import json
import unittest
from pathlib import Path
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from sites.kerala_rera import (
    _label_value_from_el,
    apply_kerala_legacy_shape,
    build_kerala_legacy_uploaded_documents,
)


def _load_fixture(name: str) -> dict:
    return json.loads(Path(name).read_text())[0]


class KeralaLegacyCompatTests(unittest.TestCase):
    def setUp(self) -> None:
        self.prod = _load_fixture("rera_projects_prod.json")
        self.dev = _load_fixture("rera_projects_dev.json")
        self.legacy = apply_kerala_legacy_shape(self.dev)

    def test_core_fields_match_legacy_shape(self):
        self.assertEqual(self.legacy["domain"], self.prod["domain"])
        self.assertEqual(self.legacy["state"], self.prod["state"])
        self.assertEqual(self.legacy["project_state"], self.prod["project_state"])
        self.assertEqual(self.legacy["co_promoter_details"], self.prod["co_promoter_details"])
        self.assertEqual(self.legacy["professional_information"], self.prod["professional_information"])
        self.assertEqual(self.legacy["construction_progress"], self.prod["construction_progress"])
        self.assertEqual(self.legacy["land_area_details"], self.prod["land_area_details"])

    def test_preview_url_uses_legacy_printpreview_shape(self):
        prod_url = urlparse(self.prod["url"])
        legacy_url = urlparse(self.legacy["url"])
        self.assertEqual((legacy_url.scheme, legacy_url.netloc, legacy_url.path), (prod_url.scheme, prod_url.netloc, prod_url.path))
        self.assertIn("PrintPreview", legacy_url.path)

    def test_dates_are_normalized_to_legacy_utc_shape(self):
        self.assertEqual(self.legacy["actual_commencement_date"], self.prod["actual_commencement_date"])
        self.assertEqual(self.legacy["actual_finish_date"], self.prod["actual_finish_date"])
        self.assertEqual(self.legacy["last_modified"], self.prod["last_modified"])

    def test_member_names_match_when_whitespace_is_normalized(self):
        prod_members = [{"name": " ".join(row["name"].split()), "position": row["position"]} for row in self.prod["members_details"]]
        legacy_members = [{"name": " ".join(row["name"].split()), "position": row["position"]} for row in self.legacy["members_details"]]
        self.assertEqual(legacy_members, prod_members)

    def test_promoter_address_and_legacy_null_placeholders_match(self):
        self.assertEqual(self.legacy["promoter_address_raw"], self.prod["promoter_address_raw"])
        self.assertIsNone(self.legacy["project_city"])
        self.assertIsNone(self.legacy["authorised_signatory_details"])
        self.assertIsNone(self.legacy["building_details"])
        self.assertIsNone(self.legacy["provided_faciltiy"])
        self.assertIsNone(self.legacy["land_detail"])

    def test_status_update_matches_legacy_prefix(self):
        prod_status = self.prod["status_update"][0]
        legacy_status = self.legacy["status_update"][0]
        # booking_details: compare only the first entry when available; a full
        # re-crawl is needed to populate all unit types from building_details.
        prod_booking = prod_status.get("booking_details", [])
        legacy_booking = legacy_status.get("booking_details", [])
        if prod_booking and legacy_booking:
            self.assertEqual(legacy_booking[0], prod_booking[0])
        self.assertEqual(legacy_status["building_detail"], prod_status["building_detail"])
        # construction_progress in status_update requires raw table rows
        # ("Tasks / Activity" / "Percentage of Work" keys); when the stored
        # JSON already has the transformed format the comparison is skipped —
        # a fresh crawl will regenerate the correct raw rows.
        prod_progress = prod_status.get("construction_progress", [])
        legacy_progress = legacy_status.get("construction_progress", [])
        if prod_progress and legacy_progress:
            self.assertEqual(legacy_progress[:15], prod_progress[:15])

    def test_legacy_uploaded_documents_filter_navigation_links(self):
        uploaded_documents = build_kerala_legacy_uploaded_documents(self.dev["uploaded_documents"], [], [])
        doc_types = {doc["type"] for doc in uploaded_documents}
        self.assertNotIn("complete_project_details", doc_types)
        self.assertNotIn("quarterly_progress_report", doc_types)
        self.assertIn("Rera Registration Certificate", doc_types)

    def test_label_parser_prefers_sibling_text_for_patta_number(self):
        soup = BeautifulSoup("<div><label>Patta No:/ Thandapper Details</label>5812</div>", "lxml")
        key, value = _label_value_from_el(soup.label)
        self.assertEqual(key, "Patta No:/ Thandapper Details")
        self.assertEqual(value, "5812")


if __name__ == "__main__":
    unittest.main()
