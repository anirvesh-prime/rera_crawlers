from __future__ import annotations

import unittest

from core.db import _missing_required_project_fields, _prepare_project_write_payload
from core.project_normalizer import normalize_project_payload


class RequiredProjectFieldsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = {
            "state": "Kerala",
            "domain": "kerala",
            "config_id": 1,
            "listing_url": "https://example.com/projects",
        }

    def test_normalize_project_payload_requires_project_name(self):
        payload = {
            "project_registration_no": "K-RERA/PRJ/KKD/051/2026",
        }

        with self.assertRaisesRegex(ValueError, "project_name"):
            normalize_project_payload(payload, self.config)

    def test_normalize_project_payload_accepts_complete_identity_fields(self):
        payload = {
            "project_name": "Vista Delrio",
            "project_registration_no": "K-RERA/PRJ/KKD/051/2026",
        }

        normalized = normalize_project_payload(payload, self.config)

        self.assertEqual(normalized["project_name"], "Vista Delrio")
        self.assertEqual(normalized["project_registration_no"], "K-RERA/PRJ/KKD/051/2026")
        self.assertTrue(normalized["key"])
        self.assertEqual(normalized["state"], "kerala")
        self.assertEqual(normalized["domain"], "kerala")
        self.assertEqual(normalized["url"], "https://example.com/projects")

    def test_normalize_project_payload_drops_project_state_and_canonicalizes_state(self):
        payload = {
            "project_name": "Vista Delrio",
            "project_registration_no": "P000000000",
            "project_state": "Maharashtra",
        }

        normalized = normalize_project_payload(
            payload,
            {
                "state": "Maharashtra",
                "domain": "maharera.maharashtra.gov.in",
                "listing_url": "https://maharera.maharashtra.gov.in/projects-search-result",
            },
        )

        self.assertEqual(normalized["state"], "MAHARASHTRA")
        self.assertNotIn("project_state", normalized)

    def test_normalize_project_payload_accepts_tamil_nadu_alias(self):
        payload = {
            "project_name": "Tamil Nadu Project",
            "project_registration_no": "TN/001",
        }

        normalized = normalize_project_payload(
            payload,
            {
                "state": "tamil_nadu",
                "domain": "rera.tn.gov.in",
                "listing_url": "https://rera.tn.gov.in/registered-building/tn",
            },
        )

        self.assertEqual(normalized["state"], "Tamil Nadu")

    def test_db_write_payload_drops_project_state_and_canonicalizes_state(self):
        prepared = _prepare_project_write_payload({
            "key": "project-key",
            "project_name": "Project",
            "project_registration_no": "P000000000",
            "url": "https://example.com",
            "state": "Maharashtra",
            "project_state": "Maharashtra",
            "domain": "maharera.maharashtra.gov.in",
        })

        self.assertEqual(prepared["state"], "MAHARASHTRA")
        self.assertNotIn("project_state", prepared)

    def test_insert_guard_flags_missing_critical_fields(self):
        payload = {
            "key": "17186159861670388125",
            "project_registration_no": "K-RERA/PRJ/KKD/051/2026",
            "url": "https://example.com/projects",
            "state": "Kerala",
            "domain": "kerala",
        }

        self.assertEqual(_missing_required_project_fields(payload), ["project_name"])

    def test_telangana_normalize_allows_missing_registration_number(self):
        payload = {
            "key": "11117233454338528439",
            "project_name": "SKV S ANANDA VILAS",
            "url": "https://rerait.telangana.gov.in/PrintPreview/PrintPreview?q=x",
            "state": "telangana",
            "domain": "rerait.telangana.gov.in",
        }

        normalized = normalize_project_payload(payload, {"state": "telangana"})

        self.assertEqual(normalized["key"], "11117233454338528439")
        self.assertNotIn("project_registration_no", normalized)

    def test_telangana_insert_guard_allows_missing_registration_number(self):
        payload = {
            "key": "11117233454338528439",
            "project_name": "SKV S ANANDA VILAS",
            "url": "https://rerait.telangana.gov.in/PrintPreview/PrintPreview?q=x",
            "state": "telangana",
            "domain": "rerait.telangana.gov.in",
        }

        self.assertEqual(_missing_required_project_fields(payload), [])


if __name__ == "__main__":
    unittest.main()
