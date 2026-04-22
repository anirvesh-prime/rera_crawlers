from __future__ import annotations

import unittest

from core.db import _missing_required_project_fields
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
        self.assertEqual(normalized["state"], "Kerala")
        self.assertEqual(normalized["domain"], "kerala")
        self.assertEqual(normalized["url"], "https://example.com/projects")

    def test_insert_guard_flags_missing_critical_fields(self):
        payload = {
            "key": "17186159861670388125",
            "project_registration_no": "K-RERA/PRJ/KKD/051/2026",
            "url": "https://example.com/projects",
            "state": "Kerala",
            "domain": "kerala",
        }

        self.assertEqual(_missing_required_project_fields(payload), ["project_name"])


if __name__ == "__main__":
    unittest.main()
