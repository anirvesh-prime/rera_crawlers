from __future__ import annotations

import unittest

from core.document_policy import (
    decide_download_rera,
    rename_document_category,
    select_document_for_download,
)


class DocumentPolicyTests(unittest.TestCase):
    def test_decide_download_matches_normalized_label(self):
        allowed, matched = decide_download_rera("rajasthan", "Uploaded Approved Site Plan.pdf")
        self.assertTrue(allowed)
        self.assertEqual(matched, "Approved Site Plan")

    def test_select_document_falls_back_to_filename(self):
        selected = select_document_for_download(
            "odisha",
            {
                "label": "miscellaneous document",
                "url": "https://example.com/files/approved_layout_plan.pdf",
            },
            {},
            domain="rera.odisha.gov.in",
        )
        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["label"], "Approved layout 1")
        self.assertEqual(selected["type"], "Approved layout 1")
        self.assertEqual(selected["matched_category"], "Approved layout")

    def test_repeated_architect_docs_are_numbered(self):
        counters: dict[str, int] = {}
        first = rename_document_category("architect", "ARCHITECT", counters)
        second = rename_document_category("architect", "ARCHITECT", counters)
        self.assertEqual(first, "ARCHITECT Certificate 1")
        self.assertEqual(second, "ARCHITECT Certificate 2")

    def test_repeated_generic_docs_are_numbered(self):
        counters: dict[str, int] = {}
        first = rename_document_category("layout plan", "Layout Plan", counters)
        second = rename_document_category("layout plan", "Layout Plan", counters)
        self.assertEqual(first, "Layout Plan 1")
        self.assertEqual(second, "Layout Plan 2")

    def test_unconfigured_state_skips_documents(self):
        selected = select_document_for_download(
            "pondicherry",
            {
                "label": "Registration Certificate",
                "url": "https://example.com/getdocument?DOC_ID=1",
            },
            {},
        )
        self.assertIsNone(selected)


if __name__ == "__main__":
    unittest.main()
