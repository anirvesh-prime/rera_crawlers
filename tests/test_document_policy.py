from __future__ import annotations

import unittest
from unittest import mock

from core.document_policy import (
    decide_download_rera,
    rename_document_category,
    select_document_for_download,
)
from core.project_normalizer import build_document_filename
from core.project_normalizer import existing_uploaded_document_entry


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

    def test_select_document_falls_back_to_doc_filename_field(self):
        selected = select_document_for_download(
            "maharashtra",
            {
                "label": "miscellaneous document",
                "filename": "Architect Certificate.pdf",
            },
            {},
        )
        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["label"], "Architect Certificate 1")
        self.assertEqual(selected["type"], "Architect Certificate 1")

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

    def test_puducherry_declaration_form_b_allowed(self):
        # The crawler normalises the raw link text to "Declaration (Form B)";
        # "Form B" in STATE_DOC_DICT matches because "formb" ⊆ "declarationformb".
        selected = select_document_for_download(
            "puducherry",
            {
                "label": "Declaration (Form B)",
                "url": "https://prera.py.gov.in/reraAppOffice/getdocument?DOC_ID=12329",
            },
            {},
        )
        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["type"], "Form B 1")

    def test_puducherry_registration_certificate_allowed(self):
        # "Registration Certificate" in STATE_DOC_DICT matches the crawler's
        # canonical label "Project Registration Certificate" via substring match.
        selected = select_document_for_download(
            "puducherry",
            {
                "label": "Project Registration Certificate",
                "url": "https://prera.py.gov.in/reraAppOffice/getdocument?DOC_ID=12383",
            },
            {},
        )
        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected["type"], "Registration Certificate 1")

    def test_puducherry_unknown_doc_skipped(self):
        """Documents not in the Puducherry allowlist should be blocked."""
        selected = select_document_for_download(
            "puducherry",
            {
                "label": "some random document",
                "url": "https://prera.py.gov.in/reraAppOffice/getdocument?DOC_ID=99999",
            },
            {},
        )
        self.assertIsNone(selected)

    def test_build_document_filename_uses_old_label_only_naming(self):
        filename = build_document_filename(
            {
                "label": "Structural drawings 1",
                "url": "https://example.com/getdocument?DOC_ID=12345",
            }
        )
        self.assertEqual(filename, "structural_drawings_1.pdf")

    def test_build_document_filename_keeps_known_extension(self):
        filename = build_document_filename(
            {
                "label": "Project Specification 2",
                "url": "https://example.com/files/specification.docx",
            }
        )
        self.assertEqual(filename, "project_specification_2.docx")

    def test_existing_uploaded_document_requires_matching_identity_url(self):
        with mock.patch("core.db.get_document_by_type_and_url", return_value=None) as get_by_url, \
             mock.patch("core.db.get_document_by_type") as get_by_type:
            reused, s3_key = existing_uploaded_document_entry(
                "project-1",
                {
                    "type": "Rera Registration Certificate 1",
                    "url": "https://example.com/document?documentId=222",
                },
            )

        self.assertIsNone(reused)
        self.assertIsNone(s3_key)
        get_by_url.assert_called_once_with(
            "project-1",
            "Rera Registration Certificate 1",
            "https://example.com/document?documentId=222",
        )
        get_by_type.assert_not_called()

    def test_existing_uploaded_document_reuses_same_type_and_identity_url(self):
        existing = {
            "s3_key": "documents/project-1/rera_registration_certificate_1.pdf",
            "file_name": "rera_registration_certificate_1.pdf",
        }
        with mock.patch("core.db.get_document_by_type_and_url", return_value=existing), \
             mock.patch(
                 "core.s3.get_s3_url",
                 return_value="http://docs.primetenders.com/documents/project-1/rera_registration_certificate_1.pdf",
             ):
            reused, s3_key = existing_uploaded_document_entry(
                "project-1",
                {
                    "type": "Rera Registration Certificate 1",
                    "url": "https://example.com/document?documentId=111",
                },
            )

        self.assertEqual(s3_key, "documents/project-1/rera_registration_certificate_1.pdf")
        self.assertEqual(
            reused,
            {
                "type": "Rera Registration Certificate 1",
                "link": "https://example.com/document?documentId=111",
                "s3_link": (
                    "http://docs.primetenders.com/documents/project-1/"
                    "rera_registration_certificate_1.pdf"
                ),
                "updated": True,
            },
        )


if __name__ == "__main__":
    unittest.main()
