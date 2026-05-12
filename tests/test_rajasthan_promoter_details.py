from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from sites.rajasthan_rera import (
    BASE_URL,
    _build_doc_url,
    _is_real_document,
    _resolve_relative_url,
)


# ── response mock helper ───────────────────────────────────────────────────────

def _resp(content: bytes = b"", content_type: str = "application/pdf") -> MagicMock:
    r = MagicMock()
    r.content = content
    r.headers = {"Content-Type": content_type}
    return r


# ══════════════════════════════════════════════════════════════════════════════
# 1. _is_real_document
# ══════════════════════════════════════════════════════════════════════════════

class TestIsRealDocument(unittest.TestCase):

    def test_none_response_is_false(self):
        self.assertFalse(_is_real_document(None))

    def test_pdf_magic_bytes_is_true(self):
        self.assertTrue(_is_real_document(_resp(b"%PDF-1.4 content")))

    def test_html_content_type_is_false(self):
        self.assertFalse(_is_real_document(_resp(b"<html>Error</html>", "text/html; charset=utf-8")))

    def test_plain_text_content_type_is_false(self):
        self.assertFalse(_is_real_document(_resp(b"Not found", "text/plain")))

    def test_non_html_with_content_is_true(self):
        self.assertTrue(_is_real_document(_resp(b"\x89PNG\r\n\x1a\n", "image/png")))

    def test_non_html_empty_content_is_false(self):
        self.assertFalse(_is_real_document(_resp(b"", "application/octet-stream")))


# ══════════════════════════════════════════════════════════════════════════════
# 2. _build_app_url / _build_cert_url
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildUrlHelpers(unittest.TestCase):
    # _build_app_url and _build_cert_url were merged into _build_doc_url
    # when the Rajasthan crawler was rewritten from JSON-API to HTML scraping.

    def test_none_returns_none(self):
        self.assertIsNone(_build_doc_url(None))

    def test_zero_string_returns_none(self):
        self.assertIsNone(_build_doc_url("0"))

    def test_empty_returns_none(self):
        self.assertIsNone(_build_doc_url(""))

    def test_absolute_https_passthrough(self):
        url = "https://example.com/doc.pdf"
        self.assertEqual(_build_doc_url(url), url)

    def test_absolute_http_passthrough(self):
        url = "http://example.com/doc.pdf"
        self.assertEqual(_build_doc_url(url), url)

    @patch("sites.rajasthan_rera._resolve_relative_url")
    def test_relative_path_calls_resolver(self, mock_resolve):
        mock_resolve.return_value = "https://rera.rajasthan.gov.in/Content/uploads/doc.pdf"
        result = _build_doc_url("../Content/uploads/doc.pdf")
        mock_resolve.assert_called_once()
        self.assertEqual(result, "https://rera.rajasthan.gov.in/Content/uploads/doc.pdf")

    @patch("sites.rajasthan_rera._resolve_relative_url")
    def test_tilde_relative_path_calls_resolver(self, mock_resolve):
        mock_resolve.return_value = "https://rera.rajasthan.gov.in/Content/uploads/Certificate/cert.pdf"
        result = _build_doc_url("~/Content/uploads/Certificate/cert.pdf")
        mock_resolve.assert_called_once()
        self.assertIsNotNone(result)


# NOTE: TestFetchProjectDetail, TestIterWebsiteDocuments,
# TestIterViewProjectDocuments, TestExtractViewProjectFields, and
# TestNewFieldsExtraction tested the old JSON-API based implementation
# (_fetch_project_detail, _iter_website_documents, _iter_view_project_documents,
# _extract_view_project_fields). The Rajasthan crawler was rewritten to use
# Playwright HTML scraping (_parse_detail_html, _parse_viewproject_html,
# _parse_detail_docs). Those test classes have been removed accordingly.

if __name__ == "__main__":
    unittest.main()

