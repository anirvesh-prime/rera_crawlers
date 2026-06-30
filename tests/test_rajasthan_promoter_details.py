from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from bs4 import BeautifulSoup

from sites.rajasthan_rera import (
    BASE_URL,
    _build_doc_url,
    _parse_detail_docs,
    _parse_detail_html,
    _parse_viewproject_html,
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


class TestCurrentViewProjectNewParser(unittest.TestCase):

    def test_inline_label_layout_extracts_core_fields(self):
        html = """
        <div id="pdfContent">
          <table>
            <tr><td>
              <p><span class="label">Project Name:</span> VENTURA</p>
              <p><span class="label">Rajasthan RERA Reg. No.:</span> RAJ/P/2024/3058</p>
              <p><span class="label">Project Address:</span> Khasra No./ Plot No.PLOT NO O-35 A , Village- JAIPUR , 6-D ENGINEERS COLONY , Jaipur - 302021 (Rajasthan)</p>
              <p><span class="label">Tehsil:</span> Sanganer</p>
              <p><span class="label">District:</span> Jaipur</p>
              <p><span class="label">State:</span> Rajasthan</p>
            </td></tr>
          </table>
          <table>
            <tr>
              <td><span class="label">Project Type:</span> Group Housing</td>
              <td><span class="label">Actual Commencement Date:</span> 01-01-2024</td>
            </tr>
            <tr>
              <td><span class="label">Estimated Finish Date:</span> 31-12-2026</td>
              <td><span class="label">Total Area of Project:</span> 2429.23 Sq Mtrs</td>
            </tr>
            <tr>
              <td><span class="label">Saleable Area:</span> 16681 Sq Mtrs</td>
              <td><span class="label">Project Status:</span> COMPLETED</td>
            </tr>
          </table>
          <table>
            <tr><th colspan="2">Promoter Details</th></tr>
            <tr>
              <td><span class="label">Promoter Name:</span> J S BUILDCOM</td>
              <td><span class="label">Promoter Type:</span> Partnership Firm</td>
            </tr>
            <tr><td><span class="label">Mobile Number:</span> 9460005613</td></tr>
            <tr><td><span class="label">Office Address:</span> SHOP NO A-181, JAIPUR</td></tr>
            <tr><td><span class="label">Partners:</span> JAMANA DEVI, SHWETA DALMIA</td></tr>
            <tr><td><span class="label">Project Estimated Cost (Rs.):</span> 300000000</td></tr>
          </table>
        </div>
        """

        parsed = _parse_detail_html(BeautifulSoup(html, "lxml"))

        self.assertEqual(parsed["project_name"], "VENTURA")
        self.assertEqual(parsed["project_registration_no"], "RAJ/P/2024/3058")
        self.assertEqual(parsed["project_type"], "group-housing")
        self.assertEqual(parsed["actual_commencement_date"], "2024-01-01 00:00:00+00:00")
        self.assertEqual(parsed["estimated_finish_date"], "2026-12-31 00:00:00+00:00")
        self.assertEqual(parsed["land_area"], 2429.23)
        self.assertEqual(parsed["construction_area"], 16681.0)
        self.assertEqual(parsed["status_of_the_project"], "COMPLETED")
        self.assertEqual(parsed["project_city"], "Jaipur")
        self.assertEqual(parsed["project_location_raw"]["pin_code"], "302021")
        self.assertEqual(parsed["promoter_contact_details"]["phone"], "9460005613")
        self.assertEqual(parsed["promoters_details"]["type_of_firm"], "Partnership Firm")
        self.assertEqual(parsed["members_details"], [{"name": "JAMANA DEVI"}, {"name": "SHWETA DALMIA"}])
        self.assertEqual(parsed["project_cost_detail"]["estimated_project_cost"], "300000000")

    def test_current_layout_ignores_footer_document_links(self):
        html = """
        <app-viewprojectnew>
          <div id="pdfContent">
            <p><span class="label">Project Name:</span> VENTURA</p>
          </div>
        </app-viewprojectnew>
        <footer>
          <a href="https://reraapp.rajasthan.gov.in/Content/pdf/Real_Estate_Act_2016.pdf">Act</a>
        </footer>
        """

        self.assertEqual(_parse_detail_docs(BeautifulSoup(html, "lxml")), [])

    def test_allottee_details_populate_building_details(self):
        html = """
        <html><body>
          <h3>Allottee Details</h3>
          <p>Building : VENTURA (Apartment : unit 01 (1st floor to 10th floor) with servent room , Block : 1, Carpet Area : 205.6 sq. meters) Number Of Apartments: 10, Booked: 5</p>
          <table>
            <tr><th>Sr.No.</th><th>Unit/Flat Detail</th><th>Booking Status</th></tr>
            <tr><td>1.</td><td>201</td><td>Unsold</td></tr>
          </table>
          <p>Building : VENTURA (Apartment : unit 02-11th floor with servent room and private terrace, Block : 1, Carpet Area : 203.52 sq. meters) Number Of Apartments: 1, Booked: 0</p>
          <table>
            <tr><th>Sr.No.</th><th>Unit/Flat Detail</th><th>Booking Status</th></tr>
            <tr><td>1.</td><td>1202</td><td>Unsold</td></tr>
          </table>
        </body></html>
        """

        parsed = _parse_viewproject_html(BeautifulSoup(html, "lxml"))

        self.assertEqual(parsed["building_details"], [
            {
                "flat_type": "unit 01 (1st floor to 10th floor) with servent room",
                "block_name": "1",
                "carpet_area": "205.6",
                "no_of_units": "10",
                "booking_detail": "5",
            },
            {
                "flat_type": "unit 02-11th floor with servent room and private terrace",
                "block_name": "1",
                "carpet_area": "203.52",
                "no_of_units": "1",
                "booking_detail": "0",
            },
        ])


# NOTE: TestFetchProjectDetail, TestIterWebsiteDocuments,
# TestIterViewProjectDocuments, TestExtractViewProjectFields, and
# TestNewFieldsExtraction tested the old JSON-API based implementation
# (_fetch_project_detail, _iter_website_documents, _iter_view_project_documents,
# _extract_view_project_fields). The Rajasthan crawler was rewritten to use
# Selenium HTML scraping (_parse_detail_html, _parse_viewproject_html,
# _parse_detail_docs). Those test classes have been removed accordingly.

if __name__ == "__main__":
    unittest.main()
