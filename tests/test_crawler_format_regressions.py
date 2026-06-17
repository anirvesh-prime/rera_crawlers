from __future__ import annotations

import json
import unittest
from unittest import mock

from bs4 import BeautifulSoup

from core.project_normalizer import document_result_entry
from sites import gujarat_rera, himachal_pradesh_rera
from sites.andhra_pradesh_rera import _scrape_detail_page as scrape_andhra_detail
from sites.tamil_nadu_rera import _parse_listing_row as parse_tn_listing_row
from sites.karnataka_rera import _parse_detail as parse_karnataka_detail, _sentinel_check


class CrawlerFormatRegressionTests(unittest.TestCase):
    def test_document_result_entry_accepts_legacy_kwargs(self):
        entry = document_result_entry(
            {"label": "Form C", "url": "https://example.com/form-c.pdf"},
            s3_url="https://s3.example.com/form-c.pdf",
            s3_key="ignored-by-helper",
            md5="ignored-by-helper",
        )

        self.assertEqual(
            entry,
            {
                "type": "Form C",
                "link": "https://example.com/form-c.pdf",
                "s3_link": "https://s3.example.com/form-c.pdf",
                "updated": True,
            },
        )

    def test_karnataka_detail_uses_mapped_internal_fields(self):
        html = """
        <table>
          <tr><td>Project Name</td><td>Divya Layout</td></tr>
          <tr><td>Promoter Name</td><td>Lalitha D</td></tr>
          <tr><td>District</td><td>Ballari</td></tr>
          <tr><td>Taluk</td><td>Ballari</td></tr>
          <tr><td>Village</td><td>Ballari Village</td></tr>
          <tr><td>Pin Code</td><td>583103</td></tr>
          <tr><td>Latitude</td><td>15.137786</td></tr>
          <tr><td>Longitude</td><td>76.943996</td></tr>
          <tr><td>Survey / Resurvey Number</td><td>820/3</td></tr>
          <tr><td>Website</td><td>https://example.com</td></tr>
          <tr><td>GST No</td><td>29ABCDE1234F1Z5</td></tr>
          <tr><td>PAN No</td><td>ABCDE1234F</td></tr>
          <tr><td>Trade Licence / Registration No</td><td>REG-123</td></tr>
          <tr><td>Objective</td><td>Residential Layout</td></tr>
          <tr><td>Bank Name</td><td>Kotak Mahindra Bank</td></tr>
          <tr><td>Account No</td><td>9646521238</td></tr>
          <tr><td>Account Name</td><td>Lalitha D RERA Account</td></tr>
          <tr><td>IFSC</td><td>KKBK0008228</td></tr>
          <tr><td>Branch</td><td>Ballari Branch</td></tr>
          <tr><td>Cost of Land</td><td>6127785</td></tr>
          <tr><td>Estimated Construction Cost</td><td>8000000</td></tr>
          <tr><td>Total Project Cost</td><td>14127785</td></tr>
          <tr><td>Date of Commencement</td><td>15-05-2021</td></tr>
          <tr><td>Proposed Date of Completion</td><td>23-03-2024</td></tr>
          <tr><td>Completion Date</td><td>23-03-2024</td></tr>
          <tr><td>Date of Approval</td><td>05-07-2023</td></tr>
          <tr><td>Land Area</td><td>8094 Sq Mtr</td></tr>
          <tr><td>Total Completion Percentage</td><td>75</td></tr>
        </table>
        """

        parsed = parse_karnataka_detail(
            html,
            "ACK/KA/RERA/1248/469/PR/110223/006823",
            "Bagalkot",
            0,
        )

        self.assertEqual(parsed["project_name"], "Divya Layout")
        self.assertEqual(parsed["promoter_name"], "Lalitha D")
        self.assertEqual(parsed["project_city"], "BALLARI")
        self.assertEqual(parsed["project_location_raw"]["district"], "Ballari")
        self.assertEqual(parsed["project_location_raw"]["pin_code"], "583103")
        self.assertEqual(parsed["project_location_raw"]["survey_resurvey_number"], "820/3")
        self.assertEqual(parsed["project_location_raw"]["processed_latitude"], 15.137786)
        self.assertEqual(parsed["project_location_raw"]["processed_longitude"], 76.943996)
        self.assertEqual(
            parsed["promoter_contact_details"],
            {"website": "https://example.com"},
        )
        self.assertEqual(parsed["promoters_details"]["gst_no"], "29ABCDE1234F1Z5")
        self.assertEqual(parsed["bank_details"]["IFSC"], "KKBK0008228")
        self.assertEqual(parsed["project_cost_detail"]["total_project_cost"], "14127785")
        self.assertEqual(
            parsed["construction_progress"],
            [{"title": "total_completion_percentage", "progress_percentage": "75 %"}],
        )

    def test_andhra_detail_ignores_generic_site_notice_links(self):
        html = """
        <html>
          <body>
            <a href="/RERA/DOCUMENTS/Notice/APRERAT_CAUSE_LIST.pdf">Cause List</a>
            <a href="/RERA/Views/downloads.aspx">Forms Download</a>
          </body>
        </html>
        """
        soup = BeautifulSoup(html, "lxml")

        parsed = scrape_andhra_detail(
            soup,
            "https://rera.ap.gov.in/RERA/Views/Project.aspx?enc=test",
        )

        self.assertNotIn("uploaded_documents", parsed)
        self.assertNotIn("promoter_address_raw", parsed)

    def test_tamil_nadu_current_listing_branch_preserves_detail_links(self):
        # 9-column master listing row layout: S.No, Reg+date, Promoter, Project,
        # Approval text, Expiry date, Links/lat-lng, FormC img, Status/empty.
        html = """
        <tr>
          <td>1</td>
          <td>TNRERA/29/LO/4544/2025 dated 12-09-2025</td>
          <td>M/s.Shri Rishabdev Infrastructures LLP</td>
          <td>Project Name: RESIDENZA LILIUM By Urban Tree Ph1 - Residential Layout</td>
          <td><a href="/public/storage/upload/approval.pdf">Approval</a></td>
          <td>30.09.2026</td>
          <td>
            <a href="/public-view1/building/pfirm/3c289c20-ca91-11f0-bd54-ef6224abb025">Promoter</a>
            <a href="/public-view2/building/pfirm/060e6e00-cab8-11f0-a493-1dc986406559">Project</a>
            <span>Latitude 12.770558 Longitude 80.186468</span>
          </td>
          <td><a href="/formcqr/3c2c3140-ca91-11f0-be7a-49ff3448b535"><img alt="" /></a></td>
          <td></td>
        </tr>
        """
        row = BeautifulSoup(html, "lxml").find_all("td")

        parsed = parse_tn_listing_row(row)

        self.assertEqual(
            parsed["promoter_url"],
            "https://rera.tn.gov.in/public-view1/building/pfirm/3c289c20-ca91-11f0-bd54-ef6224abb025",
        )
        self.assertEqual(
            parsed["detail_url"],
            "https://rera.tn.gov.in/public-view2/building/pfirm/060e6e00-cab8-11f0-a493-1dc986406559",
        )
        self.assertEqual(
            parsed["form_c_url"],
            "https://rera.tn.gov.in/formcqr/3c2c3140-ca91-11f0-be7a-49ff3448b535",
        )
        self.assertEqual(parsed["latitude"], "12.770558")
        self.assertEqual(parsed["longitude"], "80.186468")

    # NOTE: test_gujarat_extract_fields_maps_dev_sections was removed because
    # _extract_fields(basic, detail, qpr, promoter_profile) no longer exists —
    # Gujarat was rewritten from JSON-API scraping to Selenium HTML scraping
    # (_extract_html_fields, _parse_overview_card, etc.).

    def test_gujarat_handle_document_falls_back_to_download_response(self):
        # _curl_bytes was removed; the fallback is now download_response().
        fake_resp = mock.MagicMock()
        fake_resp.content = b"%PDF-1.4 test bytes enough" * 10
        with mock.patch.object(gujarat_rera, "download_response", return_value=fake_resp):
            with mock.patch.object(
                gujarat_rera, "upload_document", return_value="abc/file.pdf"
            ):
                with mock.patch.object(
                    gujarat_rera,
                    "get_s3_url",
                    return_value="https://docs.primetenders.com/abc/file.pdf",
                ):
                    with mock.patch.object(gujarat_rera, "upsert_document"):
                        result = gujarat_rera._handle_document(
                            "abc",
                            {
                                "label": "Rera Registration Certificate 1",
                                "type": "Rera Registration Certificate 1",
                                "url": "https://example.com/doc.pdf",
                            },
                            1,
                            "gujarat_rera",
                            mock.MagicMock(),
                            mock.MagicMock(),
                        )

        self.assertEqual(result["s3_link"], "https://docs.primetenders.com/abc/file.pdf")

    def test_himachal_extract_documents_keeps_every_pdf_with_distinct_labels(self):
        html = """
        <table>
          <tr><th></th><th></th><th>Year 1</th><th>Year 2</th><th>Year 3</th></tr>
          <tr>
            <td>1.</td>
            <td>Income Tax Return (ITR) Acknowledgement *</td>
            <td><a href="/CommonControls/ViewOpenFile?path=itr-1">View</a> Uploaded on 01/01/2024</td>
            <td><a href="/CommonControls/ViewOpenFile?path=itr-2">View</a> Uploaded on 01/01/2024</td>
            <td><a href="/CommonControls/ViewOpenFile?path=itr-3">View</a> Uploaded on 01/01/2024</td>
          </tr>
          <tr>
            <td>2.</td>
            <td>Project Report</td>
            <td></td>
            <td></td>
            <td></td>
          </tr>
        </table>
        <table>
          <tr>
            <td>1.</td>
            <td>Sanctioned Letter by TCP/ULB(s)/Local Authority *</td>
            <td><a href="/CommonControls/ViewOpenFile?path=drawing-1">View</a> Uploaded on 13/12/2021 03:44 PM Drawing 1</td>
          </tr>
        </table>
        """

        docs = himachal_pradesh_rera._extract_documents(BeautifulSoup(html, "lxml"))

        self.assertEqual(
            docs,
            [
                {
                    "type": "Income Tax Return (ITR) Acknowledgement * (Year 1)",
                    "link": "https://hprera.nic.in/CommonControls/ViewOpenFile?path=itr-1",
                    "updated": True,
                },
                {
                    "type": "Income Tax Return (ITR) Acknowledgement * (Year 2)",
                    "link": "https://hprera.nic.in/CommonControls/ViewOpenFile?path=itr-2",
                    "updated": True,
                },
                {
                    "type": "Income Tax Return (ITR) Acknowledgement * (Year 3)",
                    "link": "https://hprera.nic.in/CommonControls/ViewOpenFile?path=itr-3",
                    "updated": True,
                },
                {"type": "Project Report"},
                {
                    "type": "Sanctioned Letter by TCP/ULB(s)/Local Authority *",
                    "link": "https://hprera.nic.in/CommonControls/ViewOpenFile?path=drawing-1",
                    "updated": True,
                },
            ],
        )


class KarnatakaSentinelCheckTests(unittest.TestCase):
    """
    Tests for _sentinel_check in karnataka_rera.py.

    The sentinel now uses the same direct ``/projectViewDetails`` search that
    powers the ``--target-reg-no`` flag:
      1. Look up the project by ``sentinel_registration_no`` from config via
         ``_search_by_reg_no``.
      2. Use the ack_no from the search result to drive ``_fetch_detail``.
      3. Cross-check the returned ``project_registration_no`` against config.
      4. Compare extracted fields against ``state_projects_sample/karnataka.json``.

    Transient portal hiccups (empty search, no HTML, network timeouts) skip
    the run with a warning and return ``True`` rather than failing the whole
    crawl.
    """

    BASELINE = {
        "acknowledgement_no": "ACK/KA/RERA/1248/469/PR/110223/006823",
        "project_registration_no": "PRM/KA/RERA/1248/469/PR/050723/006033",
        "project_name": "DIVYA LAYOUT",
        "promoter_name": "LALITHA D",
        "status_of_the_project": "Ongoing",
        "project_city": "BALLARI",
        "project_location_raw": {"district": "Ballari"},
        "bank_details": {"IFSC": "KKBK0008228"},
        "uploaded_documents": [{"link": "https://rera.karnataka.gov.in/cert", "type": "Certificate"}],
    }

    CONFIG = {
        "id": "karnataka_rera",
        "sentinel_registration_no": "PRM/KA/RERA/1248/469/PR/050723/006033",
    }

    SEARCH_ROW = {
        "acknowledgement_no": "ACK/KA/RERA/1248/469/PR/110223/006823",
        "project_registration_no": "PRM/KA/RERA/1248/469/PR/050723/006033",
        "project_name": "DIVYA LAYOUT",
        "promoter_name": "LALITHA D",
        "project_city": "BALLARI",
        "project_location_raw": {"district": "Ballari"},
    }

    FRESH = {
        "project_registration_no": "PRM/KA/RERA/1248/469/PR/050723/006033",
        "acknowledgement_no": "ACK/KA/RERA/1248/469/PR/110223/006823",
        "project_name": "DIVYA LAYOUT",
        "promoter_name": "LALITHA D",
        "status_of_the_project": "Ongoing",
        "project_city": "BALLARI",
        "project_location_raw": {"district": "Ballari"},
        "bank_details": {"IFSC": "KKBK0008228"},
        "uploaded_documents": [{"link": "https://rera.karnataka.gov.in/cert", "type": "Certificate"}],
    }

    def _make_logger(self):
        logger = mock.MagicMock()
        logger.info = mock.MagicMock()
        logger.warning = mock.MagicMock()
        logger.error = mock.MagicMock()
        return logger

    def _run(self, *, search_return=..., fetch_return=None, parse_return=None,
             coverage_return=True, config=None, baseline=None):
        """Helper: run _sentinel_check with all I/O mocked out."""
        config = config if config is not None else self.CONFIG
        baseline = baseline if baseline is not None else self.BASELINE
        search_return = self.SEARCH_ROW if search_return is ... else search_return
        fetch_return = fetch_return if fetch_return is not None else ("<html/>", {})
        parse_return = parse_return if parse_return is not None else self.FRESH
        logger = self._make_logger()

        with mock.patch("builtins.open", mock.mock_open(read_data=json.dumps(baseline))):
            with mock.patch("sites.karnataka_rera._search_by_reg_no",
                            return_value=search_return) as mock_search:
                with mock.patch("sites.karnataka_rera._fetch_detail",
                                return_value=fetch_return) as mock_fetch:
                    with mock.patch("sites.karnataka_rera._parse_detail",
                                    return_value=parse_return):
                        with mock.patch("sites.karnataka_rera.insert_crawl_error") as mock_ice:
                            with mock.patch("core.sentinel_utils.check_field_coverage",
                                            return_value=coverage_return):
                                result = _sentinel_check(config, run_id=1, logger=logger)
        return result, logger, mock_search, mock_fetch, mock_ice

    # ── Happy-path ────────────────────────────────────────────────────────────

    def test_happy_path_returns_true(self):
        result, _, _, _, _ = self._run()
        self.assertTrue(result)

    def test_searches_by_reg_no_from_config(self):
        """_search_by_reg_no must be called with the sentinel reg_no from
        config — NOT with the ack_no from the baseline JSON."""
        _, _, mock_search, _, _ = self._run()
        mock_search.assert_called_once()
        called_reg = mock_search.call_args[0][0]
        self.assertEqual(called_reg, self.CONFIG["sentinel_registration_no"])
        self.assertNotEqual(called_reg, self.BASELINE["acknowledgement_no"])

    def test_fetch_detail_uses_ack_from_search_result(self):
        """The ack_no fed to _fetch_detail must come from the search result row,
        not from any pre-known constant."""
        _, _, _, mock_fetch, _ = self._run()
        mock_fetch.assert_called_once()
        called_ack = mock_fetch.call_args[0][0]
        self.assertEqual(called_ack, self.SEARCH_ROW["acknowledgement_no"])

    # ── Early-exit / skip cases ───────────────────────────────────────────────

    def test_skips_when_no_sentinel_registration_no_in_config(self):
        result, logger, mock_search, mock_fetch, _ = self._run(
            config={"id": "karnataka_rera"},
        )
        self.assertTrue(result)
        mock_search.assert_not_called()
        mock_fetch.assert_not_called()
        logger.warning.assert_called()

    def test_skips_when_baseline_file_not_found(self):
        logger = self._make_logger()
        with mock.patch("builtins.open", side_effect=FileNotFoundError):
            with mock.patch("sites.karnataka_rera._search_by_reg_no") as mock_search:
                with mock.patch("sites.karnataka_rera._fetch_detail") as mock_fetch:
                    result = _sentinel_check(self.CONFIG, run_id=1, logger=logger)
        self.assertTrue(result)
        mock_search.assert_not_called()
        mock_fetch.assert_not_called()
        logger.warning.assert_called()

    def test_skips_when_search_returns_no_match(self):
        """Transient portal hiccup → return True with a warning, don't fail."""
        result, logger, _, mock_fetch, _ = self._run(search_return=None)
        self.assertTrue(result)
        mock_fetch.assert_not_called()
        logger.warning.assert_called()

    def test_skips_when_search_row_has_no_ack_no(self):
        bad_row = dict(self.SEARCH_ROW, acknowledgement_no="")
        result, logger, _, mock_fetch, _ = self._run(search_return=bad_row)
        self.assertTrue(result)
        mock_fetch.assert_not_called()
        logger.warning.assert_called()

    def test_skips_when_fetch_returns_no_html(self):
        """Transient portal hiccup → return True with a warning, don't fail."""
        result, logger, _, _, _ = self._run(fetch_return=(None, {}))
        self.assertTrue(result)
        logger.warning.assert_called()

    # ── Failure cases ─────────────────────────────────────────────────────────

    def test_returns_false_when_parse_returns_empty(self):
        result, logger, _, _, _ = self._run(parse_return={})
        self.assertFalse(result)
        logger.error.assert_called()

    def test_returns_false_on_reg_no_mismatch(self):
        fresh_wrong_reg = dict(self.FRESH,
                               project_registration_no="PRM/KA/RERA/WRONG/REG/NO")
        result, logger, _, _, mock_ice = self._run(parse_return=fresh_wrong_reg)
        self.assertFalse(result)
        logger.error.assert_called()
        mock_ice.assert_called_once()
        error_type = mock_ice.call_args[0][2]
        self.assertEqual(error_type, "SENTINEL_FAILED")

    def test_returns_false_when_coverage_below_threshold(self):
        result, _, _, _, mock_ice = self._run(coverage_return=False)
        self.assertFalse(result)
        mock_ice.assert_called_once()
        error_type = mock_ice.call_args[0][2]
        self.assertEqual(error_type, "SENTINEL_FAILED")

    def test_reg_no_comparison_is_case_insensitive(self):
        fresh_lower = dict(self.FRESH,
                           project_registration_no=self.FRESH["project_registration_no"].lower())
        result, _, _, _, _ = self._run(parse_return=fresh_lower)
        self.assertTrue(result)

    def test_returns_true_when_search_raises_timeout(self):
        """Timeout-like exceptions are treated as transient → skip + return True."""
        logger = self._make_logger()
        with mock.patch("builtins.open", mock.mock_open(read_data=json.dumps(self.BASELINE))):
            with mock.patch("sites.karnataka_rera._search_by_reg_no",
                            side_effect=RuntimeError("ReadTimeout while POSTing")):
                with mock.patch("sites.karnataka_rera.insert_crawl_error"):
                    result = _sentinel_check(self.CONFIG, run_id=1, logger=logger)
        self.assertTrue(result)
        logger.warning.assert_called()

    def test_returns_false_when_search_raises_non_transient(self):
        """Non-transient exceptions abort the run (return False)."""
        logger = self._make_logger()
        with mock.patch("builtins.open", mock.mock_open(read_data=json.dumps(self.BASELINE))):
            with mock.patch("sites.karnataka_rera._search_by_reg_no",
                            side_effect=RuntimeError("parser blew up")):
                with mock.patch("sites.karnataka_rera.insert_crawl_error"):
                    result = _sentinel_check(self.CONFIG, run_id=1, logger=logger)
        self.assertFalse(result)
        logger.error.assert_called()


if __name__ == "__main__":
    unittest.main()
