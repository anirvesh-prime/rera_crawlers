from __future__ import annotations

import unittest

from sites.chhattisgarh_rera import LISTING_URL, _timeout_for_url


class ChhattisgarhTimeoutTests(unittest.TestCase):
    def test_listing_page_gets_extended_read_timeout(self):
        timeout = _timeout_for_url(LISTING_URL)

        self.assertEqual(timeout.connect, 20.0)
        self.assertEqual(timeout.read, 180.0)
        self.assertEqual(timeout.write, 30.0)
        self.assertEqual(timeout.pool, 30.0)

    def test_detail_page_uses_standard_detail_timeout(self):
        timeout = _timeout_for_url(
            "https://rera.cgstate.gov.in/Promoter_Reg_Only_View_Application_new.aspx?MyID=MzM2"
        )

        self.assertEqual(timeout.read, 90.0)

    def test_project_documents_get_longer_document_timeout(self):
        timeout = _timeout_for_url(
            "https://rera.cgstate.gov.in/ProjectDocuments/example.pdf"
        )

        self.assertEqual(timeout.read, 120.0)


if __name__ == "__main__":
    unittest.main()
