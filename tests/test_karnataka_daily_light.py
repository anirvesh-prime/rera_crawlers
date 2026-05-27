from __future__ import annotations

from contextlib import ExitStack
import unittest
from unittest import mock

from core.config import settings
from sites import karnataka_rera


_ACK = "ACK/KA/RERA/1272/394/PR/120526/010244"
_REG = "PRM/KA/RERA/1272/394/PR/150526/008658"
_CONFIG = {
    "id": "karnataka_rera",
    "state": "karnataka",
    "domain": "rera.karnataka.gov.in",
    "config_id": 9,
}


def _listing_row() -> dict:
    return {
        "acknowledgement_no": _ACK,
        # Listing page now supplies the real registration number directly.
        "project_registration_no": _REG,
        "project_name": "Listing Project",
        "promoter_name": "Listing Promoter",
        "project_city": "BENGALURU URBAN",
        "project_location_raw": {"district": "Bengaluru Urban"},
        "data": {},
    }


def _detail() -> dict:
    return {
        "project_registration_no": _REG,
        "project_name": "Detail Project",
        "promoter_name": "Detail Promoter",
        "project_city": "BENGALURU URBAN",
        "project_location_raw": {"district": "Bengaluru Urban"},
        "data": {},
    }


class KarnatakaDailyLightTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_item_limit = settings.CRAWL_ITEM_LIMIT

    def tearDown(self) -> None:
        settings.CRAWL_ITEM_LIMIT = self.original_item_limit

    def _run_with_patches(self, extra_patches: list):
        settings.CRAWL_ITEM_LIMIT = 0
        base_patches = [
            mock.patch.object(karnataka_rera, "CrawlerLogger", return_value=mock.MagicMock()),
            mock.patch.object(karnataka_rera, "DISTRICTS", ["Bengaluru Urban"]),
            mock.patch.object(karnataka_rera, "_sentinel_check", return_value=True),
            mock.patch.object(karnataka_rera, "load_checkpoint", return_value={}),
            mock.patch.object(karnataka_rera, "save_checkpoint"),
            mock.patch.object(karnataka_rera, "reset_checkpoint"),
            mock.patch.object(karnataka_rera, "random_delay"),
            mock.patch.object(karnataka_rera, "_post_listing", return_value="page-1"),
            mock.patch.object(karnataka_rera, "_extract_listing_rows", return_value=[_listing_row()]),
            mock.patch.object(karnataka_rera, "_fetch_detail", return_value=("<html/>", {})),
            mock.patch.object(karnataka_rera, "_parse_detail", return_value=_detail()),
            mock.patch.object(karnataka_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(karnataka_rera, "merge_data_sections", return_value={}),
            mock.patch.object(
                karnataka_rera,
                "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: {
                    **payload,
                    "key": karnataka_rera.generate_project_key(payload["project_registration_no"]),
                },
            ),
            mock.patch.object(karnataka_rera, "insert_crawl_error"),
        ]
        with ExitStack() as stack:
            mocks = [stack.enter_context(patcher) for patcher in base_patches + extra_patches]
            counts = karnataka_rera.run(_CONFIG, run_id=123, mode="daily_light")
        return counts, mocks

    def test_daily_light_skips_existing_project_from_listing_reg_no(self):
        # The listing row now carries the real registration number, so the key
        # is generated and the DB check fires BEFORE any detail-page fetch.
        get_project = mock.patch.object(
            karnataka_rera,
            "get_project_by_key",
            return_value={"key": karnataka_rera.generate_project_key(_REG)},
        )
        extract_docs = mock.patch.object(karnataka_rera, "_extract_documents")
        upsert = mock.patch.object(karnataka_rera, "upsert_project")
        process_docs = mock.patch.object(karnataka_rera, "_process_documents")

        counts, mocks = self._run_with_patches([get_project, extract_docs, upsert, process_docs])

        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_skipped"], 1)
        self.assertEqual(counts["projects_updated"], 0)
        self.assertEqual(counts["documents_uploaded"], 0)
        # Detail page must NOT be fetched when listing already has the reg_no
        # and the project is already in the DB.
        fetch_detail_mock = mocks[9]   # _fetch_detail is base_patches[9]
        fetch_detail_mock.assert_not_called()
        mocks[-3].assert_not_called()  # _extract_documents
        mocks[-2].assert_not_called()  # upsert_project
        mocks[-1].assert_not_called()  # _process_documents

    def test_daily_light_does_not_upload_documents_for_updated_project(self):
        get_project = mock.patch.object(karnataka_rera, "get_project_by_key", return_value=None)
        extract_docs = mock.patch.object(
            karnataka_rera,
            "_extract_documents",
            return_value=[{"link": "https://rera.karnataka.gov.in/document.pdf", "type": "Certificate"}],
        )
        upsert = mock.patch.object(karnataka_rera, "upsert_project", return_value="updated")
        process_docs = mock.patch.object(karnataka_rera, "_process_documents")

        counts, mocks = self._run_with_patches([get_project, extract_docs, upsert, process_docs])

        self.assertEqual(counts["projects_found"], 1)
        self.assertEqual(counts["projects_updated"], 1)
        self.assertEqual(counts["documents_uploaded"], 0)
        mocks[-1].assert_not_called()


if __name__ == "__main__":
    unittest.main()
