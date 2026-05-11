from __future__ import annotations

from contextlib import ExitStack
import unittest
from unittest import mock

from core.config import settings
from sites import uttar_pradesh_rera


class UttarPradeshItemLimitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_limit = settings.CRAWL_ITEM_LIMIT

    def tearDown(self) -> None:
        settings.CRAWL_ITEM_LIMIT = self.original_limit

    def test_run_respects_crawl_item_limit(self):
        settings.CRAWL_ITEM_LIMIT = 1
        processed_keys: list[str] = []

        stubs = [
            {
                "reg_no": "UPRERAPRJ1001",
                "project_name": "Project One",
                "promoter_name": "Promoter One",
                "district": "Agra",
                "project_type": "Residential",
                "row_index": 0,
            },
            {
                "reg_no": "UPRERAPRJ1002",
                "project_name": "Project Two",
                "promoter_name": "Promoter Two",
                "district": "Agra",
                "project_type": "Residential",
                "row_index": 1,
            },
        ]

        def upsert_side_effect(payload: dict) -> str:
            processed_keys.append(payload["key"])
            return "new"

        patches = [
            mock.patch.object(uttar_pradesh_rera, "_UP_DISTRICTS", ["Agra"]),
            mock.patch.object(uttar_pradesh_rera, "_sentinel_check", return_value=True),
            mock.patch.object(uttar_pradesh_rera, "load_checkpoint", return_value={}),
            mock.patch.object(uttar_pradesh_rera, "save_checkpoint"),
            mock.patch.object(uttar_pradesh_rera, "reset_checkpoint"),
            mock.patch.object(uttar_pradesh_rera, "random_delay"),
            mock.patch.object(uttar_pradesh_rera, "_fetch_district_listing", return_value=stubs),
            mock.patch.object(
                uttar_pradesh_rera,
                "_fetch_detail_html_playwright",
                side_effect=[
                    ("<html>detail-1</html>", "https://example.com/1"),
                    ("<html>detail-2</html>", "https://example.com/2"),
                ],
            ),
            mock.patch.object(
                uttar_pradesh_rera,
                "_parse_detail_page",
                side_effect=lambda html, reg_no, district: {
                    "project_registration_no": reg_no,
                    "project_city": district.upper(),
                },
            ),
            mock.patch.object(uttar_pradesh_rera, "_fetch_full_detail_html", return_value=("", "")),
            mock.patch.object(uttar_pradesh_rera, "_extract_documents", return_value=[]),
            mock.patch.object(uttar_pradesh_rera, "get_project_by_key", return_value=None),
            mock.patch.object(uttar_pradesh_rera, "upsert_project", side_effect=upsert_side_effect),
            mock.patch.object(uttar_pradesh_rera, "insert_crawl_error"),
            mock.patch.object(uttar_pradesh_rera, "get_machine_context", return_value=("host", "127.0.0.1")),
            mock.patch.object(uttar_pradesh_rera, "merge_data_sections", return_value={}),
            mock.patch.object(
                uttar_pradesh_rera,
                "normalize_project_payload",
                side_effect=lambda payload, config, machine_name, machine_ip: payload,
            ),
        ]

        with ExitStack() as stack:
            for patcher in patches:
                stack.enter_context(patcher)
            counts = uttar_pradesh_rera.run(
                {"id": "uttar_pradesh_rera", "config_id": 1},
                run_id=123,
                mode="weekly_deep",
            )

        self.assertEqual(processed_keys, [uttar_pradesh_rera.generate_project_key("UPRERAPRJ1001")])
        self.assertEqual(counts["projects_found"], 2)
        self.assertEqual(counts["projects_new"], 1)


if __name__ == "__main__":
    unittest.main()
