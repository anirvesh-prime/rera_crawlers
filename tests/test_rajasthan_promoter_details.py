from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from sites.rajasthan_rera import (
    BASE_URL,
    _DETAIL_API_TO_FIELD,
    _LIST_API_TO_FIELD,
    _build_app_url,
    _build_cert_url,
    _extract_project_website_documents,
    _extract_view_project_fields,
    _is_real_document,
    _iter_view_project_documents,
    _iter_website_documents,
)


# ── response mock helper ───────────────────────────────────────────────────────

def _resp(content: bytes = b"", content_type: str = "application/pdf") -> MagicMock:
    r = MagicMock()
    r.content = content
    r.headers = {"Content-Type": content_type}
    return r


# ── shared fixtures ────────────────────────────────────────────────────────────

_PROMOTER_DETAILS = {
    "OrgName": "AKSHAT APARTMENTS PRIVATE LIMITED",
    "OfficeNo": "01416604751",
    "WebSiteURL": "www.akshatapartments.com",
    "PartnerModel": [
        {"PartnerName": "Sunil Jain", "Designation": "Managing Director", "ImgPath": "../img/sunil.jpg"},
        {"PartnerName": "Nisha Jain", "Designation": "Director",          "ImgPath": "../img/nisha.jpg"},
    ],
    "Address": {
        "StateName": "Rajasthan", "DistrictName": "Jaipur", "Taluka": "Jaipur",
        "VillageName": None, "PlotNumber": "A-27/13-A", "WardNumber": None,
        "StreetName": "Kanti Chandra Road, Banipark", "ZipCode": "302016",
    },
    "PastExprienceDetails": [
        {"ProjectName": "Akshat Meadows- Phase 1", "Address": "Hathoj Mod"},
        {"ProjectName": "Akshat Traiyalokya",      "Address": "Banipark"},
    ],
}

_FULL_VIEW_DATA = {
    "GetProjectBasic": {
        "PlotNo": "5", "VillageName": "KANOTA", "DistrictName": "Jaipur",
        "PinCode": "303012", "StateName": "Rajasthan",
        "ActualCommencementDate": "2025-01-01", "ActualfinishDate": None,
    },
    "PlotDetails": [{"PlotArea": 100.5, "TotalPlots": 20}, {"PlotArea": 50.0, "TotalPlots": 10}],
    "GetProjectCostDetail": {"TotalCost": 5000000},
    "ProjectProFessionAlDetail": [{"Name": "Arch Ltd", "Role": "Architect"}],
    # provided_faciltiy is now sourced from ProjectCommanArea
    "ProjectCommanArea": {
        "ProjectDevelopementWork": [{"Name": "Water Supply", "Proposed": 1, "Completion": 0}],
        "CommonAreaItemsCharged": None,
        "ProjectCommonAreaDetails": None,
    },
    "ProjectLitigations": [{"CaseNo": "123"}],
    "PromoterDetails": _PROMOTER_DETAILS,
}

_WEBSITE_DATA_WITH_DOCS = {
    "ProjectId": "kD6ZefpBIHk=",
    "DateofRegistration": "17-04-2026",
    "GetDocumentsList": [{
        "DocumentUrl": "../Content/uploads/doc1.pdf",
        "ApplicationDocumentName": "Legal Title Report",
        "MasterType": "ProjectDocumentLegal",
        "CreatedOn": "10/11/2025",
    }],
}


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

    def test_build_app_url_none_returns_none(self):
        self.assertIsNone(_build_app_url(None))

    def test_build_app_url_zero_string_returns_none(self):
        self.assertIsNone(_build_app_url("0"))

    def test_build_app_url_empty_returns_none(self):
        self.assertIsNone(_build_app_url(""))

    def test_build_app_url_absolute_https_passthrough(self):
        url = "https://example.com/doc.pdf"
        self.assertEqual(_build_app_url(url), url)

    def test_build_app_url_absolute_http_passthrough(self):
        url = "http://example.com/doc.pdf"
        self.assertEqual(_build_app_url(url), url)

    @patch("sites.rajasthan_rera._resolve_relative_url")
    def test_build_app_url_relative_calls_resolver(self, mock_resolve):
        mock_resolve.return_value = "https://reraapp.rajasthan.gov.in/Content/uploads/doc.pdf"
        result = _build_app_url("../Content/uploads/doc.pdf")
        mock_resolve.assert_called_once()
        self.assertEqual(result, "https://reraapp.rajasthan.gov.in/Content/uploads/doc.pdf")

    def test_build_cert_url_empty_returns_none(self):
        self.assertIsNone(_build_cert_url(""))

    def test_build_cert_url_zero_returns_none(self):
        self.assertIsNone(_build_cert_url("0"))

    def test_build_cert_url_none_returns_none(self):
        self.assertIsNone(_build_cert_url(None))

    def test_build_cert_url_absolute_passthrough(self):
        url = "https://example.com/cert.pdf"
        self.assertEqual(_build_cert_url(url), url)

    @patch("sites.rajasthan_rera._resolve_relative_url")
    def test_build_cert_url_relative_calls_resolver(self, mock_resolve):
        mock_resolve.return_value = "https://reraapp.rajasthan.gov.in/Content/uploads/Certificate/cert.pdf"
        result = _build_cert_url("~/Content/uploads/Certificate/cert.pdf")
        mock_resolve.assert_called_once()
        self.assertIsNotNone(result)


# ══════════════════════════════════════════════════════════════════════════════
# 3. _fetch_project_detail
# ══════════════════════════════════════════════════════════════════════════════

class TestFetchProjectDetail(unittest.TestCase):

    def _make_api_resp(self, proj: dict) -> MagicMock:
        r = MagicMock()
        r.json.return_value = {"Data": {"Project": [proj]}}
        return r

    @patch("sites.rajasthan_rera.safe_post", return_value=None)
    def test_returns_empty_on_no_response(self, _):
        from sites.rajasthan_rera import _fetch_project_detail
        self.assertEqual(_fetch_project_detail("enc123", MagicMock()), {})

    @patch("sites.rajasthan_rera.safe_post")
    def test_maps_all_detail_fields(self, mock_post):
        from sites.rajasthan_rera import _fetch_project_detail
        mock_post.return_value = self._make_api_resp({
            "ProjectName": "Sky Homes 5",
            "PromoterName": "Akshat Apts",
            "RegistrationNo": "RAJ/P/2025/4508",
            "DateofRegistration": "17-04-2026",
            "ProjectCategory": "Group Housing",
            "RevisedDateOfComplation": "31-12-2027",
        })
        result = _fetch_project_detail("enc123", MagicMock())
        self.assertEqual(result["project_name"],            "Sky Homes 5")
        self.assertEqual(result["promoter_name"],           "Akshat Apts")
        self.assertEqual(result["project_registration_no"], "RAJ/P/2025/4508")
        # DateofRegistration → submitted_date (registration date shown in site header),
        # normalised from dd-mm-yyyy to ISO YYYY-MM-DD HH:MM:SS+00:00
        self.assertEqual(result["submitted_date"],          "2026-04-17 00:00:00+00:00")
        self.assertNotIn("approved_on_date", result)  # approved_on_date comes from listing APPROVEDON
        self.assertEqual(result["project_type"],            "group-housing")
        # RevisedDateOfComplation normalised from dd-mm-yyyy to ISO
        self.assertEqual(result["estimated_finish_date"],   "2027-12-31 00:00:00+00:00")

    @patch("sites.rajasthan_rera.safe_post")
    def test_promoter_contact_extracted(self, mock_post):
        from sites.rajasthan_rera import _fetch_project_detail
        mock_post.return_value = self._make_api_resp({
            "promotermobileno": "9829066855",
            "promoteremail": "akshat@gmail.com",
        })
        result = _fetch_project_detail("enc123", MagicMock())
        self.assertEqual(result["promoter_contact_details"]["phone"], "9829066855")
        self.assertEqual(result["promoter_contact_details"]["email"],  "akshat@gmail.com")

    @patch("sites.rajasthan_rera.safe_post")
    def test_promoter_address_extracted(self, mock_post):
        from sites.rajasthan_rera import _fetch_project_detail
        mock_post.return_value = self._make_api_resp({
            "DetailsofPromoter": "A-27/13-A, Banipark",
            "PromoterType": "Partnership Firm",
        })
        result = _fetch_project_detail("enc123", MagicMock())
        self.assertEqual(result["promoter_address_raw"]["details_of_promoter"], "A-27/13-A, Banipark")
        self.assertEqual(result["promoter_address_raw"]["promoter_type"],        "Partnership Firm")

    @patch("sites.rajasthan_rera.safe_post")
    def test_building_details_extracted(self, mock_post):
        from sites.rajasthan_rera import _fetch_project_detail
        mock_post.return_value = self._make_api_resp({
            "TotalBuildingCount": "24",
            "SanctionedbuildingCount": "22",
            "NotSanctionedbuildingCount": "2",
            "AggregateAreaOpenSpace": "1660.90",
        })
        result = _fetch_project_detail("enc123", MagicMock())
        self.assertEqual(result["building_details"]["total_buildings"],       "24")
        self.assertEqual(result["building_details"]["sanctioned_buildings"],  "22")
        self.assertEqual(result["building_details"]["open_space_area"],       "1660.90")

    @patch("sites.rajasthan_rera.safe_post")
    def test_project_location_extracted(self, mock_post):
        from sites.rajasthan_rera import _fetch_project_detail
        mock_post.return_value = self._make_api_resp({
            "ProjectLocation": "Khasra No./ Plot No.5, Jaipur",
        })
        result = _fetch_project_detail("enc123", MagicMock())
        self.assertEqual(result["project_location_raw"]["project_location"], "Khasra No./ Plot No.5, Jaipur")

    @patch("sites.rajasthan_rera.safe_post")
    def test_zero_value_fields_excluded(self, mock_post):
        from sites.rajasthan_rera import _fetch_project_detail
        mock_post.return_value = self._make_api_resp({
            "ProjectName": "Test Project",
            "PromoterName": "0",
        })
        result = _fetch_project_detail("enc123", MagicMock())
        self.assertEqual(result["project_name"], "Test Project")
        self.assertNotIn("promoter_name", result)

    @patch("sites.rajasthan_rera.safe_post")
    def test_data_key_has_source_api_and_raw(self, mock_post):
        from sites.rajasthan_rera import _fetch_project_detail
        mock_post.return_value = self._make_api_resp({"ProjectName": "Test"})
        result = _fetch_project_detail("enc123", MagicMock())
        self.assertEqual(result["data"]["source_api"], "GetProjectById")
        self.assertIn("raw", result["data"])

    @patch("sites.rajasthan_rera.safe_post")
    def test_land_area_details_extracted(self, mock_post):
        from sites.rajasthan_rera import _fetch_project_detail
        mock_post.return_value = self._make_api_resp({
            "Rectified_PhaseArea": 2649.52,
            "AggregateAreaOpenSpace": "1660.90",
        })
        result = _fetch_project_detail("enc123", MagicMock())
        self.assertIn("land_area_details", result)
        self.assertEqual(result["land_area_details"]["rectified_phase_area"], 2649.52)




# ══════════════════════════════════════════════════════════════════════════════
# 4. _iter_website_documents
# ══════════════════════════════════════════════════════════════════════════════

class TestIterWebsiteDocuments(unittest.TestCase):

    def _iter(self, node) -> list[dict]:
        docs: list[dict] = []
        seen: set[str] = set()
        with patch("sites.rajasthan_rera._build_app_url",
                   side_effect=lambda p: f"https://host/{p}" if p and p != "0" else None):
            _iter_website_documents(node, docs=docs, seen=seen)
        return docs

    def test_empty_dict_produces_no_docs(self):
        self.assertEqual(self._iter({}), [])

    def test_document_url_added(self):
        docs = self._iter({"DocumentUrl": "doc.pdf", "ApplicationDocumentName": "Title Report"})
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["label"], "Title Report")

    def test_duplicate_url_not_added_twice(self):
        node = {
            "DocumentUrl": "doc.pdf",
            "ApplicationDocumentName": "Doc A",
            "Nested": {"DocumentUrl": "doc.pdf", "ApplicationDocumentName": "Doc B"},
        }
        docs = self._iter(node)
        urls = [d["url"] for d in docs]
        self.assertEqual(len(urls), len(set(urls)))

    def test_label_priority_application_doc_name(self):
        docs = self._iter({
            "DocumentUrl": "a.pdf",
            "ApplicationDocumentName": "Legal Report",
            "DocumentName": "fallback.pdf",
            "MasterType": "SomeType",
        })
        self.assertEqual(docs[0]["label"], "Legal Report")

    def test_label_falls_back_to_document_name(self):
        docs = self._iter({"DocumentUrl": "a.pdf", "DocumentName": "Chain.pdf", "MasterType": "T"})
        self.assertEqual(docs[0]["label"], "Chain.pdf")

    def test_label_falls_back_to_master_type(self):
        docs = self._iter({"DocumentUrl": "a.pdf", "MasterType": "ProjectDocumentLegal"})
        self.assertEqual(docs[0]["label"], "ProjectDocumentLegal")

    def test_label_defaults_to_document(self):
        docs = self._iter({"DocumentUrl": "a.pdf"})
        self.assertEqual(docs[0]["label"], "document")

    def test_list_of_nodes_traversed(self):
        node = [
            {"DocumentUrl": "a.pdf", "ApplicationDocumentName": "Doc A"},
            {"DocumentUrl": "b.pdf", "ApplicationDocumentName": "Doc B"},
        ]
        docs = self._iter(node)
        self.assertEqual(len(docs), 2)

    def test_get_list_key_sets_section_for_children(self):
        node = {"GetDocumentsList": [{"DocumentUrl": "doc.pdf"}]}
        docs = self._iter(node)
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["label"], "GetDocumentsList")

    def test_none_document_url_skipped(self):
        docs = self._iter({"DocumentUrl": None, "ApplicationDocumentName": "Ghost"})
        self.assertEqual(docs, [])

    def test_none_values_stripped_from_entry(self):
        docs = self._iter({"DocumentUrl": "a.pdf", "ApplicationDocumentName": "X", "CreatedOn": None})
        self.assertNotIn("upload_date", docs[0])


# ══════════════════════════════════════════════════════════════════════════════
# 5. _iter_view_project_documents
# ══════════════════════════════════════════════════════════════════════════════

class TestIterViewProjectDocuments(unittest.TestCase):

    def _iter(self, node) -> list[dict]:
        docs: list[dict] = []
        seen: set[str] = set()
        with patch("sites.rajasthan_rera._build_app_url",
                   side_effect=lambda p: f"https://host/{p}" if p and p != "0" else None):
            _iter_view_project_documents(node, docs=docs, seen=seen)
        return docs

    def test_certi_path_produces_certificate_doc(self):
        docs = self._iter({"CertiPath": "Certificate/cert.pdf"})
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["label"],    "RERA Registration Certificate")
        self.assertEqual(docs[0]["category"], "certificate")

    def test_drawings_file_url_produces_common_area_doc(self):
        docs = self._iter({"DrawingsFileURL": "drawings/plan.pdf"})
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["label"], "Common Area Drawing")

    def test_url_with_document_name_produces_building_plan(self):
        docs = self._iter({"Url": "plans/building.pdf", "DocumentName": "Building Plan A"})
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["label"], "Building Plan A")

    def test_url_without_document_name_not_added(self):
        docs = self._iter({"Url": "plans/building.pdf"})
        urls = [d["url"] for d in docs]
        self.assertNotIn("https://host/plans/building.pdf", urls)

    def test_document_url_produces_standard_doc(self):
        docs = self._iter({"DocumentUrl": "uploads/form.pdf", "ApplicationDocumentName": "Form B"})
        self.assertIn("Form B", [d["label"] for d in docs])

    def test_duplicate_url_across_fields_not_added_twice(self):
        node = {"CertiPath": "cert.pdf", "DocumentUrl": "cert.pdf"}
        urls = [d["url"] for d in self._iter(node)]
        self.assertEqual(len(urls), len(set(urls)))

    def test_list_items_traversed(self):
        docs = self._iter([{"CertiPath": "c1.pdf"}, {"CertiPath": "c2.pdf"}])
        self.assertEqual(len(docs), 2)

    def test_nested_dict_traversed(self):
        docs = self._iter({"GetBuildingDetails": [{"Url": "plan.pdf", "DocumentName": "Plan A"}]})
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["label"], "Plan A")



# ══════════════════════════════════════════════════════════════════════════════
# 6. _extract_view_project_fields
# ══════════════════════════════════════════════════════════════════════════════

class TestExtractViewProjectFields(unittest.TestCase):

    def _x(self, d): return _extract_view_project_fields(d)

    # ── basics ────────────────────────────────────────────────────────────────

    def test_empty_returns_empty(self):
        self.assertEqual(self._x({}), {})

    def test_basic_as_dict_extracts_commencement_date(self):
        r = self._x({"GetProjectBasic": {"ActualCommencementDate": "2025-01-01"}})
        self.assertEqual(r["actual_commencement_date"], "2025-01-01")

    def test_basic_as_list_extracts_commencement_date(self):
        r = self._x({"GetProjectBasic": [{"ActualCommencementDate": "2025-01-01"}]})
        self.assertEqual(r["actual_commencement_date"], "2025-01-01")

    def test_actual_finish_date_extracted(self):
        r = self._x({"GetProjectBasic": {"ActualfinishDate": "2027-12-31"}})
        self.assertEqual(r["actual_finish_date"], "2027-12-31")

    def test_null_finish_date_excluded(self):
        r = self._x({"GetProjectBasic": {"ActualfinishDate": None}})
        self.assertNotIn("actual_finish_date", r)

    # ── raw_address ───────────────────────────────────────────────────────────

    def test_raw_address_contains_plot_village_district_pin(self):
        r = self._x({"GetProjectBasic": {
            "PlotNo": "5", "VillageName": "KANOTA",
            "DistrictName": "Jaipur", "PinCode": "303012", "StateName": "Rajasthan",
        }})
        for token in ("Plot No.5", "KANOTA", "Jaipur", "303012"):
            self.assertIn(token, r["raw_address"])

    def test_raw_address_omitted_when_no_location_fields(self):
        r = self._x({"GetProjectBasic": {"ActualCommencementDate": "2025-01-01"}})
        self.assertNotIn("raw_address", r)

    def test_raw_address_defaults_state_to_rajasthan(self):
        r = self._x({"GetProjectBasic": {"PlotNo": "1", "DistrictName": "Jodhpur"}})
        self.assertIn("Rajasthan", r["raw_address"])

    def test_raw_address_without_pin_code(self):
        r = self._x({"GetProjectBasic": {"PlotNo": "1", "DistrictName": "Jodhpur"}})
        self.assertIn("Jodhpur", r["raw_address"])

    # ── PlotDetails ───────────────────────────────────────────────────────────

    def test_plot_details_extracted(self):
        r = self._x({"PlotDetails": [{"PlotArea": 100.5, "TotalPlots": 20}]})
        self.assertEqual(r["plot_details"], [{"carpet_area": "100.5", "no_of_units": "20"}])

    def test_multiple_plot_entries(self):
        r = self._x({"PlotDetails": [{"PlotArea": 100.0, "TotalPlots": 10}, {"PlotArea": 200.0, "TotalPlots": 5}]})
        self.assertEqual(len(r["plot_details"]), 2)

    def test_plot_none_total_plots_becomes_zero(self):
        r = self._x({"PlotDetails": [{"PlotArea": 50.0, "TotalPlots": None}]})
        self.assertEqual(r["plot_details"][0]["no_of_units"], "0")

    def test_plot_item_missing_plot_area_excluded(self):
        r = self._x({"PlotDetails": [{"TotalPlots": 10}]})
        self.assertNotIn("plot_details", r)

    # ── structured fields ─────────────────────────────────────────────────────

    def test_project_cost_detail_extracted(self):
        # Uses flat-dict fallback path — values formatted as Indian currency
        # 6_000_000 = ₹60,00,000.00 | 30_000_000 = ₹3,00,00,000.00 | 24_000_000 = ₹2,40,00,000.00
        r = self._x({"GetProjectCostDetail": {
            "CostofLand": "6000000.00",
            "TotalProjectCost": "30000000.00",
            "EstimatedCostofConstruction": "24000000.00",
        }})
        self.assertEqual(r["project_cost_detail"]["cost_of_land"], "₹60,00,000.00")
        self.assertEqual(r["project_cost_detail"]["estimated_project_cost"], "₹3,00,00,000.00")
        self.assertEqual(r["project_cost_detail"]["estimated_construction_cost"], "₹2,40,00,000.00")

    def test_project_cost_detail_fallback_to_raw_when_no_known_keys(self):
        # When none of the known cost keys are present, fall back to the raw dict
        r = self._x({"GetProjectCostDetail": {"UnknownKey": 999}})
        self.assertEqual(r["project_cost_detail"], {"UnknownKey": 999})

    def test_professional_information_extracted(self):
        # Keys Name/Role (CamelCase) → normalized to name/role
        profs = [{"Name": "Arch Ltd", "Role": "Architect"}]
        result = self._x({"ProjectProFessionAlDetail": profs})["professional_information"]
        self.assertEqual(result[0]["name"], "Arch Ltd")
        self.assertEqual(result[0]["role"], "Architect")

    def test_professional_information_lowercase_keys_passthrough(self):
        # Already-lowercase keys (name/role) must also be recognised
        profs = [{"name": "Eng Co", "role": "Engineer", "email": "e@x.com",
                  "phone": "1234567890", "address": "123 Main St"}]
        result = self._x({"ProjectProFessionAlDetail": profs})["professional_information"]
        self.assertEqual(result[0]["name"], "Eng Co")
        self.assertEqual(result[0]["email"], "e@x.com")
        self.assertEqual(result[0]["phone"], "1234567890")

    def test_provided_facility_extracted(self):
        # provided_faciltiy is now sourced from ProjectCommanArea.ProjectDevelopementWork
        r = self._x({"ProjectCommanArea": {
            "ProjectDevelopementWork": [{"Name": "Water Supply", "Proposed": 1, "Completion": 0}],
        }})
        fac = r.get("provided_faciltiy") or {}
        self.assertIn("amenities", fac)
        self.assertEqual(fac["amenities"][0]["name"], "Water Supply")
        self.assertTrue(fac["amenities"][0]["proposed"])

    def test_provided_facility_common_areas(self):
        r = self._x({"CommonAreaItemsCharged": [
            {"Items": "Swimming Pool", "Checked": True},
            {"Items": "Gym", "Checked": False},
        ]})
        fac = r.get("provided_faciltiy") or {}
        self.assertIn("common_areas", fac)
        names = [ca["name"] for ca in fac["common_areas"]]
        self.assertIn("Swimming Pool", names)
        self.assertIn("Gym", names)

    def test_provided_facility_parking(self):
        r = self._x({"ProjectCommanArea": {
            "ProjectCommonAreaDetails": [
                {"TypeName": "Stilt Area", "NoOfCars": 10, "NoOfTwoWeelers": 20,
                 "NoOfCycles": 0, "MechanicalCarParking": 0,
                 "NoOfVisitorCarParking": 2, "NoOfVisitorScooterParking": 3,
                 "CarParkingAllocated": 0, "ScooterParkingAllocated": 0},
            ],
        }})
        fac = r.get("provided_faciltiy") or {}
        self.assertIn("parking", fac)
        self.assertEqual(fac["parking"][0]["type"], "Stilt Area")
        self.assertEqual(fac["parking"][0]["cars"], 10)

    def test_complaints_litigation_extracted(self):
        r = self._x({"ProjectLitigations": [{"CaseNo": "123"}]})
        self.assertEqual(r["complaints_litigation_details"], [{"CaseNo": "123"}])

    # ── promoter_details ──────────────────────────────────────────────────────

    def test_promoter_details_present(self):
        self.assertIn("promoter_details", self._x({"PromoterDetails": {"OfficeNo": "0141"}}))

    def test_no_promoter_details_when_absent(self):
        self.assertNotIn("promoter_details", self._x({}))

    def test_no_promoter_details_when_empty(self):
        self.assertNotIn("promoter_details", self._x({"PromoterDetails": {}}))

    def test_office_no_extracted(self):
        r = self._x({"PromoterDetails": {"OfficeNo": "01416604751"}})
        self.assertEqual(r["promoter_details"]["office_no"], "01416604751")

    def test_website_extracted(self):
        r = self._x({"PromoterDetails": {"WebSiteURL": "www.akshatapartments.com"}})
        self.assertEqual(r["promoter_details"]["website"], "www.akshatapartments.com")

    def test_partners_extracted_img_path_stripped(self):
        r = self._x({"PromoterDetails": {"PartnerModel": [
            {"PartnerName": "Sunil Jain", "Designation": "MD", "ImgPath": "../img.jpg"},
        ]}})
        self.assertNotIn("ImgPath", r["promoter_details"]["partners"][0])
        self.assertEqual(r["promoter_details"]["partners"][0]["PartnerName"], "Sunil Jain")

    def test_promoter_address_none_values_excluded(self):
        r = self._x({"PromoterDetails": {"Address": {"DistrictName": "Jaipur", "VillageName": None}}})
        # Keys are normalized: DistrictName → district, VillageName → village
        self.assertIn("district", r["promoter_details"]["address"])
        self.assertEqual(r["promoter_details"]["address"]["district"], "Jaipur")
        # VillageName was None so its normalized key "village" must be absent
        self.assertNotIn("village", r["promoter_details"]["address"])

    def test_past_experience_extracted(self):
        r = self._x({"PromoterDetails": {"PastExprienceDetails": [{"ProjectName": "Old Project"}]}})
        self.assertEqual(r["promoter_details"]["past_experience"][0]["ProjectName"], "Old Project")

    def test_missing_office_no_not_in_output(self):
        r = self._x({"PromoterDetails": {"WebSiteURL": "example.com"}})
        self.assertNotIn("office_no", r["promoter_details"])

    def test_missing_partners_not_in_output(self):
        r = self._x({"PromoterDetails": {"OfficeNo": "0141"}})
        self.assertNotIn("partners", r["promoter_details"])

    # ── full fixture smoke test ───────────────────────────────────────────────

    def test_full_view_data_all_expected_keys_present(self):
        r = self._x(_FULL_VIEW_DATA)
        for key in ("actual_commencement_date", "raw_address", "plot_details",
                    "project_cost_detail", "professional_information",
                    "provided_faciltiy", "complaints_litigation_details", "promoter_details"):
            self.assertIn(key, r, msg=f"Missing key: {key}")



# ══════════════════════════════════════════════════════════════════════════════
# 7. New fields added in latest audit
# ══════════════════════════════════════════════════════════════════════════════

class TestNewFieldsExtraction(unittest.TestCase):
    """Cover every field added in the 'make sure all data is collected' audit."""

    def _x(self, d): return _extract_view_project_fields(d)

    # ── phone key renamed from mobile ────────────────────────────────────────

    def test_promoter_contact_uses_phone_key(self):
        # _fetch_project_detail now stores contact under "phone", not "mobile"
        from unittest.mock import patch as _patch, MagicMock as _MM
        from sites.rajasthan_rera import _fetch_project_detail
        resp = _MM()
        resp.json.return_value = {"Data": {"Project": [{"promotermobileno": "9829066855"}]}}
        with _patch("sites.rajasthan_rera.safe_post", return_value=resp):
            result = _fetch_project_detail("enc123", _MM())
        self.assertIn("phone", result["promoter_contact_details"])
        self.assertNotIn("mobile", result["promoter_contact_details"])

    # ── land_area top-level float ─────────────────────────────────────────────

    def test_land_area_promoted_to_top_level(self):
        from unittest.mock import patch as _patch, MagicMock as _MM
        from sites.rajasthan_rera import _fetch_project_detail
        resp = _MM()
        resp.json.return_value = {"Data": {"Project": [{"Rectified_PhaseArea": 2649.52}]}}
        with _patch("sites.rajasthan_rera.safe_post", return_value=resp):
            result = _fetch_project_detail("enc123", _MM())
        self.assertAlmostEqual(result["land_area"], 2649.52)

    # ── project_location_raw structured dict ──────────────────────────────────

    def test_project_location_raw_is_dict_with_keys(self):
        r = self._x({"GetProjectBasic": {
            "PlotNo": "5", "VillageName": "KANOTA", "DistrictName": "Jaipur",
            "PinCode": "303012", "StateName": "Rajasthan",
        }})
        self.assertIsInstance(r["project_location_raw"], dict)
        self.assertIn("district", r["project_location_raw"])
        self.assertIn("state",    r["project_location_raw"])
        self.assertIn("pin_code", r["project_location_raw"])

    def test_project_location_raw_contains_raw_address(self):
        r = self._x({"GetProjectBasic": {"PlotNo": "1", "DistrictName": "Jodhpur"}})
        self.assertIn("raw_address", r["project_location_raw"])

    def test_project_location_raw_absent_when_no_location_data(self):
        r = self._x({"GetProjectBasic": {"ActualCommencementDate": "2025-01-01"}})
        self.assertNotIn("project_location_raw", r)

    # ── unit counts ───────────────────────────────────────────────────────────

    def test_residential_unit_count_extracted(self):
        r = self._x({"GetProjectBasic": {"TotalResidentialUnit": 120}})
        self.assertEqual(r["number_of_residential_units"], 120)

    def test_commercial_unit_count_extracted(self):
        r = self._x({"GetProjectBasic": {"TotalCommercialUnit": 5}})
        self.assertEqual(r["number_of_commercial_units"], 5)

    def test_zero_unit_count_not_extracted(self):
        r = self._x({"GetProjectBasic": {"TotalResidentialUnit": 0}})
        self.assertNotIn("number_of_residential_units", r)

    # ── construction_area ─────────────────────────────────────────────────────

    def test_construction_area_extracted(self):
        r = self._x({"GetProjectBasic": {"BuiltupArea": 5000.5}})
        self.assertAlmostEqual(r["construction_area"], 5000.5)

    # ── project_description ───────────────────────────────────────────────────

    def test_project_description_extracted(self):
        r = self._x({"GetProjectBasic": {"ProjectDescription": "A luxury township"}})
        self.assertEqual(r["project_description"], "A luxury township")

    def test_empty_project_description_not_extracted(self):
        r = self._x({"GetProjectBasic": {"ProjectDescription": ""}})
        self.assertNotIn("project_description", r)

    def test_project_description_from_project_remark(self):
        # Many projects store description in ProjectRemark, not ProjectDescription
        r = self._x({"GetProjectBasic": {"ProjectRemark": "Servant rooms sold to allottees only."}})
        self.assertEqual(r["project_description"], "Servant rooms sold to allottees only.")

    # ── rich building_details list ────────────────────────────────────────────

    def test_building_details_from_get_building_details_key(self):
        # GetBuildingDetails items now contain nested GetAppartmentDetails
        r = self._x({"GetBuildingDetails": [
            {"Name": "Tower 1", "GetAppartmentDetails": [
                {"ApartmentType": "2BHK", "CarpetArea": 85.5, "NumberOfApartments": 24,
                 "BulidingBlockText": "T1"},
            ]},
        ]})
        self.assertIsInstance(r["building_details"], list)
        self.assertEqual(r["building_details"][0]["flat_type"], "2BHK")
        self.assertEqual(r["building_details"][0]["carpet_area"], "85.5")
        self.assertEqual(r["building_details"][0]["no_of_units"], "24")

    def test_building_details_with_block_name(self):
        r = self._x({"GetBuildingDetails": [
            {"Name": "Tower A", "GetAppartmentDetails": [
                {"ApartmentType": "3BHK", "BulidingBlockText": "Tower A"},
            ]},
        ]})
        self.assertEqual(r["building_details"][0]["block_name"], "Tower A")

    def test_empty_building_detail_item_skipped(self):
        r = self._x({"GetBuildingDetails": [{}]})
        self.assertNotIn("building_details", r)

    def test_open_area_zero_emitted_as_decimal_string(self):
        # open_area must always be present, even when 0.0 (the 'or' short-circuit bug was fixed)
        r = self._x({"GetBuildingDetails": [
            {"Name": "B1", "GetAppartmentDetails": [
                {"ApartmentType": "2BHK", "CarpetArea": 75.0, "AreaOfVerandah": 0.0},
            ]},
        ]})
        self.assertIn("open_area", r["building_details"][0])
        self.assertEqual(r["building_details"][0]["open_area"], "0.00")

    def test_open_area_nonzero_emitted_as_decimal_string(self):
        r = self._x({"GetBuildingDetails": [
            {"Name": "B1", "GetAppartmentDetails": [
                {"ApartmentType": "Penthouse", "CarpetArea": 200.0, "AreaOfVerandah": 231.3},
            ]},
        ]})
        self.assertEqual(r["building_details"][0]["open_area"], "231.30")

    def test_open_area_absent_in_api_defaults_to_zero_string(self):
        # When AreaOfVerandah/OpenArea are not present at all, default to "0.00"
        r = self._x({"GetBuildingDetails": [
            {"Name": "B1", "GetAppartmentDetails": [
                {"ApartmentType": "1BHK", "CarpetArea": 50.0},
            ]},
        ]})
        self.assertEqual(r["building_details"][0]["open_area"], "0.00")

    # ── construction_progress ─────────────────────────────────────────────────

    def test_construction_progress_extracted(self):
        # Construction progress comes from GanttChartModel milestones
        r = self._x({"GanttChartModel": [
            {"Milestone": "Foundation", "FromDate": None, "ToDate": None},
            {"Milestone": "Structure", "FromDate": "/Date(1762540200000)/", "ToDate": None},
        ]})
        self.assertIsInstance(r["construction_progress"], list)
        self.assertEqual(r["construction_progress"][0]["title"], "Foundation")
        self.assertEqual(r["construction_progress"][1]["title"], "Structure")
        self.assertIn("from_date", r["construction_progress"][1])

    def test_construction_progress_empty_list_not_extracted(self):
        r = self._x({"GanttChartModel": [], "GanttChartModelcomm": []})
        self.assertNotIn("construction_progress", r)

    # ── bank_details ──────────────────────────────────────────────────────────

    def test_bank_details_extracted(self):
        # Bank details are flat fields inside GetProjectBasic (collection account)
        r = self._x({"GetProjectBasic": {
            "BankName": "SBI", "IFSCCode": "SBIN0001234", "BankAccountNo": "123456789",
            "BranchName": "Jaipur Main", "BankAddress": "MI Road", "AccountHolderName": "Proj AC",
        }})
        self.assertIsInstance(r["bank_details"], list)
        bd = r["bank_details"][0]
        self.assertEqual(bd["bank_name"],    "SBI")
        self.assertEqual(bd["IFSC"],         "SBIN0001234")
        self.assertEqual(bd["account_no"],   "123456789")
        self.assertEqual(bd["branch"],       "Jaipur Main")
        self.assertEqual(bd["account_type"], "Collection Account (100%)")

    def test_bank_details_three_account_types(self):
        # All three account types (collection, retention, promoter) are extracted
        r = self._x({"GetProjectBasic": {
            "BankName": "SBI",   "BankAccountNo": "111",
            "BankNameRetention": "HDFC", "BankAccountNoRetention": "222",
            "BankNamePromoter":  "ICICI", "BankAccountNoPromoter": "333",
        }})
        types = [bd["account_type"] for bd in r["bank_details"]]
        self.assertIn("Collection Account (100%)",  types)
        self.assertIn("RERA Retention Account (70%)", types)
        self.assertIn("Promoter's Account",          types)

    # ── co_promoter_details ───────────────────────────────────────────────────

    def test_co_promoter_details_extracted(self):
        r = self._x({"GetCoPromoterDetails": [{"Name": "Partner Corp", "Share": "50%"}]})
        self.assertEqual(r["co_promoter_details"], [{"Name": "Partner Corp", "Share": "50%"}])

    def test_co_promoter_as_dict_wrapped_in_list(self):
        r = self._x({"GetCoPromoterDetails": {"Name": "Solo Partner"}})
        self.assertIsInstance(r["co_promoter_details"], list)

    # ── members_details ───────────────────────────────────────────────────────

    def test_members_details_extracted_from_partner_model(self):
        r = self._x({"PromoterDetails": {"PartnerModel": [
            {"PartnerName": "Sunil Jain", "Designation": "MD", "ImgPath": "x.jpg"},
        ]}})
        self.assertIn("members_details", r)
        self.assertEqual(r["members_details"][0]["name"],     "Sunil Jain")
        self.assertEqual(r["members_details"][0]["position"], "MD")
        self.assertNotIn("ImgPath", r["members_details"][0])

    # ── promoters_details (org name / type) ──────────────────────────────────

    def test_promoters_details_extracted(self):
        r = self._x({"PromoterDetails": {
            "OrgName": "Akshat Apartments Pvt Ltd",
            "OrgType": "Private Limited Company",
        }})
        self.assertIn("promoters_details", r)
        # name is captured in top-level promoter_name; only type_of_firm in promoters_details
        self.assertNotIn("name", r["promoters_details"])
        self.assertEqual(r["promoters_details"]["type_of_firm"], "Private Limited Company")

    def test_promoters_details_absent_when_org_name_missing(self):
        r = self._x({"PromoterDetails": {"OfficeNo": "0141"}})
        self.assertNotIn("promoters_details", r)

    # ── promoter_address_raw structured list ──────────────────────────────────

    def test_promoter_address_raw_is_list(self):
        r = self._x({"PromoterDetails": {"Address": {"DistrictName": "Jaipur", "ZipCode": "302001"}}})
        self.assertIsInstance(r["promoter_address_raw"], list)
        # Keys should be normalised to schema names
        self.assertEqual(r["promoter_address_raw"][0]["district"], "Jaipur")
        self.assertEqual(r["promoter_address_raw"][0]["pin_code"], "302001")

    def test_promoter_address_raw_full_key_mapping(self):
        r = self._x({"PromoterDetails": {"Address": {
            "PlotNumber":   "A-181",
            "StreetName":   "Kalwar Road",
            "VillageName":  "JAIPUR",
            "DistrictName": "Jaipur",
            "Taluka":       "Jaipur",
            "StateName":    "Rajasthan",
            "ZipCode":      "302012",
        }}})
        addr = r["promoter_address_raw"][0]
        self.assertEqual(addr["house_no_building_name"], "A-181")
        self.assertEqual(addr["locality"],  "Kalwar Road")
        self.assertEqual(addr["village"],   "JAIPUR")
        self.assertEqual(addr["district"],  "Jaipur")
        self.assertEqual(addr["taluk"],     "Jaipur")
        self.assertEqual(addr["state"],     "Rajasthan")
        self.assertEqual(addr["pin_code"],  "302012")
        # Raw API keys must NOT appear in output
        self.assertNotIn("PlotNumber",   addr)
        self.assertNotIn("DistrictName", addr)


if __name__ == "__main__":
    unittest.main()
