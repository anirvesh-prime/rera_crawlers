"""
Master list of all RERA sites to crawl.
To disable a site without deleting it, set enabled=False.
To add a new site, append an entry and set enabled=False until tested.

crawler_type options:
  'static'      - server-rendered HTML, use httpx + BeautifulSoup
  'api'         - Angular/SPA site with discoverable JSON API, use httpx directly
  'playwright'  - pure JS SPA with no discoverable API, use Playwright
"""

from __future__ import annotations

from collections.abc import Sequence

SITES: list[dict] = [
    {
        "id": "kerala_rera",
        "name": "Kerala RERA",
        "state_code": "KL",
        "state": "kerala",
        "domain": "rera.kerala.gov.in",
        "listing_url": "https://rera.kerala.gov.in/explore-projects",
        "crawler_module": "sites.kerala_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "K-RERA/PRJ/ERN/363/2020",
        "config_id": 1,
    },
    {
        "id": "rajasthan_rera",
        "name": "Rajasthan RERA",
        "state_code": "RJ",
        "state": "rajasthan",
        "domain": "rera.rajasthan.gov.in",
        "listing_url": "https://rera.rajasthan.gov.in/ProjectList?status=3",
        "crawler_module": "sites.rajasthan_rera",
        "crawler_type": "api",
        "enabled": True,
        "rate_limit_delay": (1, 3),
        "max_retries": 3,
        # RAJ/P/2024/3058 (VENTURA by J S BUILDCOM) — used as the sample project
        # for dry-run comparisons (state_projects_sample/rajasthan.json).
        "sentinel_registration_no": "RAJ/P/2024/3058",
        "config_id": 2,
    },
    {
        "id": "odisha_rera",
        "name": "Odisha RERA",
        "state_code": "OD",
        "state": "odisha",
        "domain": "rera.odisha.gov.in",
        "listing_url": "c",
        "crawler_module": "sites.odisha_rera",
        "crawler_type": "playwright",
        "enabled": True,
        "rate_limit_delay": (2, 5),
        "max_retries": 3,
        "sentinel_registration_no": "RP/11/2026/01471",
        "config_id": 3,
    },
    {
        "id": "pondicherry_rera",
        "name": "Pondicherry RERA",
        "state_code": "PY",
        "state": "puducherry",
        "domain": "prera.py.gov.in",
        "listing_url": "https://prera.py.gov.in/reraAppOffice/viewDefaulterProjects",
        "crawler_module": "sites.pondicherry_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "",
        "config_id": 4,
    },
    {
        "id": "bihar_rera",
        "name": "Bihar RERA",
        "state_code": "BR",
        "state": "bihar",
        "domain": "rera.bihar.gov.in",
        "listing_url": "https://rera.bihar.gov.in/RegisteredPP.aspx",
        "crawler_module": "sites.bihar_rera",
        # Listing is server-rendered ASP.NET; detail pages use __doPostBack (Playwright).
        "crawler_type": "playwright",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "BRERAP05734-1/994/R-766/2019",
        "config_id": 5,
    },
    {
        "id": "punjab_rera",
        "name": "Punjab RERA",
        "state_code": "PB",
        "state": "punjab",
        "domain": "rera.punjab.gov.in",
        "listing_url": "https://rera.punjab.gov.in/reraindex/publicview/projectinfo",
        "crawler_module": "sites.punjab_rera",
        "crawler_type": "playwright",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "",
        "config_id": 6,
    },
    {
        "id": "maharashtra_rera",
        "name": "Maharashtra RERA",
        "state_code": "MH",
        "state": "MAHARASHTRA",
        "domain": "maharera.maharashtra.gov.in",
        "listing_url": "https://maharera.maharashtra.gov.in/projects-search-result",
        "crawler_module": "sites.maharashtra_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "",
        # dry-run comparison: start at page 4700 to hit recent, well-populated projects
        # (page 0 has cert_id=1, an old sparse project with no bank/SPOC/inventory data)
        "dry_run_compare_start_page": 4700,
        "config_id": 7,
    },
    {
        "id": "gujarat_rera",
        "name": "Gujarat RERA",
        "state_code": "GJ",
        "state": "gujarat",
        "domain": "gujrera.gujarat.gov.in",
        # Angular SPA — crawler uses the JSON API directly (sequential ID iteration).
        "listing_url": "https://gujrera.gujarat.gov.in/#/home-p/registered-project-listing",
        "crawler_module": "sites.gujarat_rera",
        "crawler_type": "api",
        "enabled": True,
        "rate_limit_delay": (1, 3),
        "max_retries": 3,
        # Sentinel and dry-run sample align to a known live project ID.
        "sentinel_registration_no": "PR/GJ/SURAT/CHORYASI/Surat Municipal Corporation/RAA16644/250326/311231",
        "sentinel_project_id": 30502,
        # Match dry-run comparison against the live sample project (proj_id=30502).
        "dry_run_compare_start_page": 30501,
        "config_id": 8,
    },
    {
        "id": "karnataka_rera",
        "name": "Karnataka RERA",
        "state_code": "KA",
        "state": "karnataka",
        "domain": "rera.karnataka.gov.in",
        "listing_url": "https://rera.karnataka.gov.in/viewAllProjects",
        "crawler_module": "sites.karnataka_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (2, 5),
        "max_retries": 3,
        "sentinel_registration_no": "PRM/KA/RERA/1248/469/PR/050723/006033",
        "config_id": 9,
    },
    {
        "id": "haryana_rera",
        "name": "Haryana RERA",
        "state_code": "HR",
        "state": "haryana",
        "domain": "haryanarera.gov.in",
        "listing_url": "https://haryanarera.gov.in/admincontrol/registered_projects/2",
        "crawler_module": "sites.haryana_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "RERA-PKL-456-2019",
        "config_id": 11813,
    },
    {
        "id": "delhi_rera",
        "name": "Delhi RERA",
        "state_code": "DL",
        "state": "delhi",
        "domain": "rera.delhi.gov.in",
        "listing_url": "https://rera.delhi.gov.in/registered_promoters_list",
        "crawler_module": "sites.delhi_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "DLRERA2023P0017",
        "config_id": 10,
        # Secondary-page URLs extracted per-row from the listing HTML:
        # - directors_url_base + node_id  → co_promoter_details / members_details
        # - qpr_history_url_base + reg_no → status_update (QPR submission history)
        "directors_url_base": "https://rera.delhi.gov.in/promoter_directors/",
        "qpr_history_url_base": "https://rera.delhi.gov.in/online_view_periodic_progress_reports_history/",
        # Fields that are NOT available on any public Delhi RERA page and
        # therefore cannot be scraped (project_type, building_details,
        # land_area, land_area_details, construction_area, project_cost_detail,
        # professional_information, project_description,
        # estimated_commencement_date, project_images).
        "fetch_directors": True,
        "fetch_qpr_history": True,
    },
    {
        "id": "tamil_nadu_rera",
        "name": "Tamil Nadu RERA",
        "state_code": "TN",
        "state": "tamil_nadu",
        "domain": "rera.tn.gov.in",
        # Covers Building, Normal Layout, and Regularisation Layout projects.
        # The crawler auto-discovers year-listing URLs from three CMS index pages.
        "listing_url": "https://rera.tn.gov.in/registered-building/tn",
        "cms_index_urls": [
            "https://rera.tn.gov.in/cms/reg_projects_building_tamilnadu.php",
            "https://rera.tn.gov.in/cms/reg_projects_normallayout_tamilnadu.php",
            "https://rera.tn.gov.in/cms/reg_projects_regularisationlayout_tamilnadu.php",
        ],
        "crawler_module": "sites.tamil_nadu_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (1, 3),
        "max_retries": 3,
        # Layout sample project used for dry-run comparisons
        "sentinel_registration_no": "TNRERA/29/LO/4544/2025",
        "config_id": 14374,
    },
    {
        "id": "jharkhand_rera",
        "name": "Jharkhand RERA",
        "state_code": "JH",
        "state": "jharkhand",
        "domain": "jharera.jharkhand.gov.in",
        # Server-rendered MVC listing; pagination via ?page=N query param; detail via GET.
        "listing_url": "https://jharera.jharkhand.gov.in/Home/OnlineRegisteredProjectsList",
        "crawler_module": "sites.jharkhand_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "JHARERA/PROJECT/146/2022",
        "config_id": 14209,
    },
    {
        "id": "andhra_pradesh_rera",
        "name": "Andhra Pradesh RERA",
        "state_code": "AP",
        "state": "andhra pradesh",
        "domain": "rera.ap.gov.in",
        # ASP.NET GridView listing — all rows in initial HTML (DataTables client-side).
        # Detail pages use an encrypted 'enc' query-parameter URL.
        "listing_url": "https://rera.ap.gov.in/RERA/Views/Reports/ApprovedProjects.aspx",
        "crawler_module": "sites.andhra_pradesh_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "P03290013808",
        "config_id": 11793,
    },
    {
        "id": "chhattisgarh_rera",
        "name": "Chhattisgarh RERA",
        "state_code": "CG",
        "state": "chhattisgarh",
        "domain": "rera.cgstate.gov.in",
        # ASP.NET WebForms listing — all 2088 project stubs embedded in one page
        # as a JavaScript map-markers JSON array (lat/lon + MyID link).
        # Detail pages: /Promoter_Reg_Only_View_Application_new.aspx?MyID={base64_id}
        "listing_url": "https://rera.cgstate.gov.in/Approved_project_List.aspx",
        "crawler_module": "sites.chhattisgarh_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "PCGRERA200618000247",
        "config_id": 11805,
    },
    {
        "id": "goa_rera",
        "name": "Goa RERA",
        "state_code": "GA",
        "state": "goa",
        "domain": "rera.goa.gov.in",
        "listing_url": "https://rera.goa.gov.in/reraApp/home",
        "crawler_module": "sites.goa_rera",
        # Playwright needed: listing page has captcha; detail pages use httpx (no captcha).
        "crawler_type": "playwright",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "PRGO02231914",
        "sentinel_project_url": "https://rera.goa.gov.in/reraApp/viewProjectDetailPage?projectID=Ospqf/NrToXvyoNgHCYymA==",
        "config_id": 11806,
    },
    {
        "id": "tripura_rera",
        "name": "Tripura RERA",
        "state_code": "TR",
        "state": "tripura",
        "domain": "reraonline.tripura.gov.in",
        # Server-rendered Java MVC listing — paginated table (startFrom offset).
        # Detail pages: /viewProjectDetailPage?projectID=N
        "listing_url": "https://reraonline.tripura.gov.in/viewApprovedProjects",
        "crawler_module": "sites.tripura_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "PRTR03240386",
        "config_id": 11807,
    },
    {
        "id": "wb_rera",
        "name": "West Bengal RERA",
        "state_code": "WB",
        "state": "west bengal",
        "domain": "rera.wb.gov.in",
        # Single static listing page (DataTables, all districts, all ~4,250 projects).
        # Detail pages: /project_details.php?procode=N
        "listing_url": "https://rera.wb.gov.in/district_project.php?dcode=0",
        "crawler_module": "sites.west_bengal_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (1, 3),
        "max_retries": 3,
        "sentinel_registration_no": "WBRERA/P/ALI/2023/000353",
        "config_id": 11815,
    },
    {
        "id": "telangana_rera",
        "name": "Telangana RERA",
        "state_code": "TS",
        "state": "telangana",
        "domain": "rerait.telangana.gov.in",
        # ASP.NET search-form listing with CAPTCHA; results are server-rendered HTML.
        # Detail pages use an encrypted q-param PrintPreview URL.
        "listing_url": "https://rerait.telangana.gov.in/SearchList/Search",
        "crawler_module": "sites.telangana_rera",
        "crawler_type": "playwright",
        "enabled": False,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "",
        "config_id": 11811,
    },
    {
        "id": "assam_rera",
        "name": "Assam RERA",
        "state_code": "AS",
        "state": "assam",
        "domain": "rera.assam.gov.in",
        # Single static listing page (DataTables, client-side, all ~1200+ projects).
        # Listing URL: /admincontrol/registered_projects/1
        # Detail pages: /view_project/searchprojectDetail/{id}
        # Form-A pages: /view_project/project_preview_open/{id}
        # Certificate:  /view_project/view_certificate/{base64_id}
        # Documents:    /project/view_uploaded_Document_open_public/{base64_id}
        "listing_url": "https://rera.assam.gov.in/admincontrol/registered_projects/1",
        "crawler_module": "sites.assam_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        # RERAA KM 49 OF 2024-2025 (UTTARAYAN HARMONY 2) — sample project used for
        # dry-run comparisons (state_projects_sample/assam.json).
        "sentinel_registration_no": "RERAA KM 49 OF 2024-2025",
        "config_id": 11804,
    },
    {
        "id": "himachal_pradesh_rera",
        "name": "Himachal Pradesh RERA",
        "state_code": "HP",
        "state": "himachal pradesh",
        "domain": "hprera.nic.in",
        # Dashboard-driven AJAX portal. GetFilteredProjectsPV returns all registered
        # projects in one response (HTML cards + embedded JSON marker array).
        # Detail sections are separate AJAX endpoints keyed by encrypted 'qs' token.
        "listing_url": "https://hprera.nic.in/PublicDashboard",
        "crawler_module": "sites.himachal_pradesh_rera",
        "crawler_type": "api",
        "enabled": True,
        "rate_limit_delay": (1, 3),
        "max_retries": 3,
        # AURAMAH VALLEY PHASE-I (RERAHPSHP01190048) — matches the sample project in
        # state_projects_sample/himachal_pradesh.json.
        "sentinel_registration_no": "RERAHPSHP01190048",
        "config_id": 11808,
    },
    {
        "id": "madhya_pradesh_rera",
        "name": "Madhya Pradesh RERA",
        "state_code": "MP",
        "state": "madhya pradesh",
        "domain": "rera.mp.gov.in",
        # PHP site. Listing AJAX endpoint returns all ~8,255 projects in one HTML
        # table (DataTables client-side). Registration number is only on the detail
        # page, so every project requires a detail fetch.
        # Listing AJAX: /project-all-loop.php?show=20&pagenum=1
        # Detail pages: /view_project_details.php?id=<base64_id>
        "listing_url": "https://www.rera.mp.gov.in/all-projects/",
        "listing_ajax_url": "https://www.rera.mp.gov.in/project-all-loop.php",
        "detail_base_url": "https://www.rera.mp.gov.in/view_project_details.php",
        "crawler_module": "sites.madhya_pradesh_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (1, 3),
        "max_retries": 3,
        # P-BPL-24-4939 (RESIDENTIAL CUM COMMERCIAL PROJECT AT ALAM NAGAR - BHOPAL)
        # used as the sample project for dry-run comparisons
        # (state_projects_sample/madhya_pradesh.json).
        "sentinel_registration_no": "P-BPL-24-4939",
        "sentinel_detail_url": "https://www.rera.mp.gov.in/view_project_details.php?id=L2NFS0wybnFhMFppUVV3MVduMFpEZz09",
        "config_id": 12898,
    },
    {
        "id": "uttarakhand_rera",
        "name": "Uttarakhand RERA",
        "state_code": "UK",
        "state": "uttarakhand",
        "domain": "ukrera.uk.gov.in",
        # Server-rendered Java MVC (Spring Tiles). All registered projects are
        # returned on a single listing page (pagination is present in JS but
        # disabled server-side for public view).  Detail pages redirect via 302
        # to a session-encrypted URL; follow_redirects + session cookie required.
        # The portal's TLS configuration requires unsafe legacy renegotiation —
        # crawler uses get_legacy_ssl_context() from core.crawler_base.
        "listing_url": "https://ukrera.uk.gov.in/viewRegisteredProjects",
        "detail_base_url": "https://ukrera.uk.gov.in/viewProjectDetailPage",
        "crawler_module": "sites.uttarakhand_rera",
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        # AARDH KUMBH ENCLAVE (UKREP11250000693 / projectID=1159) — matches
        # state_projects_sample/uttarakhand.json.
        "sentinel_registration_no": "UKREP11250000693",
        "sentinel_project_id": 1159,
        "config_id": 11814,
    },
    {
        "id": "uttar_pradesh_rera",
        "name": "Uttar Pradesh RERA",
        "state_code": "UP",
        "state": "uttar pradesh",
        "domain": "www.up-rera.in",
        # District-wise listing: frm_allprojectdistrictwise.aspx?districtname={district}
        # All 75 UP districts are iterated; each page is an ASP.NET WebForms GridView
        # with all projects for that district in the initial HTML response (no pagination).
        # "View Detail" buttons use __doPostBack — Playwright is required for detail pages.
        "listing_url": "https://www.up-rera.in/frm_allprojectdistrictwise.aspx",
        "crawler_module": "sites.uttar_pradesh_rera",
        "crawler_type": "playwright",
        "enabled": True,
        "rate_limit_delay": (2, 5),
        "max_retries": 3,
        # Ace Divino — UPRERAPRJ6734 (Gautam Buddha Nagar) is the sample project in
        # state_projects_sample/uttar_pradesh.json. Used as sentinel and dry-run baseline.
        "sentinel_registration_no": "UPRERAPRJ6734",
        "sentinel_district": "Gautam Buddha Nagar",
        "config_id": 11816,
    },
]


def parse_site_selection(site_args: Sequence[str] | None) -> list[str]:
    """Normalize repeated/comma-separated --site arguments into unique site ids."""
    if not site_args:
        return []

    selected: list[str] = []
    seen: set[str] = set()

    for raw_value in site_args:
        for site_id in raw_value.split(","):
            normalized = site_id.strip()
            if not normalized or normalized in seen:
                continue
            selected.append(normalized)
            seen.add(normalized)

    return selected


def select_sites(
    site_args: Sequence[str] | None,
    catalog: Sequence[dict] | None = None,
) -> tuple[list[dict], list[str], list[str]]:
    """
    Resolve which sites to run.

    Default behavior returns enabled sites only.
    Explicit site selection can include disabled sites so they can still be tested
    without joining the default production run.
    """
    available_sites = list(catalog or SITES)
    selected_ids = parse_site_selection(site_args)

    if not selected_ids:
        return [site for site in available_sites if site["enabled"]], [], []

    sites_by_id = {site["id"]: site for site in available_sites}
    unknown = [site_id for site_id in selected_ids if site_id not in sites_by_id]
    selected_sites = [sites_by_id[site_id] for site_id in selected_ids if site_id in sites_by_id]
    disabled = [site["id"] for site in selected_sites if not site["enabled"]]
    return selected_sites, unknown, disabled
