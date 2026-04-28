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
        "sentinel_registration_no": "",
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
        "sentinel_registration_no": "",
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
        "enabled": False,
        "rate_limit_delay": (1, 3),
        "max_retries": 3,
        "sentinel_registration_no": "PR/GJ/SURAT/CHORASI/SUDA/CAA00202/A1C/EX1/041221",
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
        "enabled": False,
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
        "enabled": False,
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
        "enabled": False,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "DLRERA2023P0017",
        "config_id": 10,
    },
    {
        "id": "tamil_nadu_rera",
        "name": "Tamil Nadu RERA",
        "state_code": "TN",
        "state": "tamil_nadu",
        "domain": "rera.tn.gov.in",
        "listing_url": "https://rera.tn.gov.in/registered-building/tn",
        "crawler_module": "sites.tamil_nadu_rera",
        "crawler_type": "static",
        "enabled": False,
        "rate_limit_delay": (1, 3),
        "max_retries": 3,
        "sentinel_registration_no": "TNRERA/29/BLG/0001/2026",
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
        "enabled": False,
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
        "enabled": False,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "P03290013808",
        "config_id": 11793,
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
        "enabled": False,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "PRTR03240386",
        "config_id": 11807,
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
