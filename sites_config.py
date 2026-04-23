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
        "crawler_type": "static",
        "enabled": True,
        "rate_limit_delay": (2, 4),
        "max_retries": 3,
        "sentinel_registration_no": "",
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
        "config_id": 7,
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
