"""
core/sentinel_utils.py

Shared sentinel health-check utilities.

The standard sentinel only checks that a known project is reachable.
check_field_coverage() adds a *data-quality* gate on top: even if the
portal is still up, a significant drop in the number of populated fields
signals that the site's HTML structure changed in a way the crawler no
longer handles correctly.

Usage (in any _sentinel_check):

    from core.sentinel_utils import check_field_coverage
    if not check_field_coverage(fresh_record, baseline_record, logger=logger):
        return False
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.logger import CrawlerLogger


# Fields that represent real extracted data (excludes system/bookkeeping columns
# like key, url, domain, state, retrieved_on, last_crawled_date, etc.)
COVERAGE_FIELDS: frozenset[str] = frozenset({
    "project_name",
    "promoter_name",
    "status_of_the_project",
    "project_pin_code",
    "project_city",
    "project_state",
    "estimated_commencement_date",
    "estimated_finish_date",
    "approved_on_date",
    "actual_commencement_date",
    "actual_finish_date",
    "land_area",
    "construction_area",
    "number_of_residential_units",
    "number_of_commercial_units",
    "promoters_details",
    "members_details",
    "bank_details",
    "project_cost_detail",
    "project_location_raw",
    "professional_information",
    "promoter_address_raw",
    "promoter_contact_details",
    "uploaded_documents",
    "project_type",
    "project_description",
    "acknowledgement_no",
})


def _is_empty(val: object) -> bool:
    """Return True for None, empty string, empty list, or empty dict."""
    if val is None:
        return True
    if isinstance(val, (str, list, dict)):
        return len(val) == 0
    return False


def check_field_coverage(
    fresh: dict,
    baseline: dict,
    threshold: float = 0.80,
    logger: "CrawlerLogger | None" = None,
) -> bool:
    """
    Return True if `fresh` covers at least `threshold` fraction of the fields
    that `baseline` has populated (measured over COVERAGE_FIELDS only).

    Args:
        fresh:     Freshly-scraped project dict.
        baseline:  Reference dict (state_projects_sample or stored DB record).
        threshold: Minimum acceptable coverage ratio (default 80 %).
        logger:    Optional CrawlerLogger for structured logging.

    Returns:
        True  → coverage is acceptable; crawl may proceed.
        False → coverage dropped too far; sentinel fails.
    """
    baseline_populated = {
        f for f in COVERAGE_FIELDS if not _is_empty(baseline.get(f))
    }
    if not baseline_populated:
        if logger:
            logger.warning(
                "Sentinel coverage: baseline has no populated fields to compare",
                step="sentinel",
            )
        return True

    fresh_populated = {
        f for f in baseline_populated if not _is_empty(fresh.get(f))
    }
    ratio = len(fresh_populated) / len(baseline_populated)
    missing = sorted(baseline_populated - fresh_populated)

    if logger:
        logger.info(
            f"Sentinel coverage: {len(fresh_populated)}/{len(baseline_populated)} "
            f"fields ({ratio:.0%}) — threshold {threshold:.0%}",
            step="sentinel",
            covered=len(fresh_populated),
            expected=len(baseline_populated),
        )

    if ratio < threshold:
        if logger:
            logger.error(
                f"Sentinel coverage too low: {ratio:.0%} < {threshold:.0%}. "
                f"Missing fields: {missing}",
                step="sentinel",
                missing_fields=missing,
                coverage_ratio=round(ratio, 3),
            )
        return False

    if missing and logger:
        logger.warning(
            f"Sentinel: {len(missing)} field(s) absent but above threshold — {missing}",
            step="sentinel",
        )
    return True
