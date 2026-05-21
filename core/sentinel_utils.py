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

from typing import TYPE_CHECKING, Any

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


def click_tab_with_retry(
    page: Any,
    selector: str,
    *,
    label: str | None = None,
    attempts: int = 2,
    click_timeout_ms: int = 15_000,
    settle_ms: int = 6_000,
    networkidle_timeout_ms: int = 15_000,
    logger: "CrawlerLogger | None" = None,
) -> bool:
    """
    Click a tab/link on an Angular SPA with bounded retries and relaxed timeouts.

    The Promoter / Documents / similar tabs on sentinel pages occasionally fail
    to register the first click when the underlying XHR is still in flight.
    A single retry covers the transient case without making the sentinel hang.

    Returns True on success, False if all attempts were exhausted. On False the
    caller should continue gracefully — coverage will reveal the missing data.
    """
    tab_label = label or selector
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            page.click(selector, timeout=click_timeout_ms)
            page.wait_for_timeout(settle_ms)
            try:
                page.wait_for_load_state("networkidle", timeout=networkidle_timeout_ms)
            except Exception:
                # networkidle is best-effort — Angular often keeps a long-poll open.
                pass
            return True
        except Exception as exc:
            last_exc = exc
            if logger:
                logger.warning(
                    f"Sentinel: tab click failed (attempt {attempt}/{attempts}) "
                    f"for {tab_label!r} — {exc}",
                    step="sentinel",
                )
            if attempt < attempts:
                # Brief settle before retry; gives Angular time to recover.
                try:
                    page.wait_for_timeout(1_000)
                except Exception:
                    pass
    if logger and last_exc is not None:
        logger.warning(
            f"Sentinel: giving up on tab {tab_label!r} after {attempts} attempts",
            step="sentinel",
        )
    return False
