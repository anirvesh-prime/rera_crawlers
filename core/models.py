from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator

from core.project_normalizer import parse_bool, parse_datetime, parse_float, parse_int


class ProjectRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    key: str
    project_registration_no: str
    url: str
    state: str
    domain: str

    project_name: str | None = None
    project_type: str | None = None
    promoter_name: str | None = None
    status_of_the_project: str | None = None
    acknowledgement_no: str | None = None
    project_pin_code: str | None = None
    project_city: str | None = None
    project_state: str | None = None
    project_description: str | None = None
    crawl_machine_ip: str | None = None
    machine_name: str | None = None
    images: str | None = None

    project_location_raw: dict | list | None = None
    promoter_address_raw: dict | list | None = None
    promoter_contact_details: dict | list | None = None
    bank_details: dict | list | None = None
    project_cost_detail: dict | list | None = None
    building_details: dict | list | None = None
    complaints_litigation_details: dict | list | None = None
    uploaded_documents: dict | list | None = None
    authorised_signatory_details: dict | list | None = None
    co_promoter_details: dict | list | None = None
    provided_faciltiy: dict | list | None = None
    professional_information: dict | list | None = None
    development_agreement_detail: dict | list | None = None
    construction_progress: dict | list | None = None
    land_detail: dict | list | None = None
    document_urls: dict | list | None = None
    members_details: dict | list | None = None
    data: dict | list | None = None
    promoters_details: dict | list | None = None
    old_updates: dict | list | None = None
    status_update: dict | list | None = None
    land_area_details: dict | list | None = None
    proposed_timeline: dict | list | None = None

    submitted_date: datetime | None = None
    last_modified: datetime | None = None
    estimated_commencement_date: datetime | None = None
    actual_commencement_date: datetime | None = None
    estimated_finish_date: datetime | None = None
    actual_finish_date: datetime | None = None
    approved_on_date: datetime | None = None
    retrieved_on: datetime | None = None
    last_updated: datetime | None = None
    last_crawled_date: datetime | None = None
    checked_updates_date: datetime | None = None

    past_experience_of_promoter: int | None = None
    number_of_residential_units: int | None = None
    number_of_commercial_units: int | None = None
    config_id: int | None = None

    land_area: float | None = None
    construction_area: float | None = None
    total_floor_area_under_commercial_or_other_uses: float | None = None
    total_floor_area_under_residential: float | None = None

    is_updated: bool = False
    is_duplicate: bool = False
    iw_part_processed: bool | None = None
    iw_processed: bool = False
    checked_updates: bool = False
    rera_housing_found: bool = False
    is_live: bool = False

    updated_fields: list[str] | None = None
    project_images: list[str] | None = None
    detail_images: list[str] | None = None
    lister_images: list[str] | None = None
    doc_ocr_url: list[str] | None = None
    alternative_rera_ids: list[str] | None = None

    @field_validator(
        "key",
        "project_registration_no",
        "url",
        "state",
        "domain",
        mode="before",
    )
    @classmethod
    def required_str_not_empty(cls, value: Any) -> str:
        if value is None or not str(value).strip():
            raise ValueError("required project field cannot be empty")
        return str(value).strip()

    @field_validator(
        "submitted_date",
        "last_modified",
        "estimated_commencement_date",
        "actual_commencement_date",
        "estimated_finish_date",
        "actual_finish_date",
        "approved_on_date",
        "retrieved_on",
        "last_updated",
        "last_crawled_date",
        "checked_updates_date",
        mode="before",
    )
    @classmethod
    def parse_dates(cls, value: Any) -> datetime | None:
        return parse_datetime(value)

    @field_validator(
        "past_experience_of_promoter",
        "number_of_residential_units",
        "number_of_commercial_units",
        "config_id",
        mode="before",
    )
    @classmethod
    def parse_int_fields(cls, value: Any) -> int | None:
        return parse_int(value)

    @field_validator(
        "land_area",
        "construction_area",
        "total_floor_area_under_commercial_or_other_uses",
        "total_floor_area_under_residential",
        mode="before",
    )
    @classmethod
    def parse_float_fields(cls, value: Any) -> float | None:
        return parse_float(value)

    @field_validator(
        "is_updated",
        "is_duplicate",
        "iw_part_processed",
        "iw_processed",
        "checked_updates",
        "rera_housing_found",
        "is_live",
        mode="before",
    )
    @classmethod
    def parse_boolean_fields(cls, value: Any) -> bool | None:
        return parse_bool(value)

    def to_db_dict(self) -> dict[str, Any]:
        return self.model_dump(exclude_none=True)


class DocumentRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    project_key: str
    document_type: str
    original_url: str
    file_name: str
    md5_checksum: str
    file_size_bytes: int
    s3_key: str
    s3_bucket: str
