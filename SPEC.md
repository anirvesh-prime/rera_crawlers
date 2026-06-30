# RERA Crawlers — Comprehensive Technical Specification

## 1. Project Overview

A production-grade web crawling framework targeting ~25 Indian state RERA (Real Estate Regulatory Authority) portals. The system extracts structured project metadata and all associated documents (PDFs), stores metadata in PostgreSQL, and uploads documents to AWS S3. No live AI calls are made during crawling.

### Goals
- Crawl all registered real estate projects from ~25 RERA state portals
- Extract full project metadata (listing + detail page)
- Download and store all project documents (PDFs) to S3
- Track crawl state, checkpoints, and errors in PostgreSQL
- Run reliably overnight via cron with resume-on-failure capability

---

## 2. Target Sites

`sites_config.py` is the authoritative catalog. It controls production enablement,
site IDs, state/domain metadata, crawler module, crawler type, sentinel project,
and `config_id`.

| Site ID | State / Authority | Primary URL | Implementation Type | Enabled |
|---|---|---|---|---|
| `andhra_pradesh_rera` | Andhra Pradesh | https://rera.ap.gov.in/RERA/Views/Reports/ApprovedProjects.aspx | ASP.NET GridView listing + static detail HTML | Yes |
| `assam_rera` | Assam | https://rera.assam.gov.in/admincontrol/registered_projects/1 | CodeIgniter static DataTables listing + Form-A detail pages | Yes |
| `bihar_rera` | Bihar | https://rera.bihar.gov.in/RegisteredPP.aspx | ASP.NET listing via Selenium postbacks + static popup detail pages | Yes |
| `chhattisgarh_rera` | Chhattisgarh | https://rera.cgstate.gov.in/Approved_project_List.aspx | ASP.NET static page with embedded map-marker JSON | No |
| `delhi_rera` | Delhi | https://rera.delhi.gov.in/registered_promoters_list | Drupal table listing; all core project data inline | Yes |
| `goa_rera` | Goa | https://rera.goa.gov.in/reraApp/home | Selenium listing with CAPTCHA + static detail fetches | Yes |
| `gujarat_rera` | Gujarat | https://gujrera.gujarat.gov.in/#/home-p/registered-project-listing | Angular SPA; bulk enumeration API + Selenium detail scrape | Yes |
| `haryana_rera` | Haryana Gurugram + Panchkula | https://haryanarera.gov.in/admincontrol/registered_projects/2 | CodeIgniter static DataTables listings for two authorities | Yes |
| `himachal_pradesh_rera` | Himachal Pradesh | https://hprera.nic.in/PublicDashboard | AJAX dashboard endpoints returning HTML fragments | Yes |
| `jharkhand_rera` | Jharkhand | https://jharera.jharkhand.gov.in/Home/OnlineRegisteredProjectsList | Server-rendered MVC listing + static detail pages | Yes |
| `karnataka_rera` | Karnataka | https://rera.karnataka.gov.in/viewAllProjects | District POST listing + two-step project detail POST | Yes |
| `kerala_rera` | Kerala | https://rera.kerala.gov.in/explore-projects | Static/API hybrid; project-card listing + project HTML | Yes |
| `madhya_pradesh_rera` | Madhya Pradesh | https://www.rera.mp.gov.in/all-projects/ | PHP AJAX table; detail pages required for registration number | No |
| `maharashtra_rera` | Maharashtra | https://maharera.maharashtra.gov.in/projects-search-result | Static listing + CAPTCHA-gated Angular detail SPA | Yes |
| `odisha_rera` | Odisha | https://rera.odisha.gov.in/projects/project-list | Angular SPA listing and detail tabs | Yes |
| `pondicherry_rera` | Puducherry | https://prera.py.gov.in/reraAppOffice/viewDefaulterProjects | Selenium/static legacy SSL portal | Yes |
| `punjab_rera` | Punjab | https://rera.punjab.gov.in/reraindex/publicview/projectinfo | Selenium listing; dummy client-side CAPTCHA | Yes |
| `rajasthan_rera` | Rajasthan | https://rera.rajasthan.gov.in/ProjectList?status=3 | Pure Selenium Angular listing and details | Yes |
| `tamil_nadu_rera` | Tamil Nadu | https://rera.tn.gov.in/registered-building/tn | httpx listing year forms + Selenium detail pages | Yes |
| `telangana_rera` | Telangana | https://rerait.telangana.gov.in/SearchList/Search | ASP.NET search form with CAPTCHA + encrypted preview pages | No |
| `tripura_rera` | Tripura | https://reraonline.tripura.gov.in/search | Java MVC primary POST listing + supplementary approved listing | Yes |
| `uttarakhand_rera` | Uttarakhand | https://ukrera.uk.gov.in/viewRegisteredProjects | Java Spring MVC static listing + encrypted redirect details | Yes |
| `uttar_pradesh_rera` | Uttar Pradesh | https://www.up-rera.in/frm_allprojectdistrictwise.aspx | District-wise ASP.NET listings + Selenium detail postbacks | Yes |
| `wb_rera` | West Bengal | https://rera.wb.gov.in/district_project.php?dcode=0 | Selenium DataTables extraction + static detail pages | Yes |

### Site Classification (drives extraction strategy inside the Selenium crawler)
- **Type 1 — Static/Server-rendered**: Parse the rendered HTML with `BeautifulSoup4`. Fetched via the shared `SeleniumSession` so all sites share one TLS/cookie path.
- **Type 2 — API-backed SPA**: Call the underlying REST/JSON endpoint from inside the Selenium driver (via the in-browser `fetch` bridge on `SeleniumSession`).
- **Type 3 — Pure JS SPA (no discoverable API)**: Render the page in Selenium (Chromium WebDriver) and scrape the resulting DOM via `SeleniumPageAdapter` / `BeautifulSoup4`.

Every new site must be classified before a crawler is written. Classification process:
1. Open browser DevTools → Network tab → filter XHR/Fetch
2. If JSON API calls found → Type 2
3. If page renders without JS → Type 1
4. Otherwise → Type 3

All three types run on top of a single `SeleniumSession` (Chromium WebDriver) — the classification only governs how the page contents are extracted, not the underlying transport.

### CAPTCHA Handling Policy
- If a site has a real, server-validated CAPTCHA, use `core/captcha_solver.py` as the shared integration point instead of embedding site-specific socket/OCR code inside the crawler.
- Inside a Selenium-driven flow, first extract the rendered CAPTCHA image from the page (via `WebElement.screenshot()` on the `<img>` or `<canvas>` element) and call `solve_captcha_from_page(...)`. Keep a site-specific fallback only when the portal exposes a more reliable bypass, such as a readable canvas draw sequence or reusable token flow.
- Do not force the shared solver into pages where the CAPTCHA is only client-side decoration and the backend does not validate it. In those cases, keep the simpler site-specific path.
- Current status:
  - Maharashtra uses the shared solver against the rendered CAPTCHA canvas, with canvas-text interception retained as fallback.
  - Punjab keeps its dummy client-side CAPTCHA fill because the backend does not verify the image text.

---

## 3. Technology Stack

| Component | Library/Tool | Version |
|-----------|-------------|---------|
| Language | Python | 3.13 |
| HTTP client | `httpx` | latest |
| HTML parsing | `beautifulsoup4` + `lxml` | latest |
| Browser automation | `selenium` + `webdriver-manager` (Chromium) | latest |
| PostgreSQL driver | `psycopg[binary]` (psycopg3) | latest |
| S3 client | `boto3` | latest |
| Data validation | `pydantic` + `pydantic-settings` | v2 |
| Env/config | `pydantic-settings` + `.env` file | v2 |
| Scheduling | System `crontab` | — |
| Logging | Python `logging` (structured JSON) | stdlib |

---

## 4. Project Folder Structure

```
rera_crawlers/
├── SPEC.md                        # This document
├── .env                           # Secrets (never committed)
├── .env.example                   # Template with all keys, no values
├── .gitignore
├── requirements.txt
│
├── core/                          # Shared infrastructure modules
│   ├── __init__.py
│   ├── config.py                  # Pydantic-settings config loader
│   ├── db.py                      # PostgreSQL connection + helpers (psycopg3)
│   ├── s3.py                      # S3 upload and checksum helpers
│   ├── logger.py                  # Structured JSON logger
│   ├── models.py                  # Pydantic models for project + document
│   ├── crawler_base.py            # Shared utilities (delays, UA rotation, retries, key generation)
│   └── checkpoint.py              # Crawl checkpoint read/write helpers
│
├── sites/                         # One script per site
│   ├── kerala_rera.py
│   ├── rajasthan_rera.py
│   ├── odisha_rera.py
│   ├── pondicherry_rera.py
│   └── ...                        # One file per additional site
│
├── sites_config.py                # Master list of all sites + enable/disable flags
├── run_crawlers.py                # Orchestrator — reads sites_config, runs enabled sites
│
└── logs/                          # JSON log files, one per run (gitignored)
```

---

## 5. Database Schema

Using the configured PostgreSQL instance. The primary projects table schema is as follows:

```sql
-- Primary table: one row per unique project
CREATE TABLE rera_projects (
    key                                        TEXT NOT NULL PRIMARY KEY,
    project_name                                 TEXT,
    project_type                                 TEXT,
    promoter_name                                TEXT,
    project_registration_no                      TEXT,
    status_of_the_project                        TEXT,
    acknowledgement_no                           TEXT,
    project_pin_code                             TEXT,
    project_city                                 TEXT,
    project_state                                TEXT,
    project_location_raw                         JSONB,
    promoter_address_raw                         JSONB,
    promoter_contact_details                     JSONB,
    submitted_date                               TIMESTAMPTZ,
    last_modified                                TIMESTAMPTZ,
    estimated_commencement_date                  TIMESTAMPTZ,
    actual_commencement_date                     TIMESTAMPTZ,
    estimated_finish_date                        TIMESTAMPTZ,
    actual_finish_date                           TIMESTAMPTZ,
    approved_on_date                             TIMESTAMPTZ,
    past_experience_of_promoter                  INTEGER,
    bank_details                                 JSONB,
    land_area                                    DOUBLE PRECISION,
    construction_area                            DOUBLE PRECISION,
    total_floor_area_under_commercial_or_other_uses DOUBLE PRECISION,
    total_floor_area_under_residential           DOUBLE PRECISION,
    project_cost_detail                          JSONB,
    number_of_residential_units                  INTEGER,
    number_of_commercial_units                   INTEGER,
    building_details                             JSONB,
    complaints_litigation_details                JSONB,
    uploaded_documents                           JSONB,
    authorised_signatory_details                 JSONB,
    co_promoter_details                          JSONB,
    project_description                          TEXT,
    provided_faciltiy                            JSONB,
    professional_information                     JSONB,
    development_agreement_detail                 JSONB,
    construction_progress                        JSONB,
    land_detail                                  JSONB,
    document_urls                                JSONB,
    members_details                              JSONB,
    retrieved_on                                 TIMESTAMPTZ DEFAULT now(),
    config_id                                    INTEGER,
    data                                         JSONB,
    promoters_details                            JSONB,
    domain                                       TEXT,
    state                                        TEXT,
    crawl_machine_ip                             TEXT,
    machine_name                                 TEXT,
    is_updated                                   BOOLEAN DEFAULT false,
    is_duplicate                                 BOOLEAN DEFAULT false,
    url                                          TEXT NOT NULL,
    last_updated                                 TIMESTAMPTZ,
    updated_fields                               TEXT[],
    project_images                               TEXT[],
    detail_images                                TEXT[],
    lister_images                                TEXT[],
    images                                       TEXT,
    old_updates                                  JSONB DEFAULT '[]'::jsonb,
    status_update                                JSONB,
    iw_part_processed                            BOOLEAN,
    iw_processed                                 BOOLEAN DEFAULT false,
    last_crawled_date                            TIMESTAMPTZ DEFAULT now(),
    land_area_details                            JSONB,
    doc_ocr_url                                  TEXT[],
    proposed_timeline                            JSONB,
    checked_updates                              BOOLEAN DEFAULT false,
    checked_updates_date                         TIMESTAMPTZ,
    rera_housing_found                           BOOLEAN DEFAULT false,
    is_live                                      BOOLEAN DEFAULT false,
    alternative_rera_ids                         TEXT[]
);
```

### Projects Table Field Reference

`NULL` means the column is nullable with no explicit DB default. `none` means there is no DB default; the column may still be required by the schema.

#### Identity and Core Metadata

| Column | Type | Default | Purpose |
|-------|------|---------|---------|
| `key` | `text` | `none` | Primary unique project identifier. |
| `project_name` | `text` | `NULL` | Project title or name from the source. |
| `project_type` | `text` | `NULL` | Project category or type. |
| `promoter_name` | `text` | `NULL` | Main promoter or developer name. |
| `project_registration_no` | `text` | `NULL` | RERA registration number. |
| `status_of_the_project` | `text` | `NULL` | Current project status. |
| `acknowledgement_no` | `text` | `NULL` | Filing or application acknowledgement number. |
| `project_description` | `text` | `NULL` | Free-text project summary. |
| `url` | `text` | `none` | Project detail page URL. |
| `domain` | `text` | `NULL` | Source website domain. |
| `state` | `text` | `NULL` | Normalized crawl/state bucket used operationally. |
| `config_id` | `integer` | `NULL` | Owning crawler config ID. |
| `data` | `jsonb` | `NULL` | Full raw or normalized source payload snapshot. |

#### Location and Contact Details

| Column | Type | Default | Purpose |
|-------|------|---------|---------|
| `project_pin_code` | `text` | `NULL` | Project postal code. |
| `project_city` | `text` | `NULL` | Project city or town. |
| `project_state` | `text` | `NULL` | Project state as extracted from source. |
| `project_location_raw` | `jsonb` | `NULL` | Raw structured location payload. |
| `promoter_address_raw` | `jsonb` | `NULL` | Raw promoter address payload. |
| `promoter_contact_details` | `jsonb` | `NULL` | Phone, email, and other contact metadata. |

#### Dates and Timeline

| Column | Type | Default | Purpose |
|-------|------|---------|---------|
| `submitted_date` | `timestamptz` | `NULL` | Registration or submission date. |
| `last_modified` | `timestamptz` | `NULL` | Last modified date from source. |
| `estimated_commencement_date` | `timestamptz` | `NULL` | Planned start date. |
| `actual_commencement_date` | `timestamptz` | `NULL` | Actual start date. |
| `estimated_finish_date` | `timestamptz` | `NULL` | Planned completion date. |
| `actual_finish_date` | `timestamptz` | `NULL` | Actual completion date. |
| `approved_on_date` | `timestamptz` | `NULL` | Approval or registration approval date. |
| `retrieved_on` | `timestamptz` | `now()` | When this row was fetched or inserted. |
| `last_updated` | `timestamptz` | `NULL` | Last time our system updated changed fields. |
| `last_crawled_date` | `timestamptz` | `now()` | Last crawl or check time. |
| `checked_updates_date` | `timestamptz` | `NULL` | When update-check completed. |

#### Project Size, Counts, and Financials

| Column | Type | Default | Purpose |
|-------|------|---------|---------|
| `past_experience_of_promoter` | `integer` | `NULL` | Count or measure of promoter experience. |
| `land_area` | `double precision` | `NULL` | Total land area numeric value. |
| `construction_area` | `double precision` | `NULL` | Total construction area numeric value. |
| `total_floor_area_under_commercial_or_other_uses` | `double precision` | `NULL` | Commercial or other-use floor area. |
| `total_floor_area_under_residential` | `double precision` | `NULL` | Residential floor area. |
| `number_of_residential_units` | `integer` | `NULL` | Residential unit count. |
| `number_of_commercial_units` | `integer` | `NULL` | Commercial unit count. |
| `bank_details` | `jsonb` | `NULL` | Project escrow or bank information. |
| `project_cost_detail` | `jsonb` | `NULL` | Cost breakdown payload. |
| `land_area_details` | `jsonb` | `NULL` | Detailed land area structure and units. |
| `proposed_timeline` | `jsonb` | `NULL` | Planned milestone or timeline payload. |

#### Structured Project Detail Payloads

| Column | Type | Default | Purpose |
|-------|------|---------|---------|
| `building_details` | `jsonb` | `NULL` | Tower, building, or unit structure. |
| `complaints_litigation_details` | `jsonb` | `NULL` | Complaints or litigation metadata. |
| `authorised_signatory_details` | `jsonb` | `NULL` | Authorized signatory metadata. |
| `co_promoter_details` | `jsonb` | `NULL` | Co-promoter records. |
| `provided_faciltiy` | `jsonb` | `NULL` | Amenities or facilities payload. |
| `professional_information` | `jsonb` | `NULL` | Architect, engineer, CA, and similar professional details. |
| `development_agreement_detail` | `jsonb` | `NULL` | Development agreement metadata. |
| `construction_progress` | `jsonb` | `NULL` | Progress or status breakdown. |
| `land_detail` | `jsonb` | `NULL` | Land ownership or land-detail payload. |
| `members_details` | `jsonb` | `NULL` | Member, partner, or director details. |
| `promoters_details` | `jsonb` | `NULL` | Additional promoter list or details. |
| `status_update` | `jsonb` | `NULL` | Status update payload or history. |

#### Documents and Images

| Column | Type | Default | Purpose |
|-------|------|---------|---------|
| `uploaded_documents` | `jsonb` | `NULL` | Raw uploaded-document objects before or with S3 links. |
| `document_urls` | `jsonb` | `NULL` | Final uploaded document links used by crawlers or app. |
| `doc_ocr_url` | `text[]` | `NULL` | OCR output URLs for documents. |
| `project_images` | `text[]` | `NULL` | Project image URLs or paths. |
| `detail_images` | `text[]` | `NULL` | Detail-page screenshots or images. |
| `lister_images` | `text[]` | `NULL` | Listing-page screenshots or images. |
| `images` | `text` | `NULL` | Legacy or general image field. |

#### Crawl Operations and Update Tracking

| Column | Type | Default | Purpose |
|-------|------|---------|---------|
| `crawl_machine_ip` | `text` | `NULL` | Machine IP that crawled the row. |
| `machine_name` | `text` | `NULL` | Host, pod, or machine name. |
| `is_updated` | `boolean` | `false` | Whether an update diff was detected. |
| `is_duplicate` | `boolean` | `false` | Whether the row was marked duplicate by dedupe logic. |
| `updated_fields` | `text[]` | `NULL` | List of fields changed in the last update. |
| `old_updates` | `jsonb` | `'[]'::jsonb` | Historical snapshots of prior values or updates. |
| `iw_part_processed` | `boolean` | `NULL` | Internal workflow partial-processed flag. |
| `iw_processed` | `boolean` | `false` | Internal workflow processed flag. |
| `checked_updates` | `boolean` | `false` | Whether recrawl or update-check was completed. |
| `rera_housing_found` | `boolean` | `false` | Whether housing-specific data was found. |
| `is_live` | `boolean` | `false` | Whether the row is considered live or active. |
| `alternative_rera_ids` | `text[]` | `NULL` | Additional source IDs mapped to the same project. |

### Projects Table Operational Notes

The following notes describe how these fields behave in the current repository, including lifecycle, known setters, and whether a field appears active, derived, or legacy in the present RERA flow.

#### Core Source Fields

| Field | Default | What It Is For | Lifecycle / Setter | Notes |
|---|---|---|---|---|
| `key` | `none` | Unique row ID / conflict key. | Initial scrape or generated before write. | Primary key. The crawler can generate it from unique columns if missing in spider flow. |
| `project_name` | `NULL` | Project title. | Initial scrape. | |
| `project_type` | `NULL` | Project/category type. | Initial scrape. | |
| `promoter_name` | `NULL` | Lead promoter/developer name. | Initial scrape. | |
| `project_registration_no` | `NULL` | RERA registration number. | Initial scrape. | Used heavily by `rera_update_spider.py` to refind projects. |
| `status_of_the_project` | `NULL` | Project status text from source. | Initial scrape where available. | I only found one writer in `tools/gujrera_seeds_crawler.py`; not part of the main RERA update path. |
| `acknowledgement_no` | `NULL` | Source acknowledgement/application number. | Initial scrape. | |
| `project_pin_code` | `NULL` | Project location pincode. | No active writer found. | Likely intended source field; I did not find a current setter in this repo. |
| `project_city` | `NULL` | Project city. | Initial scrape in some crawlers. | I found a setter in `custom_crawlers/housing_api_crawler/housing.py`; not in current RERA pipeline. |
| `project_state` | `NULL` | Project state from source payload. | Initial scrape. | Also reused by document-path logic in extractor init and data extractors. |
| `project_location_raw` | `NULL` | Raw structured project-location blob. | Initial scrape / normalized source payload. | |
| `promoter_address_raw` | `NULL` | Raw structured promoter address blob. | Initial scrape / normalized source payload. | |
| `promoter_contact_details` | `NULL` | Structured promoter contact JSON. | Initial scrape / normalized source payload. | |
| `submitted_date` | `NULL` | Registration/submission timestamp. | Initial scrape; later tracked in comparator. | Included in comparator `DATE_FIELDS`. |
| `last_modified` | `NULL` | Source-side last modified timestamp. | Initial scrape where available. | Tracked in comparator `DATE_FIELDS`, but I did not find a current RERA setter. |
| `estimated_commencement_date` | `NULL` | Planned start date. | Initial scrape; later tracked in comparator. | In comparator `DATE_FIELDS`. |
| `actual_commencement_date` | `NULL` | Actual start date. | Initial scrape; later tracked in comparator. | In comparator `DATE_FIELDS` and `RERA_UPDATES`. |
| `estimated_finish_date` | `NULL` | Planned completion date. | Initial scrape; later tracked in comparator. | |
| `actual_finish_date` | `NULL` | Actual completion date. | Initial scrape; later tracked in comparator. | In comparator `DATE_FIELDS` and `RERA_UPDATES`. |
| `approved_on_date` | `NULL` | Approval / registration date. | Initial scrape; later tracked in comparator. | |
| `past_experience_of_promoter` | `NULL` | Numeric promoter-experience metric. | Initial scrape. | |
| `bank_details` | `NULL` | Bank / escrow details. | No active writer found. | Likely source payload field, but I did not find an active setter. |
| `land_area` | `NULL` | Numeric land area. | Initial scrape; later tracked in comparator. | Included in `RERA_UPDATES`. |
| `construction_area` | `NULL` | Numeric construction area. | Initial scrape; later tracked in comparator. | Included in `RERA_UPDATES`. |
| `total_floor_area_under_commercial_or_other_uses` | `NULL` | Commercial/other floor area. | No active writer found. | Present in schema, no active setter found. |
| `total_floor_area_under_residential` | `NULL` | Residential floor area. | No active writer found. | Present in schema, no active setter found. |
| `project_cost_detail` | `NULL` | Structured project cost breakdown. | Initial scrape. | Source payload style field; current writer appears config/API driven. |
| `number_of_residential_units` | `NULL` | Residential unit count. | No active writer found. | Present in schema, no active setter found. |
| `number_of_commercial_units` | `NULL` | Commercial unit count. | No active writer found. | Present in schema, no active setter found. |
| `building_details` | `NULL` | Building / wing / floor / unit details. | Initial scrape; later tracked in comparator. | |
| `complaints_litigation_details` | `NULL` | Complaints/litigation metadata. | No active writer found. | Present in schema, no active setter found. |
| `authorised_signatory_details` | `NULL` | Authorized signatory / SPOC data. | Initial scrape. | |
| `co_promoter_details` | `NULL` | Co-promoter records. | Initial scrape. | |
| `project_description` | `NULL` | Free-text project summary. | No active writer found. | Present in schema, no active setter found. |
| `provided_faciltiy` | `NULL` | Amenities / facilities JSON. | No active writer found. | Likely intended as `provided_facility`; no active setter found. |
| `professional_information` | `NULL` | Architect / engineer / CA / related professional data. | Initial scrape. | |
| `development_agreement_detail` | `NULL` | Development agreement payload. | No active writer found. | Present in schema, no active setter found. |
| `construction_progress` | `NULL` | Construction-progress payload. | Initial scrape; also used in status-update snapshots and comparator. | In `STATUS_COLUMNS` and `RERA_UPDATES`. |
| `land_detail` | `NULL` | Detailed land payload. | No active writer found. | Present in schema, no active setter found. |
| `members_details` | `NULL` | Member / director / partner details. | No active writer found. | Present in schema, no active setter found. |
| `data` | `NULL` | Full raw/normalized source payload snapshot. | Initial scrape. | Comparator ignores it so it does not trigger update churn. |
| `promoters_details` | `NULL` | Extended promoter list/details. | Initial scrape. | |

#### Documents and Media

| Field | Default | What It Is For | Lifecycle / Setter | Notes |
|---|---|---|---|---|
| `uploaded_documents` | `NULL` | Per-document structured records before/with upload metadata. | Initial scrape, then document-upload enrichment. | RERA docs are built in `config_execute_scripts.py`; `data_extractors.py` later injects `s3_link` into each entry. |
| `document_urls` | `NULL` | Final uploaded document links used downstream. | Derived / normalized. | For RERA, `final_extractors.py` converts `uploaded_documents[*]` to `{"link","type"}` entries. |
| `project_images` | `NULL` | Array of project image URLs. | Initial scrape in some crawlers. | I only found an active setter in `custom_crawlers/housing_api_crawler/housing.py`. |
| `detail_images` | `NULL` | Detail page screenshots/images. | No active writer found. | Likely legacy/optional crawler output. |
| `lister_images` | `NULL` | Lister page screenshots/images. | No active writer found. | Likely legacy/optional crawler output. |
| `images` | `NULL` | Legacy catch-all image field. | Initial scrape / legacy. | Broadly present in crawler code, but not clearly part of current RERA path. |
| `doc_ocr_url` | `NULL` | OCR output URLs for documents. | No active writer found in current RERA flow. | Comparator explicitly ignores it. |

#### Crawl Metadata

| Field | Default | What It Is For | Lifecycle / Setter | Notes |
|---|---|---|---|---|
| `retrieved_on` | `now()` | When the row was first inserted/retrieved. | DB default. | The crawler intentionally skips setting it in `pipelines.py`; PostgreSQL fills it. |
| `config_id` | `NULL` | Crawler config ID that owns the row. | Crawl-start metadata. | Set in `initialize_item()` in `rera_update_spider.py` and related spiders. |
| `domain` | `NULL` | Normalized source domain. | Pipeline metadata. | Set in `process_item()` from `url` using `get_proper_domain()`. |
| `state` | `NULL` | Operational crawl/state bucket. | Initial scrape / config metadata. | Used by scheduler/update logic and doc-renaming logic. Not DB-defaulted. |
| `crawl_machine_ip` | `NULL` | IP of crawler host. | Pipeline metadata. | Set in `process_item()`. |
| `machine_name` | `NULL` | Host / pod / machine name. | Pipeline metadata. | Set in `process_item()`. |
| `url` | `none` | Canonical detail page URL. | Initial scrape / crawl target. | Required field; also used for domain derivation. |
| `is_duplicate` | `false` | Persistent duplicate marker. | Pipeline metadata by default; some specialized flows may set later. | `process_item()` forces it to `False` on normal write. Spider duplicate detection mainly uses transient `duplicate` flags. |

#### Update Tracking and Recrawl Control

| Field | Default | What It Is For | Lifecycle / Setter | Notes |
|---|---|---|---|---|
| `is_updated` | `false` | Marks that a meaningful change was detected versus stored row. | Update tracking. | Set in `final_extractors.py` when `updated_fields` is non-empty. |
| `last_updated` | `NULL` | Last time a meaningful update was recorded. | Update tracking. | Set by `rera_data_comparator.py` from new update events or latest `old_updates`. |
| `updated_fields` | `NULL` | List of columns changed in latest comparison pass. | Update tracking. | Built in `rera_data_comparator.py`. |
| `old_updates` | `'[]'::jsonb` | Historical value snapshots of prior states. | Update tracking. | Comparator appends and deduplicates history; DB default is empty list. |
| `status_update` | `NULL` | Snapshot list of status-related structures. | Derived / update-status wrapper. | `rera_status_updates()` prepares it from `construction_progress`, `booking_details`, `building_details`, `proposed_timeline`. |
| `last_crawled_date` | `now()` | Last comparison/crawl attempt time. | DB default initially, then update tracking. | Comparator always refreshes it in `finally`. |
| `proposed_timeline` | `NULL` | Planned milestone/timeline structure. | Initial scrape; also used in status snapshots and comparator. | In `STATUS_COLUMNS` and `RERA_UPDATES`. |
| `checked_updates` | `false` | Whether update-check flow marked the row handled. | Update-spider control. | Set `True` by update spider/final extractors; recrawl-reset flows may set it back to `false`. |
| `checked_updates_date` | `NULL` | When update-check completed or was picked. | Update-spider control. | Set in `rera_update_spider.py` and `final_extractors.py`; can be nulled to force recrawl. |

#### Operational / Workflow Flags

| Field | Default | What It Is For | Lifecycle / Setter | Notes |
|---|---|---|---|---|
| `iw_part_processed` | `NULL` | Internal workflow partial-processing flag. | No active writer found. | I did not find a current setter in this repo. |
| `iw_processed` | `false` | Internal workflow processed flag. | No active writer found in current RERA flow. | Comparator explicitly ignores it. |
| `is_live` | `false` | Whether the row is considered live/eligible for update crawling. | No active RERA writer found in this repo. | Important: I did not find code here that derives this from "today vs end date" for `rera_projects`. The update spider only queries rows where `is_live` is already true. |
| `rera_housing_found` | `false` | Flag for housing-source enrichment / match. | No active writer found. | Present in schema, no current setter found. |

#### Additional / Legacy / Rarely Used Fields

| Field | Default | What It Is For | Lifecycle / Setter | Notes |
|---|---|---|---|---|
| `land_area_details` | `NULL` | Land area with units/breakdown. | Initial scrape; later tracked in comparator. | Included in `RERA_UPDATES`. |
| `alternative_rera_ids` | `NULL` | Alternate IDs mapped to the same project. | No active writer found. | Present in schema, no current setter found. |

#### Short Version By Behavior

- `retrieved_on`: first-write timestamp, filled by DB default.
- `config_id`, `crawl_machine_ip`, `machine_name`, `domain`: crawl/runtime metadata added by the pipeline.
- `uploaded_documents`: raw document objects from scrape, later enriched with uploaded S3 links.
- `document_urls`: normalized final uploaded document links, derived from `uploaded_documents` for RERA.
- `updated_fields`, `old_updates`, `last_updated`, `last_crawled_date`, `is_updated`: comparator-owned update history.
- `checked_updates`, `checked_updates_date`: update-spider / recrawl-control fields.
- `is_live`, `iw_processed`, `iw_part_processed`, `rera_housing_found`, `alternative_rera_ids`: no active setter found in this repo for the current RERA flow.

### Supporting Tables

```sql
-- Tracks every crawl run (one row per site per run)
CREATE TABLE crawl_runs (
    id                  SERIAL PRIMARY KEY,
    site_id             TEXT NOT NULL,           -- matches sites_config key e.g. 'kerala_rera'
    run_type            TEXT NOT NULL,           -- 'daily_light' | 'weekly_deep'
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    status              TEXT NOT NULL,           -- 'running' | 'completed' | 'failed' | 'partial'
    projects_found      INTEGER DEFAULT 0,
    projects_new        INTEGER DEFAULT 0,
    projects_updated    INTEGER DEFAULT 0,
    projects_skipped    INTEGER DEFAULT 0,
    documents_uploaded  INTEGER DEFAULT 0,
    error_count         INTEGER DEFAULT 0,
    sentinel_passed     BOOLEAN,
    notes               TEXT
);

-- Tracks individual errors per project/document during a run
CREATE TABLE crawl_errors (
    id              SERIAL PRIMARY KEY,
    run_id          INTEGER REFERENCES crawl_runs(id),
    site_id         TEXT NOT NULL,
    project_key     TEXT,                        -- NULL if error occurred before key was known
    error_type      TEXT NOT NULL,               -- 'EXTRACTION_FAILED' | 'SITE_STRUCTURE_CHANGED'
                                                 -- | 'HTTP_ERROR' | 'VALIDATION_FAILED'
                                                 -- | 'S3_UPLOAD_FAILED' | 'SENTINEL_FAILED'
    error_message   TEXT,
    url             TEXT,
    occurred_at     TIMESTAMPTZ DEFAULT now(),
    raw_data        JSONB                        -- raw extracted dict before validation failed
);

-- Tracks resume checkpoints per site and run type
CREATE TABLE crawl_checkpoints (
    site_id             TEXT NOT NULL,
    run_type            TEXT NOT NULL,           -- 'daily_light' | 'weekly_deep'
    last_page           INTEGER,                 -- last successfully processed listing page
    last_project_key    TEXT,                    -- last successfully processed project key
    last_run_id         INTEGER REFERENCES crawl_runs(id),
    updated_at          TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (site_id, run_type)
);

-- Tracks every uploaded document with its MD5 checksum for deduplication
CREATE TABLE rera_project_documents (
    id              SERIAL PRIMARY KEY,
    project_key     TEXT NOT NULL,               -- FK to projects.key
    document_type   TEXT,                        -- 'registration_certificate' | 'extension_certificate' etc.
    original_url    TEXT,                        -- source URL the document was downloaded from
    s3_key          TEXT NOT NULL,               -- full S3 key
    s3_bucket       TEXT NOT NULL,
    file_name       TEXT,
    md5_checksum    TEXT NOT NULL,               -- for change detection on re-crawl
    file_size_bytes INTEGER,
    uploaded_at     TIMESTAMPTZ DEFAULT now(),
    last_verified   TIMESTAMPTZ                  -- last time checksum was re-verified
);
```

---

## 6. Unique Key Generation

Every project record uses a stable hash-based primary key.

```python
import os
import siphash24

UINT64_MASK = (1 << 64) - 1

def generate_project_key(registration_number: str) -> str:
    """
    Generates the production project key.
    Uses siphash24 over the stripped registration number, seeded from
    PYTHONHASHSEED, then renders the 64-bit result as an unsigned decimal string.
    """
    seed = int(os.environ.get("PYTHONHASHSEED", "0"))
    key = seed.to_bytes(16, byteorder="little")
    raw = registration_number.strip().encode("utf-8")
    digest = siphash24.siphash24(raw, key=key).intdigest()
    return str(digest & UINT64_MASK)
```

- `registration_number`: the official RERA registration number as it appears on the site; whitespace is stripped before hashing
- The hashed input is the stripped registration number only
- The `key` column in `rera_projects` stores this value
- The same key is used as the S3 path prefix: `s3://bucket/{key}/filename.pdf`

---

## 7. S3 Document Storage

### Structure
```
s3://{BUCKET_NAME}/
  {project_key}/
    registration_certificate.pdf
    extension_certificate.pdf
    quarterly_report_Q1_2024.pdf
    registration_order.pdf
    ...
```

### Rules
- All documents visible on a project's detail page are downloaded and uploaded
- Document filenames are sanitized (spaces → underscores, special chars stripped)
- If multiple documents of same type exist, append `_1`, `_2` suffix
- `document_urls` JSONB column in `rera_projects` stores list of S3 URLs
- `rera_project_documents` table stores one row per document with MD5 checksum

### Upload Logic
```
1. Download PDF bytes from source URL
2. Compute MD5 of bytes
3. Check rera_project_documents table: does a row exist with same project_key + original_url?
   a. No row → upload to S3, insert into rera_project_documents
   b. Row exists, MD5 matches → skip (no change)
   c. Row exists, MD5 differs → re-upload to S3 (overwrite), update rera_project_documents
```

---

## 8. Crawl Strategy — Two-Tier

### Daily Light Crawl (listing pages only)
- Runs every night at a configured time via cron
- Hits listing/search pages only (no detail page visits)
- For each project found on listing:
  - Compute `key = generate_project_key(reg_no)`
  - Query `rera_projects` for this key
  - **Not found** → add to deep crawl queue immediately
  - **Found, `last_modified` unchanged** → skip
  - **Found, `last_modified` changed** → add to deep crawl queue
- Updates `projects.last_crawled_date` for all seen projects
- Updates `projects.is_live = true` for seen projects, `false` for ones no longer on site

### Weekly Deep Crawl (full detail + documents)
- Runs once per week (e.g., Sunday 1am) via separate cron entry
- Visits every project's detail page regardless of `last_modified`
- Re-downloads all documents, checks MD5, uploads only changed ones
- Re-validates all fields through Pydantic models
- Updates `old_updates` JSONB array with a snapshot of previous values on any field change

---

## 9. Deduplication Logic

### Project-level
```
hash_key = generate_project_key(registration_number)

if key NOT IN rera_projects:
    → full deep crawl (detail page + all documents)
    → insert new row into projects

elif projects.last_modified != newly_scraped_last_modified:
    → deep crawl (detail page + document check)
    → capture changed fields into updated_fields[]
    → push old values into old_updates JSONB array
    → update row, set is_updated=true, last_updated=now()

else:
    → skip, only update last_crawled_date
```

### Document-level
```
md5 = md5(downloaded_bytes)

if no matching row in rera_project_documents (by project_key + original_url):
    → upload to S3
    → insert into rera_project_documents

elif rera_project_documents.md5_checksum != md5:
    → re-upload to S3 (overwrite same key)
    → update rera_project_documents row (new checksum, uploaded_at)

else:
    → skip upload
```

---

## 10. Sentinel Health Check

Each site's crawler defines a **sentinel project**: a known project whose data is already stored in the DB.

At the start of every crawl run (before processing any pages):
1. Fetch the sentinel project's detail page
2. Run extraction logic on it
3. Compare key fields (project_name, promoter_name, registration_number) against DB record
4. **Pass** → proceed with crawl, log `sentinel_passed=True` in `crawl_runs`
5. **Fail** → abort site crawl, insert error into `crawl_errors` with `error_type='SENTINEL_FAILED'`, log `sentinel_passed=False`, move to next site

Sentinel projects are defined per-site in `sites_config.py`.

---

## 11. Orchestrator — `run_crawlers.py`

```
run_crawlers.py --mode daily_light   # or weekly_deep
```

Flow:
1. Parse `--mode` argument (`daily_light` | `weekly_deep`)
2. Load `sites_config.py` — list of all site configs
3. Filter to `enabled=True` sites
4. For each enabled site (in order):
   a. Create `crawl_runs` row → status='running'
   b. Call site's crawler module `run(config, run_id, mode)`
   c. On completion → update `crawl_runs` row → status='completed'
   d. On exception → update `crawl_runs` → status='failed', log to `crawl_errors`
   e. Always move to next site (one site failure never blocks others)
5. Write final JSON summary log to `logs/` directory
```

---

## 12. `sites_config.py` Structure

```python
SITES = [
    {
        "id": "kerala_rera",
        "name": "Kerala RERA",
        "state_code": "KL",
        "state": "Kerala",
        "domain": "rera.kerala.gov.in",
        "listing_url": "https://rera.kerala.gov.in/projects",
        "crawler_module": "sites.kerala_rera",
        "crawler_type": "selenium",      # all crawlers now run on SeleniumSession
        "enabled": True,
        "rate_limit_delay": (2, 4),      # (min_seconds, max_seconds) random delay
        "max_retries": 3,
        "sentinel_registration_no": "K-RERA/PRJ/ERN/001/2021",
        "config_id": 1,                  # maps to config_id in rera_projects
    },
    {
        "id": "rajasthan_rera",
        "name": "Rajasthan RERA",
        "state_code": "RJ",
        "state": "Rajasthan",
        "domain": "rera.rajasthan.gov.in",
        "listing_url": "https://rera.rajasthan.gov.in/ProjectList?status=3",
        "crawler_module": "sites.rajasthan_rera",
        "crawler_type": "api",
        "enabled": True,
        "rate_limit_delay": (1, 3),
        "max_retries": 3,
        "sentinel_registration_no": "RAJ/P/2021/001",
        "config_id": 2,
    },
    # ... remaining ~23 sites
]
```

---

## 13. Per-Site Crawler Module Interface

Every site script in `sites/` must implement exactly one function:

```python
def run(config: dict, run_id: int, mode: str) -> dict:
    """
    Args:
        config: the site's dict from sites_config.SITES
        run_id: the crawl_runs.id for this run
        mode: 'daily_light' | 'weekly_deep'
    Returns:
        dict with keys: projects_found, projects_new, projects_updated,
                        projects_skipped, documents_uploaded, error_count
    """
```

Internal structure of each site script:
```
run()
 ├── sentinel_check()          # abort if fails
 ├── fetch_listing_pages()     # paginate through all listing pages
 │    └── for each project on listing:
 │         ├── compute key
 │         ├── check DB (dedup logic)
 │         └── if needs deep crawl → deep_crawl_project()
 ├── deep_crawl_project()
 │    ├── fetch detail page
 │    ├── extract all fields
 │    ├── validate via Pydantic ProjectRecord model
 │    ├── upsert into rera_projects
 │    └── process_documents()
 └── process_documents()
      ├── find all PDF links on detail page
      ├── for each PDF: download → md5 → compare → upload if changed
      └── update document_urls in projects row
```

---

## 14. Core Module Details

### `core/config.py`
```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # PostgreSQL
    POSTGRES_HOST: str
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str

    # AWS S3
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str = "ap-south-1"
    S3_BUCKET_NAME: str

    # Crawler
    # Fixed SipHash seed used for deterministic project-key generation.
    PYTHONHASHSEED: str = "0"
    LOG_DIR: str = "logs"
    USER_AGENTS: list[str] = [...]   # pool of real browser UA strings

    class Config:
        env_file = ".env"
```

### `core/db.py`
- Provides `get_connection()` using psycopg3 connection pool
- Helpers: `upsert_project()`, `get_project_by_key()`, `insert_crawl_run()`,
  `update_crawl_run()`, `insert_crawl_error()`, `get_checkpoint()`, `set_checkpoint()`

### `core/s3.py`
- `upload_document(project_key, filename, bytes_data) -> s3_key`
- `compute_md5(bytes_data) -> str`

### `core/logger.py`
- Structured JSON logger, writes to `logs/YYYY-MM-DD_HH-MM-SS_{site_id}.jsonl`
- Log levels: DEBUG, INFO, WARNING, ERROR
- Every log line includes: `timestamp`, `site_id`, `run_id`, `level`, `message`, `extra`

### `core/models.py` (Pydantic)
```python
class ProjectRecord(BaseModel):
    key: str
    project_name: str | None
    project_type: str | None
    promoter_name: str | None
    project_registration_no: str           # required
    status_of_the_project: str | None
    project_state: str                     # required
    url: str                               # required
    state: str                             # required
    domain: str                            # required
    # ... all other fields matching DB schema
    # dates parsed to datetime objects
    # numeric fields coerced from strings

    @field_validator('project_registration_no')
    def reg_no_must_not_be_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('registration number cannot be empty')
        return v.strip()
```

### `core/crawler_base.py`
- `random_delay(min_s, max_s)` — `time.sleep(random.uniform(min_s, max_s))`
- `get_random_ua()` — picks from UA pool in settings
- `safe_get(url, retries, delay) -> httpx.Response` — GET with retry + backoff
- `safe_post(url, data, retries, delay) -> httpx.Response`
- `SeleniumSession(...)` — context manager that lazily launches headless Chromium and exposes `get()` / `fetch()` / `download()` plus a `driver()` accessor
- `page_adapter(session) -> SeleniumPageAdapter` — Page-style adapter (goto/locator/evaluate/wait_for_load_state) for use by sites that need a browser-page-like API

### `core/checkpoint.py`
- `get_checkpoint(site_id, run_type) -> dict | None`
- `set_checkpoint(site_id, run_type, page, project_key, run_id)`
- `clear_checkpoint(site_id, run_type)` — called on successful run completion

---

## 15. `.env.example`

```env
# PostgreSQL (local)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=rera_crawlers
POSTGRES_USER=
POSTGRES_PASSWORD=

# AWS S3
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=ap-south-1
S3_BUCKET_NAME=

# Crawler
PYTHONHASHSEED=0
LOG_DIR=logs
```

---

## 16. Crontab Entries

```cron
# Daily light crawl — every night at 2:00 AM
# Keep PYTHONHASHSEED fixed so project keys stay stable across runs.
0 2 * * * PYTHONHASHSEED=0 cd /path/to/rera_crawlers && /path/to/venv/bin/python run_crawlers.py --mode daily_light >> /path/to/rera_crawlers/logs/cron.log 2>&1

# Weekly deep crawl — every Sunday at 1:00 AM
0 1 * * 0 PYTHONHASHSEED=0 cd /path/to/rera_crawlers && /path/to/venv/bin/python run_crawlers.py --mode weekly_deep >> /path/to/rera_crawlers/logs/cron.log 2>&1
```

---

## 17. Logging & Observability

### JSON Log File (per run)
Written to `logs/YYYY-MM-DD_HHMMSS_{site_id}.jsonl`. One JSON object per line:
```json
{"timestamp": "2026-04-14T02:00:01Z", "site_id": "kerala_rera", "run_id": 42, "level": "INFO", "message": "Sentinel check passed", "extra": {}}
{"timestamp": "2026-04-14T02:00:15Z", "site_id": "kerala_rera", "run_id": 42, "level": "INFO", "message": "Project processed", "extra": {"key": "123456", "action": "new"}}
{"timestamp": "2026-04-14T02:15:00Z", "site_id": "kerala_rera", "run_id": 42, "level": "ERROR", "message": "Validation failed", "extra": {"url": "...", "error": "..."}}
```

### DB Tables Used for Observability
| Table | Purpose |
|-------|---------|
| `crawl_runs` | One row per site per run — status, counts, duration |
| `crawl_errors` | One row per error — type, message, URL, raw data |
| `crawl_checkpoints` | One row per site — last processed page + project |

### Useful Queries
```sql
-- Last run status per site
SELECT site_id, run_type, status, started_at, projects_new, error_count
FROM crawl_runs ORDER BY started_at DESC LIMIT 50;

-- Recent errors
SELECT site_id, error_type, error_message, occurred_at
FROM crawl_errors ORDER BY occurred_at DESC LIMIT 20;

-- Sites that failed their sentinel check
SELECT site_id, started_at FROM crawl_runs
WHERE sentinel_passed = false ORDER BY started_at DESC;
```

---

## 18. Error Types Reference

| error_type | Meaning | Action |
|-----------|---------|--------|
| `SENTINEL_FAILED` | Sentinel project data doesn't match DB | Abort site crawl |
| `SITE_STRUCTURE_CHANGED` | Key selectors return empty on known page | Abort site crawl |
| `HTTP_ERROR` | Non-200 response after all retries | Log, skip project, continue |
| `VALIDATION_FAILED` | Pydantic model rejected extracted data | Log raw data, skip project |
| `S3_UPLOAD_FAILED` | boto3 upload raised exception | Log, retry once, skip document |
| `EXTRACTION_FAILED` | Exception during field extraction | Log, skip project |
| `CHECKPOINT_RESUME` | Run resumed from checkpoint (informational) | Info log only |

---

## 19. Data Flow Diagram

```
CRON
 │
 ▼
run_crawlers.py --mode daily_light/weekly_deep
 │
 ├─► sites_config.py (filter enabled=True sites)
 │
 └─► For each site:
      │
      ├─► core/logger.py (init log file)
      ├─► core/db.py (create crawl_runs row)
      ├─► core/checkpoint.py (load resume point if any)
      │
      ├─► sites/{site_id}.py → run()
      │    │
      │    ├─► sentinel_check()
      │    │    └─► fetch detail page of known project
      │    │        compare with DB → pass/fail
      │    │
      │    ├─► fetch_listing_pages()  [daily: listing only]
      │    │    ├─► SeleniumSession.get / fetch (Type 1/2) or rendered DOM (Type 3)
      │    │    ├─► parse project cards
      │    │    ├─► dedup check (key + last_modified)
      │    │    └─► checkpoint saved after each page
      │    │
      │    └─► deep_crawl_project()  [new/updated projects]
      │         ├─► fetch detail page
      │         ├─► extract all fields
      │         ├─► core/models.py → Pydantic validation
      │         ├─► core/db.py → upsert_project()
      │         │    └─► push old values to old_updates[]
      │         └─► process_documents()
      │              ├─► find all PDF links
      │              ├─► download bytes
      │              ├─► core/s3.py → compute_md5()
      │              ├─► compare with rera_project_documents table
      │              ├─► upload if changed → s3_key
      │              └─► update rera_project_documents + document_urls
      │
      └─► core/db.py (update crawl_runs → completed/failed)
```

---

## 20. Adding a New Site — Checklist

1. **Classify the site**: Open DevTools, check if it's static/API/SPA
2. **Find a sentinel project**: Pick any known registered project on the site
3. **Add entry to `sites_config.py`**: Fill all required fields, set `enabled=False` initially
4. **Create `sites/{site_id}.py`**: Implement `run()` function following the interface
5. **Test locally**: `python -c "from sites.new_site import run; run(config, 0, 'weekly_deep')"`
6. **Verify DB row**: Check `rera_projects`, verify all fields populated correctly
7. **Verify S3**: Confirm documents uploaded under correct key
8. **Enable site**: Set `enabled=True` in `sites_config.py`

---

## 21. Known Constraints & Notes

- Keep `PYTHONHASHSEED` fixed across environments and cron runs. It is used as the SipHash seed for deterministic project-key generation.
- Selenium uses Chromium via `webdriver-manager`, which auto-downloads a matching `chromedriver` on first run; no manual install step is required.
- PostgreSQL is running locally for development; for production it will be AWS RDS PostgreSQL
- No AI/LLM calls are made at any point during crawling. All extraction is rule-based (CSS selectors, regex, JSON path).
- Documents are uploaded as-is (raw PDF bytes). No OCR or content extraction happens in the crawler (the `doc_ocr_url` field exists in the schema for a separate downstream process).
- The `data` JSONB column in `projects` stores the complete raw extracted dict before field mapping, as a safety net.
- The `old_updates` JSONB array stores snapshots: `[{"updated_at": "...", "fields": {"project_name": "old value"}}]`
- `is_live` exists in the schema, but the current crawler flow in this repo does not actively derive it from listing presence. Do not rely on it unless a downstream process populates it.

---

## 22. Per-Crawler Implementation Notes and Niches

This section captures the state-specific implementation details that matter when
debugging, extending, or deploying each crawler. The short version: every crawler
has the same `run(config, run_id, mode)` public interface, but the portal
mechanics differ heavily by state.

### Andhra Pradesh — `andhra_pradesh_rera`

- Portal type: ASP.NET WebForms, server-rendered HTML with jQuery DataTables.
- Listing source: `ApprovedProjects.aspx`, table id `ContentPlaceHolder1_gvApprovedProject`.
- Listing niche: all rows are present in the initial HTML; DataTables only decorates client-side.
- Detail routing: each row carries `onclick="openProject(enc)"`; the encrypted `enc` token builds `Project.aspx?{enc}`.
- Extracts: promoter, location, financials, building details, professional details, member details, and uploaded documents.
- Documents: uploaded-document PDF links are downloaded, checksummed, uploaded to S3, and written to `rera_project_documents`.
- Sentinel: `P03290013808`.
- Config ID: `11793`.

### Assam — `assam_rera`

- Portal type: static CodeIgniter/PHP HTML.
- Listing source: `/admincontrol/registered_projects/1`.
- Listing niche: the full project catalog is embedded in one client-side DataTables table, `#compliant_hearing`.
- Detail flow: project detail page gives core project metadata and a Form-A link; the Form-A page carries richer land, bank, cost, facility, unit, and uploaded-document data.
- Extracts: registration number, project/promoter names, location, acknowledgement ID, dates, status, bank, land, costs, units, facilities, and uploaded documents.
- Documents: certificate/Form-A/uploaded document links are normalized and S3-backed.
- Sentinel: `RERAA KM 49 OF 2024-2025`.
- Config ID: `11804`.

### Bihar — `bihar_rera`

- Portal type: ASP.NET GridView listing plus popup detail pages.
- Listing source: `RegisteredPP.aspx`.
- Listing niche: pagination is handled through Selenium because direct ViewState POST replay is unreliable across the server cluster.
- Detail routing: project-name links trigger `__doPostBack(..., PrintIndicator$N)` and open `Filanprint.aspx?id=...` popups; Selenium captures the popup URL, then detail HTML is parsed statically.
- Extracts: listing project name, registration number, promoter, address, submitted date, plus detail project type/status, dates, land/construction area, description, bank, contact, members, professional info, and building details.
- QPR niche: `QRCODE.aspx` is available for quarterly progress/report-linked data.
- Documents: selected links are downloaded/uploaded; detail work can use the shared detail worker pool.
- Sentinel: `BRERAP05734-1/994/R-766/2019`.
- Config ID: `5`.

### Chhattisgarh — `chhattisgarh_rera`

- Portal type: ASP.NET WebForms static HTML.
- Listing source: `/Approved_project_List.aspx`.
- Listing niche: one large page embeds a JavaScript JSON marker array with about 2,088 projects, including lat/lon, registration number, district, tehsil, `MyID`, and detail URL.
- Detail routing: `/Promoter_Reg_Only_View_Application_new.aspx?MyID={base64_id}`.
- Extracts: form fields, dropdowns, textareas, quarterly updates, inventory/building data, professionals, and documents.
- Timeout niche: uses separate listing/detail/document timeout profiles because listing and document responses are large/slow.
- Enabled: currently disabled in production defaults.
- Sentinel: `PCGRERA200618000247`.
- Config ID: `11805`.

### Delhi — `delhi_rera`

- Portal type: Drupal 7 static listing.
- Listing source: `/registered_promoters_list`, paginated with `?page=N`.
- Listing niche: there are no detail pages for core data; the listing row contains promoter contact, project address, registration number, validity/status, certificate link, and QPR-history link.
- Detail extras: optional director/member and QPR history pages are fetched through configured URL bases.
- Extracts: promoter name/address/email/phone, project name/location, registration number, valid-until date, construction status, certificate PDF, directors/members, and QPR history.
- Known unavailable fields: several detail-heavy fields are not published by the portal and are intentionally not fabricated.
- Sentinel: `DLRERA2023P0017`.
- Config ID: `10`.

### Goa — `goa_rera`

- Portal type: Selenium listing with CAPTCHA; detail pages can be fetched after listing.
- Listing source: `/reraApp/search` after submitting the home search form.
- CAPTCHA niche: uses rendered captcha extraction and solver attempts; if solving fails and `sentinel_project_url` is configured, dry-run/sentinel work can process that detail URL directly.
- Detail parsing: Bootstrap grid label/value rows and tables for applicant contact, inventory, architects, engineers, documents, and construction-progress panels.
- Documents: project detail document links are selected, downloaded, checksummed, and uploaded.
- Sentinel: `PRGO02231914`.
- Config ID: `11806`.

### Gujarat — `gujarat_rera`

- Portal type: Angular SPA with a usable bulk enumeration endpoint.
- Listing source: `/dashboard/get-district-wise-projectlist/0/0/all/all/all`.
- Listing niche: the public search form is difficult to automate, so the crawler warms up the SPA and calls the bulk endpoint from the browser context; this returns every registered project with `projectRegId` and `regNo`.
- Detail routing: Selenium navigates to `/#/project-preview?id={base64(projectRegId)}` and scrapes rendered Angular HTML.
- Registration-number niche: the enumeration stub is the source of truth; the detail page is not trusted to re-extract registration number.
- Document niche: View File buttons trigger `/vdms/getDocMetadata/{uid}`; the crawler triggers those handlers to collect tokens and downloads from VDMS endpoints.
- Date niche: API `/Date(ms)/` values are interpreted as IST midnight and normalized to the repo's UTC convention.
- Sentinel: `PR/GJ/SURAT/CHORYASI/Surat Municipal Corporation/RAA16644/250326/311231`.
- Config ID: `8`.

### Haryana — `haryana_rera`

- Portal type: static CodeIgniter/PHP HTML.
- Listing sources: Gurugram `/admincontrol/registered_projects/2` and Panchkula `/admincontrol/registered_projects/1`.
- Listing niche: both authorities are combined into one crawler/config ID; both pages expose full client-side DataTables.
- Detail routing: `/view_project/project_preview_open/{id}` Form REP-I pages.
- Extracts: listing stubs plus Form REP-I detail fields, registration certificate, QPR, uploaded docs, location, dates, type/status, bank/land/cost/unit/facility data when available.
- Dedupe niche: projects are de-duplicated across the two authority listings by registration number.
- Sentinel: `RERA-PKL-456-2019`.
- Config ID: `11813`.

### Himachal Pradesh — `himachal_pradesh_rera`

- Portal type: dashboard AJAX endpoints returning HTML fragments.
- Listing flow: `GetMainContent` obtains hidden filter values; `GetFilteredProjectsPV` returns a large HTML payload containing project cards plus a JavaScript `markers` JSON array.
- Listing niche: no cookies are required; endpoints accept GET/AJAX requests directly.
- Detail routing: each card has a `data-qs` token; the token fetches five detail sections: Promoter, Project, Bank, AssociatedProfessionals, and Documents.
- Extracts: marker/listing data, project/promoter/bank/professional/detail-section fields, and documents.
- Sentinel: `RERAHPSHP01190048`.
- Config ID: `11808`.

### Jharkhand — `jharkhand_rera`

- Portal type: server-rendered MVC.
- Listing source: `/Home/OnlineRegisteredProjectsList`, paginated with `?page=N`.
- Detail routing: listing rows link to `/Home/ViewProjectProfile/{id}`.
- Detail niche: some fields are wrapped in comments or inconsistent Bootstrap rows; parser searches both normal labels and comment text.
- Coordinate niche: portal labels are swapped in observed pages; parser maps "Project Longitude" to latitude and "Project Latitude" to longitude where required.
- Extracts: project type, dates, promoter/contact/promoters, bank, co-promoters, professionals, building/floor/flat details, land detail, cost, and documents.
- Document niche: collects Section 1 documents plus development-work document labels from Section 5/provided-facility rows.
- Sentinel: `JHARERA/PROJECT/146/2022`.
- Config ID: `14209`.

### Karnataka — `karnataka_rera`

- Portal type: static HTML POST flows.
- Listing source: `/projectViewDetails`, not `/viewAllProjects`.
- Listing niche: `/viewAllProjects` embeds a global catalog regardless of POST filters; using it for district traversal mislabels districts. The crawler posts each district to `/projectViewDetails`.
- Detail routing is two-step: POST `/projectViewDetails` with `appNo=<ack_no>` to get the internal numeric DB ID, then POST `/projectDetails` with `action=<numeric_id>`.
- Drillability niche: rejected applications have no `showFileApplicationPreview` link and are skipped.
- Documents: registration certificate at `/certificate?CER_NO=<registration_no>` plus `/download_jc?DOC_ID=<encoded_id>` document links; blank DOC_ID links are skipped.
- Parallelism: detail fetching can use the shared detail worker pool.
- Sentinel: `PRM/KA/RERA/1248/469/PR/050723/006033`.
- Config ID: `9`.

### Kerala — `kerala_rera`

- Portal type: static/API hybrid.
- Listing source: `/explore-projects`.
- Listing niche: paginates project cards, normally 20 cards per page.
- Detail routing: project cards link to `/projects/{id}` detail pages.
- TLS niche: the portal certificate chain can be incomplete; the Selenium-backed session ignores certificate errors.
- Documents: signed certificate, QPR link, complete project details link, and uploaded document links are normalized and uploaded.
- Parallelism: project-card detail work can use `core.details_pool`.
- Sentinel: `K-RERA/PRJ/TSR/167/2022`.
- Config ID: `1`.

### Madhya Pradesh — `madhya_pradesh_rera`

- Portal type: PHP static/AJAX site.
- Listing source: `/project-all-loop.php?show=20&pagenum=1`.
- Listing niche: one AJAX response returns the full table of about 8,255 projects.
- Registration-number niche: registration number is not present in listing, so every project must fetch the detail page even in modes that would normally skip known projects early.
- Detail routing: `/view_project_details.php?id=<base64_id>`.
- Extracts: project info, location, bank, promoter, consultants, unit inventory, project documents, QPR, and photo gallery.
- Enabled: currently disabled in production defaults.
- Sentinel: `P-BPL-24-4939`.
- Config ID: `12898`.

### Maharashtra — `maharashtra_rera`

- Portal type: static listing plus CAPTCHA-gated Angular detail SPA.
- Listing source: `/projects-search-result`.
- Scale niche: roughly 47,000+ projects across about 4,776 pages of 10 cards.
- Pagination niche: `?page=0` and `?page=1` both return the first page; crawler starts with the clean URL, then uses adjusted page numbers.
- Detail routing: detail domain is `maharerait.maharashtra.gov.in`; pages are Angular SPA screens gated by canvas CAPTCHA.
- CAPTCHA niche: uses `core.captcha_solver.solve_captcha_from_page()` against rendered canvas, with canvas-text interception as fallback.
- Detail-session niche: each project gets its own Selenium session; no token reuse is required.
- Tuning niche: aggressive page/CAPTCHA/data-label timeout constants avoid long stalls at scale.
- Sentinel: `PP1190002502346`.
- Config ID: `7`.

### Odisha — `odisha_rera`

- Portal type: Angular SPA.
- Listing source: `/projects/project-list`.
- Listing niche: cards are processed sequentially on each listing page; detail is reached by clicking `View Details`, then returning to the listing.
- Detail sections: Project Overview, Promoter Details, and Documents tabs.
- Extracts: registration date, full location, building type, professionals, bank accounts, financial details, promoter company/address/entity/directors/email/GST, and uploaded PDFs.
- Document niche: DMS PDF handling uses `reraapps.odisha.gov.in/dms` endpoints including `fileDecryptHandlerForPdfPublic`.
- Sentinel: `RP/11/2026/01471`.
- Config ID: `3`.

### Puducherry / Pondicherry — `pondicherry_rera`

- Portal type: Selenium/static legacy SSL portal.
- Listing source: `/reraAppOffice/viewDefaulterProjects`.
- Listing niche: one slow ~1.3 MB page exposes about 363 project cards; timeouts are intentionally generous.
- Detail routing: `/reraAppOffice/viewProjectDetailPage?projectID=N`.
- Extracts: listing card name/address/reg no/promoter/type/status, plus detail promoter, registration date, address, project type/status/description/cost, dates, district/taluk/village, applicant contact, and documents.
- Document niche: raw Puducherry filenames are normalized through a small canonical label map, e.g. Form B and registration certificate.
- Sentinel: `PRPY133450`.
- Config ID: `4`.

### Punjab — `punjab_rera`

- Portal type: Selenium with AJAX/DataTables.
- Listing source: `/reraindex/publicview/projectinfo`.
- CAPTCHA niche: the page requires a CAPTCHA input client-side, but the server does not validate the image text; the crawler fills a dummy six-character string.
- Listing niche: after Search, `#viewProjectPVList` contains all rows in the DOM; pagination is client-side.
- Detail routing: hidden `hdnProjectID`, `hdnPromoterID`, and `hdnPromoterType` values feed the modal detail request; parser extracts labeled table-cell pairs from the Bootstrap modal.
- Documents: modal/detail document links are selected and uploaded.
- Sentinel: `PBRERA-LDH44-PR0597`.
- Config ID: `6`.

### Rajasthan — `rajasthan_rera`

- Portal type: pure Selenium Angular SPA.
- Listing source: `/ProjectList?status=3`.
- Listing niche: all data is obtained by rendering the public site; the crawler does not use private/direct REST APIs.
- Detail routing: Selenium navigates to each detail page and waits for Angular-rendered HTML before parsing with BeautifulSoup.
- Date niche: `/Date(ms)/` timestamps are interpreted in IST and normalized to the repo's UTC output convention.
- Documents: all rendered links pointing to PDFs/downloadable files are collected and passed through document policy.
- Sentinel: `RAJ/P/2024/3058`.
- Config ID: `2`.

### Tamil Nadu — `tamil_nadu_rera`

- Portal type: httpx listings plus Selenium JS detail pages.
- Listing sources: registered-building, normal layout, and regularisation layout CMS/index flows.
- Listing niche: each master page has a Laravel CSRF `_token` and year selector; the crawler posts `_token + year` back to get the server-rendered table for that year.
- Row data: registration number, promoter, project name/description, expiry date, promoter UUID, project UUID, lat/lng, and Form-C URL.
- Detail routing: `public-view1` and `public-view2` are JS-rendered and use Selenium.
- Documents: Form-C QR-code PDF plus `/public/storage/upload/*.pdf` links.
- Sentinel: `TNRERA/29/LO/4544/2025`.
- Config ID: `14374`.

### Telangana — `telangana_rera`

- Portal type: ASP.NET search form with CAPTCHA.
- Listing source: `/SearchList/Search`.
- CAPTCHA niche: uses `core.captcha_solver.captcha_to_text`; several CAPTCHA image selectors are supported.
- Pagination niche: server-rendered result table paginates through ASP.NET `__doPostBack`.
- Detail routing: listing rows expose encrypted `q` PrintPreview URL parameters and base64 `data_cert` payloads encoding ProjectID/AppID/UserID.
- Registration-number niche: stable registration number is derived from detail page, falling back to `TG-{AppID}`.
- Documents: selected certificates/preview documents use character-code constants for certificate and preview downloads.
- Enabled: currently disabled in production defaults.
- Sentinel: not configured.
- Config ID: `11811`.

### Tripura — `tripura_rera`

- Portal type: Java MVC static/server-rendered pages.
- Primary listing: POST `/search` with `searchTxt=''` and `startFrom=N`, five projects per page.
- Supplementary listing: GET `/viewApprovedProjects` includes older approved/completed projects not shown by `/search`.
- Detail routing niche: supplementary cards often lack detail links, so `PRTR{MM}{YY}{NNNN}` maps to `projectID=int(last_4_digits)`.
- Session niche: uses a persistent client/session so the homepage `JSESSIONID` warm-up cookie is reused.
- Extracts: listing status/type/location/promoter, detail labels, members/directors, contact fallbacks, images, land/construction area, and documents.
- Document niche: empty `DOC_ID` links and site-wide static nav documents are skipped.
- Sentinel: `PRTR03240386`.
- Config ID: `11807`.

### Uttarakhand — `uttarakhand_rera`

- Portal type: Java Spring MVC / Tiles static listing.
- Listing source: `/viewRegisteredProjects`.
- Listing niche: all registered projects are returned on one page; public pagination is disabled even though JS remnants exist.
- TLS niche: portal requires unsafe legacy SSL renegotiation; shared legacy SSL context support is used.
- Detail routing: `viewProjectDetailPage?projectID=N` redirects to a session-encrypted URL; redirects are followed with the same client.
- Data-format niches: email obfuscation (`[at]`, `[dot]`) is decoded; Java `Date.toString()` values and `DD-MM-YYYY` end dates are normalized.
- Documents/images: `download?DOC_ID=N` links and `reraimage?IMG_ID=N` project images.
- Sentinel: `UKREP11250000693`.
- Config ID: `11814`.

### Uttar Pradesh — `uttar_pradesh_rera`

- Portal type: ASP.NET WebForms listing plus Selenium detail postbacks.
- Listing source: `/frm_allprojectdistrictwise.aspx?districtname={district}` for all 75 districts.
- Listing niche: each district page contains all rows in initial HTML; client-side DataTables handles display only.
- Detail routing: `View Detail` buttons use `__doPostBack`; Selenium clicks and captures detail HTML. After the first resolved detail URL for a district, predictable URLs can be fetched directly to reduce Selenium overhead.
- Resume niche: checkpoints track district/page position and last project; stale checkpoint keys are detected and cleared.
- Documents: all PDF anchors on detail page are collected, downloaded, and uploaded.
- Sentinel: `UPRERAPRJ6734` in Gautam Buddha Nagar.
- Config ID: `11816`.

### West Bengal — `wb_rera`

- Portal type: Selenium listing plus static detail pages.
- Listing source: `/district_project.php?dcode=0`.
- Listing niche: Python `httpx` is blocked/reset by the site, so Selenium loads the page and extracts all rows through the DataTables JavaScript API, which holds the complete dataset in memory.
- Detail routing: `/project_details.php?procode=N`.
- Detail sections: project status banner, highlights, residential details, facilities, consultants, promoter details, promoter/official tables, promoter documents, sanctioned plan, legal title deed, other documents, and authenticated legal documents.
- Document niche: links often point to `doc.repository.semtwb.in`; document type is inferred from section and row. Promoter Document row 9 maps to `Plan of Development {n}`, and other project document subtitles become typed names such as `Sanction Plan {n}`.
- Sentinel: `WBRERA/P/ALI/2023/000353`.
- Config ID: `11815`.
