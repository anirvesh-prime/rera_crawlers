# rera_crawlers

Production crawler framework for Indian RERA portals. It extracts project metadata from state RERA sites, stores normalized project records in PostgreSQL, uploads documents to S3, and exposes a Flask monitoring portal for operators.

## Repository layout

```text
core/                         shared crawler infrastructure
  config.py                   .env-backed runtime settings
  crawler_base.py             Selenium/http helpers, retries, project keys, delays
  db.py                       PostgreSQL schema, upserts, comparisons, logs
  details_pool.py             per-crawler threaded detail fetch helper
  document_policy.py          document selection and upload policy helpers
  logger.py                   structured DB/local logging
  project_normalizer.py       payload normalization before DB write
  project_schema.py           required/project JSONB field definitions
  s3.py                       S3 key generation and upload helpers
sites/                        one crawler module per RERA state portal
sites_config.py               site catalog, enabled flags, sentinels, config IDs
run_crawlers.py               orchestrator CLI for production/test runs
dashboard.py                  Flask operator portal and tester UI
setup_cron.sh                 installs daily/weekly cron jobs
setup_dashboard.sh            installs dashboard as a systemd service
dry_run_compare.py            sample-output regression comparison harness
state_projects_sample/        expected sample project payloads
dry_run_outputs/              latest dry-run comparison outputs
tests/                        pytest regression and targeted crawler tests
SPEC.md                       deeper technical specification
```

## Runtime dependencies

The project is Python-based and expects PostgreSQL plus AWS S3 credentials. Browser-backed crawlers use Selenium and `webdriver-manager` for Chromium/ChromeDriver.

Install locally:

```bash
python3 -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r requirements.txt
```

Required Python packages are pinned in `requirements.txt` and include `httpx`, `beautifulsoup4`, `lxml`, `psycopg[binary]`, `boto3`, `pydantic-settings`, `flask`, `gunicorn`, `selenium`, and `webdriver-manager`.

## Configuration

Configuration is loaded from `.env` by `core/config.py`. Required values:

```dotenv
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=rera
POSTGRES_USER=rera
POSTGRES_PASSWORD=

AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=ap-south-1
S3_BUCKET_NAME=docs.primetenders.com
DRY_RUN_S3=false
```

Useful optional settings:

```dotenv
LOG_DIR=logs
LOG_LOCAL=false
DASHBOARD_LOCAL_STATE=true
TEST_MODE=false
TEST_MODE_LOG_TO_DB=false
CRAWLER_TESTER=false
CRAWL_ITEM_LIMIT=0
CRAWL_DELAY_SCALE=1.0
TARGET_REG_NO=
SKIP_DOCUMENTS=false
SCRAPE_DETAILS=true
MAX_PAGES=
MAX_PARALLEL_CRAWLERS=1
HTTP_MAX_CONNECTIONS=100
HTTP_MAX_KEEPALIVE_CONNECTIONS=20
DETAIL_WORKERS=6
```

`S3_BUCKET_NAME` is also used as the public document domain by `core/s3.py`, so uploaded document URLs are rendered as `https://{S3_BUCKET_NAME}/{project_key}/{filename}`.

## Sites

`sites_config.py` is the source of truth for all configured portals. Each entry contains the site ID, display name, state/domain metadata, listing URL, crawler module, crawler type, enabled flag, rate limit, sentinel registration number, and production `config_id`.

Configured site IDs:

```text
andhra_pradesh_rera, assam_rera, bihar_rera, chhattisgarh_rera,
delhi_rera, goa_rera, gujarat_rera, haryana_rera,
himachal_pradesh_rera, jharkhand_rera, karnataka_rera, kerala_rera,
madhya_pradesh_rera, maharashtra_rera, odisha_rera, pondicherry_rera,
punjab_rera, rajasthan_rera, tamil_nadu_rera, telangana_rera,
tripura_rera, uttarakhand_rera, uttar_pradesh_rera, wb_rera
```

The default production run includes only entries with `enabled: True`. Explicit `--site` selection can run disabled sites for testing.

## Running crawlers

Production runs should use Docker so Selenium, ChromeDriver, and Chrome all
live inside one disposable container per crawler invocation:

```bash
docker build -t rera-crawlers:latest .
python3 scripts/crawler_container.py --mode weekly_deep
python3 scripts/crawler_container.py --mode daily_light --site kerala_rera
```

The wrapper passes all unknown arguments through to `run_crawlers.py`, mounts
`logs/` into the container, loads `.env`, uses host networking by default, and
labels running containers with `com.primenumbers.rera.role=crawler`.

Useful Docker commands:

```bash
docker ps --filter label=com.primenumbers.rera.role=crawler
docker logs -f <container_id>
docker stop <container_id>
docker kill <container_id>
```

Local development can still run the Python entrypoint directly:

Run all enabled sites:

```bash
./venv/bin/python run_crawlers.py
```

Run selected sites:

```bash
./venv/bin/python run_crawlers.py --site kerala_rera --site bihar_rera
./venv/bin/python run_crawlers.py --site kerala_rera,bihar_rera
```

Run modes:

```bash
./venv/bin/python run_crawlers.py --mode daily_light
./venv/bin/python run_crawlers.py --mode weekly_deep
./venv/bin/python run_crawlers.py --mode full
./venv/bin/python run_crawlers.py --mode single
./venv/bin/python run_crawlers.py --mode incremental
./venv/bin/python run_crawlers.py --mode listing
```

Production defaults to `weekly_deep`. Parallel execution is enabled when more than one site is selected and is capped by `MAX_PARALLEL_CRAWLERS`.

Limit or remove item caps:

```bash
./venv/bin/python run_crawlers.py --item-limit 10 --site kerala_rera
./venv/bin/python run_crawlers.py --no-item-limit --site kerala_rera
```

Speed up or disable crawler throttling for a run:

```bash
./venv/bin/python run_crawlers.py --delay-scale 0.5
./venv/bin/python run_crawlers.py --delay-scale 0 --site kerala_rera
```

Target a specific registration number:

```bash
./venv/bin/python run_crawlers.py \
  --site karnataka_rera \
  --target-reg-no "PRM/KA/RERA/1251/446/PR/181122/005482"
```

Skip document uploads/downloads while still scraping project records:

```bash
./venv/bin/python run_crawlers.py --skip-documents --site kerala_rera
```

Dry-run modes:

```bash
./venv/bin/python run_crawlers.py --test --site kerala_rera --item-limit 3
./venv/bin/python run_crawlers.py --test-logs --site kerala_rera --item-limit 3
./venv/bin/python run_crawlers.py --tester --site kerala_rera --item-limit 3
```

`--test` skips S3 and DB writes. `--test-logs` still writes log tables so the run appears on the dashboard. `--tester` is for the dashboard test modal: it runs one site, writes no DB/S3 data, and streams verbose extracted-field logs to stdout.

## Database

`core/db.py` creates and migrates the required tables automatically on first connection. Main tables:

```text
rera_projects             normalized project records, keyed by deterministic project key
rera_project_documents    uploaded document metadata and checksum records
crawl_runs                one row per site run, with counts and final status
crawl_logs                structured per-step logs for DB-side diagnostics
crawl_errors              committed crawler errors for crash-safe diagnostics
crawl_checkpoints         resume checkpoints by site_id and run_type
crawl_document_events     document download/upload event stream
```

The dashboard uses direct probes: Docker labels/logs for live runs and local files under `logs/dashboard/` plus per-site JSONL logs for latest metrics, errors, sentinel results, timings, and counts.

## Dashboard portal

Run locally:

```bash
./venv/bin/python dashboard.py
./venv/bin/python dashboard.py --host 0.0.0.0 --port 8080
```

Open `http://127.0.0.1:8080` locally, or tunnel from a server:

```bash
ssh -L 8080:localhost:8080 user@server
```

Portal features:

```text
Most recent orchestrator summary
Per-state latest run table
Sentinel health table
Failure diagnostics with traceback snippets
Single-site Test Crawler modal
Live tester log streaming
Stop Crawlers modal using Docker stop/kill
Docker container probe for true running/stopped state
```

The portal probes Docker for live containers labeled `com.primenumbers.rera.role=crawler`. If a command has explicit `--site` arguments, those sites are marked running. If there is no explicit site selection, all enabled sites are marked running. A stale local state file with no matching container is shown as `stopped?`, not as live.

The Stop Crawlers modal only targets containers returned by the same Docker probe. Use graceful stop first; force kill sends `docker kill`.

## Dashboard deployment

On a Debian/Ubuntu server with systemd and sudo:

```bash
bash setup_dashboard.sh
```

Use a custom port or worker count:

```bash
PORT=9090 WORKERS=2 bash setup_dashboard.sh
```

The script:

```text
installs python3-venv/python3-pip when needed
creates ./venv
installs requirements.txt
writes /etc/systemd/system/rera-dashboard.service
runs gunicorn dashboard:app on 0.0.0.0:${PORT}
enables and restarts the service
opens the ufw port when ufw is available
prints the public URL and useful systemctl commands
```

Useful service commands:

```bash
sudo systemctl status rera-dashboard
sudo systemctl restart rera-dashboard
sudo systemctl stop rera-dashboard
sudo journalctl -u rera-dashboard -f
```

Remove the service:

```bash
bash setup_dashboard.sh --remove
```

If the server is behind a cloud firewall or security group, open the dashboard port there as well. For private access, bind or firewall the port to localhost and use SSH tunneling.

## Cron deployment

Install production cron jobs:

```bash
bash setup_cron.sh
```

Default schedule:

```text
daily_light   every day at 02:00
weekly_deep   every Sunday at 03:00
```

Customize schedule:

```bash
DAILY_HOUR=6 DAILY_MIN=15 WEEKLY_HOUR=4 WEEKLY_DOW=0 bash setup_cron.sh
```

Cron logs:

```text
logs/cron_daily.log
logs/cron_weekly.log
```

Remove cron jobs:

```bash
bash setup_cron.sh --remove
```

## Testing and regression checks

Run the pytest suite:

```bash
./venv/bin/python -m pytest
```

Run a targeted test:

```bash
./venv/bin/python -m pytest tests/test_run_crawlers_cli.py
```

Run dry-run sample comparisons:

```bash
./venv/bin/python dry_run_compare.py
./venv/bin/python dry_run_compare.py --site kerala_rera
```

Useful development checks:

```bash
./venv/bin/python -m py_compile dashboard.py run_crawlers.py core/crawler_base.py scripts/crawler_container.py
bash -n setup_dashboard.sh setup_cron.sh
```

## Operational notes

Sentinel checks verify that each crawler can still extract a known project with enough field coverage. Missing sentinel fields are shown in the dashboard.

Checkpoints are stored by `(site_id, run_type)` so a daily run does not overwrite a weekly run checkpoint.

Project keys are deterministic. `run_crawlers.py` re-execs itself with `PYTHONHASHSEED=0` so child processes produce stable keys.

Detail fetching may run in threads inside a site crawler. DB access is serialized inside `core/db.py` because psycopg connections are process-local and not safe for concurrent statements.

Avoid running multiple production orchestrators at the same time unless intentionally testing concurrency. The dashboard container probe will show all live crawler containers and can stop them.
