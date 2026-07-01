from __future__ import annotations

from typing import ClassVar
from urllib.parse import quote_plus

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    POSTGRES_HOST: str
    POSTGRES_PORT: int
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str = ""

    # AWS S3
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str
    S3_BUCKET_NAME: str

    # Crawler
    LOG_DIR: str = "logs"
    LOG_LOCAL: bool = False          # set True to also write .jsonl files locally
    DASHBOARD_LOCAL_STATE: bool = True  # dashboard reads local probes/state instead of DB
    DRY_RUN_S3: bool
    TEST_MODE: bool = False          # --test flag: skip S3 uploads and all DB writes
    # --test-logs flag: in TEST_MODE, still write the *log* tables (crawl_runs,
    # crawl_logs, crawl_document_events, crawl_errors) so the run shows up on
    # the dashboard and per-step logs are queryable.  Data writes
    # (rera_projects, rera_project_documents, checkpoints) and S3 uploads
    # remain skipped.  No effect when TEST_MODE is False.
    TEST_MODE_LOG_TO_DB: bool = False
    # --tester flag: implies --test (no S3 / no DB writes of any kind) and
    # routes verbose, per-field logs to stdout so the dashboard tester can
    # show what the crawler is extracting in real time.  Never set in cron.
    CRAWLER_TESTER: bool = False
    CRAWL_ITEM_LIMIT: int = 0        # 0 = unlimited
    CRAWL_DELAY_SCALE: float = 1.0   # scales random crawler throttling delays
    # --target-reg-no flag: when non-empty, crawlers that support it filter
    # listing rows down to the single project whose registration number
    # matches (case-insensitive) and skip the sentinel/health check for the
    # run.  Intended for targeted debugging of a specific project.
    TARGET_REG_NO: str = ""
    # --skip-documents flag: crawlers that support it still scrape/upsert project
    # records but skip document download/upload work for the run.
    SKIP_DOCUMENTS: bool = False
    SCRAPE_DETAILS: bool = True      # set False to skip detail-page fetches
    MAX_PAGES: int | None = None     # None = unlimited
    MAX_PARALLEL_CRAWLERS: int = 1   # hard cap on concurrent worker processes
    CRAWLER_AUTO_REPAIR: bool = False
    CRAWLER_AUTO_REPAIR_CODEX_BIN: str = "codex"
    CRAWLER_AUTO_REPAIR_TEST_ITEM_LIMIT: int = 3
    CRAWLER_AUTO_REPAIR_TEST_TIMEOUT_S: int = 900
    CRAWLER_AUTO_REPAIR_CODEX_TIMEOUT_S: int = 3600
    HTTP_MAX_CONNECTIONS: int = 100
    HTTP_MAX_KEEPALIVE_CONNECTIONS: int = 20
    DETAIL_WORKERS: int = 6          # threads per crawler for parallel detail fetching

    USER_AGENT_POOL: ClassVar[list[str]] = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    ]

    @property
    def postgres_dsn(self) -> str:
        if self.POSTGRES_PASSWORD:
            return (
                f"postgresql://{quote_plus(self.POSTGRES_USER)}:{quote_plus(self.POSTGRES_PASSWORD)}"
                f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
            )
        return (
            f"postgresql://{quote_plus(self.POSTGRES_USER)}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def user_agents(self) -> list[str]:
        return self.USER_AGENT_POOL


settings = Settings()
