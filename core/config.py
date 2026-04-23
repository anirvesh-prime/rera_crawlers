from __future__ import annotations

from urllib.parse import quote_plus

from pydantic_settings import BaseSettings, SettingsConfigDict


USER_AGENT_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # PostgreSQL — required, set in .env
    POSTGRES_HOST: str
    POSTGRES_PORT: int
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str = ""  # may be blank for local dev

    # AWS S3 — required, set in .env
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str
    S3_BUCKET_NAME: str
    # Public CDN base URL for document links stored in the DB.
    # e.g. https://docs.primetenders.com  — no trailing slash.
    # get_s3_url() will produce: {CDN_BASE_URL}/{s3_key}
    CDN_BASE_URL: str = ""

    # Crawler — DRY_RUN_S3 required (set in .env); others optional with safe defaults
    PYTHONHASHSEED: str = "0"
    LOG_DIR: str = "logs"
    DRY_RUN_S3: bool
    # Cap total projects processed per run. 0 = unlimited.
    CRAWL_ITEM_LIMIT: int = 0
    # Set to false to skip detail-page fetches entirely.
    SCRAPE_DETAILS: bool = True
    # Cap total pages fetched per crawler. None = unlimited.
    MAX_PAGES: int | None = None

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
        return USER_AGENT_POOL


settings = Settings()
