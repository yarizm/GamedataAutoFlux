"""Typed validation for settings.yaml.

The runtime still uses the dict-based config helpers in ``src.core.config``.
This module adds a typed validation layer so startup and diagnostics can report
misconfigured values before they become runtime failures.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class _FlexibleModel(BaseModel):
    model_config = ConfigDict(extra="allow")


class AppConfig(_FlexibleModel):
    name: str = "GamedataAutoFlux"
    version: str = "0.1.0"
    debug: bool = False


class ServerConfig(_FlexibleModel):
    host: str = "127.0.0.1"
    port: int = Field(default=8000, ge=1, le=65535)
    api_key: str = ""
    cors_origins: list[str] = Field(
        default_factory=lambda: ["http://localhost:8000", "http://127.0.0.1:8000"]
    )


class DatabaseConfig(_FlexibleModel):
    provider: str = "sqlalchemy"
    sqlalchemy_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/autoflux"


class StorageConfig(_FlexibleModel):
    reports_dir: str = "data/excel_reports"


class ProviderConfig(_FlexibleModel):
    api_key: str | None = None
    base_url: str | None = None
    model: str | None = None
    temperature: float | None = Field(default=None, ge=0, le=2)
    max_tokens: int | None = Field(default=None, ge=1)
    timeout: float | None = Field(default=None, ge=0)
    retry_count: int | None = Field(default=None, ge=0)
    retry_delay: float | None = Field(default=None, ge=0)
    fallback_to_stub: bool | None = None
    max_input_chars: int | None = Field(default=None, ge=1)


class LLMConfig(_FlexibleModel):
    provider: str = "stub"


class AgentConfig(_FlexibleModel):
    enabled: bool = True
    max_iterations: int = Field(default=10, ge=1)
    session_timeout_minutes: int = Field(default=60, ge=1)
    system_prompt: str = ""


class SteamDBConfig(_FlexibleModel):
    enabled: bool = False
    session_mode: str | None = Field(default=None, pattern="^(api_only|local_profile|managed_state)$")
    cdp_enabled: bool = False
    cdp_port: int = Field(default=9222, ge=1, le=65535)
    cdp_profile_dir: str = "data/steamdb_profile"
    request_delay: float = Field(default=8.0, ge=0)
    request_jitter: float = Field(default=4.0, ge=0)
    page_delay: float = Field(default=5.0, ge=0)
    max_games_per_session: int = Field(default=10, ge=1)
    headless: bool = True
    timeout: int = Field(default=30000, ge=1)
    cookie: str | None = None
    headers: dict[str, Any] = Field(default_factory=dict)


class SteamConfig(_FlexibleModel):
    api_key: str | None = None
    request_delay: float = Field(default=1.5, ge=0)
    batch_concurrency: int = Field(default=1, ge=1)
    collect_timeout: float = Field(default=0, ge=0)
    collect_retries: int = Field(default=0, ge=0)
    collect_retry_delay: float = Field(default=1.0, ge=0)
    max_reviews: int = Field(default=200, ge=0)
    review_language: str = "all"
    review_trend_mode: str = "summary"
    review_trend_days: int = Field(default=90, ge=1)
    review_summary_concurrency: int = Field(default=4, ge=1)
    max_review_trend_reviews: int = Field(default=10000, ge=1)
    steamdb: SteamDBConfig = Field(default_factory=SteamDBConfig)


class RequestCollectorConfig(_FlexibleModel):
    request_delay: float = Field(default=1.5, ge=0)
    batch_concurrency: int = Field(default=1, ge=1)
    collect_timeout: float = Field(default=0, ge=0)
    collect_retries: int = Field(default=0, ge=0)
    collect_retry_delay: float = Field(default=1.0, ge=0)
    session_mode: str | None = Field(default=None, pattern="^(api_only|local_profile|managed_state)$")
    timeout: float | None = Field(default=None, ge=0)
    request_retries: int | None = Field(default=None, ge=0)
    headless: bool | None = None
    playwright_timeout: int | None = Field(default=None, ge=1)


class OfficialSiteConfig(RequestCollectorConfig):
    max_pages: int = Field(default=80, ge=1)
    max_depth: int = Field(default=2, ge=0)


class MonitorConfig(_FlexibleModel):
    default_days: int = Field(default=30, ge=1)
    timezone: str = "Asia/Shanghai"
    request_delay: float = Field(default=1.5, ge=0)
    batch_concurrency: int = Field(default=1, ge=1)
    collect_timeout: float = Field(default=0, ge=0)
    collect_retries: int = Field(default=0, ge=0)
    collect_retry_delay: float = Field(default=1.0, ge=0)
    metric_concurrency: int = Field(default=4, ge=1)


class SmartCollectorLLMConfig(_FlexibleModel):
    provider: str = ""


class SmartCollectorConfig(_FlexibleModel):
    enabled: bool = True
    max_html_tokens: int = Field(default=4000, ge=100, le=50000)
    confidence_threshold: float = Field(default=0.5, ge=0, le=1)
    llm: SmartCollectorLLMConfig = Field(default_factory=SmartCollectorLLMConfig)


class GTrendsConfig(_FlexibleModel):
    geo: str = ""
    hl: str = "zh-CN"
    timeframe: str = "today 12-m"
    proxies: list[str] = Field(default_factory=list)
    retries: int = Field(default=2, ge=0)
    backoff_factor: float = Field(default=0.5, ge=0)
    collect_timeout: float = Field(default=0, ge=0)
    collect_retries: int = Field(default=0, ge=0)
    collect_retry_delay: float = Field(default=1.0, ge=0)


class SchedulerPersistenceConfig(_FlexibleModel):
    db_name: str = "scheduler.db"
    json_dir: str = "scheduler_tasks"


class SchedulerConfig(_FlexibleModel):
    max_concurrent_tasks: int = Field(default=5, ge=1)
    default_retry_count: int = Field(default=3, ge=0)
    default_retry_delay: float = Field(default=60, ge=0)
    persistence: SchedulerPersistenceConfig = Field(default_factory=SchedulerPersistenceConfig)
    cron_jobs: list[Any] = Field(default_factory=list)


class CollectorConfig(_FlexibleModel):
    request_timeout: float = Field(default=30, ge=0)
    request_delay: float = Field(default=2, ge=0)
    batch_concurrency: int = Field(default=1, ge=1)
    collect_timeout: float = Field(default=0, ge=0)
    collect_retries: int = Field(default=0, ge=0)
    collect_retry_delay: float = Field(default=1.0, ge=0)
    proxy: str | None = None
    user_agent: str = ""


class LoggingConfig(_FlexibleModel):
    level: str = "INFO"
    log_dir: str = "logs"
    rotation: str = "10 MB"
    retention: str = "30 days"


class AlertConfig(_FlexibleModel):
    enabled: bool = False
    type: str = "dingtalk"
    webhook_url: str = ""


class SettingsModel(_FlexibleModel):
    app: AppConfig = Field(default_factory=AppConfig)
    alerts: AlertConfig = Field(default_factory=AlertConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    agent: AgentConfig = Field(default_factory=AgentConfig)
    steam: SteamConfig = Field(default_factory=SteamConfig)
    steam_discussions: RequestCollectorConfig = Field(default_factory=RequestCollectorConfig)
    taptap: RequestCollectorConfig = Field(default_factory=RequestCollectorConfig)
    qimai: RequestCollectorConfig = Field(default_factory=RequestCollectorConfig)
    official_site: OfficialSiteConfig = Field(default_factory=OfficialSiteConfig)
    monitor: MonitorConfig = Field(default_factory=MonitorConfig)
    smart_collector: SmartCollectorConfig = Field(default_factory=SmartCollectorConfig)
    gtrends: GTrendsConfig = Field(default_factory=GTrendsConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    collector: CollectorConfig = Field(default_factory=CollectorConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


def validate_settings_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate settings data and return an API-friendly summary."""
    try:
        model = SettingsModel.model_validate(payload)
    except ValidationError as exc:
        return {
            "valid": False,
            "issues": [_format_validation_error(error) for error in exc.errors()],
            "normalized": {},
        }

    return {
        "valid": True,
        "issues": [],
        "normalized": model.model_dump(mode="json"),
    }


def _format_validation_error(error: dict[str, Any]) -> dict[str, Any]:
    path = ".".join(str(part) for part in error.get("loc", ()))
    return {
        "path": path or "<root>",
        "message": error.get("msg", "Invalid value"),
        "type": error.get("type", "value_error"),
    }
