"""类型化应用配置（修复清单 P3：统一配置模型）。

`AppSettings` 用 Pydantic 建模 settings.yaml 中被核心组件消费的段落，
消除散落的 `get_config("a.b.c")` 字符串路径。

设计约束：
- `get_app_settings()` 经 `config.get` 逐路径取值后构建——保持既有测试
  monkeypatch `config.get` 的覆盖接缝，不直接吃原始 dict；
- 不做缓存：构建成本微秒级，保证任何 config 变更/覆盖立即生效；
- 字段默认值 = 代码原默认值，行为零变化；
- 覆盖范围：核心组件（scheduler/pipeline/database/server/agent 基础段）。
  collectors/base 的多级回退（含 per-collector 动态键）与插件自有段
  （official_site.recipes 等）不适合静态模型，继续走 get_config。
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PersistenceSettings(BaseModel):
    db_name: str = "scheduler.db"
    json_dir: str = "scheduler_tasks"


class SchedulerSettings(BaseModel):
    max_concurrent_tasks: int = 5
    default_retry_count: int = 3
    execution_backend: str = "in_process"
    persistence: PersistenceSettings = Field(default_factory=PersistenceSettings)


class PipelineSettings(BaseModel):
    use_dag_execution: bool = True
    legacy_fallback: bool = False


class DatabaseSettings(BaseModel):
    provider: str = "sqlalchemy"
    sqlalchemy_url: str | None = None


class ServerSettings(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8000
    api_key: str = ""
    cors_origins: list[str] = Field(
        default_factory=lambda: ["http://localhost:8000", "http://127.0.0.1:8000"]
    )


class AgentSettings(BaseModel):
    enabled: bool = True
    session_timeout_minutes: int = 60


class AppSettings(BaseModel):
    scheduler: SchedulerSettings = Field(default_factory=SchedulerSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    server: ServerSettings = Field(default_factory=ServerSettings)
    agent: AgentSettings = Field(default_factory=AgentSettings)
    debug: bool = False
    app: dict[str, Any] = Field(default_factory=dict)


def _c(path: str, default: Any) -> Any:
    from src.core.config import get as _get

    return _get(path, default)


def build_app_settings() -> AppSettings:
    """经 config.get 逐路径构建类型化配置（保留测试覆盖接缝）。"""
    return AppSettings.model_validate(
        {
            "scheduler": {
                "max_concurrent_tasks": _c("scheduler.max_concurrent_tasks", 5),
                "default_retry_count": _c("scheduler.default_retry_count", 3),
                "execution_backend": _c("scheduler.execution_backend", "in_process"),
                "persistence": {
                    "db_name": _c("scheduler.persistence.db_name", "scheduler.db"),
                    "json_dir": _c("scheduler.persistence.json_dir", "scheduler_tasks"),
                },
            },
            "pipeline": {
                "use_dag_execution": _c("pipeline.use_dag_execution", True),
                "legacy_fallback": _c("pipeline.legacy_fallback", False),
            },
            "database": {
                "provider": _c("database.provider", "sqlalchemy"),
                "sqlalchemy_url": _c("database.sqlalchemy_url", None),
            },
            "server": {
                "host": _c("server.host", "127.0.0.1"),
                "port": _c("server.port", 8000),
                "api_key": _c("server.api_key", ""),
                "cors_origins": _c(
                    "server.cors_origins",
                    ["http://localhost:8000", "http://127.0.0.1:8000"],
                ),
            },
            "agent": {
                "enabled": _c("agent.enabled", True),
                "session_timeout_minutes": _c("agent.session_timeout_minutes", 60),
            },
            "debug": _c("debug", False),
            "app": {"debug": _c("app.debug", False)},
        }
    )


def get_app_settings() -> AppSettings:
    """每次现取现建：保证 config 覆盖（含测试 monkeypatch）立即生效。"""
    return build_app_settings()
