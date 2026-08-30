"""类型化应用配置测试（P3：统一配置模型）。

验收：
- 默认值与代码原默认一致（行为零变化）；
- `config.get` 的 monkeypatch 覆盖接缝继续生效（builder 经 get 取值）；
- 非法值被 Pydantic 拒绝（类型收窄）。
"""

import pytest

import src.core.config as config_mod
from src.core.app_settings import get_app_settings


def test_defaults_match_legacy_values(monkeypatch):
    """在未配置的空环境下断言默认值（autouse fixture 会加载真实 yaml）。"""
    monkeypatch.setattr(
        config_mod, "get", lambda key, default=None: default
    )
    s = get_app_settings()
    assert s.scheduler.max_concurrent_tasks == 5
    assert s.scheduler.default_retry_count == 3
    assert s.scheduler.execution_backend == "in_process"
    assert s.scheduler.persistence.db_name == "scheduler.db"
    assert s.pipeline.use_dag_execution is True
    assert s.pipeline.legacy_fallback is False
    assert s.database.provider == "sqlalchemy"
    assert s.database.sqlalchemy_url is None
    assert s.server.host == "127.0.0.1"
    assert s.server.port == 8000
    assert s.agent.enabled is True
    assert s.agent.session_timeout_minutes == 60


def test_config_get_patch_seam_still_works(monkeypatch):
    """builder 经 config.get 取值——既有测试的覆盖接缝保持。"""
    monkeypatch.setattr(
        config_mod,
        "get",
        lambda key, default=None: {
            "pipeline.use_dag_execution": False,
            "pipeline.legacy_fallback": True,
            "scheduler.max_concurrent_tasks": 9,
        }.get(key, default),
    )
    s = get_app_settings()
    assert s.pipeline.use_dag_execution is False
    assert s.pipeline.legacy_fallback is True
    assert s.scheduler.max_concurrent_tasks == 9


def test_invalid_types_rejected():
    import src.core.config as cfg

    real_get = cfg.get

    def bad_get(key, default=None):
        if key == "scheduler.max_concurrent_tasks":
            return "not-a-number"
        return real_get(key, default)

    monkey = pytest.MonkeyPatch()
    monkey.setattr(config_mod, "get", bad_get)
    try:
        with pytest.raises(Exception):
            get_app_settings()
    finally:
        monkey.undo()


def test_env_shaped_settings_roundtrip(isolated_db_config):
    """load_settings 后（含 .env/DATABASE_URL 插值）能正常构建。"""
    s = get_app_settings()
    assert isinstance(s.database.sqlalchemy_url, str) or s.database.sqlalchemy_url is None
    assert "sqlite" in (s.database.sqlalchemy_url or "") or s.database.sqlalchemy_url is None
