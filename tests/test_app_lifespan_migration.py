# tests/test_app_lifespan_migration.py
"""lifespan 自动迁移检测：旧 pipeline 快照在启动时被转成 graph（只转不删）。"""
import pytest

from src.core.pipeline import Pipeline
from src.services.sqlalchemy_pipeline_repository import SQLAlchemyPipelineRepository
from src.services.sqlalchemy_dag_repository import SQLAlchemyDAGRepository

from src.services.pipeline_dag_migration import migrate_pipelines_to_dag


async def _sf():
    from src.storage.session_factory import init_shared_session_factory
    return await init_shared_session_factory()


async def _close():
    from src.storage.session_factory import close_shared_session_factory
    await close_shared_session_factory()


@pytest.mark.asyncio
async def test_lifespan_migration_logic_migrates_legacy(isolated_db_config):
    """lifespan 调 migrate_pipelines_to_dag，旧 pipeline 被转 graph。"""
    sf = await _sf()
    try:
        repo = SQLAlchemyPipelineRepository(sf)
        await repo.save(Pipeline("pre_lifespan").add_collector("steam").add_storage("sqlalchemy"))

        # 模拟 lifespan 内的调用
        result = await migrate_pipelines_to_dag(sf)
        assert "pre_lifespan" in result["migrated"]

        dag_repo = SQLAlchemyDAGRepository(sf)
        assert await dag_repo.load("pre_lifespan") is not None
    finally:
        await _close()


@pytest.mark.asyncio
async def test_lifespan_migration_skips_already_migrated(isolated_db_config):
    sf = await _sf()
    try:
        repo = SQLAlchemyPipelineRepository(sf)
        await repo.save(Pipeline("already").add_collector("steam").add_storage("sqlalchemy"))

        await migrate_pipelines_to_dag(sf)
        result = await migrate_pipelines_to_dag(sf)
        assert "already" in result["skipped"]
        assert result["migrated"] == []
    finally:
        await _close()


@pytest.mark.asyncio
async def test_dag_repository_singleton_reset(isolated_db_config):
    """_reset_runtime_singletons 应清空 dag_repo 单例。"""
    from src.web.app import _reset_runtime_singletons, get_dag_repository

    await _sf()
    try:
        # get_dag_repository 依赖 session factory 已初始化
        repo1 = get_dag_repository()
        assert repo1 is not None
        _reset_runtime_singletons()
        # reset 后再次获取应是新实例
        import src.bootstrap.container as app_module

        assert app_module._dag_repo is None
    finally:
        await _close()


def test_pipeline_migration_failure_blocks_startup() -> None:
    from src.web.app import _ensure_pipeline_migration_success

    with pytest.raises(RuntimeError, match="broken-pipeline"):
        _ensure_pipeline_migration_success({"failed": ["broken-pipeline"]})

    _ensure_pipeline_migration_success({"failed": []})


def test_event_bus_is_single_instance_across_lifespan():
    """容器与 Scheduler 在整个 lifespan 期间必须持有同一 EventBus 实例。

    启动末尾的例行 singleton reset 不得清掉 event_bus——否则 container
    会惰性创建第二个总线，shutdown 的 clear 也清不到真正注册 hooks 的
    那一个。
    """
    from fastapi.testclient import TestClient

    from src.bootstrap import container
    from src.web.app import app

    with TestClient(app):
        bus = container.get_event_bus()
        assert bus is container.scheduler.event_bus
        bus.clear()  # 不得影响 scheduler 侧引用的同一性
        assert container.get_event_bus() is container.scheduler.event_bus
