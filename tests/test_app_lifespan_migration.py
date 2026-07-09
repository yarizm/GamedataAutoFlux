# tests/test_app_lifespan_migration.py
"""lifespan 自动迁移检测：旧 pipeline 快照在启动时被转成 graph（只转不删）。"""
import pytest

from src.core.pipeline import Pipeline
from src.services.sqlalchemy_pipeline_repository import SQLAlchemyPipelineRepository
from src.services.sqlalchemy_dag_repository import SQLAlchemyDAGRepository

from scripts.migrate_pipelines_to_dag import migrate_pipelines_to_dag


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

    sf = await _sf()
    try:
        # get_dag_repository 依赖 session factory 已初始化
        repo1 = get_dag_repository()
        assert repo1 is not None
        _reset_runtime_singletons()
        # reset 后再次获取应是新实例
        import src.web.app as app_module

        assert app_module._dag_repo is None
    finally:
        await _close()
