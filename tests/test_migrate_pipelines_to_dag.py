# tests/test_migrate_pipelines_to_dag.py
import pytest
from sqlalchemy import select

from src.core.dag import DAG
from src.core.pipeline import Pipeline
from src.services.sqlalchemy_pipeline_repository import SQLAlchemyPipelineRepository
from src.services.sqlalchemy_dag_repository import SQLAlchemyDAGRepository
from src.storage.models import SchedulerStateModel

from scripts.migrate_pipelines_to_dag import migrate_pipelines_to_dag


async def _sf():
    from src.storage.session_factory import init_shared_session_factory
    return await init_shared_session_factory()


async def _close():
    from src.storage.session_factory import close_shared_session_factory
    await close_shared_session_factory()


@pytest.mark.asyncio
async def test_migrate_pipeline_to_dag_idempotent(isolated_db_config):
    sf = await _sf()
    try:
        repo = SQLAlchemyPipelineRepository(sf)
        await repo.save(Pipeline("old1").add_collector("steam").add_storage("sqlalchemy"))

        result1 = await migrate_pipelines_to_dag(sf)
        assert "old1" in result1["migrated"]

        # 二次迁移：已标记 migrated → skipped
        result2 = await migrate_pipelines_to_dag(sf)
        assert "old1" in result2["skipped"]
        assert "old1" not in result2["migrated"]
    finally:
        await _close()


@pytest.mark.asyncio
async def test_migrate_does_not_delete_original(isolated_db_config):
    sf = await _sf()
    try:
        repo = SQLAlchemyPipelineRepository(sf)
        await repo.save(Pipeline("keep").add_collector("steam").add_storage("sqlalchemy"))

        await migrate_pipelines_to_dag(sf)

        async with sf() as session:
            rec = (
                await session.execute(
                    select(SchedulerStateModel).where(SchedulerStateModel.key == "pipeline:keep")
                )
            ).scalars().first()
            assert rec is not None  # 原记录保留
            assert rec.metadata_.get("migrated") is True

            graph_rec = (
                await session.execute(
                    select(SchedulerStateModel).where(SchedulerStateModel.key == "graph:keep")
                )
            ).scalars().first()
            assert graph_rec is not None
            assert graph_rec.state_type == "graph"
    finally:
        await _close()


@pytest.mark.asyncio
async def test_migrate_empty_store_no_error(isolated_db_config):
    sf = await _sf()
    try:
        result = await migrate_pipelines_to_dag(sf)
        assert result["migrated"] == []
        assert result["skipped"] == []
        assert result["failed"] == []
    finally:
        await _close()


@pytest.mark.asyncio
async def test_migrate_dry_run_does_not_persist(isolated_db_config):
    sf = await _sf()
    try:
        repo = SQLAlchemyPipelineRepository(sf)
        await repo.save(Pipeline("dry").add_collector("steam").add_storage("sqlalchemy"))

        result = await migrate_pipelines_to_dag(sf, dry_run=True)
        assert "dry" in result["migrated"]

        async with sf() as session:
            graph_rec = (
                await session.execute(
                    select(SchedulerStateModel).where(SchedulerStateModel.key == "graph:dry")
                )
            ).scalars().first()
            assert graph_rec is None  # dry_run 不落 graph 记录
            pipe_rec = (
                await session.execute(
                    select(SchedulerStateModel).where(SchedulerStateModel.key == "pipeline:dry")
                )
            ).scalars().first()
            assert pipe_rec.metadata_.get("migrated") is not True  # 原记录未标记
    finally:
        await _close()


@pytest.mark.asyncio
async def test_migrated_graph_loadable(isolated_db_config):
    sf = await _sf()
    try:
        repo = SQLAlchemyPipelineRepository(sf)
        await repo.save(
            Pipeline("loadable").add_collector("steam", config={"app_id": "123"})
            .add_processor("cleaner").add_storage("sqlalchemy")
        )
        await migrate_pipelines_to_dag(sf)

        dag_repo = SQLAlchemyDAGRepository(sf)
        dag = await dag_repo.load("loadable")
        assert dag is not None
        assert isinstance(dag, DAG)
        types = {n.type for n in dag.nodes}
        assert {"collector", "processor", "storage"}.issubset(types)
    finally:
        await _close()
