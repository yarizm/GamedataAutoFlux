"""
基于 SQLAlchemy 的 Pipeline 仓储实现

使用 SchedulerStateModel 表持久化 pipeline 配置快照。
"""

from __future__ import annotations

from loguru import logger
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.pipeline import Pipeline
from src.core.dag import DAG, pipeline_to_dag
from src.services.pipeline_repository import PipelineRepository
from src.storage.models import SchedulerStateModel


class SQLAlchemyPipelineRepository(PipelineRepository):
    """基于 SQLAlchemy SchedulerStateModel 的 Pipeline 仓储"""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save(self, pipeline: Pipeline) -> None:
        async with self._session_factory() as session:
            config = pipeline.to_config()

            stmt = insert(SchedulerStateModel).values(
                key=f"pipeline:{pipeline.name}",
                state_type="pipeline",
                data=config,
                metadata_={
                    "kind": "pipeline",
                    "pipeline_name": pipeline.name,
                },
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[SchedulerStateModel.key],
                set_={
                    "state_type": stmt.excluded.state_type,
                    "data": stmt.excluded.data,
                    "metadata": stmt.excluded.metadata,
                },
            )
            await session.execute(stmt)

            await session.commit()

    async def load(self, name: str) -> Pipeline | None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(SchedulerStateModel).where(SchedulerStateModel.key == f"pipeline:{name}")
            )
            db_record = result.scalars().first()
            if db_record is None or not isinstance(db_record.data, dict):
                return None
            try:
                return Pipeline.from_config(db_record.data)
            except Exception as exc:
                logger.warning(f"Failed to load pipeline {name}: {exc}")
                return None

    async def delete(self, name: str) -> bool:
        async with self._session_factory() as session:
            result = await session.execute(
                select(SchedulerStateModel).where(SchedulerStateModel.key == f"pipeline:{name}")
            )
            db_record = result.scalars().first()
            if db_record:
                await session.delete(db_record)
                await session.commit()
                return True
            return False

    async def list_all(self) -> list[Pipeline]:
        async with self._session_factory() as session:
            stmt = select(SchedulerStateModel).where(SchedulerStateModel.state_type == "pipeline")
            result = await session.execute(stmt)
            db_records = result.scalars().all()

            pipelines = []
            for r in db_records:
                if isinstance(r.data, dict):
                    try:
                        pipelines.append(Pipeline.from_config(r.data))
                    except Exception as exc:
                        logger.warning(f"Skipping malformed pipeline record {r.key}: {exc}")
                        continue
            return pipelines

    async def load_as_dag(self, name: str) -> DAG | None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(SchedulerStateModel).where(SchedulerStateModel.key == f"graph:{name}")
            )
            rec = result.scalars().first()
            if rec is not None and isinstance(rec.data, dict):
                try:
                    return DAG.from_storage(rec.data)
                except Exception as exc:
                    logger.warning(f"Failed to load graph {name}: {exc}")
            result = await session.execute(
                select(SchedulerStateModel).where(SchedulerStateModel.key == f"pipeline:{name}")
            )
            rec = result.scalars().first()
            if rec is None or not isinstance(rec.data, dict):
                return None
            try:
                pipeline = Pipeline.from_config(rec.data)
                return pipeline_to_dag(pipeline)
            except Exception as exc:
                logger.warning(f"Failed to convert legacy pipeline {name}: {exc}")
                return None
