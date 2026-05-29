"""
基于 SQLAlchemy 的 Cron 仓储实现

使用 SchedulerStateModel 表持久化 cron 任务配置。
"""

from __future__ import annotations

from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.services.cron_repository import CronJobConfig, CronRepository
from src.storage.models import SchedulerStateModel


class SQLAlchemyCronRepository(CronRepository):
    """基于 SQLAlchemy SchedulerStateModel 的 Cron 仓储"""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save(self, job: CronJobConfig) -> None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(SchedulerStateModel).where(SchedulerStateModel.key == f"cron:{job.name}")
            )
            db_record = result.scalars().first()

            data = {
                "name": job.name,
                "pipeline_name": job.pipeline_name,
                "cron_expr": job.cron_expr,
                "task_template": job.task_template,
            }

            if db_record:
                db_record.state_type = "cron"
                db_record.data = data
                db_record.metadata_ = {
                    "kind": "cron",
                    "pipeline_name": job.pipeline_name,
                }
            else:
                db_record = SchedulerStateModel(
                    key=f"cron:{job.name}",
                    state_type="cron",
                    data=data,
                    metadata_={
                        "kind": "cron",
                        "pipeline_name": job.pipeline_name,
                    },
                )
                session.add(db_record)

            await session.commit()

    async def load(self, name: str) -> CronJobConfig | None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(SchedulerStateModel).where(SchedulerStateModel.key == f"cron:{name}")
            )
            db_record = result.scalars().first()
            if db_record is None or not isinstance(db_record.data, dict):
                return None
            try:
                return CronJobConfig(
                    name=str(db_record.data.get("name", name)),
                    pipeline_name=str(db_record.data.get("pipeline_name", "")),
                    cron_expr=str(db_record.data.get("cron_expr", "")),
                    task_template=db_record.data.get("task_template", {}),
                )
            except Exception as exc:
                logger.warning(f"Failed to load cron job {name}: {exc}")
                return None

    async def delete(self, name: str) -> bool:
        async with self._session_factory() as session:
            result = await session.execute(
                select(SchedulerStateModel).where(SchedulerStateModel.key == f"cron:{name}")
            )
            db_record = result.scalars().first()
            if db_record:
                await session.delete(db_record)
                await session.commit()
                return True
            return False

    async def list_all(self) -> list[CronJobConfig]:
        async with self._session_factory() as session:
            stmt = select(SchedulerStateModel).where(SchedulerStateModel.state_type == "cron")
            result = await session.execute(stmt)
            db_records = result.scalars().all()

            jobs = []
            for r in db_records:
                if isinstance(r.data, dict):
                    try:
                        jobs.append(
                            CronJobConfig(
                                name=str(r.data.get("name", "")),
                                pipeline_name=str(r.data.get("pipeline_name", "")),
                                cron_expr=str(r.data.get("cron_expr", "")),
                                task_template=r.data.get("task_template", {}),
                            )
                        )
                    except Exception as exc:
                        logger.warning(f"Skipping malformed cron record {r.key}: {exc}")
                        continue
            return jobs
