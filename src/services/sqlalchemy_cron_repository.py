"""
基于 SQLAlchemy 的 Cron 仓储实现

使用 SchedulerStateModel 表持久化 cron 任务配置。
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.services.cron_repository import (
    CronJobConfig,
    CronRepository,
    cron_job_config_from_dict,
)
from src.storage.models import SchedulerStateModel


def _job_to_data(job: CronJobConfig) -> dict:
    return {
        "name": job.name,
        "pipeline_name": job.pipeline_name,
        "cron_expr": job.cron_expr,
        "task_template": job.task_template,
        "enabled": job.enabled,
        "timezone": job.timezone,
        "schedule_meta": job.schedule_meta,
        "description": job.description,
    }


class SQLAlchemyCronRepository(CronRepository):
    """基于 SQLAlchemy SchedulerStateModel 的 Cron 仓储"""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    @staticmethod
    def _deserialize(data: object, key: str, *, fallback_name: str = "") -> CronJobConfig:
        if not isinstance(data, dict):
            raise ValueError(f"stored cron {key} payload is malformed")
        try:
            job = cron_job_config_from_dict(data, fallback_name=fallback_name)
        except Exception as exc:
            raise ValueError(f"stored cron {key} unreadable: {exc}") from exc
        if not job.name or not job.pipeline_name or not job.cron_expr:
            raise ValueError(f"stored cron {key} payload is incomplete")
        return job

    async def save(self, job: CronJobConfig) -> None:
        async with self._session_factory() as session:
            data = _job_to_data(job)

            stmt = insert(SchedulerStateModel).values(
                key=f"cron:{job.name}",
                state_type="cron",
                data=data,
                metadata_={
                    "kind": "cron",
                    "pipeline_name": job.pipeline_name,
                    "enabled": job.enabled,
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

    async def load(self, name: str) -> CronJobConfig | None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(SchedulerStateModel).where(SchedulerStateModel.key == f"cron:{name}")
            )
            db_record = result.scalars().first()
            if db_record is None:
                return None
            return self._deserialize(db_record.data, db_record.key, fallback_name=name)

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

            jobs: list[CronJobConfig] = []
            for r in db_records:
                jobs.append(self._deserialize(r.data, r.key))
            return jobs
