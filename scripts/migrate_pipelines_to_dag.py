"""一次性迁移：把 state_type=pipeline 记录转成 state_type=graph。幂等，只转不删。"""
from __future__ import annotations

import asyncio

from loguru import logger
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.dag import pipeline_to_dag
from src.core.pipeline import Pipeline
from src.storage.models import SchedulerStateModel


async def migrate_pipelines_to_dag(
    session_factory: async_sessionmaker[AsyncSession], *, dry_run: bool = False
) -> dict:
    migrated: list[str] = []
    skipped: list[str] = []
    failed: list[str] = []

    async with session_factory() as session:
        stmt = select(SchedulerStateModel).where(SchedulerStateModel.state_type == "pipeline")
        records = (await session.execute(stmt)).scalars().all()
        for rec in records:
            name = rec.key.removeprefix("pipeline:")
            if rec.metadata_ and rec.metadata_.get("migrated") is True:
                skipped.append(name)
                continue
            if dry_run:
                migrated.append(name)
                continue
            try:
                pipeline = Pipeline.from_config(rec.data)
                dag = pipeline_to_dag(pipeline)
                payload = dag.to_storage()
                payload["kind"] = "pipeline_legacy"
                ins = insert(SchedulerStateModel).values(
                    key=f"graph:{name}",
                    state_type="graph",
                    data=payload,
                    metadata_={"kind": "pipeline_legacy", "graph_name": name, "migrated_from": rec.key},
                )
                ins = ins.on_conflict_do_update(
                    index_elements=[SchedulerStateModel.key],
                    set_={"state_type": ins.excluded.state_type, "data": ins.excluded.data, "metadata": ins.excluded.metadata},
                )
                await session.execute(ins)
                rec.metadata_ = {**(rec.metadata_ or {}), "migrated": True}
                migrated.append(name)
            except Exception as exc:
                logger.error("Failed to migrate pipeline {}: {}", name, exc)
                failed.append(name)
        await session.commit()

    return {"migrated": migrated, "skipped": skipped, "failed": failed}


async def _main() -> None:
    from src.storage.session_factory import init_shared_session_factory

    sf = await init_shared_session_factory()
    result = await migrate_pipelines_to_dag(sf)
    print(result)


if __name__ == "__main__":
    asyncio.run(_main())
