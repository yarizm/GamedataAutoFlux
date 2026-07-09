"""一次性迁移：把 state_type=pipeline 记录转成 state_type=graph。幂等，只转不删。"""
from __future__ import annotations

from typing import Any

from loguru import logger
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm.attributes import flag_modified

from src.core.dag import pipeline_to_dag
from src.core.pipeline import Pipeline
from src.storage.factory import normalize_storage_name
from src.storage.models import SchedulerStateModel


def _is_migratable_pipeline_data(data: Any) -> bool:
    """校验 pipeline 快照是否可转 DAG（需 dict 且含 name/steps）。"""
    if not isinstance(data, dict):
        return False
    if not data.get("name"):
        return False
    steps = data.get("steps")
    return isinstance(steps, list)


def _normalize_pipeline_storage_steps(data: dict[str, Any]) -> bool:
    """就地归一 pipeline 快照里的 storage 名，返回是否有改动。"""
    changed = False
    steps = data.get("steps")
    if not isinstance(steps, list):
        return False
    for step in steps:
        if not isinstance(step, dict) or step.get("type") != "storage":
            continue
        old = step.get("name")
        new = normalize_storage_name(old)
        if old != new:
            step["name"] = new
            changed = True
    return changed


def _normalize_graph_storage_nodes(data: dict[str, Any]) -> bool:
    """就地归一 graph 快照里 storage 节点 component，返回是否有改动。"""
    changed = False
    nodes = data.get("nodes")
    if not isinstance(nodes, list):
        return False
    for node in nodes:
        if not isinstance(node, dict) or node.get("type") != "storage":
            continue
        old = node.get("component")
        new = normalize_storage_name(old)
        if old != new:
            node["component"] = new
            changed = True
    return changed


async def migrate_pipelines_to_dag(
    session_factory: async_sessionmaker[AsyncSession], *, dry_run: bool = False
) -> dict:
    migrated: list[str] = []
    skipped: list[str] = []
    failed: list[str] = []

    async with session_factory() as session:
        # 先把已有 graph / pipeline 快照里的 local storage 归一为 sqlalchemy
        for state_type, normalizer in (
            ("pipeline", _normalize_pipeline_storage_steps),
            ("graph", _normalize_graph_storage_nodes),
        ):
            rows = (
                await session.execute(
                    select(SchedulerStateModel).where(SchedulerStateModel.state_type == state_type)
                )
            ).scalars().all()
            for rec in rows:
                if not isinstance(rec.data, dict):
                    continue
                if normalizer(rec.data):
                    if not dry_run:
                        flag_modified(rec, "data")
                    logger.info("Normalized storage name local→sqlalchemy on {}", rec.key)

        stmt = select(SchedulerStateModel).where(SchedulerStateModel.state_type == "pipeline")
        records = (await session.execute(stmt)).scalars().all()
        for rec in records:
            name = rec.key.removeprefix("pipeline:")
            if rec.metadata_ and rec.metadata_.get("migrated") is True:
                skipped.append(name)
                continue

            # data 为空/损坏的历史记录：标记跳过，避免每次启动 ERROR 刷屏
            if not _is_migratable_pipeline_data(rec.data):
                reason = "invalid_or_empty_data"
                if dry_run:
                    skipped.append(name)
                    continue
                logger.warning(
                    "Skip pipeline {}: data is null/invalid ({}), mark migrated without graph",
                    name,
                    type(rec.data).__name__,
                )
                rec.metadata_ = {
                    **(rec.metadata_ or {}),
                    "migrated": True,
                    "migration_skipped": reason,
                }
                flag_modified(rec, "metadata_")
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
                    metadata_={
                        "kind": "pipeline_legacy",
                        "graph_name": name,
                        "migrated_from": rec.key,
                    },
                )
                ins = ins.on_conflict_do_update(
                    index_elements=[SchedulerStateModel.key],
                    set_={
                        "state_type": ins.excluded.state_type,
                        "data": ins.excluded.data,
                        "metadata": ins.excluded.metadata,
                    },
                )
                await session.execute(ins)
                rec.metadata_ = {**(rec.metadata_ or {}), "migrated": True}
                flag_modified(rec, "metadata_")
                migrated.append(name)
            except Exception as exc:
                logger.error("Failed to migrate pipeline {}: {}", name, exc)
                failed.append(name)
        await session.commit()

    return {"migrated": migrated, "skipped": skipped, "failed": failed}
