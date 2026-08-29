"""DAG 持久化仓储，state_type='graph'。"""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.dag import DAG
from src.storage.models import SchedulerStateModel


class SQLAlchemyDAGRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save(self, dag: DAG) -> None:
        async with self._session_factory() as session:
            payload = dag.to_storage()
            stmt = insert(SchedulerStateModel).values(
                key=f"graph:{dag.name}",
                state_type="graph",
                data=payload,
                metadata_={"kind": payload.get("kind", "dag"), "graph_name": dag.name},
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[SchedulerStateModel.key],
                set_={"state_type": stmt.excluded.state_type, "data": stmt.excluded.data, "metadata": stmt.excluded.metadata},
            )
            await session.execute(stmt)
            await session.commit()

    async def load(self, name: str) -> DAG | None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(SchedulerStateModel).where(SchedulerStateModel.key == f"graph:{name}")
            )
            rec = result.scalars().first()
            if rec is None or not isinstance(rec.data, dict):
                if rec is None:
                    return None
                raise ValueError(f"stored graph {name} payload is malformed")
            try:
                return DAG.from_storage(rec.data)
            except (TypeError, ValueError, KeyError, AttributeError, IndexError) as exc:
                raise ValueError(f"stored graph {name} unreadable: {exc}") from exc

    async def delete(self, name: str) -> bool:
        async with self._session_factory() as session:
            result = await session.execute(
                select(SchedulerStateModel).where(SchedulerStateModel.key == f"graph:{name}")
            )
            rec = result.scalars().first()
            if rec:
                await session.delete(rec)
                await session.commit()
                return True
            return False

    async def list_all(self) -> list[DAG]:
        async with self._session_factory() as session:
            stmt = select(SchedulerStateModel).where(SchedulerStateModel.state_type == "graph")
            result = await session.execute(stmt)
            dags: list[DAG] = []
            for r in result.scalars().all():
                if not isinstance(r.data, dict):
                    raise ValueError(f"stored graph {r.key} payload is malformed")
                try:
                    dags.append(DAG.from_storage(r.data))
                except (TypeError, ValueError, KeyError, AttributeError, IndexError) as exc:
                    raise ValueError(f"stored graph {r.key} unreadable: {exc}") from exc
            return dags

    async def list_legacy_pipelines(self) -> list[dict]:
        """列出未迁移的旧 state_type='pipeline' 记录，供迁移脚本用。"""
        async with self._session_factory() as session:
            stmt = select(SchedulerStateModel).where(SchedulerStateModel.state_type == "pipeline")
            result = await session.execute(stmt)
            return [
                {"key": r.key, "data": r.data, "metadata": r.metadata_}
                for r in result.scalars().all() if isinstance(r.data, dict)
            ]
