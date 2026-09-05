"""
基于 SQLAlchemy 的 Task 仓储实现

使用 SchedulerStateModel 表持久化任务数据，
通过 task_status 索引实现高效状态查询。
"""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.task import Task, TaskStatus
from src.services.task_repository import TaskRepository
from src.storage.models import SchedulerStateModel, utcnow


class SQLAlchemyTaskRepository(TaskRepository):
    """基于 SQLAlchemy SchedulerStateModel 的任务仓储"""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    @staticmethod
    def _deserialize(data: object, key: str) -> Task:
        """Deserialize a stored task without hiding corrupt persistence data."""
        if not isinstance(data, dict):
            raise ValueError(f"stored task {key} payload is malformed")
        try:
            return Task.from_storage_payload(data)
        except Exception as exc:
            raise ValueError(f"stored task {key} unreadable: {exc}") from exc

    async def save(self, task: Task) -> None:
        """保存或更新任务"""
        payload = task.to_storage_payload()
        status_value = task.status.value

        async with self._session_factory() as session:
            stmt = insert(SchedulerStateModel).values(
                key=f"task:{task.id}",
                state_type="task",
                data=payload,
                metadata_={
                    "kind": "task",
                    "status": status_value,
                    "pipeline_name": task.pipeline_name,
                },
                task_status=status_value,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[SchedulerStateModel.key],
                set_={
                    "data": stmt.excluded.data,
                    "task_status": stmt.excluded.task_status,
                    "metadata": stmt.excluded.metadata,
                    "updated_at": utcnow(),
                },
            )
            await session.execute(stmt)

            await session.commit()

    async def load(self, task_id: str) -> Task | None:
        """按 ID 加载任务"""
        async with self._session_factory() as session:
            result = await session.execute(
                select(SchedulerStateModel).where(SchedulerStateModel.key == f"task:{task_id}")
            )
            db_record = result.scalars().first()
            if db_record is None:
                return None
            return self._deserialize(db_record.data, db_record.key)

    async def delete(self, task_id: str) -> bool:
        """删除任务"""
        async with self._session_factory() as session:
            result = await session.execute(
                select(SchedulerStateModel).where(SchedulerStateModel.key == f"task:{task_id}")
            )
            db_record = result.scalars().first()
            if db_record:
                await session.delete(db_record)
                await session.commit()
                return True
            return False

    async def query(self, limit: int = 100, offset: int = 0) -> list[Task]:
        """查询所有任务（分页）"""
        async with self._session_factory() as session:
            stmt = (
                select(SchedulerStateModel)
                .where(SchedulerStateModel.state_type == "task")
                .order_by(SchedulerStateModel.stored_at.desc())
                .limit(limit)
                .offset(offset)
            )
            result = await session.execute(stmt)
            db_records = result.scalars().all()

            tasks: list[Task] = []
            for r in db_records:
                tasks.append(self._deserialize(r.data, r.key))
            return tasks

    async def query_by_status(self, status: TaskStatus, limit: int = 100) -> list[Task]:
        """按状态查询任务（优先走 task_status 索引）"""
        async with self._session_factory() as session:
            status_value = status.value
            # 优先走 task_status 索引
            stmt = (
                select(SchedulerStateModel)
                .where(
                    SchedulerStateModel.state_type == "task",
                    SchedulerStateModel.task_status == status_value,
                )
                .order_by(SchedulerStateModel.stored_at.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            db_records = result.scalars().all()

            # Removed full table scan fallback on empty index

            tasks: list[Task] = []
            for r in db_records:
                task = self._deserialize(r.data, r.key)
                if task.status == status:
                    tasks.append(task)
                if len(tasks) >= limit:
                    break
            return tasks

    async def list_keys(self, prefix: str = "", limit: int = 100) -> list[str]:
        """列出任务键"""
        key_prefix = prefix if prefix.startswith("task:") else f"task:{prefix}"
        async with self._session_factory() as session:
            stmt = select(SchedulerStateModel.key).where(SchedulerStateModel.state_type == "task")
            if key_prefix:
                stmt = stmt.where(SchedulerStateModel.key.like(f"{key_prefix}%"))
            stmt = stmt.limit(limit)
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def count_by_status(self) -> dict[str, int]:
        """按状态统计任务数量"""
        async with self._session_factory() as session:
            stmt = (
                select(
                    SchedulerStateModel.task_status,
                    func.count(SchedulerStateModel.key),
                )
                .where(SchedulerStateModel.state_type == "task")
                .group_by(SchedulerStateModel.task_status)
            )
            result = await session.execute(stmt)
            counts: dict[str, int] = {}
            for status, count in result.all():
                if status:
                    counts[status] = count
            return counts
