from __future__ import annotations

from typing import Any
from src.storage.models import utcnow
from loguru import logger
from sqlalchemy import select, func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from src.core.registry import registry
from src.storage.base import BaseStorage, StorageRecord, QueryResult
from src.storage.models import Base, SchedulerStateModel


@registry.register("storage", "sqlalchemy_scheduler")
class SQLAlchemySchedulerStorage(BaseStorage):
    """
    SQLAlchemy based storage specifically for the Scheduler state.
    """

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self._engine = None
        self._session_factory = None
        self._owns_engine = False

    async def initialize(self) -> None:
        # 优先使用共享 session factory（如果已初始化）
        from src.storage.session_factory import get_session_factory as _get_shared

        try:
            self._session_factory = _get_shared()
            self._owns_engine = False
            logger.info("SQLAlchemy scheduler storage using shared session factory")
            return
        except RuntimeError:
            pass  # 未初始化，自建 engine

        url = self.config.get(
            "sqlalchemy_url", "postgresql+asyncpg://postgres:postgres@localhost:5432/autoflux"
        )
        logger.info(f"Initializing SQLAlchemy scheduler storage with URL: {url}")
        self._engine = create_async_engine(url, echo=False)
        self._session_factory = async_sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        self._owns_engine = True

        # Create tables if not exist
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            # Auto-migrate: add columns that may be missing from older schema
            await self._migrate_scheduler_table(conn)

    @staticmethod
    async def _migrate_scheduler_table(conn) -> None:
        """Add metadata, stored_at and task_status columns if they don't exist (for upgrades from old schema)."""
        from sqlalchemy import inspect as sa_inspect

        def _run_migration(sync_conn):
            try:
                inspector = sa_inspect(sync_conn)
                existing = {col["name"] for col in inspector.get_columns("scheduler_states")}
                if "metadata" not in existing:
                    sync_conn.execute(
                        text("ALTER TABLE scheduler_states ADD COLUMN metadata JSON DEFAULT '{}'")
                    )
                    logger.info("Migrated scheduler_states: added metadata column")
                if "stored_at" not in existing:
                    sync_conn.execute(
                        text("ALTER TABLE scheduler_states ADD COLUMN stored_at TIMESTAMP")
                    )
                    logger.info("Migrated scheduler_states: added stored_at column")
                if "task_status" not in existing:
                    sync_conn.execute(
                        text("ALTER TABLE scheduler_states ADD COLUMN task_status VARCHAR")
                    )
                    logger.info("Migrated scheduler_states: added task_status column")
                    try:
                        sync_conn.execute(
                            text(
                                "CREATE INDEX ix_scheduler_states_task_status "
                                "ON scheduler_states (task_status)"
                            )
                        )
                        logger.info("Created index on scheduler_states.task_status")
                    except Exception:
                        pass  # Index may already exist or DB doesn't support it
            except Exception as exc:
                logger.warning(f"Schema migration skipped (non-critical): {exc}")

        await conn.run_sync(_run_migration)

    async def save(self, record: StorageRecord) -> None:
        if self._session_factory is None:
            await self.initialize()

        data_to_save = record.data
        if hasattr(data_to_save, "model_dump"):
            data_to_save = data_to_save.model_dump()

        state_type = record.key.split(":")[0] if ":" in record.key else "unknown"

        # 自动提取 task_status 从 data 或 metadata
        task_status = None
        if state_type == "task":
            task_status = (record.metadata or {}).get("status") or (
                data_to_save.get("status") if isinstance(data_to_save, dict) else None
            )

        async with self._session_factory() as session:
            stmt = insert(SchedulerStateModel).values(
                key=record.key,
                state_type=state_type,
                data=data_to_save,
                metadata_=record.metadata or {},
                task_status=task_status,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[SchedulerStateModel.key],
                set_={
                    "state_type": stmt.excluded.state_type,
                    "data": stmt.excluded.data,
                    "metadata": stmt.excluded.metadata,
                    "task_status": stmt.excluded.task_status if task_status is not None else SchedulerStateModel.task_status,
                    "updated_at": utcnow(),
                }
            )
            await session.execute(stmt)
            await session.commit()

    async def load(self, key: str) -> StorageRecord | None:
        if self._session_factory is None:
            await self.initialize()

        async with self._session_factory() as session:
            result = await session.execute(
                select(SchedulerStateModel).where(SchedulerStateModel.key == key)
            )
            db_record = result.scalars().first()

            if not db_record:
                return None

            return StorageRecord(
                key=db_record.key,
                data=db_record.data,
                metadata=db_record.metadata_,
                stored_at=db_record.stored_at or utcnow(),
                source="scheduler",
            )

    async def query(self, query: str, limit: int = 1000, **kwargs: Any) -> QueryResult:
        if self._session_factory is None:
            await self.initialize()

        offset = max(0, int(kwargs.get("offset", 0) or 0))
        limit = min(max(1, int(limit or 1000)), 5000)

        async with self._session_factory() as session:
            stmt = select(SchedulerStateModel)

            if query.startswith("key:"):
                stmt = stmt.where(SchedulerStateModel.key.like(f"{query[4:]}%"))
            elif query.strip():
                stmt = stmt.where(SchedulerStateModel.key.like(f"%{query.strip()}%"))

            # Count total
            count_stmt = select(func.count()).select_from(stmt.subquery())
            total_result = await session.execute(count_stmt)
            total = total_result.scalar() or 0

            stmt = stmt.limit(limit).offset(offset)

            result = await session.execute(stmt)
            db_records = result.scalars().all()

            records = []
            for r in db_records:
                records.append(
                    StorageRecord(
                        key=r.key,
                        data=r.data,
                        metadata=r.metadata_,
                        stored_at=r.stored_at or utcnow(),
                        source="scheduler",
                    )
                )

            return QueryResult(records=records, total=total, query=query)

    async def query_by_task_status(
        self, status: str, limit: int = 100, offset: int = 0
    ) -> QueryResult:
        """按任务状态查询，优先走 task_status 索引。"""
        if self._session_factory is None:
            await self.initialize()

        async with self._session_factory() as session:
            # 优先走 task_status 索引
            stmt = select(SchedulerStateModel).where(
                SchedulerStateModel.state_type == "task",
                SchedulerStateModel.task_status == status,
            )
            count_stmt = select(func.count()).select_from(stmt.subquery())
            total_result = await session.execute(count_stmt)
            total = total_result.scalar() or 0

            stmt = stmt.order_by(SchedulerStateModel.stored_at.desc()).limit(limit).offset(offset)
            result = await session.execute(stmt)
            db_records = result.scalars().all()

            records = []
            for r in db_records:
                records.append(
                    StorageRecord(
                        key=r.key,
                        data=r.data,
                        metadata=r.metadata_,
                        stored_at=r.stored_at or utcnow(),
                        source="scheduler",
                    )
                )

            return QueryResult(records=records, total=total, query=f"status:{status}")

    async def delete(self, key: str) -> bool:
        if self._session_factory is None:
            await self.initialize()

        async with self._session_factory() as session:
            result = await session.execute(
                select(SchedulerStateModel).where(SchedulerStateModel.key == key)
            )
            db_record = result.scalars().first()
            if db_record:
                await session.delete(db_record)
                await session.commit()
                return True
            return False

    async def list_keys(self, prefix: str = "", limit: int = 1000) -> list[str]:
        if self._session_factory is None:
            await self.initialize()

        async with self._session_factory() as session:
            stmt = select(SchedulerStateModel.key)
            if prefix:
                stmt = stmt.where(SchedulerStateModel.key.like(f"{prefix}%"))
            stmt = stmt.limit(limit)

            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def close(self) -> None:
        if self._owns_engine and self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
