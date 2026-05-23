from __future__ import annotations

from typing import Any
from src.storage.models import utcnow
from loguru import logger
from sqlalchemy import select, func, text
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

    async def initialize(self) -> None:
        url = self.config.get("sqlalchemy_url", "postgresql+asyncpg://postgres:postgres@localhost:5432/autoflux")
        logger.info(f"Initializing SQLAlchemy scheduler storage with URL: {url}")
        self._engine = create_async_engine(url, echo=False)
        self._session_factory = async_sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )

        # Create tables if not exist
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            # Auto-migrate: add columns that may be missing from older schema
            await self._migrate_scheduler_table(conn)

    @staticmethod
    async def _migrate_scheduler_table(conn) -> None:
        """Add metadata and stored_at columns if they don't exist (for upgrades from old schema)."""
        from sqlalchemy import inspect as sa_inspect

        def _run_migration(sync_conn):
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

        await conn.run_sync(_run_migration)

    async def save(self, record: StorageRecord) -> None:
        if self._session_factory is None:
            await self.initialize()

        data_to_save = record.data
        if hasattr(data_to_save, "model_dump"):
            data_to_save = data_to_save.model_dump()
            
        state_type = record.key.split(":")[0] if ":" in record.key else "unknown"

        async with self._session_factory() as session:
            result = await session.execute(select(SchedulerStateModel).where(SchedulerStateModel.key == record.key))
            db_record = result.scalars().first()

            if db_record:
                db_record.state_type = state_type
                db_record.data = data_to_save
                db_record.metadata_ = record.metadata or {}
                db_record.updated_at = utcnow()
            else:
                db_record = SchedulerStateModel(
                    key=record.key,
                    state_type=state_type,
                    data=data_to_save,
                    metadata_=record.metadata or {}
                )
                session.add(db_record)
            
            await session.commit()

    async def load(self, key: str) -> StorageRecord | None:
        if self._session_factory is None:
            await self.initialize()
        
        async with self._session_factory() as session:
            result = await session.execute(select(SchedulerStateModel).where(SchedulerStateModel.key == key))
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
        limit = max(0, int(limit or 0))
        
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
            
            if limit > 0:
                stmt = stmt.limit(limit).offset(offset)
                
            result = await session.execute(stmt)
            db_records = result.scalars().all()
            
            records = []
            for r in db_records:
                records.append(StorageRecord(
                    key=r.key,
                    data=r.data,
                    metadata=r.metadata_,
                    stored_at=r.stored_at or utcnow(),
                    source="scheduler",
                ))
                
            return QueryResult(records=records, total=total, query=query)

    async def delete(self, key: str) -> bool:
        if self._session_factory is None:
            await self.initialize()
            
        async with self._session_factory() as session:
            result = await session.execute(select(SchedulerStateModel).where(SchedulerStateModel.key == key))
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
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
