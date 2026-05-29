from __future__ import annotations

from typing import Any
from src.storage.models import utcnow
from loguru import logger
from sqlalchemy import select, or_, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from src.core.registry import registry
from src.storage.base import BaseStorage, StorageRecord, QueryResult
from src.storage.models import Base, RecordModel

@registry.register("storage", "sqlalchemy")
class SQLAlchemyStorage(BaseStorage):
    """
    SQLAlchemy based storage with pgvector support.
    """

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self._engine = None
        self._session_factory = None

    async def initialize(self) -> None:
        url = self.config.get("sqlalchemy_url", "postgresql+asyncpg://postgres:postgres@localhost:5432/autoflux")
        logger.info(f"Initializing SQLAlchemy storage with URL: {url}")
        self._engine = create_async_engine(url, echo=False)
        self._session_factory = async_sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )

        # Create tables if not exist
        async with self._engine.begin() as conn:
            if "postgresql" in url:
                from sqlalchemy import text
                await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            await conn.run_sync(Base.metadata.create_all)

    async def save(self, record: StorageRecord) -> None:
        if self._session_factory is None:
            await self.initialize()

        meta_copy = dict(record.metadata or {})
        embedding_val = meta_copy.pop("embedding", None)
        data_to_save = record.data
        if hasattr(data_to_save, "model_dump"):
            data_to_save = data_to_save.model_dump()

        async with self._session_factory() as session:
            # Try to fetch existing
            result = await session.execute(select(RecordModel).where(RecordModel.key == record.key))
            db_record = result.scalars().first()

            if db_record:
                db_record.source = record.source
                db_record.collector = str(meta_copy.get("collector", ""))
                db_record.game_name = str(meta_copy.get("game_name", ""))
                db_record.app_id = str(meta_copy.get("app_id", ""))
                db_record.group_id = str(meta_copy.get("group_id", ""))
                db_record.task_id = str(meta_copy.get("task_id", ""))
                db_record.metadata_ = meta_copy
                db_record.tags = record.tags
                db_record.data = data_to_save
                if embedding_val is not None:
                    db_record.embedding = embedding_val
                db_record.updated_at = utcnow()
            else:
                db_record = RecordModel(
                    key=record.key,
                    source=record.source,
                    collector=str(meta_copy.get("collector", "")),
                    game_name=str(meta_copy.get("game_name", "")),
                    app_id=str(meta_copy.get("app_id", "")),
                    group_id=str(meta_copy.get("group_id", "")),
                    task_id=str(meta_copy.get("task_id", "")),
                    metadata_=meta_copy,
                    tags=record.tags,
                    data=data_to_save,
                    embedding=embedding_val,
                    stored_at=record.stored_at
                )
                session.add(db_record)
            
            await session.commit()

    async def load(self, key: str) -> StorageRecord | None:
        if self._session_factory is None:
            await self.initialize()
        
        async with self._session_factory() as session:
            result = await session.execute(select(RecordModel).where(RecordModel.key == key))
            db_record = result.scalars().first()
            
            if not db_record:
                return None
                
            return StorageRecord(
                key=db_record.key,
                data=db_record.data,
                metadata=db_record.metadata_,
                stored_at=db_record.stored_at,
                source=db_record.source,
                tags=db_record.tags
            )

    async def query(self, query: str, limit: int = 10, **kwargs: Any) -> QueryResult:
        if self._session_factory is None:
            await self.initialize()
            
        offset = max(0, int(kwargs.get("offset", 0) or 0))
        limit = max(0, int(limit or 0))
        order = str(kwargs.get("order", "desc") or "desc").lower()
        
        async with self._session_factory() as session:
            stmt = select(RecordModel)
            
            if query.startswith("source:"):
                stmt = stmt.where(RecordModel.source == query[7:])
            elif query.startswith("key:"):
                stmt = stmt.where(RecordModel.key.like(f"{query[4:]}%"))
            elif query.strip():
                q = f"%{query.strip()}%"
                stmt = stmt.where(
                    or_(
                        RecordModel.key.like(q),
                        RecordModel.source.like(q),
                        RecordModel.game_name.like(q)
                    )
                )

            # Exact matches
            for field in ("collector", "game_name", "app_id", "group_id", "task_id"):
                value = kwargs.get(field, "")
                if value:
                    stmt = stmt.where(getattr(RecordModel, field) == str(value))
                    
            # Count total
            count_stmt = select(func.count()).select_from(stmt.subquery())
            total_result = await session.execute(count_stmt)
            total = total_result.scalar() or 0
            
            # Order and limit
            if order == "asc":
                stmt = stmt.order_by(RecordModel.stored_at.asc())
            else:
                stmt = stmt.order_by(RecordModel.stored_at.desc())
                
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
                    stored_at=r.stored_at,
                    source=r.source,
                    tags=r.tags
                ))
                
            return QueryResult(records=records, total=total, query=query)

    async def semantic_search(self, query_vector: list[float], limit: int = 5, **kwargs: Any) -> QueryResult:
        if self._session_factory is None:
            await self.initialize()

        async with self._session_factory() as session:
            stmt = select(RecordModel).where(RecordModel.embedding.is_not(None))
            
            for field in ("collector", "game_name", "app_id", "group_id", "task_id"):
                value = kwargs.get(field, "")
                if value:
                    stmt = stmt.where(getattr(RecordModel, field) == str(value))
                    
            stmt = stmt.order_by(RecordModel.embedding.cosine_distance(query_vector)).limit(limit)
            result = await session.execute(stmt)
            db_records = result.scalars().all()
            
            records = []
            for r in db_records:
                records.append(StorageRecord(
                    key=r.key,
                    data=r.data,
                    metadata=r.metadata_,
                    stored_at=r.stored_at,
                    source=r.source,
                    tags=r.tags
                ))
            return QueryResult(records=records, total=len(records), query="semantic_search")

    async def delete(self, key: str) -> bool:
        if self._session_factory is None:
            await self.initialize()
            
        async with self._session_factory() as session:
            result = await session.execute(select(RecordModel).where(RecordModel.key == key))
            db_record = result.scalars().first()
            if db_record:
                await session.delete(db_record)
                await session.commit()
                return True
            return False

    async def list_keys(self, prefix: str = "", limit: int = 100) -> list[str]:
        if self._session_factory is None:
            await self.initialize()
            
        async with self._session_factory() as session:
            stmt = select(RecordModel.key)
            if prefix:
                stmt = stmt.where(RecordModel.key.like(f"{prefix}%"))
            stmt = stmt.limit(limit)
            
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def close(self) -> None:
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
