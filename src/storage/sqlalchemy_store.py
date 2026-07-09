from __future__ import annotations

from typing import Any
from src.storage.models import utcnow
from loguru import logger
from sqlalchemy import select, or_, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from src.core.registry import registry
from src.storage.base import BaseStorage, StorageRecord, QueryResult
from src.storage.models import Base, RecordModel


@registry.register("storage", "local")  # 历史别名，与 sqlalchemy 同一实现
@registry.register("storage", "sqlalchemy")
class SQLAlchemyStorage(BaseStorage):
    """
    SQLAlchemy based storage with pgvector support.
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
            logger.info("SQLAlchemy storage using shared session factory")
            return
        except RuntimeError:
            pass  # 未初始化，自建 engine

        url = self.config.get(
            "sqlalchemy_url", "postgresql+asyncpg://postgres:postgres@localhost:5432/autoflux"
        )
        logger.info(f"Initializing SQLAlchemy storage with URL: {url}")
        self._engine = create_async_engine(url, echo=False)
        self._session_factory = async_sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        self._owns_engine = True

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
            stmt = insert(RecordModel).values(
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
                stored_at=record.stored_at,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[RecordModel.key],
                set_={
                    "source": stmt.excluded.source,
                    "collector": stmt.excluded.collector,
                    "game_name": stmt.excluded.game_name,
                    "app_id": stmt.excluded.app_id,
                    "group_id": stmt.excluded.group_id,
                    "task_id": stmt.excluded.task_id,
                    "metadata": stmt.excluded.metadata,
                    "tags": stmt.excluded.tags,
                    "data": stmt.excluded.data,
                    "embedding": stmt.excluded.embedding
                    if embedding_val is not None
                    else RecordModel.embedding,
                    "updated_at": utcnow(),
                },
            )
            await session.execute(stmt)

            await session.commit()

    async def save_batch(self, records: list[StorageRecord]) -> None:
        if not records:
            return
        if self._session_factory is None:
            await self.initialize()

        # 批量构建参数列表，使用 executemany 一次性发送（减少网络往返）
        params_list = []
        for record in records:
            meta_copy = dict(record.metadata or {})
            embedding_val = meta_copy.pop("embedding", None)
            data_to_save = record.data
            if hasattr(data_to_save, "model_dump"):
                data_to_save = data_to_save.model_dump()

            params_list.append(
                {
                    "key": record.key,
                    "source": record.source,
                    "collector": str(meta_copy.get("collector", "")),
                    "game_name": str(meta_copy.get("game_name", "")),
                    "app_id": str(meta_copy.get("app_id", "")),
                    "group_id": str(meta_copy.get("group_id", "")),
                    "task_id": str(meta_copy.get("task_id", "")),
                    "metadata_": meta_copy,
                    "tags": record.tags,
                    "data": data_to_save,
                    "embedding": embedding_val,
                    "stored_at": record.stored_at,
                }
            )

        async with self._session_factory() as session:
            # 使用 bindparam 构建 executemany 语句
            from sqlalchemy import bindparam

            stmt = insert(RecordModel).values(
                key=bindparam("key"),
                source=bindparam("source"),
                collector=bindparam("collector"),
                game_name=bindparam("game_name"),
                app_id=bindparam("app_id"),
                group_id=bindparam("group_id"),
                task_id=bindparam("task_id"),
                metadata_=bindparam("metadata_"),
                tags=bindparam("tags"),
                data=bindparam("data"),
                embedding=bindparam("embedding"),
                stored_at=bindparam("stored_at"),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[RecordModel.key],
                set_={
                    "source": stmt.excluded.source,
                    "collector": stmt.excluded.collector,
                    "game_name": stmt.excluded.game_name,
                    "app_id": stmt.excluded.app_id,
                    "group_id": stmt.excluded.group_id,
                    "task_id": stmt.excluded.task_id,
                    "metadata": stmt.excluded.metadata,
                    "tags": stmt.excluded.tags,
                    "data": stmt.excluded.data,
                    "embedding": stmt.excluded.embedding,
                    "updated_at": utcnow(),
                },
            )
            await session.execute(stmt, params_list)
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
                tags=db_record.tags,
            )

    async def query(self, query: str, limit: int = 10, **kwargs: Any) -> QueryResult:
        if self._session_factory is None:
            await self.initialize()

        offset = max(0, int(kwargs.get("offset", 0) or 0))
        limit = min(max(1, int(limit or 1000)), 5000)
        order = str(kwargs.get("order", "desc") or "desc").lower()

        async with self._session_factory() as session:
            stmt = select(RecordModel)

            if query.startswith("source:"):
                stmt = stmt.where(RecordModel.source == query[7:])
            elif query.startswith("key:"):
                key_prefix = query[4:].replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                stmt = stmt.where(RecordModel.key.like(f"{key_prefix}%", escape="\\"))
            elif query.strip():
                escaped_query = (
                    query.strip().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                )
                q = f"%{escaped_query}%"
                stmt = stmt.where(
                    or_(
                        RecordModel.key.like(q, escape="\\"),
                        RecordModel.source.like(q, escape="\\"),
                        RecordModel.game_name.like(q, escape="\\"),
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
                        stored_at=r.stored_at,
                        source=r.source,
                        tags=r.tags,
                    )
                )

            return QueryResult(records=records, total=total, query=query)

    async def semantic_search(
        self, query_vector: list[float], limit: int = 5, **kwargs: Any
    ) -> QueryResult:
        from src.storage.models import VectorType
        from sqlalchemy.types import JSON

        if isinstance(VectorType, type) and issubclass(VectorType, JSON):
            raise NotImplementedError("Semantic search requires pgvector")

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
                records.append(
                    StorageRecord(
                        key=r.key,
                        data=r.data,
                        metadata=r.metadata_,
                        stored_at=r.stored_at,
                        source=r.source,
                        tags=r.tags,
                    )
                )
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
                escaped_prefix = (
                    prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                )
                stmt = stmt.where(RecordModel.key.like(f"{escaped_prefix}%", escape="\\"))
            stmt = stmt.limit(limit)

            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def close(self) -> None:
        if self._owns_engine and self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
