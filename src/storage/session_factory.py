"""
共享数据库 Session 工厂

提供全局单例的 SQLAlchemy async engine + sessionmaker，
避免 AgentService、SQLAlchemyStorage、SQLAlchemySchedulerStore 各自创建独立连接池。

使用方式:
    # 在应用启动时初始化
    from src.storage.session_factory import init_shared_session_factory, get_session_factory
    await init_shared_session_factory()

    # 在需要的地方获取 session
    factory = get_session_factory()
    async with factory() as session:
        ...
"""

from __future__ import annotations


from loguru import logger
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)

from src.core.config import get as get_config
from src.storage.models import Base

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


async def init_shared_session_factory(url: str | None = None) -> async_sessionmaker[AsyncSession]:
    """
    初始化全局共享的 SQLAlchemy async engine 和 sessionmaker。

    只应在应用启动时调用一次。后续通过 get_session_factory() 获取。
    """
    global _engine, _session_factory

    if _session_factory is not None:
        return _session_factory

    if url is None:
        url = (
            get_config("database.sqlalchemy_url")
            or "postgresql+asyncpg://postgres:postgres@localhost:5432/autoflux"
        )

    logger.info(f"Initializing shared session factory with URL: {url}")
    _engine = create_async_engine(url, echo=False)
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)

    # 创建所有表（幂等操作）
    async with _engine.begin() as conn:
        if "postgresql" in url:
            from sqlalchemy import text

            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        await conn.run_sync(Base.metadata.create_all)
        # 运行 schema 迁移（新增列等）
        await _migrate_schema(conn)

    return _session_factory


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """
    获取已初始化的 sessionmaker。

    如果未初始化，抛出 RuntimeError。
    """
    if _session_factory is None:
        raise RuntimeError(
            "SharedSessionFactory not initialized. "
            "Call init_shared_session_factory() during app startup first."
        )
    return _session_factory


def get_engine() -> AsyncEngine:
    """获取已初始化的 async engine。"""
    if _engine is None:
        raise RuntimeError(
            "SharedSessionFactory not initialized. "
            "Call init_shared_session_factory() during app startup first."
        )
    return _engine


async def close_shared_session_factory() -> None:
    """关闭共享 engine，释放连接池。在应用关闭时调用。"""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
        logger.info("Shared session factory closed")


async def _migrate_schema(conn) -> None:
    """自动迁移：为旧版本数据库添加缺失的列和索引。"""
    from sqlalchemy import inspect as sa_inspect, text

    def _run(sync_conn):
        inspector = sa_inspect(sync_conn)
        existing = {col["name"] for col in inspector.get_columns("scheduler_states")}
        if "metadata" not in existing:
            sync_conn.execute(
                text("ALTER TABLE scheduler_states ADD COLUMN metadata JSON DEFAULT '{}'")
            )
            logger.info("Migrated scheduler_states: added metadata column")
        if "stored_at" not in existing:
            sync_conn.execute(text("ALTER TABLE scheduler_states ADD COLUMN stored_at TIMESTAMP"))
            logger.info("Migrated scheduler_states: added stored_at column")
        if "task_status" not in existing:
            sync_conn.execute(text("ALTER TABLE scheduler_states ADD COLUMN task_status VARCHAR"))
            logger.info("Migrated scheduler_states: added task_status column")
            try:
                sync_conn.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS ix_scheduler_states_task_status "
                        "ON scheduler_states (task_status)"
                    )
                )
                logger.info("Created index on scheduler_states.task_status")
            except Exception:
                pass  # Index may already exist

    await conn.run_sync(_run)
