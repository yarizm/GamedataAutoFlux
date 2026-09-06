"""Alembic 迁移集成测试（真实 PostgreSQL）。

通过环境变量 `ALEMBIC_PG_URL` 提供专用测试库（例：
postgresql+asyncpg://postgres:postgres@localhost:5432/autoflux）；
未提供时跳过。CI 在 ubuntu job 里挂 pgvector/pgvector 服务运行本文件，
本地可用 docker 起一次性容器验证。

覆盖：
- 全新库 upgrade head 建出与模型一致的 schema；
- 重复执行幂等；
- 历史 create_all 遗留库自动 stamp 收编；
- 迁移失败异常向上传播（阻断启动）。
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import inspect as sa_inspect, text
from sqlalchemy.ext.asyncio import create_async_engine

PG_URL = os.environ.get("ALEMBIC_PG_URL", "").strip()

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not PG_URL, reason="需 ALEMBIC_PG_URL 指向专用 Postgres 测试库"),
]

_EXPECTED_TABLES = {"agent_sessions", "records", "scheduler_states"}


def _head_revision() -> str:
    from alembic.script import ScriptDirectory

    from src.storage.migrations import build_migration_config

    return ScriptDirectory.from_config(build_migration_config(PG_URL)).get_current_head()


async def _reset_schema(engine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))


async def _table_names(engine) -> set[str]:
    async with engine.connect() as conn:
        return set(await conn.run_sync(lambda c: sa_inspect(c).get_table_names()))


async def test_fresh_upgrade_creates_schema_matching_models():
    from src.storage.migrations import run_db_migrations

    engine = create_async_engine(PG_URL)
    try:
        await _reset_schema(engine)
        await run_db_migrations(engine, PG_URL)

        tables = await _table_names(engine)
        assert _EXPECTED_TABLES <= tables
        assert "alembic_version" in tables

        async with engine.connect() as conn:
            sched_cols = {
                col["name"]
                for col in await conn.run_sync(
                    lambda c: sa_inspect(c).get_columns("scheduler_states")
                )
            }
            version = (
                await conn.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar()
            embedding_type = (
                await conn.execute(
                    text(
                        "SELECT data_type FROM information_schema.columns "
                        "WHERE table_name='records' AND column_name='embedding'"
                    )
                )
            ).scalar()

        # 手写 ALTER 的列在 baseline 中齐备
        assert {"key", "state_type", "data", "metadata", "task_status", "stored_at"} <= sched_cols
        assert version == _head_revision()
        # pgvector 可用时为 vector（PG 里表现为扩展自定义类型 USER-DEFINED），
        # 否则按 models 回退语义降级为 jsonb
        assert embedding_type in {"vector", "jsonb", "USER-DEFINED"}
    finally:
        await engine.dispose()


async def test_rerun_is_idempotent():
    from src.storage.migrations import run_db_migrations

    engine = create_async_engine(PG_URL)
    try:
        await run_db_migrations(engine, PG_URL)
        tables_before = await _table_names(engine)
        await run_db_migrations(engine, PG_URL)
        tables_after = await _table_names(engine)
        assert tables_before == tables_after
    finally:
        await engine.dispose()


async def test_legacy_create_all_database_is_adopted():
    """无 alembic_version 但有业务表的库 → stamp head 收编，不做重复 DDL。"""
    from src.storage.migrations import run_db_migrations

    engine = create_async_engine(PG_URL)
    try:
        await _reset_schema(engine)
        # 模拟历史 create_all 留下的部分 schema
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "CREATE TABLE scheduler_states ("
                    "key varchar PRIMARY KEY, state_type varchar NOT NULL, data jsonb)"
                )
            )

        await run_db_migrations(engine, PG_URL)

        async with engine.connect() as conn:
            version = (
                await conn.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar()
            legacy_rows = (
                await conn.execute(text("SELECT count(*) FROM scheduler_states"))
            ).scalar()
            cols = {
                col["name"]
                for col in await conn.run_sync(
                    lambda c: sa_inspect(c).get_columns("scheduler_states")
                )
            }
        assert version == _head_revision()
        assert legacy_rows == 0  # 遗留表未被重建/破坏
        # legacy compat revision 必须把 create_all 早期 schema 缺的列补齐——
        # 收编不是"宣布升级完成"，而是真的把 schema 带到 baseline 形态
        assert {"metadata", "stored_at", "task_status"} <= cols
    finally:
        await engine.dispose()


async def test_migration_failure_propagates(monkeypatch):
    """迁移异常必须穿透（阻断应用启动），不得静默吞掉。"""
    from src.core.exceptions import DatabaseError
    from src.storage import migrations as migrations_mod
    from src.storage.migrations import run_db_migrations

    engine = create_async_engine(PG_URL)
    try:
        await _reset_schema(engine)

        def _boom(*args, **kwargs):
            raise RuntimeError("migration exploded")

        monkeypatch.setattr(migrations_mod.command, "upgrade", _boom)
        # 包装为 DatabaseError 后仍必须穿透（阻断应用启动）
        with pytest.raises(DatabaseError, match="migration exploded"):
            await run_db_migrations(engine, PG_URL)
    finally:
        await engine.dispose()
