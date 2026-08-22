"""Alembic 迁移环境（异步 engine，程序化运行）。

URL 优先级：显式传入（set_main_option）> 环境变量 DATABASE_URL >
settings 的 database.sqlalchemy_url。程序化入口见
`src.storage.migrations.run_db_migrations`；CLI（alembic upgrade head）
仅用于开发诊断。
"""

from __future__ import annotations

import asyncio
import os

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from src.storage.models import Base

config = context.config

# 注意：不调用 fileConfig——应用内程序化运行时日志归 loguru 管，
# CLI 场景 alembic 自带输出也够用。

target_metadata = Base.metadata


def _resolve_url() -> str:
    url = config.get_main_option("sqlalchemy.url")
    if url:
        return url
    url = os.environ.get("DATABASE_URL", "")
    if url:
        return url
    from src.core.config import get as get_config

    return str(
        get_config("database.sqlalchemy_url")
        or "postgresql+asyncpg://postgres:postgres@localhost:5432/autoflux"
    )


def run_migrations_offline() -> None:
    context.configure(
        url=_resolve_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = _resolve_url()
    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    # 应用启动路径在已运行的事件循环内调用（init_shared_session_factory），
    # asyncio.run 不能嵌套：丢到独立线程的新循环执行，主循环仅等待结果。
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(run_async_migrations())
        return

    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(asyncio.run, run_async_migrations()).result()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
