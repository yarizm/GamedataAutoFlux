"""数据库迁移的程序化入口（Alembic）。

策略：
- 空 PostgreSQL → `upgrade head` 建 baseline；
- 历史 create_all 库（无 alembic_version 但有业务表）→ 自动 `stamp head`
  纳入版本管理后升级（幂等）；
- 已在管理中的库 → 常规升级。

关键迁移失败必须阻断应用启动：异常直接向上抛，由 lifespan 终止启动。
SQLite（测试/嵌入场景）不走本模块，仍用 create_all。
"""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncEngine

from src.core.sensitive import redact_sensitive_text

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "migrations"


def _baseline_revision(cfg: Config) -> str:
    """解析迁移链的起点 revision（down_revision 为 None 的那个）。

    收编遗留库时 stamp 起点（而非 head）：head 会随未来 revision 增长，
    stamp head 会让遗留库静默跳过全部后续迁移。
    """
    from alembic.script import ScriptDirectory

    script = ScriptDirectory.from_config(cfg)
    for revision in script.walk_revisions():
        if revision.down_revision is None:
            return revision.revision
    raise RuntimeError("no baseline revision found in migration chain")


def build_migration_config(url: str) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(_SCRIPTS_DIR))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


async def run_db_migrations(engine: AsyncEngine, url: str) -> None:
    """把数据库升级到最新 revision；遗留 create_all 库自动收编。"""
    from sqlalchemy import inspect as sa_inspect

    def _detect(sync_conn) -> tuple[bool, bool]:
        inspector = sa_inspect(sync_conn)
        tables = set(inspector.get_table_names())
        has_version_table = "alembic_version" in tables
        has_app_tables = bool(tables - {"alembic_version"})
        return has_version_table, has_app_tables

    async with engine.connect() as conn:
        has_version_table, has_app_tables = await conn.run_sync(_detect)
    cfg = build_migration_config(url)

    if not has_version_table and has_app_tables:
        baseline = _baseline_revision(cfg)
        logger.info(
            f"检测到 create_all 遗留库，stamp 至 baseline {baseline} 后纳入 Alembic 管理"
        )
        command.stamp(cfg, baseline)

    try:
        command.upgrade(cfg, "head")
    except Exception as exc:
        from src.core.exceptions import DatabaseError

        raise DatabaseError(f"database migration failed: {redact_sensitive_text(str(exc))}") from exc
    logger.info("数据库 schema 已升级至最新 revision")
