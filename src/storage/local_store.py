"""
本地存储实现

使用 SQLite 存储结构化数据（任务状态、元数据），
使用 JSON 文件存储采集结果原始数据。
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite
from loguru import logger

from src.core.config import get_data_dir
from src.core.registry import registry
from src.storage.base import BaseStorage, StorageRecord, QueryResult


@registry.register("storage", "local")
class LocalStorage(BaseStorage):
    """
    本地存储: SQLite + JSON 文件。

    - 元数据和索引存储在 SQLite
    - 大数据体存储为 JSON 文件
    """

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self._db_path: Path | None = None
        self._json_dir: Path | None = None
        self._db: aiosqlite.Connection | None = None

    async def initialize(self) -> None:
        """初始化数据库和目录"""
        data_dir = get_data_dir()

        self._db_path = data_dir / self.config.get("db_name", "autoflux.db")
        self._json_dir = data_dir / self.config.get("json_dir", "results")
        self._json_dir.mkdir(parents=True, exist_ok=True)

        self._db = await aiosqlite.connect(str(self._db_path))
        self._db.row_factory = aiosqlite.Row

        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS records (
                key TEXT PRIMARY KEY,
                source TEXT DEFAULT '',
                metadata TEXT DEFAULT '{}',
                tags TEXT DEFAULT '[]',
                data_file TEXT,
                stored_at TEXT NOT NULL,
                updated_at TEXT
            )
        """)

        await self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_records_source ON records(source)
        """)
        await self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_records_stored_at ON records(stored_at)
        """)

        await self._migrate_schema()

        # 初始化 FTS5 全文搜索表 (必须在_migrate_schema之后，此时game_name列已存在)
        await self._db.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS records_fts USING fts5(
                key, source, game_name,
                content=records, content_rowid=rowid
            )
        """)
        # 同步触发器
        await self._db.execute("""
            CREATE TRIGGER IF NOT EXISTS records_ai AFTER INSERT ON records BEGIN
                INSERT INTO records_fts(rowid, key, source, game_name)
                VALUES (new.rowid, new.key, new.source, new.game_name);
            END;
        """)
        await self._db.execute("""
            CREATE TRIGGER IF NOT EXISTS records_ad AFTER DELETE ON records BEGIN
                INSERT INTO records_fts(records_fts, rowid, key, source, game_name)
                VALUES ('delete', old.rowid, old.key, old.source, old.game_name);
            END;
        """)
        await self._db.execute("""
            CREATE TRIGGER IF NOT EXISTS records_au AFTER UPDATE ON records BEGIN
                INSERT INTO records_fts(records_fts, rowid, key, source, game_name)
                VALUES ('delete', old.rowid, old.key, old.source, old.game_name);
                INSERT INTO records_fts(rowid, key, source, game_name)
                VALUES (new.rowid, new.key, new.source, new.game_name);
            END;
        """)
        
        # 初始同步（如果 FTS 表为空但记录表有数据）
        fts_count = (await (await self._db.execute("SELECT COUNT(*) FROM records_fts")).fetchone())[0]
        if fts_count == 0:
            await self._db.execute("""
                INSERT INTO records_fts(rowid, key, source, game_name)
                SELECT rowid, key, source, game_name FROM records
            """)

        await self._db.commit()
        logger.info(f"本地存储已初始化: DB={self._db_path}, JSON={self._json_dir}")

    async def _migrate_schema(self) -> None:
        """添加元数据提取列和索引，对已有数据库幂等。"""
        meta_columns = ["collector", "game_name", "app_id", "group_id", "task_id"]

        cursor = await self._db.execute("PRAGMA table_info(records)")
        existing_cols = {row[1] async for row in cursor}

        all_meta_present = all(col in existing_cols for col in meta_columns)
        if all_meta_present:
            # 检测是否有 STORED 生成列残留（生成列不允许 UPDATE）
            has_generated = False
            for col in meta_columns:
                try:
                    await self._db.execute(
                        f"UPDATE records SET {col} = {col} WHERE key = "
                        f"(SELECT key FROM records LIMIT 1) AND {col} IS NOT NULL LIMIT 1"
                    )
                except Exception as e:
                    if "generated" in str(e).lower():
                        has_generated = True
                        break
            if not has_generated:
                await self._ensure_indexes(existing_cols)
                await self._db.commit()
                return

        logger.info("Schema migration: rebuilding records table with correct schema")
        await self._rebuild_table(meta_columns, existing_cols)
        await self._db.commit()

    async def _rebuild_table(self, meta_columns: list[str], existing_cols: set[str]) -> None:
        """通过 创建新表→复制数据→替换 的方式重建 records 表。

        这是 SQLite 最兼容的 schema 变更方式，不依赖 DROP COLUMN 或 table_xinfo。
        整个过程用一个显式事务包裹，防止中途崩溃导致数据丢失。
        """
        # 检测上次迁移是否中途崩溃（records_new 残留且有数据）
        cursor = await self._db.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='records_new'"
        )
        row = await cursor.fetchone()
        if row and row[0] > 0:
            stale_count = 0
            try:
                cnt_cursor = await self._db.execute("SELECT COUNT(*) FROM records_new")
                stale_count = (await cnt_cursor.fetchone())[0]
            except Exception:
                pass
            if stale_count > 0:
                logger.warning(
                    f"Found stale records_new with {stale_count} rows from interrupted migration, recovering"
                )
                await self._db.execute("DROP TABLE IF EXISTS records")
                await self._db.execute("ALTER TABLE records_new RENAME TO records")
                # 回填已在下面执行，继续正常流程
                # existing_cols 需要更新为 records_new 的列
                cursor2 = await self._db.execute("PRAGMA table_info(records)")
                existing_cols = {r[1] async for r in cursor2}
            else:
                await self._db.execute("DROP TABLE IF EXISTS records_new")

        base_cols = ["key", "source", "metadata", "tags", "data_file", "stored_at", "updated_at"]
        all_cols = base_cols + meta_columns

        # 确定实际可复制的列
        src_cols = [c for c in all_cols if c in existing_cols]
        src_placeholders = ", ".join(src_cols)

        # 为新表生成列定义
        col_defs = [
            "key TEXT PRIMARY KEY",
            "source TEXT DEFAULT ''",
            "metadata TEXT DEFAULT '{}'",
            "tags TEXT DEFAULT '[]'",
            "data_file TEXT",
            "stored_at TEXT NOT NULL",
            "updated_at TEXT",
        ]
        for col in meta_columns:
            col_defs.append(f"{col} TEXT DEFAULT ''")

        await self._db.execute("BEGIN IMMEDIATE")
        try:
            await self._db.execute("DROP TABLE IF EXISTS records_new")
            await self._db.execute(f"CREATE TABLE records_new ({', '.join(col_defs)})")

            await self._db.execute(
                f"INSERT INTO records_new ({src_placeholders}) "
                f"SELECT {src_placeholders} FROM records"
            )

            await self._db.execute("DROP TABLE records")
            await self._db.execute("ALTER TABLE records_new RENAME TO records")
            await self._db.execute("COMMIT")
        except Exception:
            await self._db.execute("ROLLBACK")
            raise

        # 回填元数据列
        for col in meta_columns:
            try:
                await self._db.execute(
                    f"UPDATE records SET {col} = "
                    f"COALESCE(json_extract(metadata, '$.{col}'), '') "
                    f"WHERE {col} IS NULL OR {col} = ''"
                )
            except Exception:
                pass

        # 重建索引
        for idx_col in ["source", "stored_at"] + meta_columns[:3]:
            try:
                await self._db.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_records_{idx_col} ON records({idx_col})"
                )
            except Exception:
                pass

        logger.info("Schema migration: table rebuilt successfully")

    async def _ensure_indexes(self, existing_cols: set[str]) -> None:
        index_cols = ["collector", "game_name", "app_id"]
        for col in index_cols:
            if col not in existing_cols:
                continue
            try:
                await self._db.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_records_{col} ON records({col})"
                )
            except Exception as e:
                logger.warning(f"Schema migration: failed to create index on '{col}': {e}")

    async def save(self, record: StorageRecord) -> None:
        """保存记录: 元数据入 SQLite，数据体存 JSON 文件"""
        if self._db is None:
            await self.initialize()

        # 写 JSON 数据文件
        json_filename = f"{record.key.replace(':', '_')}.json"
        json_path = self._json_dir / json_filename
        data_to_save = record.data

        # Pydantic 模型序列化兼容
        if hasattr(data_to_save, "model_dump"):
            data_to_save = data_to_save.model_dump()

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=2, default=str)

        # 写 SQLite 索引（含元数据提取列，便于过滤查询）
        metadata_json = json.dumps(record.metadata, ensure_ascii=False, default=str)
        meta = record.metadata or {}
        try:
            await self._db.execute(
                """
                INSERT OR REPLACE INTO records
                (key, source, metadata, tags, data_file, stored_at, updated_at,
                 collector, game_name, app_id, group_id, task_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.key,
                    record.source,
                    metadata_json,
                    json.dumps(record.tags, ensure_ascii=False),
                    json_filename,
                    record.stored_at.isoformat(),
                    datetime.now().isoformat(),
                    str(meta.get("collector", "")),
                    str(meta.get("game_name", "")),
                    str(meta.get("app_id", "")),
                    str(meta.get("group_id", "")),
                    str(meta.get("task_id", "")),
                ),
            )
        except Exception as meta_insert_err:
            # 防御：如果元数据列不存在或是生成列，回退到基础 INSERT
            logger.warning(
                f"Failed to insert with meta columns for {record.key}: {meta_insert_err}; "
                f"falling back to basic INSERT"
            )
            await self._db.execute(
                """
                INSERT OR REPLACE INTO records
                (key, source, metadata, tags, data_file, stored_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.key,
                    record.source,
                    metadata_json,
                    json.dumps(record.tags, ensure_ascii=False),
                    json_filename,
                    record.stored_at.isoformat(),
                    datetime.now().isoformat(),
                ),
            )
        await self._db.commit()
        logger.debug(f"记录已保存: {record.key}")

    async def load(self, key: str) -> StorageRecord | None:
        """按键加载记录"""
        if self._db is None:
            await self.initialize()

        cursor = await self._db.execute("SELECT * FROM records WHERE key = ?", (key,))
        row = await cursor.fetchone()
        if row is None:
            return None

        return await self._row_to_record(row)

    async def query(self, query: str, limit: int = 10, **kwargs: Any) -> QueryResult:
        """
        查询记录。

        支持按 source 和 key 前缀过滤:
            query="source:steam"  → 按 source 筛选
            query="key:task123"   → 按 key 前缀筛选
            query="any text"     → 模糊搜索 key 和 source

        额外精确过滤参数（通过生成列）:
            collector, game_name, app_id, group_id, task_id
        """
        if self._db is None:
            await self.initialize()

        offset = max(0, int(kwargs.get("offset", 0) or 0))
        limit = max(0, int(limit or 0))
        order = str(kwargs.get("order", "desc") or "desc").lower()
        sort_dir = "ASC" if order == "asc" else "DESC"

        conditions: list[str] = []
        filter_params: list[Any] = []

        if query.startswith("source:"):
            conditions.append("source = ?")
            filter_params.append(query[7:])
        elif query.startswith("key:"):
            conditions.append("key LIKE ?")
            filter_params.append(f"{query[4:]}%")
        elif query.strip():
            # 使用 FTS5 全文搜索，清理特殊字符避免语法错误
            safe_query = re.sub(r'[^\w\s]', ' ', query)
            safe_query = re.sub(r'\s+', ' ', safe_query).strip()
            if safe_query:
                conditions.append("records.rowid IN (SELECT rowid FROM records_fts WHERE records_fts MATCH ?)")
                filter_params.append(safe_query)
            else:
                # 纯特殊字符查询回退到 LIKE
                conditions.append("(key LIKE ? OR source LIKE ? OR game_name LIKE ?)")
                filter_params.extend([f"%{query}%", f"%{query}%", f"%{query}%"])

        # 精确字段过滤（通过生成列）
        for field in ("collector", "game_name", "app_id", "group_id", "task_id"):
            value = kwargs.get(field, "")
            if value:
                conditions.append(f"{field} = ?")
                filter_params.append(str(value))

        where_sql = " AND ".join(conditions) if conditions else "1=1"

        if limit > 0:
            sql = f"""
                SELECT * FROM records
                WHERE {where_sql}
                ORDER BY stored_at {sort_dir}
                LIMIT ? OFFSET ?
            """
            params = (*filter_params, limit, offset)
        else:
            sql = f"""
                SELECT * FROM records
                WHERE {where_sql}
                ORDER BY stored_at {sort_dir}
            """
            params = (*filter_params,)

        cursor = await self._db.execute(sql, params)
        rows = await cursor.fetchall()

        records = []
        for row in rows:
            record = await self._row_to_record(row)
            if record:
                records.append(record)

        # 获取总数
        count_cursor = await self._db.execute(
            f"SELECT COUNT(*) FROM records WHERE {where_sql}",
            filter_params,
        )
        total = (await count_cursor.fetchone())[0]

        return QueryResult(records=records, total=total, query=query)

    async def delete(self, key: str) -> bool:
        """删除记录"""
        if self._db is None:
            await self.initialize()

        # 删除 JSON 文件
        cursor = await self._db.execute("SELECT data_file FROM records WHERE key = ?", (key,))
        row = await cursor.fetchone()
        if row and row["data_file"]:
            json_path = self._json_dir / row["data_file"]
            if json_path.exists():
                json_path.unlink()

        # 删除 SQLite 记录
        await self._db.execute("DELETE FROM records WHERE key = ?", (key,))
        await self._db.commit()
        return True

    async def list_keys(self, prefix: str = "", limit: int = 100) -> list[str]:
        """列出键"""
        if self._db is None:
            await self.initialize()

        if prefix:
            cursor = await self._db.execute(
                "SELECT key FROM records WHERE key LIKE ? LIMIT ?",
                (f"{prefix}%", limit),
            )
        else:
            cursor = await self._db.execute("SELECT key FROM records LIMIT ?", (limit,))

        return [row[0] for row in await cursor.fetchall()]

    async def close(self) -> None:
        """关闭数据库连接"""
        if self._db:
            await self._db.close()
            self._db = None
            logger.debug("本地存储连接已关闭")

    async def _row_to_record(self, row) -> StorageRecord | None:
        """将数据库行转为 StorageRecord"""
        try:
            data = None
            if row["data_file"]:
                json_path = self._json_dir / row["data_file"]
                if json_path.exists():
                    with open(json_path, "r", encoding="utf-8") as f:
                        data = json.load(f)

            return StorageRecord(
                key=row["key"],
                data=data,
                metadata=json.loads(row["metadata"]) if row["metadata"] else {},
                stored_at=datetime.fromisoformat(row["stored_at"]),
                source=row["source"] or "",
                tags=json.loads(row["tags"]) if row["tags"] else [],
            )
        except (json.JSONDecodeError, KeyError, ValueError, OSError) as e:
            logger.error(f"记录数据损坏，无法转换: {row['key']} - {e}")
            return None
