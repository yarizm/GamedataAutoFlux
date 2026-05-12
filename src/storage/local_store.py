"""
本地存储实现

使用 SQLite 存储结构化数据（任务状态、元数据），
使用 JSON 文件存储采集结果原始数据。
"""

from __future__ import annotations

import json
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

        await self._db.commit()
        logger.info(f"本地存储已初始化: DB={self._db_path}, JSON={self._json_dir}")

    async def _migrate_schema(self) -> None:
        """添加 JSON 提取生成列和索引，对已有数据库幂等"""
        # 获取已有列名，避免对不存在的列建索引
        cursor = await self._db.execute("PRAGMA table_info(records)")
        existing_cols = {row[1] async for row in cursor}

        generated_columns = [
            ("collector", "$.collector"),
            ("game_name", "$.game_name"),
            ("app_id", "$.app_id"),
            ("group_id", "$.group_id"),
            ("task_id", "$.task_id"),
        ]
        for col, json_path in generated_columns:
            if col in existing_cols:
                continue
            try:
                await self._db.execute(
                    f'ALTER TABLE records ADD COLUMN {col} TEXT '
                    f'GENERATED ALWAYS AS (json_extract(metadata, \'{json_path}\')) STORED'
                )
                existing_cols.add(col)
            except Exception as e:
                logger.warning(f"Schema migration: failed to add column '{col}': {e}")

        # 确保 metadata 列有 JSON 合法性约束，防止非 JSON 数据导致生成列静默 NULL
        try:
            await self._db.execute(
                "ALTER TABLE records ADD CONSTRAINT metadata_must_be_json "
                "CHECK (json_valid(metadata))"
            )
        except Exception:
            # 约束已存在或数据库不支持，忽略
            pass

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

        await self._db.commit()

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

        # 写 SQLite 索引
        await self._db.execute(
            """
            INSERT OR REPLACE INTO records
            (key, source, metadata, tags, data_file, stored_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.key,
                record.source,
                json.dumps(record.metadata, ensure_ascii=False, default=str),
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

        cursor = await self._db.execute(
            "SELECT * FROM records WHERE key = ?", (key,)
        )
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
            conditions.append("(key LIKE ? OR source LIKE ?)")
            filter_params.extend([f"%{query}%", f"%{query}%"])

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
        cursor = await self._db.execute(
            "SELECT data_file FROM records WHERE key = ?", (key,)
        )
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
            cursor = await self._db.execute(
                "SELECT key FROM records LIMIT ?", (limit,)
            )

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
