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

        await self._db.commit()
        logger.info(f"本地存储已初始化: DB={self._db_path}, JSON={self._json_dir}")

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
        """
        if self._db is None:
            await self.initialize()

        if query.startswith("source:"):
            source = query[7:]
            sql = "SELECT * FROM records WHERE source = ? ORDER BY stored_at DESC LIMIT ?"
            params = (source, limit)
        elif query.startswith("key:"):
            prefix = query[4:]
            sql = "SELECT * FROM records WHERE key LIKE ? ORDER BY stored_at DESC LIMIT ?"
            params = (f"{prefix}%", limit)
        else:
            sql = """
                SELECT * FROM records
                WHERE key LIKE ? OR source LIKE ?
                ORDER BY stored_at DESC LIMIT ?
            """
            params = (f"%{query}%", f"%{query}%", limit)

        cursor = await self._db.execute(sql, params)
        rows = await cursor.fetchall()

        records = []
        for row in rows:
            record = await self._row_to_record(row)
            if record:
                records.append(record)

        # 获取总数
        count_cursor = await self._db.execute(
            "SELECT COUNT(*) FROM records"
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
        except Exception as e:
            logger.warning(f"记录转换失败: {row['key']} - {e}")
            return None
