"""
向量数据库存储层。

当前实现提供一个本地可持久化的语义检索方案：
  - 写入阶段保存原始记录、向量和可检索文本
  - 查询阶段对查询文本做 embedding，并用余弦相似度排序
  - 当记录未带向量时，回退到关键词匹配，保证兼容旧流程

后续如果接入 ChromaDB / Qdrant / Milvus，可以保留当前接口不变。
"""

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite
from loguru import logger

from src.core.config import get as get_config
from src.core.config import get_data_dir
from src.core.registry import registry
from src.processors.embedding import EmbeddingProcessor
from src.storage.base import BaseStorage, QueryResult, StorageRecord


@registry.register("storage", "vector")
class VectorStorage(BaseStorage):
    """
    本地向量存储。

    provider:
      - local: SQLite + JSON + 本地余弦相似度检索
      - stub: 兼容旧测试场景的内存实现
    """

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self._provider = self.config.get("provider") or get_config("vector_store.provider", "local")
        self._store: dict[str, StorageRecord] = {}
        self._db: aiosqlite.Connection | None = None
        self._db_path: Path | None = None
        self._json_dir: Path | None = None
        logger.info(f"向量存储初始化 (provider={self._provider})")

    async def initialize(self) -> None:
        """初始化向量存储。"""
        if self._provider == "stub":
            logger.warning("向量存储使用 Stub 实现，数据仅保存在内存中")
            return

        data_dir = get_data_dir()
        self._db_path = data_dir / self.config.get(
            "db_name",
            get_config("vector_store.local.db_name", "vector_store.db"),
        )
        self._json_dir = data_dir / self.config.get(
            "json_dir",
            get_config("vector_store.local.json_dir", "vector_records"),
        )
        self._json_dir.mkdir(parents=True, exist_ok=True)

        self._db = await aiosqlite.connect(str(self._db_path))
        self._db.row_factory = aiosqlite.Row
        await self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS vector_records (
                key TEXT PRIMARY KEY,
                source TEXT DEFAULT '',
                metadata TEXT DEFAULT '{}',
                tags TEXT DEFAULT '[]',
                data_file TEXT,
                embedding_text TEXT DEFAULT '',
                vector_json TEXT,
                dimension INTEGER DEFAULT 0,
                stored_at TEXT NOT NULL,
                updated_at TEXT
            )
            """
        )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_vector_records_source ON vector_records(source)"
        )
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_vector_records_stored_at ON vector_records(stored_at)"
        )
        await self._db.commit()
        logger.info(f"本地向量存储已初始化: DB={self._db_path}, JSON={self._json_dir}")

    async def save(self, record: StorageRecord) -> None:
        """保存记录到向量存储。"""
        if self._provider == "stub":
            self._store[record.key] = record
            logger.debug(f"[向量-Stub] 记录已保存: {record.key}")
            return

        if self._db is None:
            await self.initialize()

        payload = record.data
        if hasattr(payload, "model_dump"):
            payload = payload.model_dump(mode="json")

        embedding_text, vector = self._extract_embedding_payload(payload)
        json_filename = f"{record.key.replace(':', '_')}.json"
        json_path = self._json_dir / json_filename

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

        await self._db.execute(
            """
            INSERT OR REPLACE INTO vector_records
            (key, source, metadata, tags, data_file, embedding_text, vector_json, dimension, stored_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.key,
                record.source,
                json.dumps(record.metadata, ensure_ascii=False, default=str),
                json.dumps(record.tags, ensure_ascii=False, default=str),
                json_filename,
                embedding_text,
                json.dumps(vector) if vector is not None else None,
                len(vector) if vector else 0,
                record.stored_at.isoformat(),
                datetime.now().isoformat(),
            ),
        )
        await self._db.commit()
        logger.debug(f"[向量-Local] 记录已保存: {record.key}")

    async def load(self, key: str) -> StorageRecord | None:
        """按键加载记录。"""
        if self._provider == "stub":
            return self._store.get(key)

        if self._db is None:
            await self.initialize()

        cursor = await self._db.execute(
            "SELECT * FROM vector_records WHERE key = ?",
            (key,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return await self._row_to_record(row)

    async def query(self, query: str, limit: int = 10, **kwargs: Any) -> QueryResult:
        """
        语义查询。

        优先走向量相似度；若没有可用向量，则回退到关键词匹配。
        """
        if self._provider == "stub":
            return self._keyword_query_stub(query, limit)

        if self._db is None:
            await self.initialize()

        rows = await self._fetch_rows(limit=max(limit * 10, 100))
        if not rows:
            return QueryResult(records=[], total=0, query=query)

        vector_rows = [row for row in rows if row["vector_json"]]
        min_score = float(kwargs.get("min_score", -1.0))

        if vector_rows:
            try:
                query_vector = await self._embed_query(query)
                scored_rows = []
                for row in vector_rows:
                    row_vector = json.loads(row["vector_json"])
                    score = self._cosine_similarity(query_vector, row_vector)
                    if score >= min_score:
                        scored_rows.append((score, row))
                scored_rows.sort(key=lambda item: item[0], reverse=True)

                records = []
                for score, row in scored_rows[:limit]:
                    record = await self._row_to_record(row)
                    if record is None:
                        continue
                    record.metadata = {**record.metadata, "similarity_score": score}
                    records.append(record)

                return QueryResult(records=records, total=len(scored_rows), query=query)
            except Exception as exc:
                logger.warning(f"[向量-Local] 语义查询失败，回退关键词检索: {exc}")

        return await self._keyword_query_local(query, rows, limit)

    async def delete(self, key: str) -> bool:
        """删除记录。"""
        if self._provider == "stub":
            return self._store.pop(key, None) is not None

        if self._db is None:
            await self.initialize()

        record = await self.load(key)
        if record is None:
            return False

        json_path = self._json_dir / f"{key.replace(':', '_')}.json"
        if json_path.exists():
            json_path.unlink()

        await self._db.execute("DELETE FROM vector_records WHERE key = ?", (key,))
        await self._db.commit()
        return True

    async def list_keys(self, prefix: str = "", limit: int = 100) -> list[str]:
        """列出键。"""
        if self._provider == "stub":
            keys = [k for k in self._store if k.startswith(prefix)] if prefix else list(self._store.keys())
            return keys[:limit]

        if self._db is None:
            await self.initialize()

        if prefix:
            cursor = await self._db.execute(
                "SELECT key FROM vector_records WHERE key LIKE ? ORDER BY stored_at DESC LIMIT ?",
                (f"{prefix}%", limit),
            )
        else:
            cursor = await self._db.execute(
                "SELECT key FROM vector_records ORDER BY stored_at DESC LIMIT ?",
                (limit,),
            )
        return [row[0] for row in await cursor.fetchall()]

    async def close(self) -> None:
        """关闭连接。"""
        if self._provider == "stub":
            logger.debug(f"[向量-Stub] 关闭，共 {len(self._store)} 条记录")
            return

        if self._db is not None:
            await self._db.close()
            self._db = None

    async def _fetch_rows(self, limit: int) -> list[aiosqlite.Row]:
        cursor = await self._db.execute(
            """
            SELECT * FROM vector_records
            ORDER BY stored_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return await cursor.fetchall()

    async def _row_to_record(self, row: aiosqlite.Row) -> StorageRecord | None:
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
        except Exception as exc:
            logger.warning(f"[向量-Local] 记录恢复失败: {row['key']} - {exc}")
            return None

    async def _keyword_query_local(
        self,
        query: str,
        rows: list[aiosqlite.Row],
        limit: int,
    ) -> QueryResult:
        query_lower = query.lower()
        matched_rows = []
        for row in rows:
            haystack = " ".join(
                [
                    row["key"] or "",
                    row["source"] or "",
                    row["embedding_text"] or "",
                ]
            ).lower()
            if query_lower in haystack:
                matched_rows.append(row)

        records = []
        for row in matched_rows[:limit]:
            record = await self._row_to_record(row)
            if record is not None:
                record.metadata = {**record.metadata, "match_type": "keyword"}
                records.append(record)

        return QueryResult(records=records, total=len(matched_rows), query=query)

    def _keyword_query_stub(self, query: str, limit: int) -> QueryResult:
        matches = []
        query_lower = query.lower()
        for record in self._store.values():
            text = str(record.data).lower()
            if query_lower in text or query_lower in record.key.lower():
                matches.append(record)
                if len(matches) >= limit:
                    break
        return QueryResult(records=matches, total=len(matches), query=query)

    async def _embed_query(self, query: str) -> list[float]:
        embedding_config = {
            "provider": self.config.get("embedding_provider") or get_config("embedding.provider", "stub"),
            "model": self.config.get("embedding_model"),
            "api_key": self.config.get("embedding_api_key"),
            "base_url": self.config.get("embedding_base_url"),
            "dimensions": self.config.get("embedding_dimensions") or get_config("embedding.qwen.dimensions", 1024),
            "text_type": "query",
            "output_type": self.config.get("embedding_output_type") or get_config(
                "embedding.qwen.output_type",
                "dense",
            ),
            "instruct": self.config.get("query_instruct") or get_config("embedding.qwen.instruct"),
            "fallback_to_stub": self.config.get("fallback_to_stub", True),
        }
        embedder = EmbeddingProcessor(embedding_config)
        embeddings, _, _ = await embedder._generate_embedding([query])
        return embeddings[0]

    def _extract_embedding_payload(self, payload: Any) -> tuple[str, list[float] | None]:
        if isinstance(payload, dict):
            vector = payload.get("embedding")
            if isinstance(vector, list) and vector:
                return payload.get("embedding_text", ""), [float(item) for item in vector]
            if isinstance(payload.get("embedding_text"), str):
                return payload["embedding_text"], None
        return str(payload), None

    def _cosine_similarity(self, left: list[float], right: list[float]) -> float:
        if not left or not right or len(left) != len(right):
            return -1.0

        numerator = sum(l * r for l, r in zip(left, right))
        left_norm = math.sqrt(sum(l * l for l in left))
        right_norm = math.sqrt(sum(r * r for r in right))
        if left_norm == 0 or right_norm == 0:
            return -1.0
        return numerator / (left_norm * right_norm)
