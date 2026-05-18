"""数据查询与操作服务 — 供路由和 Agent 工具统一调用"""

from __future__ import annotations

from typing import Any

from loguru import logger

from src.core.task import Task
from src.storage.base import StorageRecord
from src.storage.local_store import LocalStorage
from src.services._utils import build_record_summary


class DataService:
    """封装 LocalStorage 查询、记录摘要、批量操作等可复用逻辑"""

    def __init__(self) -> None:
        self._store: LocalStorage | None = None

    async def _get_store(self) -> LocalStorage:
        if self._store is None:
            self._store = LocalStorage()
            await self._store.initialize()
        return self._store

    async def close(self) -> None:
        if self._store:
            await self._store.close()
            self._store = None

    # ---- 查询 ----

    async def query_records(
        self,
        query: str = "",
        limit: int = 10,
        offset: int = 0,
        order: str = "desc",
        **filters: Any,
    ):
        """分页查询记录"""
        store = await self._get_store()
        return await store.query(query, limit=limit, offset=offset, order=order, **filters)

    async def load_record(self, key: str) -> StorageRecord | None:
        store = await self._get_store()
        return await store.load(key)

    async def load_records_by_keys(self, keys: list[str]) -> list[StorageRecord]:
        store = await self._get_store()
        records: list[StorageRecord] = []
        for key in keys:
            record = await store.load(key)
            if record:
                records.append(record)
        return records

    async def load_source_records(self, limit: int = 1000) -> list[StorageRecord]:
        store = await self._get_store()
        result = await store.query("key:", limit=limit)
        return list(result.records)

    async def search_records(self, query: str, limit: int = 1000) -> list[StorageRecord]:
        store = await self._get_store()
        result = await store.query(query, limit=limit)
        return list(result.records)

    # ---- 摘要 ----

    def record_summary(self, record: StorageRecord) -> dict[str, Any] | None:
        return build_record_summary(record)

    # ---- 写入 ----

    async def save_record(self, record: StorageRecord) -> None:
        store = await self._get_store()
        await store.save(record)

    # ---- 删除 ----

    async def delete_record(self, key: str) -> bool:
        store = await self._get_store()
        return await store.delete(key)

    async def delete_records(self, keys: list[str]) -> dict[str, Any]:
        store = await self._get_store()
        failed: list[str] = []
        for key in keys:
            try:
                await store.delete(key)
            except Exception as e:
                logger.warning(f"Failed to delete record {key}: {e}")
                failed.append(key)
        deleted = len(keys) - len(failed)
        return {"deleted": deleted, "total": len(keys), "failed": failed}

    # ---- 导出 ----

    async def export_records(self, keys: list[str]) -> dict[str, Any]:
        records = await self.load_records_by_keys(keys)
        data: dict[str, Any] = {}
        for record in records:
            export_item: dict[str, Any] = {
                "key": record.key,
                "source": record.source,
                "metadata": record.metadata,
                "stored_at": record.stored_at.isoformat() if record.stored_at else None,
            }
            if record.data is not None:
                export_item["data"] = record.data
            data[record.key] = export_item
        return {"count": len(records), "records": data}

    # ---- 游戏/分组汇总 ----

    async def get_game_summaries(self, limit: int = 1000) -> list[dict[str, Any]]:
        """按游戏分组汇总记录"""
        from src.services._utils import extract_record_identity, record_group

        records = await self.load_source_records(limit=limit)
        grouped: dict[str, dict[str, Any]] = {}
        for record in records:
            identity = extract_record_identity(record)
            if not identity:
                continue
            group = record_group(record)
            grouped_key = (
                f"group:{group['group_id']}" if group.get("group_id") else identity["game_key"]
            )
            game = grouped.setdefault(
                grouped_key,
                {
                    "game_key": grouped_key,
                    "game_name": group.get("group_name") or identity["game_name"],
                    "app_id": identity.get("app_id"),
                    "total_records": 0,
                    "latest_stored_at": None,
                    "group_id": group.get("group_id", ""),
                    "group_name": group.get("group_name", ""),
                    "sources": [],
                },
            )
            game["total_records"] += 1
            from src.services._utils import max_iso

            game["latest_stored_at"] = max_iso(
                game["latest_stored_at"], record.stored_at.isoformat() if record.stored_at else None
            )
            source_name = record.source or "unknown"
            source_bucket = next((s for s in game["sources"] if s["name"] == source_name), None)
            if source_bucket is None:
                game["sources"].append(
                    {
                        "name": source_name,
                        "collector": record.metadata.get("collector", ""),
                        "count": 1,
                        "latest_stored_at": record.stored_at.isoformat()
                        if record.stored_at
                        else None,
                    }
                )
            else:
                source_bucket["count"] += 1
        return sorted(grouped.values(), key=lambda g: g["latest_stored_at"] or "", reverse=True)

    # ---- 刷新任务构建 ----

    def build_refresh_task(self, record: StorageRecord) -> Task:
        """从已有记录的元数据构建刷新采集任务"""
        from src.core.task import TaskTarget

        metadata = record.metadata or {}
        source_task = metadata.get("source_task", {}) if isinstance(metadata, dict) else {}

        merged_params: dict[str, Any] = dict(source_task.get("params", {}))
        merged_params.update(metadata.get("refresh_params", {}))

        task_name = (
            metadata.get("task_name") or source_task.get("task_name") or f"Refresh {record.key}"
        )

        targets_raw = source_task.get("targets") or metadata.get("targets") or []
        targets: list[dict[str, Any]] = []
        for t in targets_raw:
            if isinstance(t, dict):
                targets.append(
                    {
                        "name": t.get("name", ""),
                        "target_type": t.get("target_type", "game"),
                        "params": {**t.get("params", {}), **merged_params},
                    }
                )

        task = Task(
            name=task_name,
            targets=[
                TaskTarget(
                    name=t.get("name", ""),
                    target_type=t.get("target_type", "game"),
                    params=t.get("params", {}),
                )
                for t in targets
            ]
            if targets
            else [],
            pipeline_name=metadata.get("pipeline_name") or source_task.get("pipeline_name", ""),
            config={
                "report": {"enabled": metadata.get("report_enabled", False)},
                "data_group": metadata.get("data_group", ""),
            },
        )
        return task
