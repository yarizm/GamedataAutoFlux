"""Pipeline 业务服务（修复清单第三批：拆薄 Scheduler Phase 2）。

本服务是 Pipeline 注册表的**所有者**：内存注册、持久化、按名解析
（内存 → 持久化 graph/pipeline → 内置模板投影）与删除全部在此。
Scheduler 只作执行引擎，经组合时持有的本服务实例读取 Pipeline；
历史调用方经 Scheduler 的兼容 shim 或容器 getter 访问，行为不变。

依赖以 callables 注入（repo/task_store/persist/后台任务/启动态），
本模块对 Scheduler 零依赖。
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any, Callable

from loguru import logger

if TYPE_CHECKING:
    from src.core.pipeline import Pipeline


class PipelineService:
    def __init__(
        self,
        *,
        get_pipeline_repo: "Callable[[], Any]",
        get_task_store: "Callable[[], Any]",
        persist_pipeline: "Callable[[Pipeline], Any]",
        create_background_task: "Callable[[Any], Any]",
        is_started: "Callable[[], bool]",
    ) -> None:
        self._get_pipeline_repo = get_pipeline_repo
        self._get_task_store = get_task_store
        self._persist_pipeline = persist_pipeline
        self._create_background_task = create_background_task
        self._is_started = is_started
        self._pipelines: dict[str, Pipeline] = {}
        self._lock = threading.Lock()

    @property
    def registry(self) -> dict[str, "Pipeline"]:
        """内部注册表视图（WorkerClaimCoordinator 等执行路径只读使用）。"""
        return self._pipelines

    def replace_registry(self, mapping: "dict[str, Pipeline]") -> None:
        """启动恢复：整体替换注册表（SchedulerStateService.restore_pipelines）。"""
        with self._lock:
            self._pipelines.clear()
            self._pipelines.update(mapping or {})

    # ---------- CRUD ----------

    def register_pipeline(self, pipeline: "Pipeline") -> None:
        """注册 Pipeline 配置；已启动时后台持久化。"""
        with self._lock:
            self._pipelines[pipeline.name] = pipeline
        if self._is_started():
            self._create_background_task(self._persist_pipeline(pipeline))
        logger.info(f"Pipeline 已注册: {pipeline.name}")

    def get_pipeline(self, name: str) -> "Pipeline | None":
        with self._lock:
            return self._pipelines.get(name)

    def get_all_pipelines(self) -> "list[Pipeline]":
        with self._lock:
            return list(self._pipelines.values())

    async def save_pipeline(self, pipeline: "Pipeline") -> None:
        """保存 Pipeline 并持久化。"""
        with self._lock:
            self._pipelines[pipeline.name] = pipeline
        logger.info(f"Pipeline saved: {pipeline.name}")
        await self._persist_pipeline(pipeline)

    async def delete_pipeline(self, name: str) -> bool:
        """删除 Pipeline 及其持久化快照。"""
        with self._lock:
            if name not in self._pipelines:
                return False
            del self._pipelines[name]
        repo = self._get_pipeline_repo()
        if repo is not None:
            await repo.delete(name)
        else:
            task_store = self._get_task_store()
            if task_store is not None:
                await task_store.delete(f"pipeline:{name}")
        logger.info(f"Pipeline deleted: {name}")
        return True

    async def resolve_pipeline(self, name: str) -> "Pipeline | None":
        """按名解析：内存 → 持久化 graph/pipeline 投影 → 内置模板物化。

        仓储/序列化错误必须穿透（None 只表示"不存在"），
        不得静默落到语义不同的内置模板。
        """
        if not name:
            return None
        pipeline = self.get_pipeline(name)
        if pipeline is not None:
            return pipeline

        repo = self._get_pipeline_repo()
        if repo is not None and hasattr(repo, "load_as_dag"):
            dag = await repo.load_as_dag(name)
            if dag is not None:
                from src.core.dag import dag_to_pipeline

                pipeline = dag_to_pipeline(dag)
                if not pipeline.steps:
                    return None
                with self._lock:
                    self._pipelines[name] = pipeline
                return pipeline

        # 内置模板（steam_basic / taptap_basic / ...）按需物化
        try:
            from src.core.pipeline_templates import PIPELINE_TEMPLATES

            template = next(
                (item for item in PIPELINE_TEMPLATES if item.get("id") == name),
                None,
            )
            if template is not None:
                from src.core.pipeline import Pipeline

                pipeline = Pipeline.from_config(
                    {
                        "name": name,
                        "description": template.get("description") or template.get("name") or name,
                        "steps": template.get("steps") or [],
                    }
                )
                if pipeline.steps:
                    with self._lock:
                        self._pipelines[name] = pipeline
                    return pipeline
        except (ImportError, TypeError, ValueError, KeyError, AttributeError, IndexError) as exc:
            logger.warning(f"resolve_pipeline template materialize failed for {name}: {exc}")
        return None
