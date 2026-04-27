"""
Pipeline 编排引擎

将采集器 → 处理器 → 存储 串联为可配置的流水线。
支持 Builder 模式构建、进度回调和错误处理。

使用示例:
    pipeline = (
        Pipeline("steam_monitor")
        .add_collector("steam", config={...})
        .add_processor("cleaner")
        .add_processor("embedding")
        .add_storage("sqlite")
        .add_storage("vector")
    )

    result = await pipeline.execute(task)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Awaitable

from loguru import logger

from src.collectors.base import BaseCollector, CollectTarget, CollectResult
from src.core.registry import registry
from src.core.task import Task, TaskStatus
from src.processors.base import BaseProcessor, ProcessInput, ProcessOutput
from src.storage.base import BaseStorage, StorageRecord


class StepType(str, Enum):
    """Pipeline 步骤类型"""
    COLLECTOR = "collector"
    PROCESSOR = "processor"
    STORAGE = "storage"


@dataclass
class PipelineStep:
    """Pipeline 步骤定义"""
    step_type: StepType
    component_name: str
    config: dict[str, Any] = field(default_factory=dict)
    instance: Any = None  # 运行时实例


@dataclass
class PipelineResult:
    """Pipeline 执行结果"""
    pipeline_name: str
    task_id: str
    success: bool = True
    collect_results: list[CollectResult] = field(default_factory=list)
    process_results: list[ProcessOutput] = field(default_factory=list)
    output_records: list[StorageRecord] = field(default_factory=list)
    storage_count: int = 0
    generated_report_id: str | None = None
    generated_report_title: str | None = None
    generated_report_matched_records: int = 0
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: datetime | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float | None:
        if self.completed_at is None:
            return None
        return (self.completed_at - self.started_at).total_seconds()


# 进度回调类型：(task_id, progress, message)
ProgressCallback = Callable[[str, float, str], Awaitable[None] | None]


class Pipeline:
    """
    Pipeline 编排引擎。

    将 collector → processor(s) → storage(s) 串联为流水线。
    通过 Builder 模式构建，执行时自动管理组件生命周期。
    """

    def __init__(self, name: str):
        self.name = name
        self.steps: list[PipelineStep] = []
        self._progress_callback: ProgressCallback | None = None

    def add_collector(self, name: str, config: dict[str, Any] | None = None) -> Pipeline:
        """添加采集步骤"""
        self.steps.append(
            PipelineStep(
                step_type=StepType.COLLECTOR,
                component_name=name,
                config=config or {},
            )
        )
        return self

    def add_processor(self, name: str, config: dict[str, Any] | None = None) -> Pipeline:
        """添加处理步骤"""
        self.steps.append(
            PipelineStep(
                step_type=StepType.PROCESSOR,
                component_name=name,
                config=config or {},
            )
        )
        return self

    def add_storage(self, name: str, config: dict[str, Any] | None = None) -> Pipeline:
        """添加存储步骤"""
        self.steps.append(
            PipelineStep(
                step_type=StepType.STORAGE,
                component_name=name,
                config=config or {},
            )
        )
        return self

    def on_progress(self, callback: ProgressCallback) -> Pipeline:
        """设置进度回调"""
        self._progress_callback = callback
        return self

    async def _report_progress(self, task_id: str, progress: float, message: str) -> None:
        """内部进度上报"""
        if self._progress_callback:
            result = self._progress_callback(task_id, progress, message)
            if asyncio.iscoroutine(result):
                await result

    def _get_collectors(self) -> list[PipelineStep]:
        return [s for s in self.steps if s.step_type == StepType.COLLECTOR]

    def _get_processors(self) -> list[PipelineStep]:
        return [s for s in self.steps if s.step_type == StepType.PROCESSOR]

    def _get_storages(self) -> list[PipelineStep]:
        return [s for s in self.steps if s.step_type == StepType.STORAGE]

    async def execute(self, task: Task) -> PipelineResult:
        """
        执行 Pipeline。

        流程:
            1. 实例化所有组件
            2. 调用 collector.collect() 获取原始数据
            3. 依次调用 processor.process() 处理数据
            4. 调用 storage.save() 持久化结果
            5. 清理组件资源

        Args:
            task: 要执行的任务

        Returns:
            PipelineResult 执行结果
        """
        result = PipelineResult(pipeline_name=self.name, task_id=task.id)
        collector_steps = self._get_collectors()
        processor_steps = self._get_processors()
        storage_steps = self._get_storages()

        # 总步骤数用于计算进度
        total_phases = len(collector_steps) + len(processor_steps) + len(storage_steps)
        if total_phases == 0:
            logger.warning(f"Pipeline [{self.name}] 没有配置任何步骤")
            result.completed_at = datetime.now()
            return result

        current_phase = 0

        try:
            # === Phase 1: 实例化组件 ===
            logger.info(f"Pipeline [{self.name}] 开始实例化组件...")
            await self._instantiate_steps(collector_steps, "collector")
            await self._instantiate_steps(processor_steps, "processor")
            await self._instantiate_steps(storage_steps, "storage")

            # === Phase 2: 采集 ===
            all_collect_results: list[CollectResult] = []
            targets = [
                CollectTarget(name=t.name, target_type=t.target_type, params=t.params)
                for t in task.targets
            ]

            for step in collector_steps:
                collector: BaseCollector = step.instance
                step_name = f"collect:{step.component_name}"
                task.add_step_log(step_name, TaskStatus.RUNNING, f"开始采集 ({len(targets)} 个目标)")
                logger.info(f"Pipeline [{self.name}] → 采集: {step.component_name}")

                await collector.setup(step.config)
                collect_results = await collector.collect_batch(targets)
                all_collect_results.extend(collect_results)

                success_count = sum(1 for r in collect_results if r.success)
                failed_results = [r for r in collect_results if not r.success]
                failed_error = "; ".join(
                    f"{r.target.name}: {r.error or '未知错误'}"
                    for r in failed_results
                )
                task.add_step_log(
                    step_name,
                    TaskStatus.SUCCESS if not failed_results else TaskStatus.FAILED,
                    f"采集完成: {success_count}/{len(collect_results)} 成功",
                    error=failed_error or None,
                )
                result.errors.extend(
                    f"collect:{step.component_name}:{r.target.name}: {r.error or '未知错误'}"
                    for r in failed_results
                )
                await collector.teardown()

                current_phase += 1
                progress = current_phase / total_phases * 0.9  # 预留 10% 给最终处理
                await self._report_progress(task.id, progress, f"采集完成: {step.component_name}")
                task.update_progress(progress)

            result.collect_results = all_collect_results
            successful_collects = [
                cr for cr in all_collect_results
                if cr.success and cr.data is not None
            ]
            if collector_steps and not successful_collects:
                result.success = False
                if not result.errors:
                    result.errors.append("所有采集目标均失败")
                result.completed_at = datetime.now()
                await self._report_progress(task.id, 1.0, "Pipeline 执行失败")
                task.update_progress(1.0, "Pipeline 执行失败")
                logger.warning(
                    f"Pipeline [{self.name}] 采集阶段无有效结果: {'; '.join(result.errors)}"
                )
                return result

            # === Phase 3: 处理 ===
            # 将采集结果转为处理器输入
            current_data: list[ProcessInput] = [
                ProcessInput(
                    data=cr.data,
                    metadata={**cr.metadata, "target": cr.target.name, "collected_at": cr.collected_at.isoformat()},
                    source=cr.target.name,
                )
                for cr in all_collect_results
                if cr.success and cr.data is not None
            ]

            for step in processor_steps:
                processor: BaseProcessor = step.instance
                step_name = f"process:{step.component_name}"
                task.add_step_log(step_name, TaskStatus.RUNNING, f"处理 {len(current_data)} 条数据")
                logger.info(f"Pipeline [{self.name}] → 处理: {step.component_name}")

                process_results = await processor.process_batch(current_data)
                result.process_results = process_results

                # 将处理结果转为下一个处理器的输入
                current_data = [
                    ProcessInput(
                        data=pr.data,
                        metadata=pr.metadata,
                        source=pr.processor_name,
                    )
                    for pr in process_results
                    if pr.success and pr.data is not None
                ]

                success_count = sum(1 for r in process_results if r.success)
                task.add_step_log(
                    step_name, TaskStatus.SUCCESS,
                    f"处理完成: {success_count}/{len(process_results)} 成功"
                )

                current_phase += 1
                progress = current_phase / total_phases * 0.9
                await self._report_progress(task.id, progress, f"处理完成: {step.component_name}")
                task.update_progress(progress)

            # === Phase 4: 存储 ===
            records = [
                StorageRecord(
                    key=f"{task.id}:{pd.source}:{i}",
                    data=pd.data,
                    metadata=_build_storage_metadata(task, pd.metadata),
                    source=pd.source,
                )
                for i, pd in enumerate(current_data)
            ]
            result.output_records = list(records)

            for step in storage_steps:
                storage: BaseStorage = step.instance
                step_name = f"storage:{step.component_name}"
                task.add_step_log(step_name, TaskStatus.RUNNING, f"存储 {len(records)} 条记录")
                logger.info(f"Pipeline [{self.name}] → 存储: {step.component_name}")

                await storage.initialize()
                await storage.save_batch(records)
                result.storage_count += len(records)

                task.add_step_log(
                    step_name, TaskStatus.SUCCESS,
                    f"存储完成: {len(records)} 条记录"
                )
                await storage.close()

                current_phase += 1
                progress = current_phase / total_phases * 0.9
                await self._report_progress(task.id, progress, f"存储完成: {step.component_name}")
                task.update_progress(progress)

            # === 完成 ===
            result.success = True
            result.completed_at = datetime.now()
            await self._report_progress(task.id, 1.0, "Pipeline 执行完成")
            task.update_progress(1.0, "Pipeline 执行完成")

            logger.info(
                f"Pipeline [{self.name}] 执行完成: "
                f"采集 {len(result.collect_results)} 条, "
                f"存储 {result.storage_count} 条, "
                f"耗时 {result.duration_seconds:.1f}s"
            )

        except Exception as e:
            logger.error(f"Pipeline [{self.name}] 执行失败: {e}")
            result.success = False
            result.errors.append(str(e))
            result.completed_at = datetime.now()

        return result

    async def _instantiate_steps(
        self, steps: list[PipelineStep], component_type: str
    ) -> None:
        """实例化步骤中的组件"""
        for step in steps:
            cls_ = registry.get(component_type, step.component_name)
            step.instance = cls_(config=step.config)
            logger.debug(f"  实例化 [{component_type}] {step.component_name}")

    def to_config(self) -> dict[str, Any]:
        """导出 Pipeline 配置（用于持久化和 API）"""
        return {
            "name": self.name,
            "steps": [
                {
                    "type": step.step_type.value,
                    "name": step.component_name,
                    "config": step.config,
                }
                for step in self.steps
            ],
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Pipeline:
        """从配置字典恢复 Pipeline"""
        pipeline = cls(name=config["name"])
        for step_cfg in config.get("steps", []):
            step_type = step_cfg["type"]
            name = step_cfg["name"]
            step_config = step_cfg.get("config", {})
            if step_type == "collector":
                pipeline.add_collector(name, step_config)
            elif step_type == "processor":
                pipeline.add_processor(name, step_config)
            elif step_type == "storage":
                pipeline.add_storage(name, step_config)
        return pipeline


def _build_storage_metadata(task: Task, metadata: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(metadata or {})
    target_params = enriched.get("target_params")
    if not isinstance(target_params, dict):
        target_params = {}
        target_name = enriched.get("target")
        for target in task.targets:
            if not target_name or target.name == target_name:
                target_params = dict(target.params)
                enriched.setdefault("target_type", target.target_type)
                break

    data_group = task.config.get("data_group", {})
    if not isinstance(data_group, dict):
        data_group = {}
    group_name = str(data_group.get("name") or data_group.get("group_name") or "").strip()
    group_id = str(data_group.get("id") or data_group.get("group_id") or group_name).strip()

    enriched["source_task"] = {
        "task_id": task.id,
        "task_name": task.name,
        "pipeline_name": task.pipeline_name,
        "collector_name": task.collector_name,
        "target": enriched.get("target", ""),
        "target_type": enriched.get("target_type", ""),
        "target_params": target_params,
        "task_config": task.config,
        "created_at": task.created_at.isoformat(),
    }
    if group_name or group_id:
        enriched["group_id"] = group_id or group_name
        enriched["group_name"] = group_name or group_id

    refresh = task.config.get("refresh", {})
    if isinstance(refresh, dict):
        for key in (
            "refresh_parent_key",
            "refresh_series_id",
            "refresh_run_id",
            "refresh_kind",
            "scheduled_job_id",
        ):
            if refresh.get(key) not in (None, ""):
                enriched[key] = refresh[key]

    return enriched
