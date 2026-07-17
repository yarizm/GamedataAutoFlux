"""
任务模型与状态机

定义任务的生命周期状态和数据模型。
任务是工作流的最小执行单元，由 Pipeline 驱动执行。
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator

from src.core.errors import ErrorCode, resolve_error_code
from src.core.sensitive import redact_sensitive, redact_sensitive_text


class TaskStatus(str, Enum):
    """任务状态"""

    PENDING = "pending"  # 等待执行
    RUNNING = "running"  # 执行中
    SUCCESS = "success"  # 执行成功
    FAILED = "failed"  # 执行失败
    CANCELLED = "cancelled"  # 已取消
    RETRYING = "retrying"  # 重试中


# Coarse execution phase for list/detail/WS (string, not strict enum for forward compat)
PHASE_PENDING = "pending"
PHASE_RUNNING_COLLECT = "running_collect"
PHASE_RUNNING_PROCESS = "running_process"
PHASE_RUNNING_STORE = "running_store"
PHASE_RUNNING_REPORT = "running_report"
PHASE_SUCCESS = "success"
PHASE_FAILED = "failed"
PHASE_CANCELLED = "cancelled"
PHASE_RETRYING = "retrying"


def infer_phase_from_message(message: str) -> str | None:
    """Best-effort phase from progress/log message (pipeline English labels)."""
    m = (message or "").lower()
    if not m:
        return None
    if any(k in m for k in ("collect", "scrap", "fetch", "target")):
        return PHASE_RUNNING_COLLECT
    if any(k in m for k in ("process", "clean", "embed", "vector", "transform")):
        return PHASE_RUNNING_PROCESS
    if any(k in m for k in ("storage", "store", "persist", "write")):
        return PHASE_RUNNING_STORE
    if "report" in m:
        return PHASE_RUNNING_REPORT
    if "layer " in m and "done" in m:
        return PHASE_RUNNING_COLLECT
    return None


class TaskPriority(int, Enum):
    """任务优先级"""

    LOW = 0
    NORMAL = 5
    HIGH = 10
    URGENT = 20


class TaskTarget(BaseModel):
    """任务目标定义"""

    name: str = Field(..., description="目标名称")
    target_type: str = Field(default="default", description="目标类型")
    params: dict[str, Any] = Field(default_factory=dict, description="目标参数")


class TaskStepLog(BaseModel):
    """任务步骤日志"""

    step_name: str = Field(..., description="步骤名称")
    status: TaskStatus = Field(default=TaskStatus.PENDING, description="步骤状态")
    started_at: datetime | None = Field(default=None, description="开始时间")
    completed_at: datetime | None = Field(default=None, description="完成时间")
    message: str = Field(default="", description="日志信息")
    error: str | None = Field(default=None, description="错误信息")


class Task(BaseModel):
    """
    任务模型。

    任务状态转换:
        PENDING → RUNNING → SUCCESS
                         ↘ FAILED → RETRYING → RUNNING
                         ↘ CANCELLED
    """

    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12], description="任务 ID")
    name: str = Field(..., description="任务名称")
    description: str = Field(default="", description="任务描述")

    # 状态
    status: TaskStatus = Field(default=TaskStatus.PENDING, description="当前状态")
    progress: float = Field(default=0.0, ge=0.0, le=1.0, description="进度 0.0~1.0")
    phase: str | None = Field(default=PHASE_PENDING, description="粗粒度执行阶段")
    current_step: str | None = Field(default=None, description="当前步骤/节点名")
    progress_detail: dict[str, Any] | None = Field(
        default=None,
        description="进度细节（targets/layer 等，可选）",
    )

    # 配置
    pipeline_name: str = Field(default="", description="关联的 Pipeline 名称")
    collector_name: str = Field(default="", description="采集器名称")
    targets: list[TaskTarget] = Field(default_factory=list, description="采集目标列表")
    config: dict[str, Any] = Field(default_factory=dict, description="运行时配置")
    priority: TaskPriority = Field(default=TaskPriority.NORMAL, description="优先级")

    # 结果
    result: Any = Field(default=None, description="执行结果")
    stored_result_summary: dict[str, Any] | None = Field(
        default=None,
        exclude=True,
        description="Persisted lightweight result summary restored without the full result object",
    )
    error: str | None = Field(default=None, description="错误信息（脱敏人类可读）")
    error_code: str | None = Field(default=None, description="结构化错误码 ErrorCode 值")
    step_logs: list[TaskStepLog] = Field(default_factory=list, description="步骤日志")

    # 调度
    retry_count: int = Field(default=0, description="已重试次数")
    max_retries: int = Field(default=3, description="最大重试次数")
    cron_expr: str | None = Field(default=None, description="定时调度 cron 表达式")

    # 时间戳
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="创建时间"
    )
    started_at: datetime | None = Field(default=None, description="开始执行时间（每次重试重置）")
    first_started_at: datetime | None = Field(
        default=None, description="首次开始执行时间（跨重试保留）"
    )
    completed_at: datetime | None = Field(default=None, description="完成时间")

    @field_validator("created_at", mode="before")
    @classmethod
    def _ensure_created_at_utc(cls, v: Any) -> datetime:
        if isinstance(v, str):
            v = datetime.fromisoformat(v.replace("Z", "+00:00"))
        if isinstance(v, datetime) and v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

    def start(self) -> None:
        """标记任务开始执行"""
        if self.is_terminal:
            raise RuntimeError(f"Cannot start task {self.id} from terminal state {self.status}")
        self.status = TaskStatus.RUNNING
        now = datetime.now(timezone.utc)
        self.started_at = now
        if self.first_started_at is None:
            self.first_started_at = now  # 仅在首次启动时设置，重试不覆盖
        self.progress = 0.0
        self.phase = PHASE_RUNNING_COLLECT
        self.current_step = None
        self.progress_detail = None
        self.error = None
        self.error_code = None

    def complete(self, result: Any = None) -> None:
        """标记任务成功完成"""
        if self.is_terminal:
            return
        self.status = TaskStatus.SUCCESS
        self.completed_at = datetime.now(timezone.utc)
        self.progress = 1.0
        self.result = result
        self.phase = PHASE_SUCCESS
        self.current_step = None
        self.error = None
        self.error_code = None

    def fail(
        self,
        error: str,
        *,
        error_code: str | ErrorCode | None = None,
        exc: Exception | None = None,
    ) -> None:
        """标记任务失败；写入结构化 error_code（可推断）。"""
        if self.is_terminal:
            return
        self.status = TaskStatus.FAILED
        self.completed_at = datetime.now(timezone.utc)
        self.error = error
        code = resolve_error_code(error_code=error_code, error_message=error, exc=exc)
        self.error_code = code.value
        self.phase = PHASE_FAILED

    def cancel(self) -> None:
        """取消任务"""
        if self.is_terminal:
            return
        self.status = TaskStatus.CANCELLED
        self.completed_at = datetime.now(timezone.utc)
        self.phase = PHASE_CANCELLED

    def retry(self) -> bool:
        """
        尝试重试。

        Returns:
            是否可以重试（未超过最大次数）
        """
        if self.status != TaskStatus.FAILED:
            return False
        if self.max_retries is None or self.retry_count >= self.max_retries:
            return False
        self.retry_count += 1
        self.status = TaskStatus.RETRYING
        self.error = None
        self.error_code = None
        self.started_at = datetime.now(timezone.utc)
        self.completed_at = None
        self.phase = PHASE_RETRYING
        self.current_step = None
        return True

    def update_progress(
        self,
        progress: float,
        message: str = "",
        *,
        phase: str | None = None,
        current_step: str | None = None,
        progress_detail: dict[str, Any] | None = None,
    ) -> None:
        """更新进度；可选写入 phase / current_step / progress_detail。"""
        self.progress = min(max(progress, 0.0), 1.0)
        if phase:
            self.phase = phase
        else:
            inferred = infer_phase_from_message(message)
            if inferred and self.status == TaskStatus.RUNNING:
                self.phase = inferred
        if current_step is not None:
            self.current_step = current_step
        elif message and self.status == TaskStatus.RUNNING:
            # keep short operator-facing step label
            self.current_step = message[:120]
        if progress_detail is not None:
            self.progress_detail = progress_detail
        if message:
            self.step_logs.append(
                TaskStepLog(
                    step_name="progress_update",
                    status=TaskStatus.RUNNING,
                    started_at=datetime.now(timezone.utc),
                    message=message,
                )
            )
            if len(self.step_logs) > 500:
                self.step_logs = self.step_logs[-500:]

    def add_step_log(
        self,
        step_name: str,
        status: TaskStatus,
        message: str = "",
        error: str | None = None,
    ) -> None:
        """添加步骤日志"""
        self.step_logs.append(
            TaskStepLog(
                step_name=step_name,
                status=status,
                started_at=datetime.now(timezone.utc),
                completed_at=datetime.now(timezone.utc)
                if status in (TaskStatus.SUCCESS, TaskStatus.FAILED)
                else None,
                message=message,
                error=error,
            )
        )
        if len(self.step_logs) > 500:
            self.step_logs = self.step_logs[-500:]

    @property
    def is_terminal(self) -> bool:
        """是否处于终态"""
        return self.status in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED)

    @property
    def duration_seconds(self) -> float | None:
        """执行耗时（秒）— 仅当前尝试"""
        if self.started_at is None:
            return None
        end = self.completed_at or datetime.now(timezone.utc)
        return (end - self.started_at).total_seconds()

    @property
    def total_duration_seconds(self) -> float | None:
        """总执行耗时（秒）— 包含所有重试，从 first_started_at 算起"""
        if self.first_started_at is None:
            return None
        end = self.completed_at or datetime.now(timezone.utc)
        return (end - self.first_started_at).total_seconds()

    def to_summary(self) -> dict[str, Any]:
        """返回任务摘要（用于 API 响应）"""
        return {
            "id": self.id,
            "name": redact_sensitive_text(self.name),
            "status": self.status.value,
            "progress": self.progress,
            "phase": self.phase,
            "current_step": redact_sensitive_text(self.current_step) if self.current_step else None,
            "collector": redact_sensitive_text(self.collector_name),
            "targets_count": len(self.targets),
            "created_at": self.created_at.isoformat(),
            "duration": self.duration_seconds,
            "error": redact_sensitive_text(self.error) if self.error else None,
            "error_code": self.error_code,
        }

    def derived_error_presentation(self) -> dict[str, str | None]:
        """API/WS 层用：由 error_code 派生中文标题与建议（不落库）。"""
        if not self.error and not self.error_code and self.status != TaskStatus.FAILED:
            return {"error_code": None, "error_title": None, "error_suggestion": None}
        code = resolve_error_code(error_code=self.error_code, error_message=self.error)
        return {
            "error_code": code.value,
            "error_title": code.chinese_label,
            "error_suggestion": code.suggestion,
        }

    @property
    def result_summary(self) -> dict[str, Any] | None:
        """公开轻量结果摘要。"""
        return self._build_result_summary(redact=True)

    def to_storage_payload(self) -> dict[str, Any]:
        """导出可持久化的任务快照。"""
        payload = self.model_dump(mode="json", exclude={"result"})
        payload["result_summary"] = self._build_result_summary(redact=False)
        return payload

    def to_public_payload(self) -> dict[str, Any]:
        """导出可公开返回/广播的脱敏任务快照（含派生错误展示字段）。"""
        payload = redact_sensitive(self.to_storage_payload())
        payload.update(self.derived_error_presentation())
        return payload

    @classmethod
    def from_storage_payload(cls, payload: dict[str, Any]) -> "Task":
        """从持久化快照恢复任务对象。"""
        restored = dict(payload)
        stored_result_summary = restored.pop("result_summary", None)
        restored["result"] = None
        task = cls.model_validate(restored)
        if isinstance(stored_result_summary, dict):
            task.stored_result_summary = stored_result_summary
        return task

    def _build_result_summary(self, *, redact: bool) -> dict[str, Any] | None:
        """提取轻量结果摘要，避免将大对象直接持久化。"""
        if self.result is None:
            if not self.stored_result_summary:
                return None
            return (
                redact_sensitive(self.stored_result_summary)
                if redact
                else self.stored_result_summary
            )
        result = self.result
        if isinstance(result, dict):
            return redact_sensitive(result) if redact else result

        summary: dict[str, Any] = {}
        for field in (
            "success",
            "storage_count",
            "task_id",
            "pipeline_name",
            "errors",
            "collection_summary",
            "resume_state",
            "duration_seconds",
            "generated_report_id",
            "generated_report_title",
            "generated_report_matched_records",
        ):
            if hasattr(result, field):
                summary[field] = getattr(result, field)
        summary = summary or {"type": result.__class__.__name__}
        return redact_sensitive(summary) if redact else summary
