"""共享 Pydantic schema 模型，供 routes、services、agent 等层引用。"""

from .tasks import TaskPrecheckIssue, TaskPrecheckResponse

__all__ = ["TaskPrecheckIssue", "TaskPrecheckResponse"]
