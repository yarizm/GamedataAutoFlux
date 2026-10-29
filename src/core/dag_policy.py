"""DAG 节点级执行策略：重试、超时、失败处理。

对标 Dify 节点的 ``retry_config`` / ``error_strategy`` 与 n8n 的 Retry On Fail /
On Error。策略存在 ``NodeSpec.policy``，与组件自身的 ``config`` 分开：config
原样交给组件，policy 只由执行引擎消费。

失败处理（``on_error``）四种取值：

- ``fail``（默认，兼容旧图）：节点记为失败，它的普通出边不再激活（下游跳过，
  ``on_failure`` 条件边照常激活），其他分支继续跑，任务最终标记失败；
- ``stop``：节点失败后不再启动任何新节点，已在跑的节点跑完即结束，任务失败；
- ``continue``：失败视为"空输出"继续往下走，不计入任务失败；
- ``error_branch``：只激活 ``error`` 出端口（携带错误信息），普通出端口不激活，
  不计入任务失败。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

ERROR_PORT = "error"

ON_ERROR_FAIL = "fail"
ON_ERROR_STOP = "stop"
ON_ERROR_CONTINUE = "continue"
ON_ERROR_BRANCH = "error_branch"
ON_ERROR_MODES = (ON_ERROR_FAIL, ON_ERROR_STOP, ON_ERROR_CONTINUE, ON_ERROR_BRANCH)

MAX_RETRIES_LIMIT = 10
RETRY_INTERVAL_LIMIT_SECONDS = 600
TIMEOUT_LIMIT_SECONDS = 24 * 3600

_POLICY_FIELDS = frozenset({"max_retries", "retry_interval_seconds", "timeout_seconds", "on_error"})


@dataclass(frozen=True)
class NodePolicy:
    max_retries: int = 0
    retry_interval_seconds: float = 0.0
    timeout_seconds: float | None = None
    on_error: str = ON_ERROR_FAIL

    @property
    def max_attempts(self) -> int:
        return 1 + self.max_retries

    @property
    def handles_errors(self) -> bool:
        """失败是否被策略"吸收"（不计入任务失败）。"""
        return self.on_error in (ON_ERROR_CONTINUE, ON_ERROR_BRANCH)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_node_policy(raw: Any) -> list[str]:
    """返回问题描述列表；空列表表示合法。保存与执行前都会调用。"""
    if raw is None:
        return []
    if not isinstance(raw, dict):
        return ["policy must be an object"]

    issues: list[str] = []
    unknown = sorted(set(raw) - _POLICY_FIELDS)
    if unknown:
        issues.append(f"unknown policy fields: {', '.join(unknown)}")

    max_retries = raw.get("max_retries", 0)
    if not isinstance(max_retries, int) or isinstance(max_retries, bool):
        issues.append("max_retries must be an integer")
    elif not 0 <= max_retries <= MAX_RETRIES_LIMIT:
        issues.append(f"max_retries must be between 0 and {MAX_RETRIES_LIMIT}")

    interval = raw.get("retry_interval_seconds", 0)
    if not _is_number(interval):
        issues.append("retry_interval_seconds must be a number")
    elif not 0 <= interval <= RETRY_INTERVAL_LIMIT_SECONDS:
        issues.append(
            f"retry_interval_seconds must be between 0 and {RETRY_INTERVAL_LIMIT_SECONDS}"
        )

    timeout = raw.get("timeout_seconds")
    if timeout is not None:
        if not _is_number(timeout):
            issues.append("timeout_seconds must be a number")
        elif not 0 < timeout <= TIMEOUT_LIMIT_SECONDS:
            issues.append(f"timeout_seconds must be between 0 and {TIMEOUT_LIMIT_SECONDS}")

    on_error = raw.get("on_error", ON_ERROR_FAIL)
    if on_error not in ON_ERROR_MODES:
        issues.append(f"on_error must be one of: {', '.join(ON_ERROR_MODES)}")
    return issues


def parse_node_policy(raw: Any) -> NodePolicy:
    """解析策略；调用方应先 ``validate_node_policy``，这里对非法值回落默认。"""
    if not isinstance(raw, dict) or validate_node_policy(raw):
        return NodePolicy()
    timeout = raw.get("timeout_seconds")
    return NodePolicy(
        max_retries=int(raw.get("max_retries", 0)),
        retry_interval_seconds=float(raw.get("retry_interval_seconds", 0)),
        timeout_seconds=float(timeout) if timeout is not None else None,
        on_error=str(raw.get("on_error", ON_ERROR_FAIL)),
    )


def uses_error_port(raw: Any) -> bool:
    return isinstance(raw, dict) and raw.get("on_error") == ON_ERROR_BRANCH
