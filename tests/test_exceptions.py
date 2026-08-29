"""领域异常体系测试（修复清单 P3：整理异常体系）。

验收：
- 类型化异常的 error_code 经 classify_exception 直达，消息误导不影响；
- 标准库/HTTP 客户端网络异常无需关键词即判 network_unreachable；
- 第三方异常仍走消息启发式（向后兼容）；
- collect_batch 重试判定按异常类型：RetryableError 重试、
  ValidationError（DomainError）不重试。
"""

import httpx
import pytest

from src.collectors.base import BaseCollector, CollectResult, CollectTarget
from src.core.errors import ErrorCode, classify_exception
from src.core.task import Task, TaskTarget
from src.core.exceptions import (
    AuthenticationError,
    DomainError,
    InfrastructureError,
    LoginRequiredError,
    NetworkError,
    RateLimitError,
    RetryableError,
    ValidationError,
)


def test_hierarchy_structure():
    assert issubclass(DomainError, Exception)
    assert issubclass(ValidationError, DomainError)
    assert issubclass(RetryableError, InfrastructureError)
    assert issubclass(NetworkError, RetryableError)
    assert issubclass(RateLimitError, RetryableError)
    assert issubclass(LoginRequiredError, AuthenticationError)


def test_typed_error_code_wins_over_misleading_message():
    """类型声明的错误码优先于消息关键词（重试判定不被误导性文案带偏）。"""
    assert classify_exception(RateLimitError("totally a timeout, trust me")) is ErrorCode.rate_limited
    assert classify_exception(ValidationError("connection refused somewhere")) is ErrorCode.invalid_params
    assert classify_exception(LoginRequiredError("429 too many requests")) is ErrorCode.login_required


def test_explicit_error_code_override_on_instance():
    exc = NetworkError("x", error_code=ErrorCode.anti_bot_blocked)
    assert classify_exception(exc) is ErrorCode.anti_bot_blocked


def test_stdlib_and_httpx_network_exceptions_classified_by_type():
    assert classify_exception(TimeoutError("dead")) is ErrorCode.network_unreachable
    assert classify_exception(ConnectionError("reset")) is ErrorCode.network_unreachable
    assert classify_exception(httpx.ConnectTimeout("upstream")) is ErrorCode.network_unreachable
    assert classify_exception(httpx.ReadError("socket")) is ErrorCode.network_unreachable


def test_plain_exception_still_uses_message_heuristics():
    assert classify_exception(Exception("HTTP 429 too many requests")) is ErrorCode.rate_limited
    assert classify_exception(Exception("getaddrinfo failed")) is ErrorCode.network_unreachable
    assert classify_exception(Exception("weird third-party failure")) is ErrorCode.unknown


def _attempts_collector(exc_factory, times):
    class _ExcCollector(BaseCollector):
        calls = 0

        async def collect(self, target: CollectTarget) -> CollectResult:
            _ExcCollector.calls += 1
            if _ExcCollector.calls <= times:
                raise exc_factory()
            return CollectResult(target=target, success=True, data={"ok": True})

    return _ExcCollector


@pytest.mark.asyncio
async def test_retryable_exception_is_retried_and_recovers():
    collector = _attempts_collector(lambda: RateLimitError("slow down"), times=2)(
        config={"collect_retries": 3, "collect_retry_delay": 0}
    )
    results = await collector.collect_batch([CollectTarget(name="t")])
    assert len(results) == 1 and results[0].success is True
    assert collector.calls == 3


@pytest.mark.asyncio
async def test_domain_error_is_not_retried():
    """业务校验失败重试无意义：类型化 DomainError 不进重试循环。"""
    collector = _attempts_collector(lambda: ValidationError("bad target"), times=99)(
        config={"collect_retries": 3, "collect_retry_delay": 0}
    )
    results = await collector.collect_batch([CollectTarget(name="t")])
    assert len(results) == 1
    assert results[0].success is False
    assert results[0].error_code == ErrorCode.invalid_params.value
    assert collector.calls == 1  # 无重试


def test_new_infra_error_codes_typed():
    from src.core.exceptions import (
        AgentError,
        BrowserError,
        CollectorSetupError,
        DatabaseError,
        DAGValidationError,
        WorkerError,
    )
    from src.core.errors import ErrorCode

    assert classify_exception(DAGValidationError("bad dag")) is ErrorCode.dag_validation
    assert classify_exception(CollectorSetupError("missing")) is ErrorCode.collector_setup
    assert classify_exception(DatabaseError("conn")) is ErrorCode.database
    assert classify_exception(BrowserError("page crashed")) is ErrorCode.browser
    assert classify_exception(WorkerError("stale")) is ErrorCode.worker
    assert classify_exception(AgentError("llm down")) is ErrorCode.agent
    # BrowserError 可重试
    assert issubclass(BrowserError, RetryableError)


@pytest.mark.asyncio
async def test_dag_node_failure_carries_typed_code_to_result():
    """节点抛类型化异常 → DAGResult.error_code 贯通 PipelineResult。"""
    from src.core.dag import DAG, NodeSpec, PortSpec
    from src.core.dag_executor import DAGExecutor

    dag = DAG(
        name="typed_code_probe",
        nodes=[NodeSpec("c1", "collector", "_nonexistent_collector", {}, [], [PortSpec("records")], set())],
        edges=[],
    )
    result = await DAGExecutor().execute(Task(name="t", targets=[TaskTarget(name="g")]), dag)
    assert result.success is False
    assert result.error_code == "collector_setup"
