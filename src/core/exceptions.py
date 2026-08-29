"""领域异常体系（修复清单 P3：整理异常体系）。

目标：重试、告警、错误码推断等决策优先依赖异常**类型**，而不是对
异常消息做关键词匹配。消息启发式（`errors.classify_exception` 的
字符串分支）保留为第三方异常的兜底。

分层约定：
- `AutofluxError`        根；可携带显式 `error_code`
- `DomainError`          业务规则违反（可恢复，由调用方处理）
- `ValidationError`      参数/配置无效
- `InfrastructureError`  基础设施故障（DB/网络/外部服务）
- `RetryableError`       基础设施故障中可重试的子类（重试决策的依据）
- `NetworkError` / `RateLimitError`  具体可重试场景
- `AuthenticationError`  凭证缺失；`LoginRequiredError` 需要登录态

新增可重试场景时：继承 `RetryableError` 并声明 `error_code`，
`_RETRYABLE_COLLECT_ERROR_CODES` 决定该错误码是否重试。
"""

from __future__ import annotations


from src.core.errors import ErrorCode


class AutofluxError(Exception):
    """项目异常根类。"""

    error_code: ErrorCode | None = None

    def __init__(self, message: str = "", *, error_code: ErrorCode | None = None) -> None:
        super().__init__(message)
        if error_code is not None:
            self.error_code = error_code


class DomainError(AutofluxError):
    """业务规则违反：请求本身不成立，重试无意义。"""


class ValidationError(DomainError):
    """参数/配置校验失败。"""

    error_code = ErrorCode.invalid_params


class InfrastructureError(AutofluxError):
    """基础设施故障（数据库/网络/外部服务）。"""


class RetryableError(InfrastructureError):
    """可重试的基础设施故障。"""


class NetworkError(RetryableError):
    """网络不可达/超时/DNS 失败。"""

    error_code = ErrorCode.network_unreachable


class RateLimitError(RetryableError):
    """触发频率限制/配额耗尽。"""

    error_code = ErrorCode.rate_limited


class DAGValidationError(AutofluxError):
    """DAG 结构校验失败（保存/执行前拦截）。"""

    error_code = ErrorCode.dag_validation


class CollectorSetupError(InfrastructureError):
    """采集器组件缺失或实例化失败。"""

    error_code = ErrorCode.collector_setup


class DatabaseError(InfrastructureError):
    """数据库连接/schema/迁移故障。"""

    error_code = ErrorCode.database


class BrowserError(RetryableError):
    """浏览器（Playwright）导航或页面操作故障。"""

    error_code = ErrorCode.browser


class WorkerError(AutofluxError):
    """Worker 注册/心跳/claim 协议故障。"""

    error_code = ErrorCode.worker


class AgentError(AutofluxError):
    """Agent 服务/LLM 调用故障。"""

    error_code = ErrorCode.agent


class AuthenticationError(AutofluxError):
    """凭证缺失或无效。"""

    error_code = ErrorCode.missing_credentials


class LoginRequiredError(AuthenticationError):
    """需要浏览器登录态（Cookie/持久化 profile 失效）。"""

    error_code = ErrorCode.login_required


def exception_error_code(exc: BaseException) -> ErrorCode | None:
    """从异常类型提取显式错误码；非 Autoflux 异常返回 None。"""
    code = getattr(exc, "error_code", None)
    if isinstance(code, ErrorCode):
        return code
    return None


def _stdlib_network_reason(exc: BaseException) -> bool:
    """标准库/常见客户端库的网络类异常（消息无关）。"""
    import httpx

    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return True
    if isinstance(
        exc,
        (
            httpx.TimeoutException,
            httpx.NetworkError,
            httpx.ProtocolError,
            httpx.InvalidURL,
        ),
    ):
        return True
    return False


def classify_typed_exception(exc: BaseException) -> ErrorCode | None:
    """类型驱动的错误码推断；未识别的类型返回 None（回落消息启发式）。"""
    explicit = exception_error_code(exc)
    if explicit is not None:
        return explicit
    try:
        if _stdlib_network_reason(exc):
            return ErrorCode.network_unreachable
    except ImportError:  # pragma: no cover - httpx 是必装依赖，防御性兜底
        pass
    return None
