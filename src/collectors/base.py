"""
数据采集器抽象基类

所有数据采集器必须继承 BaseCollector 并实现其抽象方法。
通过 @registry.register("collector", "name") 装饰器注册到系统。

生命周期:
    setup() → collect() [可多次调用] → teardown()
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from loguru import logger

from src.core.errors import ErrorCode, classify_exception
from src.core.sensitive import redact_sensitive, redact_sensitive_text


_RETRYABLE_COLLECT_ERROR_CODES = {
    ErrorCode.network_unreachable.value,
    ErrorCode.rate_limited.value,
    ErrorCode.unknown.value,
}


class CollectTarget(BaseModel):
    """采集目标"""

    model_config = ConfigDict(extra="allow")

    name: str = Field(..., description="目标名称，如游戏名")
    target_type: str = Field(default="default", description="目标类型")
    params: dict[str, Any] = Field(default_factory=dict, description="额外参数")


class CollectResult(BaseModel):
    """采集结果"""

    target: CollectTarget = Field(..., description="对应的采集目标")
    data: Any = Field(default=None, description="采集到的数据")
    metadata: dict[str, Any] = Field(default_factory=dict, description="元数据")
    collected_at: datetime = Field(default_factory=datetime.now, description="采集时间")
    success: bool = Field(default=True, description="是否成功")
    error: str | None = Field(default=None, description="错误信息")
    error_code: str | None = Field(default=None, description="结构化错误码")
    raw_data: Any = Field(default=None, description="原始数据（用于调试）")

    def to_summary(self) -> dict[str, Any]:
        """生成带错误分类信息的摘要，供 Agent 工具和 WebUI 使用"""
        base: dict[str, Any] = {
            "target": redact_sensitive_text(self.target.name),
            "target_type": redact_sensitive_text(self.target.target_type),
            "success": self.success,
            "collected_at": self.collected_at.isoformat(),
        }
        if self.target.params:
            base["target_params"] = redact_sensitive(self.target.params)
        retry_summary = _collect_retry_summary(self.metadata)
        if retry_summary:
            base["retry"] = retry_summary

        if self.success:
            base["status"] = "ok"
            return base

        if self.error_code:
            try:
                code = ErrorCode(self.error_code)
            except ValueError:
                code = ErrorCode.unknown
                logger.warning(f"Unknown error_code value: {self.error_code!r}")
        else:
            code = ErrorCode.unknown
        base.update(
            {
                "status": "error",
                "error": redact_sensitive_text(self.error or ""),
                "error_code": code.value,
                "error_label": code.chinese_label,
                "suggestion": code.suggestion,
                "severity": code.severity,
            }
        )
        return base


class BaseCollector(ABC):
    """
    数据采集器抽象基类。

    子类实现示例:
        @registry.register("collector", "steam")
        class SteamCollector(BaseCollector):
            async def setup(self, config):
                self.session = httpx.AsyncClient(...)

            async def collect(self, target):
                resp = await self.session.get(...)
                return CollectResult(target=target, data=resp.json())

            async def teardown(self):
                await self.session.aclose()
    """

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}
        self._is_setup = False

    async def setup(self, config: dict[str, Any] | None = None) -> None:
        """
        初始化采集器（如建立连接、登录等）。
        默认实现为 no-op，子类按需覆盖。

        Args:
            config: 运行时配置，会合并到 self.config
        """
        if config:
            self.config.update(config)
        self._is_setup = True

    @abstractmethod
    async def collect(self, target: CollectTarget) -> CollectResult:
        """
        执行数据采集。

        Args:
            target: 采集目标

        Returns:
            CollectResult 包含采集到的数据
        """
        ...

    async def collect_batch(self, targets: list[CollectTarget]) -> list[CollectResult]:
        """
        批量采集。默认逐个调用 collect()。

        可通过 config.batch_concurrency 显式开启有限并发；默认值为 1，
        以避免破坏依赖登录态、浏览器页面或外部频控的采集器。

        Args:
            targets: 采集目标列表

        Returns:
            结果列表
        """
        concurrency = _coerce_positive_int(
            self.config.get("batch_concurrency"),
            default=_resolve_batch_concurrency_default(self),
        )
        collect_timeout = _resolve_collect_timeout(self)
        collect_retries = _resolve_collect_retries(self)
        collect_retry_delay = _resolve_collect_retry_delay(self)
        max_attempts = collect_retries + 1

        async def _safe_collect(target: CollectTarget) -> CollectResult:
            last_retry_error = ""
            last_retry_error_code = ""
            for attempt in range(1, max_attempts + 1):
                try:
                    if collect_timeout > 0:
                        result = await asyncio.wait_for(
                            self.collect(target),
                            timeout=collect_timeout,
                        )
                    else:
                        result = await self.collect(target)
                    result = _finalize_collect_result(
                        self,
                        result,
                        collect_timeout=collect_timeout,
                        attempt=attempt,
                        max_attempts=max_attempts,
                        last_retry_error=last_retry_error,
                        last_retry_error_code=last_retry_error_code,
                    )
                    if (
                        result.success
                        or attempt >= max_attempts
                        or not _is_retryable_collect_error(result.error_code or "")
                    ):
                        return result
                    last_retry_error = result.error or result.error_code or ""
                    last_retry_error_code = result.error_code or ""
                    await _sleep_before_retry(
                        self,
                        target,
                        attempt=attempt,
                        retry_delay=collect_retry_delay,
                        error=result.error or result.error_code or "",
                    )
                    continue
                except Exception as e:
                    error = _collection_error_message(e, collect_timeout=collect_timeout)
                    code = classify_exception(Exception(error))
                    if attempt < max_attempts and _is_retryable_collect_error(code.value):
                        last_retry_error = error
                        last_retry_error_code = code.value
                        await _sleep_before_retry(
                            self,
                            target,
                            attempt=attempt,
                            retry_delay=collect_retry_delay,
                            error=error,
                        )
                        continue
                    return CollectResult(
                        target=target,
                        success=False,
                        error=error,
                        error_code=code.value,
                        metadata=_build_failure_metadata(
                            self,
                            target,
                            code.value,
                            collect_timeout=collect_timeout,
                            attempt=attempt,
                            max_attempts=max_attempts,
                            last_retry_error=last_retry_error,
                            last_retry_error_code=last_retry_error_code,
                        ),
                    )

            return CollectResult(target=target, success=False, error="Collect retry exhausted")

        if concurrency <= 1 or len(targets) <= 1:
            return [await _safe_collect(target) for target in targets]

        semaphore = asyncio.Semaphore(concurrency)

        async def _guarded_collect(target: CollectTarget) -> CollectResult:
            async with semaphore:
                return await _safe_collect(target)

        results = await asyncio.gather(*(_guarded_collect(target) for target in targets))
        return results

    async def teardown(self) -> None:
        """
        清理资源（如关闭连接）。
        默认实现为 no-op，子类按需覆盖。
        """
        self._is_setup = False

    def validate_config(self, config: dict[str, Any] | None = None) -> bool:
        """
        校验配置是否满足采集器需求。
        默认返回 True，子类按需覆盖。

        Args:
            config: 要校验的配置

        Returns:
            是否合法
        """
        return True

    async def __aenter__(self):
        await self.setup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.teardown()
        return False


def _coerce_positive_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _build_failure_metadata(
    collector: BaseCollector,
    target: CollectTarget,
    error_code: str,
    *,
    collect_timeout: float = 0,
    attempt: int = 1,
    max_attempts: int = 1,
    last_retry_error: str = "",
    last_retry_error_code: str = "",
) -> dict[str, Any]:
    metadata = {
        "collector": _collector_config_key(collector),
        "target": redact_sensitive_text(target.name),
        "target_type": redact_sensitive_text(target.target_type),
        "target_params": redact_sensitive(target.params),
        "error_code": error_code,
        "collect_timeout": collect_timeout if collect_timeout > 0 else None,
        "attempts": attempt if max_attempts > 1 else None,
        "max_attempts": max_attempts if max_attempts > 1 else None,
        "retry_attempts": max(0, attempt - 1) if max_attempts > 1 else None,
        "last_retry_error": (
            redact_sensitive_text(last_retry_error)
            if attempt > 1 and last_retry_error
            else None
        ),
        "last_retry_error_code": (
            last_retry_error_code if attempt > 1 and last_retry_error_code else None
        ),
    }
    return {key: value for key, value in metadata.items() if value not in (None, "", {})}


def _finalize_collect_result(
    collector: BaseCollector,
    result: CollectResult,
    *,
    collect_timeout: float,
    attempt: int,
    max_attempts: int,
    last_retry_error: str = "",
    last_retry_error_code: str = "",
) -> CollectResult:
    result.metadata = redact_sensitive(result.metadata or {})
    if result.success:
        _annotate_retry_metadata(
            result,
            attempt=attempt,
            max_attempts=max_attempts,
            last_retry_error=last_retry_error,
            last_retry_error_code=last_retry_error_code,
        )
        return result

    code = result.error_code or classify_exception(Exception(result.error or "")).value
    result.error_code = code
    if result.error:
        result.error = redact_sensitive_text(result.error)
    result.metadata = {
        **(result.metadata or {}),
        **_build_failure_metadata(
            collector,
            result.target,
            code,
            collect_timeout=collect_timeout,
            attempt=attempt,
            max_attempts=max_attempts,
            last_retry_error=last_retry_error,
            last_retry_error_code=last_retry_error_code,
        ),
    }
    return result


def _collect_retry_summary(metadata: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        return {}
    attempts = _coerce_non_negative_int(metadata.get("attempts"), default=0)
    max_attempts = _coerce_non_negative_int(metadata.get("max_attempts"), default=0)
    retry_attempts = _coerce_non_negative_int(metadata.get("retry_attempts"), default=0)
    if attempts <= 0 and max_attempts <= 0 and retry_attempts <= 0:
        return {}
    summary = {
        "attempts": attempts or 1,
        "max_attempts": max_attempts or attempts or 1,
        "retry_attempts": retry_attempts,
    }
    last_retry_error = str(metadata.get("last_retry_error") or "").strip()
    if last_retry_error:
        summary["last_retry_error"] = redact_sensitive_text(last_retry_error)
    last_retry_error_code = str(metadata.get("last_retry_error_code") or "").strip()
    if last_retry_error_code:
        summary["last_retry_error_code"] = last_retry_error_code
    return {
        key: value
        for key, value in summary.items()
        if not isinstance(value, int) or value >= 0
    }


def _annotate_retry_metadata(
    result: CollectResult,
    *,
    attempt: int,
    max_attempts: int,
    last_retry_error: str = "",
    last_retry_error_code: str = "",
) -> None:
    if max_attempts <= 1:
        return
    retry_metadata = {
        **(result.metadata or {}),
        "attempts": attempt,
        "max_attempts": max_attempts,
        "retry_attempts": attempt - 1,
    }
    if attempt > 1 and last_retry_error:
        retry_metadata["last_retry_error"] = redact_sensitive_text(last_retry_error)
    if attempt > 1 and last_retry_error_code:
        retry_metadata["last_retry_error_code"] = last_retry_error_code
    result.metadata = retry_metadata


async def _sleep_before_retry(
    collector: BaseCollector,
    target: CollectTarget,
    *,
    attempt: int,
    retry_delay: float,
    error: str,
) -> None:
    sleep_time = max(0.0, retry_delay * attempt)
    logger.warning(
        "[Collector] retrying collector={} target={} attempt={} delay={}s error={}",
        _collector_config_key(collector) or collector.__class__.__name__,
        redact_sensitive_text(target.name),
        attempt + 1,
        f"{sleep_time:g}",
        redact_sensitive_text(error),
    )
    if sleep_time > 0:
        await asyncio.sleep(sleep_time)


def _is_retryable_collect_error(error_code: str) -> bool:
    return error_code in _RETRYABLE_COLLECT_ERROR_CODES


def _collection_error_message(exc: Exception, *, collect_timeout: float = 0) -> str:
    if isinstance(exc, TimeoutError) and collect_timeout > 0:
        return f"Collect timeout after {collect_timeout:g}s"
    return redact_sensitive_text(str(exc))


def _resolve_batch_concurrency_default(collector: BaseCollector) -> int:
    try:
        from src.core.config import get as get_config

        global_default = _coerce_positive_int(
            get_config("collector.batch_concurrency", 1),
            default=1,
        )
        collector_key = _collector_config_key(collector)
        if collector_key:
            return _coerce_positive_int(
                get_config(f"{collector_key}.batch_concurrency", global_default),
                default=global_default,
            )
        return global_default
    except Exception:
        return 1


def _resolve_collect_timeout(collector: BaseCollector) -> float:
    explicit = _first_non_negative_float(
        collector.config,
        ("collect_timeout_seconds", "collect_timeout"),
    )
    if explicit is not None:
        return explicit

    try:
        from src.core.config import get as get_config

        missing = object()
        global_value = get_config("collector.collect_timeout_seconds", missing)
        if global_value is missing:
            global_value = get_config("collector.collect_timeout", 0)
        global_default = _coerce_non_negative_float(global_value, default=0)

        collector_key = _collector_config_key(collector)
        if collector_key:
            collector_value = get_config(f"{collector_key}.collect_timeout_seconds", missing)
            if collector_value is missing:
                collector_value = get_config(f"{collector_key}.collect_timeout", missing)
            if collector_value is not missing:
                return _coerce_non_negative_float(collector_value, default=global_default)
        return global_default
    except Exception:
        return 0


def _resolve_collect_retries(collector: BaseCollector) -> int:
    explicit = _first_non_negative_int(
        collector.config,
        ("collect_retries", "collect_retry_count"),
    )
    if explicit is not None:
        return explicit

    try:
        from src.core.config import get as get_config

        missing = object()
        global_value = get_config("collector.collect_retries", missing)
        if global_value is missing:
            global_value = get_config("collector.collect_retry_count", 0)
        global_default = _coerce_non_negative_int(global_value, default=0)

        collector_key = _collector_config_key(collector)
        if collector_key:
            collector_value = get_config(f"{collector_key}.collect_retries", missing)
            if collector_value is missing:
                collector_value = get_config(f"{collector_key}.collect_retry_count", missing)
            if collector_value is not missing:
                return _coerce_non_negative_int(collector_value, default=global_default)
        return global_default
    except Exception:
        return 0


def _resolve_collect_retry_delay(collector: BaseCollector) -> float:
    explicit = _first_non_negative_float(
        collector.config,
        ("collect_retry_delay_seconds", "collect_retry_delay"),
    )
    if explicit is not None:
        return explicit

    try:
        from src.core.config import get as get_config

        missing = object()
        global_value = get_config("collector.collect_retry_delay_seconds", missing)
        if global_value is missing:
            global_value = get_config("collector.collect_retry_delay", 1.0)
        global_default = _coerce_non_negative_float(global_value, default=1.0)

        collector_key = _collector_config_key(collector)
        if collector_key:
            collector_value = get_config(f"{collector_key}.collect_retry_delay_seconds", missing)
            if collector_value is missing:
                collector_value = get_config(f"{collector_key}.collect_retry_delay", missing)
            if collector_value is not missing:
                return _coerce_non_negative_float(collector_value, default=global_default)
        return global_default
    except Exception:
        return 1.0


def _coerce_non_negative_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _coerce_non_negative_float(value: Any, *, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _first_non_negative_float(values: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        if key not in values:
            continue
        try:
            parsed = float(values[key])
        except (TypeError, ValueError):
            continue
        if parsed >= 0:
            return parsed
    return None


def _first_non_negative_int(values: dict[str, Any], keys: tuple[str, ...]) -> int | None:
    for key in keys:
        if key not in values:
            continue
        try:
            parsed = int(values[key])
        except (TypeError, ValueError):
            continue
        if parsed >= 0:
            return parsed
    return None


def _collector_config_key(collector: BaseCollector) -> str:
    explicit = collector.config.get("collector_name") or collector.config.get("collector")
    if explicit:
        return str(explicit)

    class_name = collector.__class__.__name__
    known_names = {
        "SteamCollector": "steam",
        "SteamDiscussionsCollector": "steam_discussions",
        "TapTapCollector": "taptap",
        "QimaiCollector": "qimai",
        "OfficialSiteCollector": "official_site",
        "MonitorCollector": "monitor",
        "GoogleTrendsCollector": "gtrends",
        "DynamicPlaywrightCollector": "dynamic_playwright",
    }
    return known_names.get(class_name, "")
