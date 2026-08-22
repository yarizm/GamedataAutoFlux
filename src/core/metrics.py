"""轻量进程内运行时指标：计数器与观测值，`GET /api/metrics` 暴露。

不引入第三方指标后端；进程重启归零属于预期语义——任务历史等长期统计
以数据库为准，本模块提供执行链路的即时可观测性（成功率、重试、
DAG 节点结果、collector 耗时、fallback 与副作用失败计数）。

键名约定：`name{label=v,...}`（label 排序保证稳定）。业务代码只经
`metrics.inc/observe` 埋点，快照仅给 API 层用。
"""

from __future__ import annotations

import threading
import time
from typing import Any


class MetricsRegistry:
    """线程安全的计数器 + 观测值聚合（count/sum/avg/max）。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, float] = {}
        self._observations: dict[str, tuple[int, float, float]] = {}  # (count, sum, max)

    @staticmethod
    def _key(name: str, labels: dict[str, Any]) -> str:
        if not labels:
            return name
        inner = ",".join(f"{k}={labels[k]}" for k in sorted(labels))
        return f"{name}{{{inner}}}"

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        key = self._key(name, labels)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0.0) + value

    def observe(self, name: str, value: float, **labels: Any) -> None:
        key = self._key(name, labels)
        with self._lock:
            count, total, peak = self._observations.get(key, (0, 0.0, 0.0))
            self._observations[key] = (count + 1, total + value, max(peak, value))

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            timers = {
                key: {
                    "count": count,
                    "sum": round(total, 6),
                    "avg": round(total / count, 6) if count else 0.0,
                    "max": round(peak, 6),
                }
                for key, (count, total, peak) in self._observations.items()
            }
            return {
                "counters": dict(self._counters),
                "timers": timers,
            }

    def reset(self) -> None:
        """仅测试/诊断用。"""
        with self._lock:
            self._counters.clear()
            self._observations.clear()

    def timer(self, name: str, **labels: Any) -> "_Timer":
        """`with metrics.timer("collector_duration_seconds", type="collector"):`"""
        return _Timer(self, name, **labels)


# 进程级单例（与 event_bus 同层级；不参与业务装配，故不进 container）
metrics = MetricsRegistry()


class _Timer:
    """observe 用上下文计时器。"""

    __slots__ = ("_registry", "_name", "_labels", "_start")

    def __init__(self, registry: MetricsRegistry, name: str, **labels: Any) -> None:
        self._registry = registry
        self._name = name
        self._labels = labels
        self._start = 0.0

    def __enter__(self) -> "_Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self._registry.observe(self._name, time.perf_counter() - self._start, **self._labels)
