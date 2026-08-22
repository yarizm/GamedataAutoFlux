"""Dynamic Playwright executor 生命周期测试（修复清单 P1）。

验收：一个 Collector 退出不会影响其他正在执行的 Collector。
旧实现的类级共享 `_SINGLE_EXECUTOR` 在 teardown 里被全局 shutdown，
并发场景下另一个实例的 `run_in_executor` 会直接 RuntimeError。
"""

import pytest

from autoflux_plugin_dynamic_playwright.collector import DynamicPlaywrightCollector


def test_collectors_get_independent_executors():
    a = DynamicPlaywrightCollector()
    b = DynamicPlaywrightCollector()
    assert a._get_executor() is not b._get_executor()


async def test_teardown_does_not_kill_other_collectors_executor():
    a = DynamicPlaywrightCollector()
    b = DynamicPlaywrightCollector()
    exec_a = a._get_executor()
    exec_b = b._get_executor()

    # a 退出（setup 未完成/失败路径：无 worker loop，但 executor 已存在）
    await a.teardown()

    # b 仍能正常调度任务
    assert exec_b.submit(lambda: 42).result(timeout=5) == 42
    # a 自己的 executor 已关闭且引用已清空
    with pytest.raises(RuntimeError):
        exec_a.submit(lambda: 1)
    assert a._executor is None


async def test_teardown_releases_executor_after_partial_setup():
    """setup 中途失败（executor 已建、worker loop 未就绪）teardown 也要回收。"""
    collector = DynamicPlaywrightCollector()
    executor = collector._get_executor()
    assert collector._worker_loop is None

    await collector.teardown()

    assert collector._executor is None
    with pytest.raises(RuntimeError):
        executor.submit(lambda: 1)


async def test_two_collectors_can_run_concurrently():
    """两个实例并发调度互不阻塞（各自独占 worker 线程）。"""
    a = DynamicPlaywrightCollector()
    b = DynamicPlaywrightCollector()

    fut_a = a._get_executor().submit(lambda: "a")
    fut_b = b._get_executor().submit(lambda: "b")
    assert fut_a.result(timeout=5) == "a"
    assert fut_b.result(timeout=5) == "b"

    await a.teardown()
    await b.teardown()
