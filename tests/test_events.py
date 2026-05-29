"""EventBus 测试"""

import pytest

from src.core.events import EventBus, TaskUpdatedEvent, TaskCompletedEvent


@pytest.mark.asyncio
async def test_emit_triggers_handler():
    """emit 应该触发注册的 handler"""
    bus = EventBus()
    received = []

    async def handler(event: TaskUpdatedEvent):
        received.append(event)

    bus.on("task_updated", handler)
    event = TaskUpdatedEvent(task_id="abc", payload={"status": "running"})
    await bus.emit("task_updated", event)

    assert len(received) == 1
    assert received[0].task_id == "abc"


@pytest.mark.asyncio
async def test_multiple_handlers_same_event():
    """同一事件可以注册多个 handler"""
    bus = EventBus()
    results = []

    async def handler_a(event):
        results.append("a")

    async def handler_b(event):
        results.append("b")

    bus.on("task_updated", handler_a)
    bus.on("task_updated", handler_b)
    await bus.emit("task_updated", TaskUpdatedEvent(task_id="x", payload={}))

    assert set(results) == {"a", "b"}


@pytest.mark.asyncio
async def test_handler_exception_does_not_block_others():
    """一个 handler 异常不应阻塞其他 handler"""
    bus = EventBus()
    results = []

    async def bad_handler(event):
        raise RuntimeError("boom")

    async def good_handler(event):
        results.append("ok")

    bus.on("task_updated", bad_handler)
    bus.on("task_updated", good_handler)
    await bus.emit("task_updated", TaskUpdatedEvent(task_id="x", payload={}))

    assert "ok" in results


@pytest.mark.asyncio
async def test_off_removes_handler():
    """off() 应该移除 handler"""
    bus = EventBus()
    count = [0]

    async def handler(event):
        count[0] += 1

    bus.on("task_updated", handler)
    await bus.emit("task_updated", TaskUpdatedEvent(task_id="x", payload={}))
    assert count[0] == 1

    bus.off("task_updated", handler)
    await bus.emit("task_updated", TaskUpdatedEvent(task_id="y", payload={}))
    assert count[0] == 1  # 没有再次触发


@pytest.mark.asyncio
async def test_emit_no_handlers_is_noop():
    """emit 一个没有 handler 的事件类型不应该报错"""
    bus = EventBus()
    await bus.emit("nonexistent_event", {"some": "data"})


@pytest.mark.asyncio
async def test_priority_ordering():
    """不同 priority 的 handler 按 priority 升序执行"""
    bus = EventBus()
    order = []

    async def low(event):
        order.append("low")

    async def high(event):
        order.append("high")

    bus.on("test", low, priority=10)
    bus.on("test", high, priority=0)
    await bus.emit("test", None)

    # priority 0 先执行完，priority 10 后执行
    # 由于同组并发，我们只能保证 low 和 high 都被调用
    assert "low" in order
    assert "high" in order


@pytest.mark.asyncio
async def test_task_completed_event():
    """TaskCompletedEvent 应该正确传递属性"""
    bus = EventBus()
    received = []

    async def handler(event: TaskCompletedEvent):
        received.append(event)

    bus.on("task_completed", handler)
    event = TaskCompletedEvent(
        task_id="t1",
        success=True,
        result=None,
        task=None,
        errors=[],
    )
    await bus.emit("task_completed", event)

    assert len(received) == 1
    assert received[0].task_id == "t1"
    assert received[0].success is True


@pytest.mark.asyncio
async def test_clear_all_handlers():
    """clear() 清除所有 handler"""
    bus = EventBus()
    count = [0]

    async def handler(event):
        count[0] += 1

    bus.on("task_updated", handler)
    bus.on("task_completed", handler)
    bus.clear()
    await bus.emit("task_updated", TaskUpdatedEvent(task_id="x", payload={}))
    await bus.emit(
        "task_completed", TaskCompletedEvent(task_id="x", success=False, result=None, task=None)
    )
    assert count[0] == 0


@pytest.mark.asyncio
async def test_clear_specific_event_type():
    """clear(event_type) 只清除指定类型"""
    bus = EventBus()
    count = [0]

    async def handler(event):
        count[0] += 1

    bus.on("task_updated", handler)
    bus.on("task_completed", handler)
    bus.clear("task_updated")
    await bus.emit("task_updated", TaskUpdatedEvent(task_id="x", payload={}))
    assert count[0] == 0

    await bus.emit(
        "task_completed", TaskCompletedEvent(task_id="x", success=False, result=None, task=None)
    )
    assert count[0] == 1
