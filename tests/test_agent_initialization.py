import pytest

from src.agent.agent_initialization import (
    build_histories_loaded_result,
    ensure_agent_runtime_ready,
    ensure_histories_loaded_once,
)


def test_build_histories_loaded_result_requests_resave_for_pending_recovery() -> None:
    result = build_histories_loaded_result(
        histories_loaded=True,
        history_load_failed=False,
        pending_history_recovery_thread_count=2,
    )

    assert result.histories_loaded is True
    assert result.history_load_failed is False
    assert result.needs_resave is False
    assert result.should_resave_pending_recovery is True


def test_build_histories_loaded_result_skips_resave_when_load_failed() -> None:
    result = build_histories_loaded_result(
        histories_loaded=True,
        history_load_failed=True,
        pending_history_recovery_thread_count=2,
    )

    assert result.should_resave_pending_recovery is False


@pytest.mark.asyncio
async def test_ensure_histories_loaded_once_loads_when_not_loaded() -> None:
    calls = {"count": 0}

    async def fake_load_histories() -> tuple[bool, bool]:
        calls["count"] += 1
        return True, True

    result = await ensure_histories_loaded_once(
        histories_loaded=False,
        history_load_failed=False,
        pending_history_recovery_thread_count=0,
        load_histories=fake_load_histories,
    )

    assert calls["count"] == 1
    assert result.histories_loaded is True
    assert result.history_load_failed is False
    assert result.needs_resave is True
    assert result.should_resave_pending_recovery is False


@pytest.mark.asyncio
async def test_ensure_histories_loaded_once_reuses_loaded_state() -> None:
    calls = {"count": 0}

    async def fake_load_histories() -> tuple[bool, bool]:
        calls["count"] += 1
        return True, False

    result = await ensure_histories_loaded_once(
        histories_loaded=True,
        history_load_failed=False,
        pending_history_recovery_thread_count=1,
        load_histories=fake_load_histories,
    )

    assert calls["count"] == 0
    assert result.histories_loaded is True
    assert result.should_resave_pending_recovery is True


@pytest.mark.asyncio
async def test_ensure_agent_runtime_ready_initializes_and_ensures_async() -> None:
    calls: list[str] = []

    async def fake_ensure_histories_loaded_locked() -> None:
        calls.append("histories")

    def fake_ensure_initialized() -> None:
        calls.append("initialized")

    async def fake_ensure_runtime_async(**kwargs) -> None:
        calls.append(f"async:{kwargs['provider_override']}:{kwargs['max_iterations']}")

    await ensure_agent_runtime_ready(
        ensure_histories_loaded_locked=fake_ensure_histories_loaded_locked,
        initialized=False,
        ensure_initialized=fake_ensure_initialized,
        ensure_runtime_async=fake_ensure_runtime_async,
        provider_override="deepseek",
        base_tools=["t1", "t2"],
        max_iterations=12,
    )

    assert calls == ["histories", "initialized", "async:deepseek:12"]


@pytest.mark.asyncio
async def test_ensure_agent_runtime_ready_skips_reinitialize_when_already_initialized() -> None:
    calls: list[str] = []

    async def fake_ensure_histories_loaded_locked() -> None:
        calls.append("histories")

    def fake_ensure_initialized() -> None:
        calls.append("initialized")

    async def fake_ensure_runtime_async(**kwargs) -> None:
        calls.append("async")

    await ensure_agent_runtime_ready(
        ensure_histories_loaded_locked=fake_ensure_histories_loaded_locked,
        initialized=True,
        ensure_initialized=fake_ensure_initialized,
        ensure_runtime_async=fake_ensure_runtime_async,
        provider_override=None,
        base_tools=[],
        max_iterations=10,
    )

    assert calls == ["histories", "async"]
