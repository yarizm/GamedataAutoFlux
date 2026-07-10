"""Helpers for Agent runtime initialization and persisted history bootstrap."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class EnsureHistoriesLoadedResult:
    histories_loaded: bool
    history_load_failed: bool
    needs_resave: bool
    should_resave_pending_recovery: bool


def build_histories_loaded_result(
    *,
    histories_loaded: bool,
    history_load_failed: bool,
    pending_history_recovery_thread_count: int,
) -> EnsureHistoriesLoadedResult:
    if histories_loaded:
        return EnsureHistoriesLoadedResult(
            histories_loaded=True,
            history_load_failed=history_load_failed,
            needs_resave=False,
            should_resave_pending_recovery=(
                pending_history_recovery_thread_count > 0 and not history_load_failed
            ),
        )

    return EnsureHistoriesLoadedResult(
        histories_loaded=False,
        history_load_failed=history_load_failed,
        needs_resave=False,
        should_resave_pending_recovery=False,
    )


async def ensure_histories_loaded_once(
    *,
    histories_loaded: bool,
    history_load_failed: bool,
    pending_history_recovery_thread_count: int,
    load_histories: Callable[[], Awaitable[tuple[bool, bool]]],
) -> EnsureHistoriesLoadedResult:
    current_result = build_histories_loaded_result(
        histories_loaded=histories_loaded,
        history_load_failed=history_load_failed,
        pending_history_recovery_thread_count=pending_history_recovery_thread_count,
    )
    if current_result.histories_loaded:
        return current_result

    loaded, needs_resave = await load_histories()
    return EnsureHistoriesLoadedResult(
        histories_loaded=loaded,
        history_load_failed=not loaded,
        needs_resave=loaded and needs_resave,
        should_resave_pending_recovery=False,
    )


async def ensure_agent_runtime_ready(
    *,
    ensure_histories_loaded_locked: Callable[[], Awaitable[None]],
    initialized: bool,
    ensure_initialized: Callable[[], None],
    ensure_runtime_async: Callable[..., Awaitable[None]],
    provider_override: str | None,
    base_tools: list,
    max_iterations: int,
) -> None:
    await ensure_histories_loaded_locked()
    if not initialized:
        ensure_initialized()

    await ensure_runtime_async(
        provider_override=provider_override,
        base_tools=base_tools,
        max_iterations=max_iterations,
    )
