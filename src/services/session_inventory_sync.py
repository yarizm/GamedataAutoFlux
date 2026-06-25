"""Shared best-effort session inventory sync helpers for routes and services."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from loguru import logger

from src.core.diagnostics import build_collector_session_diagnostics
from src.core.sensitive import redact_sensitive_text
from src.services.session_registry import build_session_registry_entry


def _resolve_registry_best_effort(
    get_registry: Callable[[], Any],
    *,
    context: str,
    collector_id: str = "",
    worker_id: str = "",
    task_id: str = "",
) -> Any | None:
    try:
        return get_registry()
    except Exception as exc:
        logger.warning(
            "Session registry lookup failed (context={} collector_id={} worker_id={} task_id={}): {}",
            redact_sensitive_text(context),
            redact_sensitive_text(collector_id),
            redact_sensitive_text(worker_id),
            redact_sensitive_text(task_id),
            redact_sensitive_text(str(exc)),
        )
        return None


async def sync_session_inventory_best_effort(
    registry: Any,
    diagnostics: dict[str, Any] | None,
    *,
    context: str,
    collector_id: str = "",
    worker_id: str = "",
    task_id: str = "",
) -> Any | None:
    """Attempt to sync diagnostics into persisted inventory without failing the caller."""
    if not isinstance(diagnostics, dict) or not diagnostics:
        return None
    safe_collector_id = redact_sensitive_text(
        collector_id or str(diagnostics.get("collector_id") or "")
    )
    try:
        return await registry.sync_from_diagnostics(diagnostics)
    except Exception as exc:
        logger.warning(
            "Session inventory sync failed (context={} collector_id={} worker_id={} task_id={}): {}",
            redact_sensitive_text(context),
            safe_collector_id,
            redact_sensitive_text(worker_id),
            redact_sensitive_text(task_id),
            redact_sensitive_text(str(exc)),
        )
        return None


async def sync_session_inventory_via_provider_best_effort(
    get_registry: Callable[[], Any],
    diagnostics: dict[str, Any] | None,
    *,
    context: str,
    collector_id: str = "",
    worker_id: str = "",
    task_id: str = "",
) -> Any | None:
    """Resolve the registry lazily, then sync diagnostics without failing the caller."""
    if not isinstance(diagnostics, dict) or not diagnostics:
        return None
    safe_collector_id = redact_sensitive_text(
        collector_id or str(diagnostics.get("collector_id") or "")
    )
    registry = _resolve_registry_best_effort(
        get_registry,
        context=context,
        collector_id=safe_collector_id,
        worker_id=worker_id,
        task_id=task_id,
    )
    if registry is None:
        return None
    return await sync_session_inventory_best_effort(
        registry,
        diagnostics,
        context=context,
        collector_id=safe_collector_id,
        worker_id=worker_id,
        task_id=task_id,
    )


async def load_blocked_session_entry_best_effort(
    registry: Any,
    diagnostics: dict[str, Any],
    *,
    context: str,
    worker_id: str = "",
    task_id: str = "",
):
    """Refresh blocked-session inventory if possible, then fall back to persisted/local entry."""
    fallback = build_session_registry_entry(diagnostics)
    contender = await sync_session_inventory_best_effort(
        registry,
        diagnostics,
        context=context,
        collector_id=fallback.collector_id,
        worker_id=worker_id,
        task_id=task_id,
    )
    if contender is not None:
        return contender

    get_session = getattr(registry, "get_session", None)
    if callable(get_session):
        try:
            contender = await get_session(fallback.session_id)
            if contender is not None:
                return contender
        except Exception as exc:
            logger.warning(
                "Session inventory fallback read failed (context={} collector_id={} worker_id={} task_id={}): {}",
                redact_sensitive_text(context),
                redact_sensitive_text(fallback.collector_id),
                redact_sensitive_text(worker_id),
                redact_sensitive_text(task_id),
                redact_sensitive_text(str(exc)),
            )
    return fallback


async def release_task_session_claim_via_provider_best_effort(
    get_registry: Callable[[], Any],
    task: Any,
    *,
    context: str,
    disposition: str = "released",
    worker_id: str = "",
    task_id: str = "",
):
    """Resolve the registry lazily, then release a task-bound session claim best-effort."""
    claim = task.config.get("worker_claim") if isinstance(getattr(task, "config", None), dict) else None
    collector_id = ""
    if isinstance(claim, dict):
        session_diagnostics = claim.get("session_diagnostics")
        if isinstance(session_diagnostics, dict):
            collector_id = str(session_diagnostics.get("collector_id") or "").strip()
    if not collector_id:
        collector_id = str(getattr(task, "collector_name", "") or "").strip()

    registry = _resolve_registry_best_effort(
        get_registry,
        context=context,
        collector_id=collector_id,
        worker_id=worker_id,
        task_id=task_id or str(getattr(task, "id", "") or ""),
    )
    if registry is None:
        return None
    return await release_task_session_claim_best_effort(
        registry,
        task,
        context=context,
        disposition=disposition,
        worker_id=worker_id,
        task_id=task_id,
    )


async def release_task_session_claim_best_effort(
    registry: Any,
    task: Any,
    *,
    context: str,
    disposition: str = "released",
    worker_id: str = "",
    task_id: str = "",
):
    """Release a task-bound session claim without failing the caller."""
    claim = task.config.get("worker_claim") if isinstance(getattr(task, "config", None), dict) else None
    snapshot_diagnostics = None
    if isinstance(claim, dict):
        session_diagnostics = claim.get("session_diagnostics")
        if isinstance(session_diagnostics, dict) and session_diagnostics:
            snapshot_diagnostics = session_diagnostics

    safe_worker_id = redact_sensitive_text(
        worker_id or str((claim or {}).get("worker_id") or "")
    )
    safe_task_id = redact_sensitive_text(task_id or str(getattr(task, "id", "") or ""))

    if snapshot_diagnostics:
        safe_collector_id = redact_sensitive_text(
            str(snapshot_diagnostics.get("collector_id") or "")
        )
        release_by_id = getattr(registry, "release_session_by_id", None)
        if callable(release_by_id):
            try:
                session_id = build_session_registry_entry(snapshot_diagnostics).session_id
                released = await release_by_id(
                    session_id,
                    worker_id=safe_worker_id,
                    task_id=safe_task_id,
                    disposition=disposition,
                )
                if released is not None:
                    return released
            except Exception as exc:
                logger.warning(
                    "Session inventory release-by-id failed (context={} collector_id={} worker_id={} task_id={}): {}",
                    redact_sensitive_text(context),
                    safe_collector_id,
                    safe_worker_id,
                    safe_task_id,
                    redact_sensitive_text(str(exc)),
                )

        release_session = getattr(registry, "release_session", None)
        if callable(release_session):
            try:
                return await release_session(
                    snapshot_diagnostics,
                    worker_id=safe_worker_id,
                    task_id=safe_task_id,
                    disposition=disposition,
                )
            except Exception as exc:
                logger.warning(
                    "Session inventory release failed (context={} collector_id={} worker_id={} task_id={}): {}",
                    redact_sensitive_text(context),
                    safe_collector_id,
                    safe_worker_id,
                    safe_task_id,
                    redact_sensitive_text(str(exc)),
                )
                return None

    release_session = getattr(registry, "release_session", None)
    if not callable(release_session):
        return None

    collector_name = str(getattr(task, "collector_name", "") or "").strip()
    if not collector_name:
        return None

    diagnostics = build_collector_session_diagnostics(collector_name)
    if not diagnostics:
        return None

    try:
        return await release_session(
            diagnostics,
            worker_id=safe_worker_id,
            task_id=safe_task_id,
            disposition=disposition,
        )
    except Exception as exc:
        logger.warning(
            "Session inventory fallback release failed (context={} collector_id={} worker_id={} task_id={}): {}",
            redact_sensitive_text(context),
            redact_sensitive_text(collector_name),
            safe_worker_id,
            safe_task_id,
            redact_sensitive_text(str(exc)),
        )
        return None
