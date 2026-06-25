"""Worker registry API routes."""

from __future__ import annotations

from typing import Annotated, Any, Awaitable

from fastapi import APIRouter, Body, HTTPException, Path, Query
from loguru import logger

from src.core.sensitive import redact_sensitive_text
from src.core.worker_claim_coordinator import should_retain_retry_session_claim
from src.services.session_inventory_sync import (
    load_blocked_session_entry_best_effort,
    release_task_session_claim_via_provider_best_effort,
)
from src.schemas.workers import (
    WorkerClaimTaskRequest,
    WorkerClaimTaskResponse,
    WorkerHeartbeatRequest,
    WorkerRegisterRequest,
    WorkerReconcileStaleTasksResponse,
    WorkerResponse,
    WorkerTaskArtifactRequest,
    WorkerTaskCheckpointRequest,
    WorkerTaskCompleteRequest,
    WorkerTaskEventRequest,
    WorkerTaskFailRequest,
)

router = APIRouter(tags=["workers"])


@router.post("/workers/register", response_model=WorkerResponse)
async def register_worker(
    req: Annotated[WorkerRegisterRequest, Body(description="Worker registration")],
):
    """Register or refresh a worker."""
    from src.web.app import get_worker_registry

    worker = await get_worker_registry().register(
        worker_id=req.worker_id,
        hostname=req.hostname,
        capabilities=req.capabilities,
        current_task_ids=req.current_task_ids,
        metadata=req.metadata,
    )
    return _worker_to_response(worker)


@router.post("/workers/{worker_id}/heartbeat", response_model=WorkerResponse)
async def worker_heartbeat(
    worker_id: Annotated[str, Path(description="Worker id")],
    req: Annotated[WorkerHeartbeatRequest, Body(description="Worker heartbeat")],
):
    """Update worker heartbeat and current status."""
    from src.web.app import get_worker_registry

    worker = await get_worker_registry().heartbeat(
        worker_id,
        status=req.status,
        capabilities=req.capabilities,
        current_task_ids=req.current_task_ids,
        metadata=req.metadata,
    )
    if worker is None:
        raise HTTPException(404, f"Worker not found: {worker_id}")
    return _worker_to_response(worker)


@router.get("/workers", response_model=list[WorkerResponse])
async def list_workers(
    stale_after_seconds: Annotated[
        int,
        Query(ge=1, le=86_400, description="Mark online workers stale after this many seconds"),
    ] = 120,
):
    """List registered workers."""
    from src.web.app import get_worker_registry

    workers = await get_worker_registry().list_workers(stale_after_seconds=stale_after_seconds)
    return [_worker_to_response(worker) for worker in workers]


@router.post("/workers/reconcile-stale-tasks", response_model=WorkerReconcileStaleTasksResponse)
async def reconcile_stale_worker_tasks(
    stale_after_seconds: Annotated[
        int,
        Query(ge=1, le=86_400, description="Treat workers stale after this many seconds"),
    ] = 120,
):
    """Cancel running tasks claimed by stale/offline workers."""
    from src.web.app import get_session_registry, get_worker_registry, scheduler

    workers = await get_worker_registry().list_workers(stale_after_seconds=stale_after_seconds)
    offline_workers = [worker for worker in workers if worker.status == "offline"]
    offline_worker_ids = [worker.worker_id for worker in offline_workers]
    updated_worker_ids = []
    interrupted_tasks = []
    recovered_retry_tasks = []
    worker_registry = get_worker_registry()
    for worker in offline_workers:
        worker_id = worker.worker_id
        reconciled = await scheduler.reconcile_stale_worker_tasks(
            worker_id,
            reason=f"Worker {worker_id} heartbeat exceeded {stale_after_seconds}s.",
        )
        interrupted = reconciled.get("interrupted_tasks", [])
        recovered_retry = reconciled.get("recovered_retry_tasks", [])
        interrupted_tasks.extend(task.to_public_payload() for task in interrupted)
        recovered_retry_tasks.extend(task.to_public_payload() for task in recovered_retry)
        for task in interrupted:
            await _run_side_effect_best_effort(
                release_task_session_claim_via_provider_best_effort(
                    get_session_registry,
                    task,
                    context="worker_reconcile_interrupted",
                    worker_id=worker_id,
                    task_id=task.id,
                    disposition="interrupted",
                ),
                action="release interrupted task session during reconcile",
                worker_id=worker_id,
                task_id=task.id,
            )
        for task in recovered_retry:
            await _run_side_effect_best_effort(
                release_task_session_claim_via_provider_best_effort(
                    get_session_registry,
                    task,
                    context="worker_reconcile_retry_recovered",
                    worker_id=worker_id,
                    task_id=task.id,
                    disposition="interrupted",
                ),
                action="release recovered retry task session during reconcile",
                worker_id=worker_id,
                task_id=task.id,
            )
        stale_task_ids = list(getattr(worker, "current_task_ids", []) or [])
        if interrupted or recovered_retry or stale_task_ids:
            update_worker_state = getattr(worker_registry, "update_worker_state", None)
            if update_worker_state is None:
                continue
            await _run_side_effect_best_effort(
                worker_registry.update_worker_state(
                    worker_id,
                    status="offline",
                    current_task_ids=[],
                ),
                action="clear stale worker task ids during reconcile",
                worker_id=worker_id,
            )
            updated_worker_ids.append(worker_id)
    return WorkerReconcileStaleTasksResponse(
        offline_worker_ids=offline_worker_ids,
        updated_worker_ids=updated_worker_ids,
        interrupted_tasks=interrupted_tasks,
        recovered_retry_tasks=recovered_retry_tasks,
    )


@router.get("/workers/{worker_id}", response_model=WorkerResponse)
async def get_worker(worker_id: Annotated[str, Path(description="Worker id")]):
    """Get one registered worker."""
    from src.web.app import get_worker_registry

    worker = await get_worker_registry().get_worker(worker_id)
    if worker is None:
        raise HTTPException(404, f"Worker not found: {worker_id}")
    return _worker_to_response(worker)


@router.post("/workers/{worker_id}/claim-task", response_model=WorkerClaimTaskResponse)
async def claim_task(
    worker_id: Annotated[str, Path(description="Worker id")],
    req: Annotated[WorkerClaimTaskRequest, Body(description="Task claim request")],
):
    """Claim the next pending task for a registered worker."""
    from src.web.app import get_session_registry, get_worker_registry, scheduler

    worker = await _require_registered_worker(worker_id)
    claim = await scheduler.claim_task_for_worker(
        worker_id,
        capabilities=req.capabilities if req.capabilities is not None else worker.capabilities,
        reserve_session_claim=_build_session_claim_guard(get_session_registry),
    )
    if claim is None:
        return WorkerClaimTaskResponse(worker_id=worker_id, claim_status="no_task")
    if not claim.get("task_id"):
        return WorkerClaimTaskResponse(worker_id=worker_id, **claim)
    await _run_side_effect_best_effort(
        get_worker_registry().heartbeat(
            worker_id,
            status="busy",
            current_task_ids=[claim["task_id"]],
        ),
        action="mark worker busy after task claim",
        worker_id=worker_id,
        task_id=claim["task_id"],
    )
    session_diagnostics = claim.get("session_diagnostics", {})
    if (
        isinstance(session_diagnostics, dict)
        and session_diagnostics
        and not claim.get("session_reserved")
    ):
        await _run_side_effect_best_effort(
            _bind_session_best_effort(
                get_session_registry,
                session_diagnostics,
                worker_id=worker_id,
                task_id=claim["task_id"],
            ),
            action="bind task session after task claim",
            worker_id=worker_id,
            task_id=claim["task_id"],
        )
    return WorkerClaimTaskResponse(worker_id=worker_id, **claim)


@router.post("/workers/{worker_id}/tasks/{task_id}/events")
async def append_task_event(
    worker_id: Annotated[str, Path(description="Worker id")],
    task_id: Annotated[str, Path(description="Task id")],
    req: Annotated[WorkerTaskEventRequest, Body(description="Worker task event")],
):
    """Append an event for a worker-claimed task."""
    from src.web.app import scheduler

    await _require_registered_worker(worker_id)
    event = await scheduler.append_worker_task_event(
        worker_id,
        task_id,
        req.type,
        level=req.level,
        message=req.message,
        payload=req.payload,
    )
    if event is None:
        raise HTTPException(404, f"Claimed task not found for worker: {task_id}")
    await _touch_worker_task_activity(worker_id, task_id)
    return {"event": event.to_public_payload()}


@router.post("/workers/{worker_id}/tasks/{task_id}/artifacts")
async def register_task_artifact(
    worker_id: Annotated[str, Path(description="Worker id")],
    task_id: Annotated[str, Path(description="Task id")],
    req: Annotated[WorkerTaskArtifactRequest, Body(description="Worker task artifact")],
):
    """Register an artifact for a worker-claimed task."""
    from src.web.app import scheduler

    await _require_registered_worker(worker_id)
    artifact = await scheduler.register_worker_task_artifact(
        worker_id,
        task_id,
        req.type,
        name=req.name,
        path=req.path,
        mime_type=req.mime_type,
        size=req.size,
        download_url=req.download_url,
        metadata=req.metadata,
    )
    if artifact is None:
        raise HTTPException(404, f"Claimed task not found for worker: {task_id}")
    await _touch_worker_task_activity(worker_id, task_id)
    return {"artifact": artifact.to_public_payload()}


@router.post("/workers/{worker_id}/tasks/{task_id}/checkpoints")
async def register_task_checkpoint(
    worker_id: Annotated[str, Path(description="Worker id")],
    task_id: Annotated[str, Path(description="Task id")],
    req: Annotated[WorkerTaskCheckpointRequest, Body(description="Worker task checkpoint")],
):
    """Register a checkpoint for a worker-claimed task."""
    from src.web.app import scheduler

    await _require_registered_worker(worker_id)
    checkpoint = await scheduler.register_worker_task_checkpoint(
        worker_id,
        task_id,
        recovery_level=req.recovery_level,
        cursor=req.cursor,
        state=req.state,
        stats=req.stats,
        artifacts=req.artifacts,
        metadata=req.metadata,
    )
    if checkpoint is None:
        raise HTTPException(404, f"Claimed task not found for worker: {task_id}")
    await _touch_worker_task_activity(worker_id, task_id)
    return {"checkpoint": checkpoint.to_public_payload()}


@router.post("/workers/{worker_id}/tasks/{task_id}/complete")
async def complete_task(
    worker_id: Annotated[str, Path(description="Worker id")],
    task_id: Annotated[str, Path(description="Task id")],
    req: Annotated[WorkerTaskCompleteRequest, Body(description="Worker task result")],
):
    """Mark a worker-claimed task as complete."""
    from src.web.app import get_session_registry, scheduler

    await _require_registered_worker(worker_id)
    task = await scheduler.complete_worker_task(worker_id, task_id, result=req.result)
    if task is None:
        raise HTTPException(404, f"Claimed task not found for worker: {task_id}")
    await _refresh_worker_heartbeat_best_effort(
        worker_id,
        fallback_status="online",
        current_task_ids=[],
        action="mark worker online after task completion",
        task_id=task_id,
    )
    await _run_side_effect_best_effort(
        release_task_session_claim_via_provider_best_effort(
            get_session_registry,
            task,
            context="worker_complete",
            worker_id=worker_id,
            task_id=task_id,
            disposition="released",
        ),
        action="release task session after task completion",
        worker_id=worker_id,
        task_id=task_id,
    )
    return {"task": task.to_public_payload()}


@router.post("/workers/{worker_id}/tasks/{task_id}/fail")
async def fail_task(
    worker_id: Annotated[str, Path(description="Worker id")],
    task_id: Annotated[str, Path(description="Task id")],
    req: Annotated[WorkerTaskFailRequest, Body(description="Worker task failure")],
):
    """Mark a worker-claimed task as failed."""
    from src.web.app import get_session_registry, scheduler

    await _require_registered_worker(worker_id)
    task = await scheduler.fail_worker_task(worker_id, task_id, error=req.error, result=req.result)
    if task is None:
        raise HTTPException(404, f"Claimed task not found for worker: {task_id}")
    await _refresh_worker_heartbeat_best_effort(
        worker_id,
        fallback_status="online",
        current_task_ids=[],
        action="mark worker online after task failure",
        task_id=task_id,
    )
    if not should_retain_retry_session_claim(task):
        await _run_side_effect_best_effort(
            release_task_session_claim_via_provider_best_effort(
                get_session_registry,
                task,
                context="worker_fail",
                worker_id=worker_id,
                task_id=task_id,
                disposition="released",
            ),
            action="release task session after task failure",
            worker_id=worker_id,
            task_id=task_id,
        )
    return {"task": task.to_public_payload()}


def _worker_to_response(worker) -> WorkerResponse:
    payload = worker.to_public_payload() if hasattr(worker, "to_public_payload") else worker
    return WorkerResponse(
        worker_id=payload["worker_id"],
        hostname=payload["hostname"],
        status=payload["status"],
        capabilities=payload.get("capabilities", []),
        current_task_ids=payload.get("current_task_ids", []),
        metadata=payload.get("metadata", {}),
        registered_at=payload["registered_at"],
        last_heartbeat_at=payload["last_heartbeat_at"],
    )


async def _touch_worker_task_activity(worker_id: str, task_id: str) -> None:
    """Refresh worker heartbeat when task-scoped worker activity arrives."""
    await _refresh_worker_heartbeat_best_effort(
        worker_id,
        fallback_status="busy",
        current_task_ids=[task_id],
        action="refresh worker activity after task-scoped update",
        task_id=task_id,
    )


def _build_session_claim_guard(get_registry):
    async def guard(task, worker_id: str, collector_name: str, diagnostics: dict) -> dict:
        if not isinstance(diagnostics, dict) or not diagnostics.get("requires_session"):
            return {"allowed": True}

        try:
            registry = get_registry()
        except Exception as exc:
            logger.warning(
                "Worker claim session registry lookup failed (worker_id={} task_id={} collector_id={}): {}",
                redact_sensitive_text(worker_id),
                redact_sensitive_text(task.id),
                redact_sensitive_text(collector_name),
                redact_sensitive_text(str(exc)),
            )
            return {
                "allowed": False,
                "reason": "session_registry_unavailable",
                "blocked_session": _build_blocked_session_payload(
                    task,
                    collector_name=collector_name,
                    contender=_blocked_session_fallback_entry(diagnostics),
                    reason="session_registry_unavailable",
                ),
            }
        claimed = await registry.try_claim_session(
            diagnostics,
            worker_id=worker_id,
            task_id=task.id,
        )
        if claimed is not None:
            return {"allowed": True}

        contender = await _load_blocked_session_entry_best_effort(
            registry,
            diagnostics,
            worker_id=worker_id,
            task_id=task.id,
        )
        return {
            "allowed": False,
            "reason": "session_claimed",
            "blocked_session": _build_blocked_session_payload(
                task,
                collector_name=collector_name,
                contender=contender,
                reason="session_claimed",
            ),
        }

    return guard


async def _bind_session_best_effort(
    get_registry,
    diagnostics: dict[str, Any],
    *,
    worker_id: str,
    task_id: str,
):
    registry = get_registry()
    return await registry.bind_session(
        diagnostics,
        worker_id=worker_id,
        task_id=task_id,
    )


async def _run_side_effect_best_effort(
    awaitable: Awaitable[Any],
    *,
    action: str,
    worker_id: str = "",
    task_id: str = "",
) -> None:
    try:
        await awaitable
    except Exception as exc:
        logger.warning(
            "Worker route side effect failed (action={} worker_id={} task_id={}): {}",
            redact_sensitive_text(action),
            redact_sensitive_text(worker_id),
            redact_sensitive_text(task_id),
            redact_sensitive_text(str(exc)),
        )


async def _refresh_worker_heartbeat_best_effort(
    worker_id: str,
    *,
    fallback_status: str,
    current_task_ids: list[str],
    action: str,
    task_id: str = "",
) -> None:
    from src.web.app import get_worker_registry

    registry = get_worker_registry()
    status = await _resolve_worker_status_for_heartbeat(
        registry,
        worker_id,
        fallback_status=fallback_status,
        task_id=task_id,
    )
    await _run_side_effect_best_effort(
        registry.heartbeat(
            worker_id,
            status=status,
            current_task_ids=current_task_ids,
        ),
        action=action,
        worker_id=worker_id,
        task_id=task_id,
    )


async def _resolve_worker_status_for_heartbeat(
    registry,
    worker_id: str,
    *,
    fallback_status: str,
    task_id: str = "",
) -> str:
    get_worker = getattr(registry, "get_worker", None)
    if not callable(get_worker):
        return fallback_status

    try:
        worker = await get_worker(worker_id)
    except Exception as exc:
        logger.warning(
            "Worker route state lookup failed (worker_id={} task_id={}): {}",
            redact_sensitive_text(worker_id),
            redact_sensitive_text(task_id),
            redact_sensitive_text(str(exc)),
        )
        return fallback_status

    current_status = str(getattr(worker, "status", "") or "").strip().lower()
    if current_status == "draining":
        return current_status
    return fallback_status


async def _load_blocked_session_entry_best_effort(
    registry,
    diagnostics: dict,
    *,
    worker_id: str,
    task_id: str,
):
    return await load_blocked_session_entry_best_effort(
        registry,
        diagnostics,
        context="worker_claim_denied",
        worker_id=worker_id,
        task_id=task_id,
    )


def _build_blocked_session_payload(
    task,
    *,
    collector_name: str,
    contender,
    reason: str = "session_claimed",
) -> dict[str, Any]:
    return {
        "reason": reason,
        "task_id": task.id,
        "task_name": task.name,
        "collector_id": collector_name,
        "session_id": contender.session_id,
        "session_mode": contender.session_mode,
        "worker_binding": contender.worker_binding,
        "lease_worker_id": contender.lease_worker_id,
        "lease_task_id": contender.lease_task_id,
        "account_kind": contender.account_kind,
        "account_id": contender.account_id,
    }


def _blocked_session_fallback_entry(diagnostics: dict[str, Any]):
    account = diagnostics.get("session_account", {}) if isinstance(diagnostics, dict) else {}
    if not isinstance(account, dict):
        account = {}

    return type(
        "BlockedSessionFallback",
        (),
        {
            "session_id": redact_sensitive_text(
                str(diagnostics.get("collector_id") or "") if isinstance(diagnostics, dict) else ""
            ),
            "session_mode": redact_sensitive_text(
                str(diagnostics.get("session_mode") or "") if isinstance(diagnostics, dict) else ""
            ),
            "worker_binding": redact_sensitive_text(
                str(diagnostics.get("worker_binding") or "")
                if isinstance(diagnostics, dict)
                else ""
            ),
            "lease_worker_id": "",
            "lease_task_id": "",
            "account_kind": redact_sensitive_text(str(account.get("account_kind") or "")),
            "account_id": redact_sensitive_text(str(account.get("account_id") or "")),
        },
    )()


async def _require_registered_worker(worker_id: str):
    """Return a registered worker or raise a route-level 404."""
    from src.web.app import get_worker_registry

    worker = await get_worker_registry().get_worker(worker_id)
    if worker is None:
        raise HTTPException(404, f"Worker not found: {worker_id}")
    return worker
