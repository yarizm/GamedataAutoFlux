"""Worker registry API routes."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, Path, Query

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
    from src.web.app import get_worker_registry, scheduler

    workers = await get_worker_registry().list_workers(stale_after_seconds=stale_after_seconds)
    offline_worker_ids = [worker.worker_id for worker in workers if worker.status == "offline"]
    interrupted_tasks = []
    for worker_id in offline_worker_ids:
        interrupted = await scheduler.interrupt_worker_tasks(
            worker_id,
            reason=f"Worker {worker_id} heartbeat exceeded {stale_after_seconds}s.",
        )
        interrupted_tasks.extend(task.to_public_payload() for task in interrupted)
    return WorkerReconcileStaleTasksResponse(
        offline_worker_ids=offline_worker_ids,
        interrupted_tasks=interrupted_tasks,
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
    from src.web.app import get_worker_registry, scheduler

    worker = await _require_registered_worker(worker_id)
    claim = await scheduler.claim_task_for_worker(
        worker_id,
        capabilities=req.capabilities if req.capabilities is not None else worker.capabilities,
    )
    if claim is None:
        return WorkerClaimTaskResponse(worker_id=worker_id)
    await get_worker_registry().heartbeat(
        worker_id,
        status="busy",
        current_task_ids=[claim["task_id"]],
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
    from src.web.app import get_worker_registry, scheduler

    await _require_registered_worker(worker_id)
    task = await scheduler.complete_worker_task(worker_id, task_id, result=req.result)
    if task is None:
        raise HTTPException(404, f"Claimed task not found for worker: {task_id}")
    await get_worker_registry().heartbeat(worker_id, status="online", current_task_ids=[])
    return {"task": task.to_public_payload()}


@router.post("/workers/{worker_id}/tasks/{task_id}/fail")
async def fail_task(
    worker_id: Annotated[str, Path(description="Worker id")],
    task_id: Annotated[str, Path(description="Task id")],
    req: Annotated[WorkerTaskFailRequest, Body(description="Worker task failure")],
):
    """Mark a worker-claimed task as failed."""
    from src.web.app import get_worker_registry, scheduler

    await _require_registered_worker(worker_id)
    task = await scheduler.fail_worker_task(worker_id, task_id, error=req.error, result=req.result)
    if task is None:
        raise HTTPException(404, f"Claimed task not found for worker: {task_id}")
    await get_worker_registry().heartbeat(worker_id, status="online", current_task_ids=[])
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
    from src.web.app import get_worker_registry

    await get_worker_registry().heartbeat(
        worker_id,
        status="busy",
        current_task_ids=[task_id],
    )


async def _require_registered_worker(worker_id: str):
    """Return a registered worker or raise a route-level 404."""
    from src.web.app import get_worker_registry

    worker = await get_worker_registry().get_worker(worker_id)
    if worker is None:
        raise HTTPException(404, f"Worker not found: {worker_id}")
    return worker
