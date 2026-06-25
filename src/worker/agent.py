"""Worker agent loop for the worker-claim execution backend."""

from __future__ import annotations

import asyncio
import socket
import uuid
from dataclasses import dataclass
from typing import Any

import httpx
from loguru import logger

from src.core.collector_metadata import get_collector_metadata, required_worker_capabilities
from src.core.diagnostics import build_collector_session_diagnostics
from src.core.pipeline import Pipeline, PipelineResult
from src.core.sensitive import redact_sensitive, redact_sensitive_text
from src.core.task import Task


@dataclass
class WorkerAgentConfig:
    base_url: str
    worker_id: str = ""
    hostname: str = ""
    capabilities: list[str] | None = None
    api_key: str = ""
    heartbeat_interval_seconds: float = 15.0
    claim_poll_interval_seconds: float = 3.0
    request_timeout_seconds: float = 30.0
    drain_on_shutdown: bool = True
    metadata: dict[str, Any] | None = None
    transport: httpx.AsyncBaseTransport | None = None


class WorkerAgent:
    """Minimal REST-polled worker that executes claimed pipelines locally."""

    def __init__(self, config: WorkerAgentConfig) -> None:
        self._config = config
        self.worker_id = redact_sensitive_text(
            config.worker_id.strip() or f"worker-{uuid.uuid4().hex[:12]}"
        )
        self.hostname = redact_sensitive_text(config.hostname.strip() or socket.gethostname())
        self.capabilities = _normalize_worker_capabilities(config.capabilities)
        self.metadata = redact_sensitive(config.metadata or {})
        self._client: httpx.AsyncClient | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._drain_heartbeat_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._current_task_id: str | None = None
        self._draining = False
        self._last_claim_status: dict[str, Any] = {
            "status": "not_started",
            "reason": "",
            "blocked_sessions": [],
        }

    async def start(self) -> None:
        """Register the worker and start heartbeats."""
        if self._client is None:
            headers = {}
            if self._config.api_key.strip():
                headers["X-API-Key"] = self._config.api_key.strip()
            self._client = httpx.AsyncClient(
                base_url=self._config.base_url.rstrip("/"),
                headers=headers,
                timeout=self._config.request_timeout_seconds,
                transport=self._config.transport,
            )
        await self._register()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        """Stop heartbeats and mark the worker draining/offline."""
        self.request_stop()
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
            self._heartbeat_task = None
        if self._drain_heartbeat_task is not None:
            await asyncio.gather(self._drain_heartbeat_task, return_exceptions=True)
            self._drain_heartbeat_task = None
        if self._client is not None:
            if self._config.drain_on_shutdown:
                await self._heartbeat("draining")
            await self._heartbeat("offline")
            await self._client.aclose()
            self._client = None

    def request_stop(self) -> None:
        """Ask the worker to stop polling after the current task finishes."""
        if self._config.drain_on_shutdown:
            self._draining = True
            self._schedule_status_heartbeat("draining")
        self._stop_event.set()

    def _schedule_status_heartbeat(self, status: str) -> None:
        if self._client is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._drain_heartbeat_task = loop.create_task(self._safe_heartbeat(status))

    async def _safe_heartbeat(self, status: str) -> None:
        try:
            await self._heartbeat(status)
        except Exception as exc:
            logger.warning(
                "Worker {} failed to report {} heartbeat: {}",
                self.worker_id,
                status,
                redact_sensitive_text(str(exc)),
            )

    async def run_once(self) -> bool:
        """Claim and execute at most one task."""
        claim = await self._claim_task()
        if claim is None:
            return False
        await self._execute_claim(claim)
        return True

    async def run_forever(self) -> None:
        """Poll for work until the agent is stopped."""
        try:
            while not self._stop_event.is_set():
                claimed = await self.run_once()
                if claimed:
                    continue
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=max(0.1, self._config.claim_poll_interval_seconds),
                    )
                except asyncio.TimeoutError:
                    continue
        finally:
            await self.stop()

    async def _register(self) -> None:
        await self._request(
            "POST",
            "/api/workers/register",
            json={
                "worker_id": self.worker_id,
                "hostname": self.hostname,
                "capabilities": self.capabilities,
                "metadata": self.metadata,
            },
        )

    async def _heartbeat(self, status: str) -> None:
        await self._request(
            "POST",
            f"/api/workers/{self.worker_id}/heartbeat",
            json={
                "status": status,
                "capabilities": self.capabilities,
                "current_task_ids": [self._current_task_id] if self._current_task_id else [],
                "metadata": self._heartbeat_metadata(),
            },
        )

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                try:
                    await self._heartbeat(self._heartbeat_status())
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "Worker heartbeat loop failed for {}: {}",
                        self.worker_id,
                        redact_sensitive_text(str(exc)),
                    )
                await asyncio.sleep(max(0.5, self._config.heartbeat_interval_seconds))
        except asyncio.CancelledError:
            raise

    async def _claim_task(self) -> dict[str, Any] | None:
        payload = await self._request(
            "POST",
            f"/api/workers/{self.worker_id}/claim-task",
            json={"capabilities": self.capabilities},
        )
        self._record_claim_status(payload)
        if not isinstance(payload, dict) or not payload.get("task_id"):
            return None
        return payload

    async def _execute_claim(self, claim: dict[str, Any]) -> None:
        claim_task_id = redact_sensitive_text(str(claim.get("task_id") or "")).strip()
        task_payload = claim.get("task")
        pipeline_payload = claim.get("pipeline")
        if not isinstance(task_payload, dict) or not isinstance(pipeline_payload, dict):
            safe_error = "Invalid claim payload: missing task or pipeline snapshot"
            logger.warning("Worker {} received invalid claim payload", self.worker_id)
            await self._report_invalid_claim(claim_task_id, safe_error)
            return

        try:
            task = Task.from_storage_payload(task_payload)
            pipeline = Pipeline.from_config(pipeline_payload)
        except Exception as exc:
            safe_error = f"Invalid claim payload: {redact_sensitive_text(str(exc))}"
            logger.error(
                "Worker {} failed to decode claim payload for task {}: {}",
                self.worker_id,
                claim_task_id or "<missing>",
                safe_error,
            )
            await self._report_invalid_claim(claim_task_id, safe_error)
            return

        self._bind_pipeline_callbacks(pipeline, task.id)
        self._current_task_id = task.id
        result: PipelineResult | None = None
        latest_checkpoint = claim.get("latest_checkpoint")

        try:
            result = await pipeline.execute(
                task,
                recovery_checkpoint=latest_checkpoint
                if isinstance(latest_checkpoint, dict)
                else None,
            )
            payload = self._serialize_pipeline_result(result)
            if result.success:
                await self._request(
                    "POST",
                    f"/api/workers/{self.worker_id}/tasks/{task.id}/complete",
                    json={"result": payload},
                )
            else:
                await self._request(
                    "POST",
                    f"/api/workers/{self.worker_id}/tasks/{task.id}/fail",
                    json={
                        "error": self._result_error_message(result),
                        "result": payload,
                    },
                )
        except Exception as exc:
            safe_error = redact_sensitive_text(str(exc))
            logger.error("Worker {} failed task {}: {}", self.worker_id, task.id, safe_error)
            await self._request(
                "POST",
                f"/api/workers/{self.worker_id}/tasks/{task.id}/fail",
                json={
                    "error": safe_error,
                    "result": {"success": False, "errors": [safe_error]},
                },
            )
        finally:
            self._current_task_id = None
            await self._heartbeat(self._heartbeat_status())

    def _heartbeat_status(self) -> str:
        if self._draining:
            return "draining"
        return "busy" if self._current_task_id else "online"

    def _heartbeat_metadata(self) -> dict[str, Any]:
        return redact_sensitive(
            {
                **self.metadata,
                "worker_claim": self._last_claim_status,
            }
        )

    def _record_claim_status(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            self._last_claim_status = {
                "status": "invalid_response",
                "reason": "claim_response_not_object",
                "blocked_sessions": [],
            }
            return

        status = str(
            payload.get("claim_status") or ("claimed" if payload.get("task_id") else "no_task")
        )
        reason = str(payload.get("claim_reason") or "")
        blocked_sessions = payload.get("blocked_sessions")
        if not isinstance(blocked_sessions, list):
            blocked_sessions = []
        self._last_claim_status = redact_sensitive(
            {
                "status": status,
                "reason": reason,
                "task_id": str(payload.get("task_id") or ""),
                "blocked_sessions": blocked_sessions[:5],
            }
        )

    def _bind_pipeline_callbacks(self, pipeline: Pipeline, task_id: str) -> None:
        async def on_progress(_task_id: str, progress: float, message: str) -> None:
            await self._append_event(
                task_id,
                event_type="progress",
                message=message or "Worker progress update",
                payload={"progress": progress},
            )

        async def on_event(
            _task_id: str,
            event_type: str,
            level: str,
            message: str,
            payload: dict[str, Any] | None = None,
        ) -> None:
            await self._append_event(
                task_id,
                event_type=event_type,
                level=level,
                message=message,
                payload=payload or {},
            )

        pipeline.on_progress(on_progress)
        pipeline.on_event(on_event)

    async def _append_event(
        self,
        task_id: str,
        *,
        event_type: str,
        level: str = "info",
        message: str = "",
        payload: dict[str, Any] | None = None,
    ) -> None:
        await self._request(
            "POST",
            f"/api/workers/{self.worker_id}/tasks/{task_id}/events",
            json={
                "type": event_type,
                "level": level,
                "message": message,
                "payload": redact_sensitive(payload or {}),
            },
        )

    def _serialize_pipeline_result(self, result: PipelineResult) -> dict[str, Any]:
        output_records = [record.model_dump(mode="json") for record in result.output_records]
        return redact_sensitive(
            {
                "success": result.success,
                "pipeline_name": result.pipeline_name,
                "task_id": result.task_id,
                "storage_count": result.storage_count,
                "errors": list(result.errors),
                "duration_seconds": result.duration_seconds,
                "collection_summary": result.collection_summary,
                "resume_state": result.resume_state,
                "output_records": output_records,
                "generated_report_id": result.generated_report_id,
                "generated_report_title": result.generated_report_title,
                "generated_report_matched_records": result.generated_report_matched_records,
            }
        )

    @staticmethod
    def _result_error_message(result: PipelineResult) -> str:
        safe_errors = [
            redact_sensitive_text(str(item)) for item in result.errors if str(item or "").strip()
        ]
        return "; ".join(safe_errors) if safe_errors else "Worker task failed"

    async def _request(
        self, method: str, path: str, *, json: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        if self._client is None:
            raise RuntimeError("WorkerAgent has not been started")
        response = await self._client.request(method, path, json=json)
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {"data": payload}

    async def _report_invalid_claim(self, task_id: str, error: str) -> None:
        safe_task_id = redact_sensitive_text(str(task_id or "")).strip()
        safe_error = redact_sensitive_text(str(error or "Invalid claim payload"))
        if not safe_task_id:
            logger.warning(
                "Worker {} could not report invalid claim because task_id is missing",
                self.worker_id,
            )
            return
        await self._request(
            "POST",
            f"/api/workers/{self.worker_id}/tasks/{safe_task_id}/fail",
            json={
                "error": safe_error,
                "result": {
                    "success": False,
                    "invalid_claim": True,
                    "errors": [safe_error],
                },
            },
        )


def _normalize_worker_capabilities(capabilities: list[str] | None) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()

    def add(capability: str) -> None:
        normalized = redact_sensitive_text(str(capability or "")).strip().lower()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        ordered.append(normalized)

    def discard(capability: str) -> None:
        normalized = redact_sensitive_text(str(capability or "")).strip().lower()
        if not normalized or normalized not in seen:
            return
        seen.remove(normalized)
        ordered[:] = [item for item in ordered if item != normalized]

    for item in capabilities or []:
        add(str(item))

    declared_collectors = [
        capability for capability in list(ordered) if get_collector_metadata(capability) is not None
    ]
    for collector_id in declared_collectors:
        diagnostics = build_collector_session_diagnostics(collector_id)
        session_health = (
            str((diagnostics.get("session_state") or {}).get("health") or "").strip().lower()
        )
        derived = sorted(required_worker_capabilities(collector_id))
        if session_health == "ready":
            for capability in derived:
                add(capability)
            continue
        for capability in derived:
            discard(capability)

    return ordered
