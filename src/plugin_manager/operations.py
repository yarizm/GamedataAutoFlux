"""Persistent, serialized plugin lifecycle operations."""

from __future__ import annotations

import asyncio
import logging
import uuid
from pathlib import Path

from src.plugin_manager.installer import (
    PluginInstallError,
    PluginInstaller,
    plugin_installer,
)
from src.plugin_manager.locking import PluginManagerProcessLock
from src.plugin_manager.models import OperationRecord
from src.plugin_manager.references import (
    PluginReferenceScanner,
    PluginReferenceScanError,
)
from src.plugin_manager.store import PluginStateStore, plugin_state_store, utc_now


logger = logging.getLogger(__name__)


class OperationConflictError(RuntimeError):
    """Raised when another plugin mutation is already queued or running."""

    code = "PLUGIN_OPERATION_CONFLICT"


class PluginOperationService:
    """Run at most one persistent plugin mutation at a time."""

    def __init__(
        self,
        *,
        store: PluginStateStore = plugin_state_store,
        installer: PluginInstaller = plugin_installer,
        reference_scanner: PluginReferenceScanner | None = None,
    ) -> None:
        self.store = store
        self.installer = installer
        self.reference_scanner = reference_scanner
        self._queue: asyncio.Queue[str | None] | None = None
        self._worker: asyncio.Task[None] | None = None
        self._submit_lock: asyncio.Lock | None = None
        self._process_lock: PluginManagerProcessLock | None = None

    async def start(self) -> None:
        if self._worker is not None and not self._worker.done():
            return
        from src.plugin_manager.environment import get_plugin_manager_access

        if not get_plugin_manager_access()["mutable"]:
            return
        process_lock = PluginManagerProcessLock(self.store.manager_dir)
        if not process_lock.acquire():
            logger.warning("Plugin mutation ownership is held by another process")
            return
        self._process_lock = process_lock
        try:
            self.store.initialize()
            interrupted = self.store.recover_interrupted_operations()
            if interrupted:
                logger.warning("Recovered %d interrupted plugin operation(s)", interrupted)
            self._queue = asyncio.Queue()
            self._submit_lock = asyncio.Lock()
            queued = self.store.list_operations(limit=200, states=("queued",))
            for operation in reversed(queued):
                self._queue.put_nowait(operation.operation_id)
            self._worker = asyncio.create_task(
                self._run_worker(),
                name="plugin-operation-worker",
            )
        except Exception:
            process_lock.release()
            self._process_lock = None
            raise

    async def stop(self) -> None:
        worker = self._worker
        queue = self._queue
        try:
            if worker is not None:
                if not worker.done() and queue is not None:
                    await queue.put(None)
                await worker
        finally:
            self._worker = None
            self._queue = None
            self._submit_lock = None
            process_lock = self._process_lock
            self._process_lock = None
            if process_lock is not None:
                process_lock.release()

    async def submit_catalog_install(
        self,
        plugin_id: str,
        *,
        requested_version: str | None = None,
    ) -> OperationRecord:
        return await self._submit(
            kind="install",
            plugin_id=plugin_id,
            requested_version=requested_version,
            request={"source": "catalog"},
        )

    async def submit_wheel_install(
        self,
        artifact_path: Path,
        *,
        original_filename: str,
    ) -> OperationRecord:
        resolved = artifact_path.resolve()
        try:
            relative_path = str(resolved.relative_to(self.store.manager_dir))
        except ValueError as exc:
            raise ValueError("uploaded artifact must be inside the plugin manager") from exc
        return await self._submit(
            kind="install",
            plugin_id="local-wheel",
            requested_version=None,
            request={
                "source": "upload",
                "artifact_path": relative_path,
                "original_filename": original_filename,
            },
        )

    async def submit_catalog_upgrade(
        self,
        plugin_id: str,
        *,
        requested_version: str | None = None,
    ) -> OperationRecord:
        return await self._submit(
            kind="upgrade",
            plugin_id=plugin_id,
            requested_version=requested_version,
            request={"source": "catalog"},
        )

    async def submit_uninstall(self, plugin_id: str) -> OperationRecord:
        return await self._submit(
            kind="uninstall",
            plugin_id=plugin_id,
            requested_version=None,
            request={"source": "managed"},
        )

    async def submit_rollback(self) -> OperationRecord:
        return await self._submit(
            kind="rollback",
            plugin_id="managed-environment",
            requested_version=None,
            request={"source": "generation"},
        )

    async def _submit(
        self,
        *,
        kind: str,
        plugin_id: str,
        requested_version: str | None,
        request: dict,
    ) -> OperationRecord:
        queue = self._queue
        submit_lock = self._submit_lock
        if queue is None or submit_lock is None or self._worker is None:
            raise RuntimeError("plugin operation service is not running")
        async with submit_lock:
            operation_id = f"op_{uuid.uuid4().hex}"
            operation = self.store.create_operation_if_idle(
                operation_id=operation_id,
                kind=kind,
                plugin_id=plugin_id,
                requested_version=requested_version,
                request=request,
            )
            if operation is None:
                raise OperationConflictError(
                    "Another plugin operation is already queued or running."
                )
            await queue.put(operation_id)
            return operation

    async def _run_worker(self) -> None:
        queue = self._queue
        if queue is None:  # pragma: no cover - guarded by start()
            return
        while True:
            operation_id = await queue.get()
            try:
                if operation_id is None:
                    return
                await self._execute(operation_id)
            finally:
                queue.task_done()

    async def _execute(self, operation_id: str) -> None:
        operation = self.store.get_operation(operation_id)
        if operation is None:
            logger.error("Queued plugin operation disappeared: %s", operation_id)
            return
        self.store.update_operation(
            operation_id,
            state="running",
            stage="starting",
            progress=1,
            started_at=utc_now(),
        )
        self.store.append_operation_log(operation_id, "Plugin operation started")

        if operation.kind == "uninstall" and self.reference_scanner is not None:
            managed = self.store.get_managed_plugin(operation.plugin_id)
            if managed is None:
                self._mark_failed(
                    operation,
                    "PLUGIN_NOT_INSTALLED",
                    f"Managed plugin is not installed: {operation.plugin_id}",
                )
                return
            try:
                references = await self.reference_scanner.scan(managed.collectors)
            except PluginReferenceScanError as exc:
                self._mark_failed(operation, exc.code, str(exc))
                return
            if references:
                names = ", ".join(
                    f"{item.kind}:{item.name}" for item in references[:8]
                )
                self._mark_failed(
                    operation,
                    "PLUGIN_REFERENCED",
                    f"Plugin is still referenced by {names}.",
                )
                return

        last_event: tuple[str, int, str] | None = None

        def report(stage: str, progress: int, message: str) -> None:
            nonlocal last_event
            event = (stage, progress, message)
            self.store.update_operation(
                operation_id,
                stage=stage,
                progress=progress,
            )
            if event != last_event:
                self.store.append_operation_log(operation_id, message)
                last_event = event

        try:
            result = await asyncio.to_thread(
                self._execute_sync,
                operation,
                report,
            )
        except PluginInstallError as exc:
            self._mark_failed(operation, exc.code, str(exc))
        except (OSError, ValueError) as exc:
            self._mark_failed(operation, "PLUGIN_PACKAGE_INVALID", str(exc))
        except Exception:  # pragma: no cover - defensive boundary
            logger.exception("Unexpected plugin operation failure: %s", operation_id)
            self._mark_failed(
                operation,
                "PLUGIN_OPERATION_FAILED",
                "The plugin operation failed unexpectedly. Check server logs.",
            )
        else:
            self.store.update_operation(
                operation_id,
                state="succeeded",
                stage="completed",
                progress=100,
                finished_at=utc_now(),
                restart_required=bool(result.get("restart_required")),
            )
            self.store.append_operation_log(operation_id, "Plugin operation completed")
            self.store.record_audit(
                "operation.succeeded",
                operation.plugin_id,
                operation_id=operation_id,
                details={
                    "kind": operation.kind,
                    "generation_id": result.get("generation_id"),
                    "restart_required": bool(result.get("restart_required")),
                },
            )

    def _execute_sync(self, operation: OperationRecord, report) -> dict:
        source = operation.request.get("source")
        if operation.kind == "upgrade" and source == "catalog":
            return self.installer.upgrade_catalog(
                operation.plugin_id,
                requested_version=operation.requested_version,
                progress=report,
                operation_id=operation.operation_id,
            )
        if operation.kind == "uninstall":
            return self.installer.uninstall(
                operation.plugin_id,
                progress=report,
                operation_id=operation.operation_id,
            )
        if operation.kind == "rollback":
            return self.installer.rollback(
                progress=report,
                operation_id=operation.operation_id,
            )
        if source == "catalog":
            return self.installer.install_catalog(
                operation.plugin_id,
                requested_version=operation.requested_version,
                progress=report,
                operation_id=operation.operation_id,
            )
        if source == "upload":
            artifact = self.store.manager_dir / str(operation.request["artifact_path"])
            cached_path, _metadata = self.installer.prepare_uploaded_wheel(artifact)
            try:
                return self.installer.install_uploaded(
                    cached_path,
                    progress=report,
                    operation_id=operation.operation_id,
                )
            finally:
                uploads_dir = (self.store.manager_dir / "uploads").resolve()
                resolved_artifact = artifact.resolve()
                if resolved_artifact.is_file() and resolved_artifact.is_relative_to(
                    uploads_dir
                ):
                    resolved_artifact.unlink()
                    if resolved_artifact.parent != uploads_dir:
                        try:
                            resolved_artifact.parent.rmdir()
                        except OSError:
                            pass
        raise PluginInstallError(
            "PLUGIN_OPERATION_INVALID",
            f"Unsupported plugin operation source: {source}",
        )

    def _mark_failed(
        self,
        operation: OperationRecord,
        error_code: str,
        error_message: str,
    ) -> None:
        self.store.update_operation(
            operation.operation_id,
            state="failed",
            stage="failed",
            finished_at=utc_now(),
            error_code=error_code,
            error_message=error_message,
        )
        self.store.append_operation_log(
            operation.operation_id,
            error_message,
            level="error",
        )
        self.store.record_audit(
            "operation.failed",
            operation.plugin_id,
            operation_id=operation.operation_id,
            details={"kind": operation.kind, "error_code": error_code},
        )
