"""Serialized plugin operation service tests."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from src.plugin_manager.installer import PluginInstallError
from src.plugin_manager.models import ManagedPluginRecord, PluginReference
from src.plugin_manager.operations import OperationConflictError, PluginOperationService
from src.plugin_manager.store import PluginStateStore


class FakeInstaller:
    def __init__(self, *, failure: PluginInstallError | None = None) -> None:
        self.failure = failure
        self.calls: list[tuple[str, str | None]] = []
        self.lifecycle_calls: list[str] = []

    def install_catalog(
        self,
        plugin_id: str,
        *,
        requested_version: str | None,
        progress,
        operation_id: str,
    ) -> dict:
        self.calls.append((plugin_id, requested_version))
        progress("building_wheel", 25, "Building wheel")
        if self.failure is not None:
            raise self.failure
        progress("generation_ready", 85, "Generation ready")
        return {
            "generation_id": "gen_test",
            "restart_required": True,
            "operation_id": operation_id,
        }

    def upgrade_catalog(
        self,
        plugin_id: str,
        *,
        requested_version: str | None,
        progress,
        operation_id: str,
    ) -> dict:
        self.lifecycle_calls.append(f"upgrade:{plugin_id}:{requested_version}")
        progress("generation_ready", 85, "Upgrade ready")
        return {"generation_id": "gen_upgrade", "restart_required": True}

    def uninstall(self, plugin_id: str, *, progress, operation_id: str) -> dict:
        self.lifecycle_calls.append(f"uninstall:{plugin_id}")
        progress("generation_ready", 85, "Uninstall ready")
        return {"generation_id": "gen_uninstall", "restart_required": True}

    def rollback(self, *, progress, operation_id: str) -> dict:
        self.lifecycle_calls.append("rollback")
        progress("switching_generation", 70, "Rollback ready")
        return {"generation_id": "gen_previous", "restart_required": True}


async def _wait_for_terminal(store: PluginStateStore, operation_id: str):
    for _ in range(200):
        operation = store.get_operation(operation_id)
        if operation is not None and operation.state in {"succeeded", "failed"}:
            return operation
        await asyncio.sleep(0.01)
    raise AssertionError("plugin operation did not reach a terminal state")


@pytest.mark.asyncio
async def test_operation_service_serializes_and_rejects_conflicts(tmp_path: Path) -> None:
    store = PluginStateStore(tmp_path / "manager")
    installer = FakeInstaller()
    service = PluginOperationService(store=store, installer=installer)  # type: ignore[arg-type]
    await service.start()
    try:
        operation = await service.submit_catalog_install(
            "official.youtube",
            requested_version="0.1.0",
        )
        with pytest.raises(OperationConflictError):
            await service.submit_catalog_install("official.steam")

        finished = await _wait_for_terminal(store, operation.operation_id)
        assert finished.state == "succeeded"
        assert finished.restart_required is True
        assert finished.progress == 100
        assert installer.calls == [("official.youtube", "0.1.0")]
        assert any(log["message"] == "Building wheel" for log in finished.logs)
        assert store.list_audit_events()[0]["action"] == "operation.succeeded"
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_operation_service_persists_stable_failure(tmp_path: Path) -> None:
    store = PluginStateStore(tmp_path / "manager")
    installer = FakeInstaller(
        failure=PluginInstallError("PLUGIN_INCOMPATIBLE", "Core API mismatch")
    )
    service = PluginOperationService(store=store, installer=installer)  # type: ignore[arg-type]
    await service.start()
    try:
        operation = await service.submit_catalog_install("official.youtube")
        finished = await _wait_for_terminal(store, operation.operation_id)
        assert finished.state == "failed"
        assert finished.error_code == "PLUGIN_INCOMPATIBLE"
        assert finished.error_message == "Core API mismatch"
        assert finished.logs[-1]["level"] == "error"
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_queued_operation_resumes_when_service_starts(tmp_path: Path) -> None:
    store = PluginStateStore(tmp_path / "manager")
    queued = store.create_operation(
        operation_id="op_queued",
        kind="install",
        plugin_id="official.youtube",
        requested_version=None,
        request={"source": "catalog"},
    )
    installer = FakeInstaller()
    service = PluginOperationService(store=store, installer=installer)  # type: ignore[arg-type]
    await service.start()
    try:
        finished = await _wait_for_terminal(store, queued.operation_id)
        assert finished.state == "succeeded"
        assert installer.calls == [("official.youtube", None)]
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_lifecycle_operations_dispatch_through_serial_worker(tmp_path: Path) -> None:
    store = PluginStateStore(tmp_path / "manager")
    installer = FakeInstaller()
    service = PluginOperationService(store=store, installer=installer)  # type: ignore[arg-type]
    await service.start()
    try:
        upgrade = await service.submit_catalog_upgrade(
            "official.youtube",
            requested_version="0.2.0",
        )
        assert (await _wait_for_terminal(store, upgrade.operation_id)).state == "succeeded"

        uninstall = await service.submit_uninstall("official.youtube")
        finished_uninstall = await _wait_for_terminal(store, uninstall.operation_id)
        assert finished_uninstall.state == "succeeded"
        assert finished_uninstall.restart_required is True

        rollback = await service.submit_rollback()
        assert (await _wait_for_terminal(store, rollback.operation_id)).state == "succeeded"
        assert installer.lifecycle_calls == [
            "upgrade:official.youtube:0.2.0",
            "uninstall:official.youtube",
            "rollback",
        ]
    finally:
        await service.stop()


class _ReferencedScanner:
    async def scan(self, collectors):
        return [PluginReference("pipeline", "youtube", "youtube", collectors[0])]


@pytest.mark.asyncio
async def test_worker_rechecks_references_before_uninstall(tmp_path: Path) -> None:
    store = PluginStateStore(tmp_path / "manager")
    store.replace_managed_plugins(
        [
            ManagedPluginRecord(
                plugin_id="official.youtube",
                distribution="autoflux-plugin-youtube",
                version="0.1.0",
                source_type="catalog",
                source_ref="official.youtube@0.1.0",
                artifact_path="cache/youtube.whl",
                artifact_sha256="sha",
                desired_state="disabled",
                collectors=("youtube_profiles",),
                restart_required=False,
            )
        ]
    )
    installer = FakeInstaller()
    service = PluginOperationService(
        store=store,
        installer=installer,  # type: ignore[arg-type]
        reference_scanner=_ReferencedScanner(),  # type: ignore[arg-type]
    )
    await service.start()
    try:
        operation = await service.submit_uninstall("official.youtube")
        finished = await _wait_for_terminal(store, operation.operation_id)
        assert finished.state == "failed"
        assert finished.error_code == "PLUGIN_REFERENCED"
        assert installer.lifecycle_calls == []
    finally:
        await service.stop()
