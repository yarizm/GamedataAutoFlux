"""SQLite plugin-manager state and audit persistence tests."""

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import sqlite3
from threading import Barrier

import pytest

from src.plugin_manager.models import ManagedArtifactRecord, ManagedPluginRecord
from src.plugin_manager.store import PluginStateStore


def test_operation_lifecycle_and_audit_are_persistent(tmp_path: Path) -> None:
    store = PluginStateStore(tmp_path / "manager")
    created = store.create_operation(
        operation_id="op_test",
        kind="install",
        plugin_id="official.youtube",
        requested_version="0.1.0",
        request={"source": "catalog"},
    )

    assert created.state == "queued"
    assert created.request == {"source": "catalog"}
    assert store.has_active_operation() is True

    store.update_operation(
        "op_test",
        state="running",
        stage="building_generation",
        progress=55,
        started_at="2026-07-21T00:00:00+00:00",
    )
    updated = store.append_operation_log("op_test", "Building generation")
    assert updated.state == "running"
    assert updated.progress == 55
    assert updated.logs[-1]["message"] == "Building generation"

    finished = store.update_operation(
        "op_test",
        state="succeeded",
        stage="completed",
        progress=100,
        finished_at="2026-07-21T00:01:00+00:00",
        restart_required=True,
    )
    assert finished.restart_required is True
    assert store.has_active_operation() is False
    assert store.list_operations()[0].operation_id == "op_test"

    audit = store.list_audit_events()
    assert audit[0]["action"] == "operation.queued"
    assert audit[0]["target"] == "official.youtube"


def test_managed_plugins_and_generation_replace_atomically(tmp_path: Path) -> None:
    store = PluginStateStore(tmp_path / "manager")
    youtube = ManagedPluginRecord(
        plugin_id="official.youtube",
        distribution="autoflux-plugin-youtube",
        version="0.1.0",
        source_type="catalog",
        source_ref="official.youtube@0.1.0",
        artifact_path="cache/youtube.whl",
        artifact_sha256="abc123",
        display_name="YouTube",
        collectors=("youtube_profiles", "youtube_comments"),
        runtime_name="autoflux-plugin-youtube",
        artifacts=(
            ManagedArtifactRecord(
                distribution="autoflux-plugin-youtube",
                version="0.1.0",
                path="cache/youtube.whl",
                sha256="abc123",
            ),
        ),
        installed_at="2026-07-21T00:00:00+00:00",
    )

    store.replace_managed_plugins([youtube])
    stored = store.list_managed_plugins()
    assert stored == [youtube]

    store.record_generation(
        generation_id="gen_001",
        site_packages="generations/gen_001/site-packages",
        previous_generation_id=None,
        plugins=stored,
    )
    assert store.mark_runtime_reconciled(["autoflux_plugin_youtube"]) == 1
    assert store.list_managed_plugins()[0].restart_required is False

    store.replace_managed_plugins([])
    assert store.list_managed_plugins() == []


def test_running_operation_is_recovered_as_interrupted(tmp_path: Path) -> None:
    store = PluginStateStore(tmp_path / "manager")
    store.create_operation(
        operation_id="op_interrupted",
        kind="install",
        plugin_id="official.youtube",
        requested_version=None,
        request={},
    )
    store.update_operation("op_interrupted", state="running", stage="installing")

    assert store.recover_interrupted_operations() == 1
    recovered = store.get_operation("op_interrupted")
    assert recovered is not None
    assert recovered.state == "failed"
    assert recovered.error_code == "PLUGIN_OPERATION_INTERRUPTED"


def test_terminal_operation_can_be_deleted_without_removing_audit(
    tmp_path: Path,
) -> None:
    store = PluginStateStore(tmp_path / "manager")
    store.create_operation(
        operation_id="op_failed",
        kind="install",
        plugin_id="official.steam",
        requested_version="0.1.0",
        request={"source": "catalog"},
    )
    store.update_operation(
        "op_failed",
        state="failed",
        stage="failed",
        error_code="PLUGIN_WHEEL_BUILD_FAILED",
    )

    deleted = store.delete_operation("op_failed")

    assert deleted.state == "failed"
    assert store.get_operation("op_failed") is None
    audit = store.list_audit_events()
    assert audit[0]["action"] == "operation.history.deleted"
    assert audit[0]["operation_id"] == "op_failed"
    assert audit[0]["details"] == {"kind": "install", "state": "failed"}


def test_active_operation_cannot_be_deleted(tmp_path: Path) -> None:
    store = PluginStateStore(tmp_path / "manager")
    store.create_operation(
        operation_id="op_active",
        kind="install",
        plugin_id="official.steam",
        requested_version=None,
        request={},
    )

    with pytest.raises(ValueError, match="active plugin operations"):
        store.delete_operation("op_active")

    assert store.get_operation("op_active") is not None


def test_operation_enqueue_is_atomic_across_store_instances(tmp_path: Path) -> None:
    manager_dir = tmp_path / "manager"
    stores = [PluginStateStore(manager_dir), PluginStateStore(manager_dir)]
    barrier = Barrier(2)

    def enqueue(index: int):
        barrier.wait()
        return stores[index].create_operation_if_idle(
            operation_id=f"op_{index}",
            kind="install",
            plugin_id=f"official.plugin-{index}",
            requested_version=None,
            request={},
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(enqueue, range(2)))

    assert sum(item is not None for item in results) == 1
    assert len(stores[0].list_operations(states=("queued",))) == 1


def test_store_migrates_legacy_plugin_lock_columns(tmp_path: Path) -> None:
    manager_dir = tmp_path / "manager"
    manager_dir.mkdir()
    connection = sqlite3.connect(manager_dir / "state.sqlite3")
    connection.execute(
        """
        CREATE TABLE managed_plugins (
            plugin_id TEXT PRIMARY KEY,
            distribution TEXT NOT NULL,
            version TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_ref TEXT NOT NULL,
            artifact_path TEXT NOT NULL,
            artifact_sha256 TEXT NOT NULL,
            desired_state TEXT NOT NULL DEFAULT 'enabled',
            trust TEXT NOT NULL DEFAULT 'official',
            display_name TEXT NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT '',
            collectors_json TEXT NOT NULL DEFAULT '[]',
            installed_at TEXT NOT NULL,
            restart_required INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    connection.commit()
    connection.close()

    store = PluginStateStore(manager_dir)
    store.initialize()

    connection = sqlite3.connect(store.database_path)
    columns = {row[1] for row in connection.execute("PRAGMA table_info(managed_plugins)")}
    connection.close()
    assert {"runtime_name", "artifacts_json"} <= columns
