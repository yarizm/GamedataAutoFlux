from src.services.session_registry import (
    InMemorySessionRegistry,
    StorageSessionRegistry,
    build_session_inventory_summary,
    build_session_registry_entry,
)
from src.services.session_inventory_sync import release_task_session_claim_best_effort
from src.storage.base import BaseStorage, QueryResult, StorageRecord
from src.core.task import Task


class _SessionRegistryStorage(BaseStorage):
    def __init__(self) -> None:
        super().__init__()
        self.records: dict[str, StorageRecord] = {}

    async def save(self, record: StorageRecord) -> None:
        self.records[record.key] = record

    async def load(self, key: str) -> StorageRecord | None:
        return self.records.get(key)

    async def query(self, query: str, limit: int = 10, **kwargs) -> QueryResult:
        prefix = query.removeprefix("key:") if query.startswith("key:") else query
        records = [record for key, record in self.records.items() if key.startswith(prefix)]
        return QueryResult(records=records[:limit], total=len(records), query=query)

    async def delete(self, key: str) -> bool:
        return self.records.pop(key, None) is not None


def _qimai_session_diagnostics() -> dict:
    return {
        "collector_id": "qimai",
        "display_name": "Qimai",
        "session_mode": "local_profile",
        "requires_session": True,
        "worker_binding": "sticky",
        "status": "ok",
        "session_account": {
            "account_id": "local:qimai_profile",
            "account_kind": "local_profile",
        },
        "session_state": {"health": "ready", "local_profile_ready": True},
    }


def test_build_session_registry_entry_from_diagnostics() -> None:
    entry = build_session_registry_entry(
        {
            "collector_id": "qimai",
            "display_name": "Qimai",
            "session_mode": "managed_state",
            "requires_session": True,
            "status": "ok",
            "worker_binding": "lease",
            "default_session_mode": "local_profile",
            "configured_session_mode": "managed_state",
            "session_mode_source": "config",
            "session_mode_override_status": "applied",
            "supported_session_modes": ["local_profile", "managed_state"],
            "required_worker_capabilities": ["session_mode:managed_state"],
            "credential_profiles": ["playwright_runtime", "local_browser_profile"],
            "session_account": {
                "account_id": "managed:qimai_storage_state",
                "account_kind": "managed_state",
                "locator": "data/qimai_storage_state.json",
                "locator_label": "storage_state_path",
            },
            "session_state": {
                "health": "ready",
                "mode": "managed_state",
                "storage_state_ready": True,
            },
            "session_lease": {
                "mode": "managed",
                "strategy": "exclusive_lease",
                "scope": "qimai",
                "transferable": True,
            },
        }
    )

    assert entry.session_id == "qimai:managed_state:managed:qimai_storage_state"
    assert entry.collector_id == "qimai"
    assert entry.session_mode == "managed_state"
    assert entry.account_kind == "managed_state"
    assert entry.health == "ready"
    assert entry.worker_binding == "lease"


async def test_in_memory_session_registry_upsert_and_filter() -> None:
    registry = InMemorySessionRegistry()

    qimai_entry = build_session_registry_entry(
        {
            "collector_id": "qimai",
            "display_name": "Qimai",
            "session_mode": "local_profile",
            "session_account": {
                "account_id": "local:qimai_profile",
                "account_kind": "local_profile",
            },
            "session_state": {"health": "ready"},
        }
    )
    steam_entry = build_session_registry_entry(
        {
            "collector_id": "steam",
            "display_name": "Steam",
            "session_mode": "api_only",
            "session_account": {"account_id": "", "account_kind": "not_required"},
            "session_state": {"health": "ready"},
        }
    )

    await registry.upsert(qimai_entry)
    await registry.upsert(steam_entry)

    all_entries = await registry.list_sessions()
    qimai_entries = await registry.list_sessions(collector_ids=["qimai"])
    loaded = await registry.get_session(qimai_entry.session_id)

    assert len(all_entries) == 2
    assert len(qimai_entries) == 1
    assert qimai_entries[0].collector_id == "qimai"
    assert loaded is not None
    assert loaded.session_id == qimai_entry.session_id


async def test_in_memory_session_registry_tracks_claim_and_release_lifecycle() -> None:
    registry = InMemorySessionRegistry()
    diagnostics = {
        "collector_id": "qimai",
        "display_name": "Qimai",
        "session_mode": "local_profile",
        "requires_session": True,
        "worker_binding": "sticky",
        "status": "ok",
        "session_account": {
            "account_id": "local:qimai_profile",
            "account_kind": "local_profile",
            "locator": "data/qimai_profile",
            "locator_label": "user_data_dir",
        },
        "session_state": {"health": "ready", "local_profile_ready": True},
    }

    claimed = await registry.bind_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
    )
    released = await registry.release_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
        disposition="released",
    )

    assert claimed.lease_status == "claimed"
    assert claimed.lease_worker_id == "worker-1"
    assert claimed.lease_task_id == "task-1"
    assert released.lease_status == "released"
    assert released.lease_worker_id == ""
    assert released.lease_task_id == ""
    assert released.last_worker_id == "worker-1"
    assert released.last_task_id == "task-1"


async def test_try_claim_session_rejects_other_worker_when_claimed() -> None:
    registry = InMemorySessionRegistry()
    diagnostics = {
        "collector_id": "qimai",
        "display_name": "Qimai",
        "session_mode": "local_profile",
        "requires_session": True,
        "worker_binding": "sticky",
        "status": "ok",
        "session_account": {
            "account_id": "local:qimai_profile",
            "account_kind": "local_profile",
        },
        "session_state": {"health": "ready", "local_profile_ready": True},
    }

    first = await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
    )
    rejected = await registry.try_claim_session(
        diagnostics,
        worker_id="worker-2",
        task_id="task-2",
    )
    stored = await registry.get_session(first.session_id)

    assert first is not None
    assert rejected is None
    assert stored is not None
    assert stored.lease_worker_id == "worker-1"
    assert stored.lease_task_id == "task-1"


async def test_try_claim_session_allows_same_worker_to_refresh_task() -> None:
    registry = InMemorySessionRegistry()
    diagnostics = {
        "collector_id": "qimai",
        "display_name": "Qimai",
        "session_mode": "local_profile",
        "requires_session": True,
        "worker_binding": "sticky",
        "status": "ok",
        "session_account": {
            "account_id": "local:qimai_profile",
            "account_kind": "local_profile",
        },
        "session_state": {"health": "ready", "local_profile_ready": True},
    }

    await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
    )
    refreshed = await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-2",
    )

    assert refreshed is not None
    assert refreshed.lease_worker_id == "worker-1"
    assert refreshed.lease_task_id == "task-2"
    assert refreshed.last_worker_id == "worker-1"
    assert refreshed.last_task_id == "task-2"


async def test_release_session_does_not_clear_newer_active_claim() -> None:
    registry = InMemorySessionRegistry()
    diagnostics = {
        "collector_id": "qimai",
        "display_name": "Qimai",
        "session_mode": "local_profile",
        "requires_session": True,
        "worker_binding": "sticky",
        "status": "ok",
        "session_account": {
            "account_id": "local:qimai_profile",
            "account_kind": "local_profile",
        },
        "session_state": {"health": "ready", "local_profile_ready": True},
    }

    await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
    )
    await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-2",
    )
    released = await registry.release_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
    )

    assert released.lease_status == "claimed"
    assert released.lease_worker_id == "worker-1"
    assert released.lease_task_id == "task-2"
    assert released.last_task_id == "task-2"


async def test_sync_from_diagnostics_preserves_existing_lifecycle_state() -> None:
    registry = InMemorySessionRegistry()
    diagnostics = {
        "collector_id": "qimai",
        "display_name": "Qimai",
        "session_mode": "local_profile",
        "requires_session": True,
        "worker_binding": "sticky",
        "status": "ok",
        "session_account": {
            "account_id": "local:qimai_profile",
            "account_kind": "local_profile",
        },
        "session_state": {"health": "ready", "local_profile_ready": True},
    }

    await registry.bind_session(
        diagnostics,
        worker_id="worker-sync",
        task_id="task-sync",
    )
    synced = await registry.sync_from_diagnostics(diagnostics)

    assert synced.lease_status == "claimed"
    assert synced.lease_worker_id == "worker-sync"
    assert synced.lease_task_id == "task-sync"
    assert synced.last_worker_id == "worker-sync"
    assert synced.last_task_id == "task-sync"


async def test_sync_from_diagnostics_preserves_refreshed_claim_task() -> None:
    registry = InMemorySessionRegistry()
    diagnostics = {
        "collector_id": "qimai",
        "display_name": "Qimai",
        "session_mode": "local_profile",
        "requires_session": True,
        "worker_binding": "sticky",
        "status": "ok",
        "session_account": {
            "account_id": "local:qimai_profile",
            "account_kind": "local_profile",
        },
        "session_state": {"health": "ready", "local_profile_ready": True},
    }

    await registry.try_claim_session(
        diagnostics,
        worker_id="worker-sync",
        task_id="task-1",
    )
    await registry.try_claim_session(
        diagnostics,
        worker_id="worker-sync",
        task_id="task-2",
    )
    synced = await registry.sync_from_diagnostics(diagnostics)

    assert synced.lease_status == "claimed"
    assert synced.lease_worker_id == "worker-sync"
    assert synced.lease_task_id == "task-2"
    assert synced.last_task_id == "task-2"


async def test_in_memory_release_session_by_id_releases_active_claim() -> None:
    registry = InMemorySessionRegistry()
    diagnostics = _qimai_session_diagnostics()

    claimed = await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
    )
    released = await registry.release_session_by_id(
        claimed.session_id,
        worker_id="worker-1",
        task_id="task-1",
    )

    assert claimed is not None
    assert released is not None
    assert released.lease_status == "released"
    assert released.lease_worker_id == ""
    assert released.last_worker_id == "worker-1"
    assert released.last_task_id == "task-1"


async def test_in_memory_release_session_by_id_preserves_newer_claim() -> None:
    registry = InMemorySessionRegistry()
    diagnostics = _qimai_session_diagnostics()

    claimed = await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
    )
    await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-2",
    )
    released = await registry.release_session_by_id(
        claimed.session_id,
        worker_id="worker-1",
        task_id="task-1",
    )

    assert released is not None
    assert released.lease_status == "claimed"
    assert released.lease_worker_id == "worker-1"
    assert released.lease_task_id == "task-2"
    assert released.last_task_id == "task-2"


async def test_storage_session_registry_rejects_other_worker_when_claimed() -> None:
    registry = StorageSessionRegistry(_SessionRegistryStorage())
    diagnostics = _qimai_session_diagnostics()

    first = await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
    )
    rejected = await registry.try_claim_session(
        diagnostics,
        worker_id="worker-2",
        task_id="task-2",
    )
    stored = await registry.get_session(first.session_id)

    assert first is not None
    assert rejected is None
    assert stored is not None
    assert stored.lease_status == "claimed"
    assert stored.lease_worker_id == "worker-1"
    assert stored.lease_task_id == "task-1"


async def test_storage_session_registry_allows_same_worker_to_refresh_task() -> None:
    registry = StorageSessionRegistry(_SessionRegistryStorage())
    diagnostics = _qimai_session_diagnostics()

    await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
    )
    refreshed = await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-2",
    )
    stored = await registry.get_session(refreshed.session_id)

    assert refreshed is not None
    assert stored is not None
    assert stored.lease_status == "claimed"
    assert stored.lease_worker_id == "worker-1"
    assert stored.lease_task_id == "task-2"
    assert stored.last_worker_id == "worker-1"
    assert stored.last_task_id == "task-2"


async def test_storage_session_registry_sync_preserves_refreshed_claim_task() -> None:
    registry = StorageSessionRegistry(_SessionRegistryStorage())
    diagnostics = _qimai_session_diagnostics()

    await registry.try_claim_session(
        diagnostics,
        worker_id="worker-sync",
        task_id="task-1",
    )
    await registry.try_claim_session(
        diagnostics,
        worker_id="worker-sync",
        task_id="task-2",
    )
    synced = await registry.sync_from_diagnostics(
        {
            **diagnostics,
            "status": "warning",
            "session_state": {"health": "degraded", "local_profile_ready": True},
        }
    )

    assert synced.diagnostics_status == "warning"
    assert synced.health == "degraded"
    assert synced.lease_status == "claimed"
    assert synced.lease_worker_id == "worker-sync"
    assert synced.lease_task_id == "task-2"
    assert synced.last_task_id == "task-2"


async def test_storage_release_session_by_id_releases_active_claim() -> None:
    registry = StorageSessionRegistry(_SessionRegistryStorage())
    diagnostics = _qimai_session_diagnostics()

    claimed = await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
    )
    released = await registry.release_session_by_id(
        claimed.session_id,
        worker_id="worker-1",
        task_id="task-1",
    )

    assert claimed is not None
    assert released is not None
    assert released.lease_status == "released"
    assert released.lease_worker_id == ""
    assert released.last_worker_id == "worker-1"
    assert released.last_task_id == "task-1"


async def test_storage_release_session_by_id_preserves_newer_claim() -> None:
    registry = StorageSessionRegistry(_SessionRegistryStorage())
    diagnostics = _qimai_session_diagnostics()

    claimed = await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
    )
    await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-2",
    )
    released = await registry.release_session_by_id(
        claimed.session_id,
        worker_id="worker-1",
        task_id="task-1",
    )

    assert released is not None
    assert released.lease_status == "claimed"
    assert released.lease_worker_id == "worker-1"
    assert released.lease_task_id == "task-2"
    assert released.last_task_id == "task-2"


async def test_storage_session_registry_release_does_not_clear_newer_active_claim() -> None:
    registry = StorageSessionRegistry(_SessionRegistryStorage())
    diagnostics = _qimai_session_diagnostics()

    await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
    )
    await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-2",
    )
    released = await registry.release_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
    )
    listed = await registry.list_sessions(collector_ids=["qimai"])

    assert released.lease_status == "claimed"
    assert released.lease_worker_id == "worker-1"
    assert released.lease_task_id == "task-2"
    assert released.last_task_id == "task-2"
    assert len(listed) == 1
    assert listed[0].lease_status == "claimed"
    assert listed[0].lease_worker_id == "worker-1"
    assert listed[0].lease_task_id == "task-2"


async def test_in_memory_session_registry_returns_latest_ready_entry_for_collector() -> None:
    registry = InMemorySessionRegistry()

    blocked = build_session_registry_entry(
        {
            "collector_id": "qimai",
            "display_name": "Qimai",
            "session_mode": "local_profile",
            "requires_session": True,
            "status": "error",
            "session_account": {
                "account_id": "local:qimai_profile",
                "account_kind": "local_profile",
            },
            "session_state": {"health": "blocked"},
        }
    )
    ready = build_session_registry_entry(
        {
            "collector_id": "qimai",
            "display_name": "Qimai",
            "session_mode": "managed_state",
            "requires_session": True,
            "status": "ok",
            "session_account": {
                "account_id": "managed:qimai_storage_state",
                "account_kind": "managed_state",
            },
            "session_state": {"health": "ready"},
        }
    )

    await registry.upsert(blocked)
    await registry.upsert(ready)
    latest_any = await registry.get_latest_session_for_collector("qimai")
    latest_ready = await registry.get_latest_session_for_collector("qimai", require_ready=True)

    assert latest_any is not None
    assert latest_ready is not None
    assert latest_any.collector_id == "qimai"
    assert latest_ready.health == "ready"


def test_build_session_inventory_summary_reports_latest_shape() -> None:
    qimai_entry = build_session_registry_entry(
        {
            "collector_id": "qimai",
            "display_name": "Qimai",
            "session_mode": "managed_state",
            "requires_session": True,
            "status": "ok",
            "session_account": {
                "account_id": "managed:qimai_storage_state",
                "account_kind": "managed_state",
            },
            "session_state": {
                "health": "ready",
                "storage_state_ready": True,
            },
        }
    )
    steam_entry = build_session_registry_entry(
        {
            "collector_id": "steam",
            "display_name": "Steam",
            "session_mode": "api_only",
            "requires_session": False,
            "status": "warning",
            "session_state": {
                "health": "unknown",
            },
        }
    )

    summary = build_session_inventory_summary([qimai_entry, steam_entry])

    assert summary["items"] == 2
    assert summary["collectors"] == 2
    assert summary["requires_session"] == 1
    assert summary["ready"] == 1
    assert summary["warnings"] == 1
    assert summary["errors"] == 0
    assert summary["claimed"] == 0
    assert summary["stale"] == 0
    assert summary["health"]["ready"] == 1
    assert summary["statuses"]["ok"] == 1
    assert summary["statuses"]["warning"] == 1
    assert summary["session_modes"]["managed_state"] == 1
    assert summary["lease_statuses"]["unbound"] == 2
    assert summary["latest_observed_at"] is not None


async def test_release_task_session_claim_best_effort_prefers_snapshot_release_by_id() -> None:
    registry = InMemorySessionRegistry()
    diagnostics = _qimai_session_diagnostics()
    claimed = await registry.try_claim_session(
        diagnostics,
        worker_id="worker-1",
        task_id="task-1",
    )
    task = Task(
        id="task-1",
        name="Release By Id Task",
        pipeline_name="worker_pipeline",
        collector_name="qimai",
        config={
            "worker_claim": {
                "worker_id": "worker-1",
                "session_diagnostics": diagnostics,
            }
        },
    )

    released = await release_task_session_claim_best_effort(
        registry,
        task,
        context="session_registry_test",
        worker_id="worker-1",
        task_id="task-1",
        disposition="released",
    )
    stored = await registry.get_session(claimed.session_id)

    assert released is not None
    assert stored is not None
    assert stored.lease_status == "released"
    assert stored.lease_worker_id == ""
    assert stored.last_worker_id == "worker-1"
    assert stored.last_task_id == "task-1"


async def test_release_task_session_claim_best_effort_recreates_snapshot_entry_when_missing() -> (
    None
):
    registry = InMemorySessionRegistry()
    diagnostics = _qimai_session_diagnostics()
    claimed = await registry.try_claim_session(
        diagnostics,
        worker_id="worker-2",
        task_id="task-2",
    )
    await registry.delete_session(claimed.session_id)

    task = Task(
        id="task-2",
        name="Release Missing Snapshot Task",
        pipeline_name="worker_pipeline",
        collector_name="qimai",
        config={
            "worker_claim": {
                "worker_id": "worker-2",
                "session_diagnostics": diagnostics,
            }
        },
    )

    released = await release_task_session_claim_best_effort(
        registry,
        task,
        context="session_registry_test_missing",
        worker_id="worker-2",
        task_id="task-2",
        disposition="released",
    )
    recreated = await registry.get_session(claimed.session_id)

    assert released is not None
    assert recreated is not None
    assert recreated.lease_status == "released"
    assert recreated.lease_worker_id == ""
    assert recreated.last_worker_id == "worker-2"
    assert recreated.last_task_id == "task-2"
