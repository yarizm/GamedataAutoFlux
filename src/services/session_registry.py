"""Session inventory models and persistence services."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from src.core.sensitive import redact_sensitive, redact_sensitive_text
from src.storage.base import BaseStorage, StorageRecord


class SessionRegistryEntry(BaseModel):
    """Persisted inventory snapshot for one collector session contract."""

    session_id: str
    collector_id: str
    display_name: str = ""
    session_mode: str = "api_only"
    requires_session: bool = False
    account_id: str = ""
    account_kind: str = "not_required"
    locator: str = ""
    locator_label: str = ""
    worker_binding: str = "flexible"
    health: str = "unknown"
    diagnostics_status: str = "unknown"
    required_worker_capabilities: list[str] = Field(default_factory=list)
    credential_profiles: list[str] = Field(default_factory=list)
    default_session_mode: str = "api_only"
    configured_session_mode: str = ""
    session_mode_source: str = "metadata"
    session_mode_override_status: str = "default"
    supported_session_modes: list[str] = Field(default_factory=list)
    session_state: dict[str, Any] = Field(default_factory=dict)
    session_lease: dict[str, Any] = Field(default_factory=dict)
    lease_status: str = ""
    lease_worker_id: str = ""
    lease_task_id: str = ""
    last_worker_id: str = ""
    last_task_id: str = ""
    lease_acquired_at: datetime | None = None
    lease_released_at: datetime | None = None
    observed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = "runtime_diagnostics"

    def to_public_payload(self) -> dict[str, Any]:
        """Return the API-safe representation."""
        payload = redact_sensitive(self.model_dump(mode="json"))
        payload["lease_status"] = _session_lease_status(self)
        payload["is_stale"] = payload["lease_status"] == "stale"
        return payload


class SessionRegistryService(ABC):
    """Persistence boundary for session inventory snapshots."""

    @abstractmethod
    async def upsert(self, entry: SessionRegistryEntry) -> SessionRegistryEntry:
        """Create or replace a session inventory entry."""
        ...

    async def sync_from_diagnostics(
        self,
        diagnostics: dict[str, Any],
    ) -> SessionRegistryEntry:
        """Build and persist a session inventory snapshot from diagnostics payload."""
        entry = build_session_registry_entry(diagnostics)
        existing = await self.get_session(entry.session_id)
        if existing is not None:
            entry = _merge_lifecycle_state(existing, entry)
        return await self.upsert(entry)

    async def bind_session(
        self,
        diagnostics: dict[str, Any],
        *,
        worker_id: str,
        task_id: str,
        acquired_at: datetime | None = None,
    ) -> SessionRegistryEntry:
        """Mark a persisted session snapshot as actively claimed by a worker task."""
        entry = await self.sync_from_diagnostics(diagnostics)
        if not entry.requires_session:
            return entry

        _apply_claim_state(
            entry,
            worker_id=worker_id,
            task_id=task_id,
            acquired_at=acquired_at,
        )
        return await self.upsert(entry)

    async def try_claim_session(
        self,
        diagnostics: dict[str, Any],
        *,
        worker_id: str,
        task_id: str,
        acquired_at: datetime | None = None,
    ) -> SessionRegistryEntry | None:
        """Claim a session only when it is free or already held by the same worker."""
        entry = build_session_registry_entry(diagnostics)
        if not entry.requires_session:
            return await self.upsert(entry)

        existing = await self.get_session(entry.session_id)
        if existing is not None:
            holder = redact_sensitive_text(str(existing.lease_worker_id or "")).strip()
            if _session_lease_status(existing) == "claimed" and holder and holder != worker_id:
                return None
            entry = _merge_lifecycle_state(existing, entry)

        return await self._mark_session_claimed(
            entry,
            worker_id=worker_id,
            task_id=task_id,
            acquired_at=acquired_at,
        )

    async def release_session(
        self,
        diagnostics: dict[str, Any],
        *,
        worker_id: str = "",
        task_id: str = "",
        disposition: str = "released",
        released_at: datetime | None = None,
    ) -> SessionRegistryEntry:
        """Mark a persisted session snapshot as released or interrupted."""
        entry = await self.sync_from_diagnostics(diagnostics)
        if not entry.requires_session:
            return entry

        safe_worker_id = redact_sensitive_text(str(worker_id or "")).strip()
        safe_task_id = redact_sensitive_text(str(task_id or "")).strip()
        if not _release_matches_active_claim(
            entry,
            worker_id=safe_worker_id,
            task_id=safe_task_id,
        ):
            return await self.upsert(entry)

        _apply_release_state(
            entry,
            worker_id=safe_worker_id,
            task_id=safe_task_id,
            disposition=disposition,
            released_at=released_at,
        )
        return await self.upsert(entry)

    async def release_session_by_id(
        self,
        session_id: str,
        *,
        worker_id: str = "",
        task_id: str = "",
        disposition: str = "released",
        released_at: datetime | None = None,
    ) -> SessionRegistryEntry | None:
        """Release a persisted session snapshot by its normalized session id."""
        entry = await self.get_session(session_id)
        if entry is None:
            return None
        if not entry.requires_session:
            return entry

        safe_worker_id = redact_sensitive_text(str(worker_id or "")).strip()
        safe_task_id = redact_sensitive_text(str(task_id or "")).strip()
        if not _release_matches_active_claim(
            entry,
            worker_id=safe_worker_id,
            task_id=safe_task_id,
        ):
            return entry

        _apply_release_state(
            entry,
            worker_id=safe_worker_id,
            task_id=safe_task_id,
            disposition=disposition,
            released_at=released_at,
        )
        return await self.upsert(entry)

    async def _mark_session_claimed(
        self,
        entry: SessionRegistryEntry,
        *,
        worker_id: str,
        task_id: str,
        acquired_at: datetime | None = None,
    ) -> SessionRegistryEntry:
        _apply_claim_state(
            entry,
            worker_id=worker_id,
            task_id=task_id,
            acquired_at=acquired_at,
        )
        return await self.upsert(entry)

    @abstractmethod
    async def get_session(self, session_id: str) -> SessionRegistryEntry | None:
        """Return one session inventory entry by id."""
        ...

    @abstractmethod
    async def list_sessions(
        self,
        *,
        collector_ids: list[str] | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[SessionRegistryEntry]:
        """List inventory entries ordered by most recent observation."""
        ...

    async def delete_session(self, session_id: str) -> None:
        """Delete one inventory entry when the backend supports it."""
        return None

    async def get_latest_session_for_collector(
        self,
        collector_id: str,
        *,
        require_ready: bool = False,
    ) -> SessionRegistryEntry | None:
        """Return the most recently observed session snapshot for one collector."""
        entries = await self.list_sessions(collector_ids=[collector_id], limit=200)
        for entry in entries:
            if require_ready and str(entry.health or "").strip().lower() != "ready":
                continue
            return entry
        return None


class InMemorySessionRegistry(SessionRegistryService):
    """In-memory session inventory for tests and local fallback."""

    def __init__(self) -> None:
        self._entries: dict[str, SessionRegistryEntry] = {}
        self._lock = asyncio.Lock()

    async def upsert(self, entry: SessionRegistryEntry) -> SessionRegistryEntry:
        async with self._lock:
            self._entries[entry.session_id] = entry
            return entry

    async def sync_from_diagnostics(
        self,
        diagnostics: dict[str, Any],
    ) -> SessionRegistryEntry:
        async with self._lock:
            incoming = build_session_registry_entry(diagnostics)
            entry = _merge_existing_lifecycle(
                incoming,
                existing=self._entries.get(incoming.session_id),
            )
            self._entries[entry.session_id] = entry
            return entry

    async def bind_session(
        self,
        diagnostics: dict[str, Any],
        *,
        worker_id: str,
        task_id: str,
        acquired_at: datetime | None = None,
    ) -> SessionRegistryEntry:
        async with self._lock:
            incoming = build_session_registry_entry(diagnostics)
            entry = _merge_existing_lifecycle(
                incoming,
                existing=self._entries.get(incoming.session_id),
            )
            if entry.requires_session:
                _apply_claim_state(
                    entry,
                    worker_id=worker_id,
                    task_id=task_id,
                    acquired_at=acquired_at,
                )
            self._entries[entry.session_id] = entry
            return entry

    async def try_claim_session(
        self,
        diagnostics: dict[str, Any],
        *,
        worker_id: str,
        task_id: str,
        acquired_at: datetime | None = None,
    ) -> SessionRegistryEntry | None:
        async with self._lock:
            entry = build_session_registry_entry(diagnostics)
            if not entry.requires_session:
                self._entries[entry.session_id] = entry
                return entry

            existing = self._entries.get(entry.session_id)
            if existing is not None:
                holder = redact_sensitive_text(str(existing.lease_worker_id or "")).strip()
                if _session_lease_status(existing) == "claimed" and holder and holder != worker_id:
                    return None
                entry = _merge_lifecycle_state(existing, entry)

            _apply_claim_state(
                entry,
                worker_id=worker_id,
                task_id=task_id,
                acquired_at=acquired_at,
            )
            self._entries[entry.session_id] = entry
            return entry

    async def release_session(
        self,
        diagnostics: dict[str, Any],
        *,
        worker_id: str = "",
        task_id: str = "",
        disposition: str = "released",
        released_at: datetime | None = None,
    ) -> SessionRegistryEntry:
        async with self._lock:
            entry = build_session_registry_entry(diagnostics)
            existing = self._entries.get(entry.session_id)
            if existing is not None:
                entry = _merge_lifecycle_state(existing, entry)
            if not entry.requires_session:
                self._entries[entry.session_id] = entry
                return entry

            safe_worker_id = redact_sensitive_text(str(worker_id or "")).strip()
            safe_task_id = redact_sensitive_text(str(task_id or "")).strip()
            if _release_matches_active_claim(
                entry,
                worker_id=safe_worker_id,
                task_id=safe_task_id,
            ):
                _apply_release_state(
                    entry,
                    worker_id=safe_worker_id,
                    task_id=safe_task_id,
                    disposition=disposition,
                    released_at=released_at,
                )
            self._entries[entry.session_id] = entry
            return entry

    async def get_session(self, session_id: str) -> SessionRegistryEntry | None:
        async with self._lock:
            return self._entries.get(session_id)

    async def release_session_by_id(
        self,
        session_id: str,
        *,
        worker_id: str = "",
        task_id: str = "",
        disposition: str = "released",
        released_at: datetime | None = None,
    ) -> SessionRegistryEntry | None:
        async with self._lock:
            entry = self._entries.get(session_id)
            if entry is None:
                return None
            if not entry.requires_session:
                return entry

            safe_worker_id = redact_sensitive_text(str(worker_id or "")).strip()
            safe_task_id = redact_sensitive_text(str(task_id or "")).strip()
            if _release_matches_active_claim(
                entry,
                worker_id=safe_worker_id,
                task_id=safe_task_id,
            ):
                _apply_release_state(
                    entry,
                    worker_id=safe_worker_id,
                    task_id=safe_task_id,
                    disposition=disposition,
                    released_at=released_at,
                )
                self._entries[session_id] = entry
            return entry

    async def list_sessions(
        self,
        *,
        collector_ids: list[str] | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[SessionRegistryEntry]:
        async with self._lock:
            entries = list(self._entries.values())
        return _slice_entries(
            _filter_and_sort_entries(entries, collector_ids=collector_ids),
            limit=limit,
            offset=offset,
        )

    async def delete_session(self, session_id: str) -> None:
        async with self._lock:
            self._entries.pop(session_id, None)


class StorageSessionRegistry(SessionRegistryService):
    """Storage-backed session inventory persisted in the scheduler namespace."""

    def __init__(self, storage: BaseStorage) -> None:
        self._storage = storage
        self._lock = asyncio.Lock()

    async def upsert(self, entry: SessionRegistryEntry) -> SessionRegistryEntry:
        async with self._lock:
            await self._save_unlocked(entry)
            return entry

    async def sync_from_diagnostics(
        self,
        diagnostics: dict[str, Any],
    ) -> SessionRegistryEntry:
        async with self._lock:
            entry = build_session_registry_entry(diagnostics)
            existing = _entry_from_record(await self._storage.load(_session_key(entry.session_id)))
            entry = _merge_existing_lifecycle(entry, existing=existing)
            await self._save_unlocked(entry)
            return entry

    async def bind_session(
        self,
        diagnostics: dict[str, Any],
        *,
        worker_id: str,
        task_id: str,
        acquired_at: datetime | None = None,
    ) -> SessionRegistryEntry:
        async with self._lock:
            entry = build_session_registry_entry(diagnostics)
            existing = _entry_from_record(await self._storage.load(_session_key(entry.session_id)))
            entry = _merge_existing_lifecycle(entry, existing=existing)
            if entry.requires_session:
                _apply_claim_state(
                    entry,
                    worker_id=worker_id,
                    task_id=task_id,
                    acquired_at=acquired_at,
                )
            await self._save_unlocked(entry)
            return entry

    async def try_claim_session(
        self,
        diagnostics: dict[str, Any],
        *,
        worker_id: str,
        task_id: str,
        acquired_at: datetime | None = None,
    ) -> SessionRegistryEntry | None:
        async with self._lock:
            entry = build_session_registry_entry(diagnostics)
            if not entry.requires_session:
                await self._save_unlocked(entry)
                return entry

            existing = _entry_from_record(await self._storage.load(_session_key(entry.session_id)))
            if existing is not None:
                holder = redact_sensitive_text(str(existing.lease_worker_id or "")).strip()
                if _session_lease_status(existing) == "claimed" and holder and holder != worker_id:
                    return None
                entry = _merge_lifecycle_state(existing, entry)

            _apply_claim_state(
                entry,
                worker_id=worker_id,
                task_id=task_id,
                acquired_at=acquired_at,
            )
            await self._save_unlocked(entry)
            return entry

    async def release_session(
        self,
        diagnostics: dict[str, Any],
        *,
        worker_id: str = "",
        task_id: str = "",
        disposition: str = "released",
        released_at: datetime | None = None,
    ) -> SessionRegistryEntry:
        async with self._lock:
            entry = build_session_registry_entry(diagnostics)
            existing = _entry_from_record(await self._storage.load(_session_key(entry.session_id)))
            if existing is not None:
                entry = _merge_lifecycle_state(existing, entry)
            if not entry.requires_session:
                await self._save_unlocked(entry)
                return entry

            safe_worker_id = redact_sensitive_text(str(worker_id or "")).strip()
            safe_task_id = redact_sensitive_text(str(task_id or "")).strip()
            if _release_matches_active_claim(
                entry,
                worker_id=safe_worker_id,
                task_id=safe_task_id,
            ):
                _apply_release_state(
                    entry,
                    worker_id=safe_worker_id,
                    task_id=safe_task_id,
                    disposition=disposition,
                    released_at=released_at,
                )
            await self._save_unlocked(entry)
            return entry

    async def get_session(self, session_id: str) -> SessionRegistryEntry | None:
        record = await self._storage.load(_session_key(session_id))
        return _entry_from_record(record)

    async def release_session_by_id(
        self,
        session_id: str,
        *,
        worker_id: str = "",
        task_id: str = "",
        disposition: str = "released",
        released_at: datetime | None = None,
    ) -> SessionRegistryEntry | None:
        async with self._lock:
            entry = _entry_from_record(await self._storage.load(_session_key(session_id)))
            if entry is None:
                return None
            if not entry.requires_session:
                return entry

            safe_worker_id = redact_sensitive_text(str(worker_id or "")).strip()
            safe_task_id = redact_sensitive_text(str(task_id or "")).strip()
            if _release_matches_active_claim(
                entry,
                worker_id=safe_worker_id,
                task_id=safe_task_id,
            ):
                _apply_release_state(
                    entry,
                    worker_id=safe_worker_id,
                    task_id=safe_task_id,
                    disposition=disposition,
                    released_at=released_at,
                )
                await self._save_unlocked(entry)
            return entry

    async def list_sessions(
        self,
        *,
        collector_ids: list[str] | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[SessionRegistryEntry]:
        result = await self._storage.query(f"key:{_session_prefix()}", limit=5000)
        entries = [_entry_from_record(record) for record in result.records]
        filtered = _filter_and_sort_entries(
            [entry for entry in entries if entry is not None],
            collector_ids=collector_ids,
        )
        return _slice_entries(filtered, limit=limit, offset=offset)

    async def delete_session(self, session_id: str) -> None:
        await self._storage.delete(_session_key(session_id))

    async def _save_unlocked(self, entry: SessionRegistryEntry) -> None:
        await self._storage.save(
            StorageRecord(
                key=_session_key(entry.session_id),
                data=entry.model_dump(mode="json"),
                metadata={
                    "kind": "session_inventory",
                    "session_id": entry.session_id,
                    "collector_id": entry.collector_id,
                    "session_mode": entry.session_mode,
                    "health": entry.health,
                },
                source="session_registry",
                tags=[
                    "session_inventory",
                    entry.collector_id,
                    entry.session_mode,
                    entry.health,
                ],
            )
        )


def build_session_inventory_summary(
    entries: list[SessionRegistryEntry],
) -> dict[str, Any]:
    """Build a compact summary for persisted session inventory views."""
    health: dict[str, int] = {}
    statuses: dict[str, int] = {}
    session_modes: dict[str, int] = {}
    lease_statuses: dict[str, int] = {}
    latest_observed_at: datetime | None = None

    for entry in entries:
        health_key = str(entry.health or "unknown")
        status_key = str(entry.diagnostics_status or "unknown")
        mode_key = str(entry.session_mode or "api_only")
        lease_key = _session_lease_status(entry)

        health[health_key] = health.get(health_key, 0) + 1
        statuses[status_key] = statuses.get(status_key, 0) + 1
        session_modes[mode_key] = session_modes.get(mode_key, 0) + 1
        lease_statuses[lease_key] = lease_statuses.get(lease_key, 0) + 1

        if latest_observed_at is None or entry.observed_at > latest_observed_at:
            latest_observed_at = entry.observed_at

    return {
        "items": len(entries),
        "collectors": len({entry.collector_id for entry in entries}),
        "requires_session": sum(1 for entry in entries if entry.requires_session),
        "ready": health.get("ready", 0),
        "warnings": statuses.get("warning", 0),
        "errors": statuses.get("error", 0),
        "claimed": lease_statuses.get("claimed", 0),
        "stale": lease_statuses.get("stale", 0),
        "health": health,
        "statuses": statuses,
        "session_modes": session_modes,
        "lease_statuses": lease_statuses,
        "latest_observed_at": (
            latest_observed_at.isoformat() if latest_observed_at is not None else None
        ),
    }


def build_session_registry_entry(diagnostics: dict[str, Any]) -> SessionRegistryEntry:
    """Normalize session diagnostics into a persisted inventory entry."""
    collector_id = redact_sensitive_text(str(diagnostics.get("collector_id") or "")).strip()
    session_account = diagnostics.get("session_account", {})
    session_state = diagnostics.get("session_state", {})
    session_lease = diagnostics.get("session_lease", {})
    account_id = redact_sensitive_text(str((session_account or {}).get("account_id") or "")).strip()
    session_mode = redact_sensitive_text(str(diagnostics.get("session_mode") or "api_only")).strip()
    session_id = _build_session_id(
        collector_id=collector_id,
        session_mode=session_mode,
        account_id=account_id,
    )
    observed_at = datetime.now(timezone.utc)

    return SessionRegistryEntry(
        session_id=session_id,
        collector_id=collector_id,
        display_name=redact_sensitive_text(str(diagnostics.get("display_name") or collector_id)),
        session_mode=session_mode or "api_only",
        requires_session=bool(diagnostics.get("requires_session", False)),
        account_id=account_id,
        account_kind=redact_sensitive_text(
            str((session_account or {}).get("account_kind") or "not_required")
        ),
        locator=redact_sensitive_text(str((session_account or {}).get("locator") or "")),
        locator_label=redact_sensitive_text(
            str((session_account or {}).get("locator_label") or "")
        ),
        worker_binding=redact_sensitive_text(str(diagnostics.get("worker_binding") or "flexible")),
        health=redact_sensitive_text(str((session_state or {}).get("health") or "unknown")),
        diagnostics_status=redact_sensitive_text(str(diagnostics.get("status") or "unknown")),
        required_worker_capabilities=[
            redact_sensitive_text(str(item))
            for item in diagnostics.get("required_worker_capabilities", []) or []
            if str(item or "").strip()
        ],
        credential_profiles=[
            redact_sensitive_text(str(item))
            for item in diagnostics.get("credential_profiles", []) or []
            if str(item or "").strip()
        ],
        default_session_mode=redact_sensitive_text(
            str(diagnostics.get("default_session_mode") or "api_only")
        ),
        configured_session_mode=redact_sensitive_text(
            str(diagnostics.get("configured_session_mode") or "")
        ),
        session_mode_source=redact_sensitive_text(
            str(diagnostics.get("session_mode_source") or "metadata")
        ),
        session_mode_override_status=redact_sensitive_text(
            str(diagnostics.get("session_mode_override_status") or "default")
        ),
        supported_session_modes=[
            redact_sensitive_text(str(item))
            for item in diagnostics.get("supported_session_modes", []) or []
            if str(item or "").strip()
        ],
        session_state=redact_sensitive(session_state or {}),
        session_lease=redact_sensitive(session_lease or {}),
        observed_at=observed_at,
        source="runtime_diagnostics",
    )


def _merge_lifecycle_state(
    existing: SessionRegistryEntry,
    incoming: SessionRegistryEntry,
) -> SessionRegistryEntry:
    incoming.lease_status = existing.lease_status
    incoming.lease_worker_id = existing.lease_worker_id
    incoming.lease_task_id = existing.lease_task_id
    incoming.last_worker_id = existing.last_worker_id
    incoming.last_task_id = existing.last_task_id
    incoming.lease_acquired_at = existing.lease_acquired_at
    incoming.lease_released_at = existing.lease_released_at
    return incoming


def _merge_existing_lifecycle(
    incoming: SessionRegistryEntry,
    *,
    existing: SessionRegistryEntry | None,
) -> SessionRegistryEntry:
    if existing is None:
        return incoming
    return _merge_lifecycle_state(existing, incoming)


def _release_matches_active_claim(
    entry: SessionRegistryEntry,
    *,
    worker_id: str,
    task_id: str,
) -> bool:
    if _session_lease_status(entry) != "claimed":
        return True
    active_worker_id = redact_sensitive_text(str(entry.lease_worker_id or "")).strip()
    active_task_id = redact_sensitive_text(str(entry.lease_task_id or "")).strip()
    if worker_id and active_worker_id and worker_id != active_worker_id:
        return False
    if task_id and active_task_id and task_id != active_task_id:
        return False
    return True


def _apply_release_state(
    entry: SessionRegistryEntry,
    *,
    worker_id: str,
    task_id: str,
    disposition: str,
    released_at: datetime | None = None,
) -> None:
    entry.lease_status = "interrupted" if disposition == "interrupted" else "released"
    entry.lease_worker_id = ""
    entry.lease_task_id = ""
    if worker_id:
        entry.last_worker_id = worker_id
    if task_id:
        entry.last_task_id = task_id
    entry.lease_released_at = released_at or datetime.now(timezone.utc)


def _apply_claim_state(
    entry: SessionRegistryEntry,
    *,
    worker_id: str,
    task_id: str,
    acquired_at: datetime | None = None,
) -> None:
    safe_worker_id = redact_sensitive_text(str(worker_id or "")).strip()
    safe_task_id = redact_sensitive_text(str(task_id or "")).strip()
    entry.lease_status = "claimed"
    entry.lease_worker_id = safe_worker_id
    entry.lease_task_id = safe_task_id
    entry.last_worker_id = safe_worker_id
    entry.last_task_id = safe_task_id
    entry.lease_acquired_at = acquired_at or datetime.now(timezone.utc)
    entry.lease_released_at = None


def _build_session_id(
    *,
    collector_id: str,
    session_mode: str,
    account_id: str,
) -> str:
    normalized_mode = session_mode or "api_only"
    normalized_account = account_id or "default"
    return f"{collector_id}:{normalized_mode}:{normalized_account}"


def _session_prefix() -> str:
    return "session_inventory:"


def _session_key(session_id: str) -> str:
    return f"{_session_prefix()}{session_id}"


def _entry_from_record(record: StorageRecord | None) -> SessionRegistryEntry | None:
    if record is None:
        return None
    try:
        if isinstance(record.data, SessionRegistryEntry):
            return record.data
        if isinstance(record.data, dict):
            return SessionRegistryEntry.model_validate(record.data)
    except Exception:
        return None
    return None


def _session_lease_status(entry: SessionRegistryEntry) -> str:
    lease_status = str(entry.lease_status or "").strip().lower()
    if lease_status == "claimed":
        return "claimed" if entry.lease_worker_id or entry.lease_task_id else "released"
    if lease_status in {"released", "interrupted"}:
        return lease_status
    if entry.lease_worker_id or entry.lease_task_id:
        return "claimed"
    if entry.last_worker_id or entry.last_task_id:
        return "stale"
    return "unbound"


def _filter_and_sort_entries(
    entries: list[SessionRegistryEntry],
    *,
    collector_ids: list[str] | None,
) -> list[SessionRegistryEntry]:
    allowed = {str(item).strip() for item in collector_ids or [] if str(item or "").strip()}
    filtered = [entry for entry in entries if not allowed or entry.collector_id in allowed]
    filtered.sort(
        key=lambda entry: (
            entry.observed_at,
            entry.collector_id,
            entry.session_id,
        ),
        reverse=True,
    )
    return filtered


def _slice_entries(
    entries: list[SessionRegistryEntry],
    *,
    limit: int,
    offset: int,
) -> list[SessionRegistryEntry]:
    safe_offset = max(0, int(offset or 0))
    safe_limit = min(max(1, int(limit or 200)), 1000)
    return entries[safe_offset : safe_offset + safe_limit]
