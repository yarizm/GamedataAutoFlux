"""SQLite persistence for managed plugins, generations, operations, and audit."""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.plugin_manager.environment import get_plugin_manager_dir
from src.plugin_manager.models import (
    ManagedArtifactRecord,
    ManagedPluginRecord,
    OperationRecord,
)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class PluginStateStore:
    """Small local control-plane store independent of the application database."""

    def __init__(self, manager_dir: Path | None = None) -> None:
        self._manager_dir = manager_dir
        self._lock = threading.RLock()

    @property
    def manager_dir(self) -> Path:
        return (self._manager_dir or get_plugin_manager_dir()).resolve()

    @property
    def database_path(self) -> Path:
        return self.manager_dir / "state.sqlite3"

    @contextmanager
    def _connection(self):
        self.manager_dir.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        try:
            connection.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                connection.close()
                raise
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def _read_connection(self):
        """Open query-only in read-only deployments without creating state."""

        from src.plugin_manager.environment import get_plugin_manager_access

        mutable = bool(get_plugin_manager_access()["mutable"])
        if not self.database_path.is_file():
            if not mutable:
                yield None
                return
            self.initialize()
        if mutable:
            with self._connection() as connection:
                yield connection
            return
        uri = self.database_path.resolve().as_uri() + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        with self._lock, self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS managed_plugins (
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
                    runtime_name TEXT NOT NULL DEFAULT '',
                    artifacts_json TEXT NOT NULL DEFAULT '[]',
                    installed_at TEXT NOT NULL,
                    restart_required INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS generations (
                    generation_id TEXT PRIMARY KEY,
                    site_packages TEXT NOT NULL,
                    state TEXT NOT NULL,
                    previous_generation_id TEXT,
                    lock_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    activated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS operations (
                    operation_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    plugin_id TEXT NOT NULL,
                    requested_version TEXT,
                    state TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    progress INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    restart_required INTEGER NOT NULL DEFAULT 0,
                    request_json TEXT NOT NULL DEFAULT '{}',
                    logs_json TEXT NOT NULL DEFAULT '[]'
                );

                CREATE TABLE IF NOT EXISTS audit_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target TEXT NOT NULL,
                    operation_id TEXT,
                    details_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS control_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_operations_created
                    ON operations(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_operations_state
                    ON operations(state);
                CREATE INDEX IF NOT EXISTS idx_audit_created
                    ON audit_events(created_at DESC);
                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(managed_plugins)")
            }
            if "runtime_name" not in columns:
                connection.execute(
                    "ALTER TABLE managed_plugins "
                    "ADD COLUMN runtime_name TEXT NOT NULL DEFAULT ''"
                )
            if "artifacts_json" not in columns:
                connection.execute(
                    "ALTER TABLE managed_plugins "
                    "ADD COLUMN artifacts_json TEXT NOT NULL DEFAULT '[]'"
                )

    def create_operation(
        self,
        *,
        operation_id: str,
        kind: str,
        plugin_id: str,
        requested_version: str | None,
        request: dict[str, Any],
    ) -> OperationRecord:
        self.initialize()
        created_at = utc_now()
        with self._lock, self._connection() as connection:
            connection.execute(
                """
                INSERT INTO operations (
                    operation_id, kind, plugin_id, requested_version, state,
                    stage, progress, created_at, request_json
                ) VALUES (?, ?, ?, ?, 'queued', 'queued', 0, ?, ?)
                """,
                (
                    operation_id,
                    kind,
                    plugin_id,
                    requested_version,
                    created_at,
                    json.dumps(request, ensure_ascii=False),
                ),
            )
        self.record_audit(
            "operation.queued",
            plugin_id,
            operation_id=operation_id,
            details={"kind": kind, "requested_version": requested_version},
        )
        record = self.get_operation(operation_id)
        if record is None:  # pragma: no cover - guarded by the successful insert
            raise RuntimeError("operation disappeared after insert")
        return record

    def create_operation_if_idle(
        self,
        *,
        operation_id: str,
        kind: str,
        plugin_id: str,
        requested_version: str | None,
        request: dict[str, Any],
    ) -> OperationRecord | None:
        """Atomically enqueue only when no queued or running operation exists."""

        self.initialize()
        created_at = utc_now()
        with self._lock, self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            active = connection.execute(
                "SELECT 1 FROM operations "
                "WHERE state IN ('queued', 'running') LIMIT 1"
            ).fetchone()
            if active is not None:
                return None
            connection.execute(
                """
                INSERT INTO operations (
                    operation_id, kind, plugin_id, requested_version, state,
                    stage, progress, created_at, request_json
                ) VALUES (?, ?, ?, ?, 'queued', 'queued', 0, ?, ?)
                """,
                (
                    operation_id,
                    kind,
                    plugin_id,
                    requested_version,
                    created_at,
                    json.dumps(request, ensure_ascii=False),
                ),
            )
            connection.execute(
                """
                INSERT INTO audit_events (
                    created_at, action, target, operation_id, details_json
                ) VALUES (?, 'operation.queued', ?, ?, ?)
                """,
                (
                    created_at,
                    plugin_id,
                    operation_id,
                    json.dumps(
                        {"kind": kind, "requested_version": requested_version},
                        ensure_ascii=False,
                    ),
                ),
            )
        return self.get_operation(operation_id)

    def update_operation(self, operation_id: str, **changes: Any) -> OperationRecord:
        allowed = {
            "state",
            "stage",
            "progress",
            "started_at",
            "finished_at",
            "error_code",
            "error_message",
            "restart_required",
        }
        invalid = set(changes) - allowed
        if invalid:
            raise ValueError(f"unsupported operation fields: {', '.join(sorted(invalid))}")
        if "progress" in changes:
            changes["progress"] = max(0, min(100, int(changes["progress"])))
        if "restart_required" in changes:
            changes["restart_required"] = int(bool(changes["restart_required"]))
        if not changes:
            record = self.get_operation(operation_id)
            if record is None:
                raise KeyError(operation_id)
            return record

        assignments = ", ".join(f"{name} = ?" for name in changes)
        values = list(changes.values()) + [operation_id]
        with self._lock, self._connection() as connection:
            cursor = connection.execute(
                f"UPDATE operations SET {assignments} WHERE operation_id = ?",  # noqa: S608
                values,
            )
            if cursor.rowcount != 1:
                raise KeyError(operation_id)
        record = self.get_operation(operation_id)
        if record is None:  # pragma: no cover
            raise KeyError(operation_id)
        return record

    def append_operation_log(
        self,
        operation_id: str,
        message: str,
        *,
        level: str = "info",
    ) -> OperationRecord:
        record = self.get_operation(operation_id)
        if record is None:
            raise KeyError(operation_id)
        logs = [*record.logs, {"time": utc_now(), "level": level, "message": message}]
        logs = logs[-200:]
        with self._lock, self._connection() as connection:
            connection.execute(
                "UPDATE operations SET logs_json = ? WHERE operation_id = ?",
                (json.dumps(logs, ensure_ascii=False), operation_id),
            )
        updated = self.get_operation(operation_id)
        if updated is None:  # pragma: no cover
            raise KeyError(operation_id)
        return updated

    def get_operation(self, operation_id: str) -> OperationRecord | None:
        with self._lock, self._read_connection() as connection:
            if connection is None:
                return None
            row = connection.execute(
                "SELECT * FROM operations WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
        return self._operation_from_row(row) if row is not None else None

    def list_operations(
        self,
        *,
        limit: int = 50,
        states: Iterable[str] | None = None,
    ) -> list[OperationRecord]:
        requested_states = list(states or [])
        params: list[Any] = []
        where = ""
        if requested_states:
            placeholders = ",".join("?" for _ in requested_states)
            where = f"WHERE state IN ({placeholders})"
            params.extend(requested_states)
        params.append(max(1, min(200, int(limit))))
        with self._lock, self._read_connection() as connection:
            if connection is None:
                return []
            rows = connection.execute(
                f"SELECT * FROM operations {where} ORDER BY created_at DESC LIMIT ?",  # noqa: S608
                params,
            ).fetchall()
        return [self._operation_from_row(row) for row in rows]

    def delete_operation(self, operation_id: str) -> OperationRecord:
        """Delete one terminal operation while retaining an audit event."""

        self.initialize()
        with self._lock, self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM operations WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
            if row is None:
                raise KeyError(operation_id)
            record = self._operation_from_row(row)
            if record.state in {"queued", "running"}:
                raise ValueError("active plugin operations cannot be deleted")
            connection.execute(
                "DELETE FROM operations WHERE operation_id = ?",
                (operation_id,),
            )
            connection.execute(
                """
                INSERT INTO audit_events (
                    created_at, action, target, operation_id, details_json
                ) VALUES (?, 'operation.history.deleted', ?, ?, ?)
                """,
                (
                    utc_now(),
                    record.plugin_id,
                    operation_id,
                    json.dumps(
                        {"kind": record.kind, "state": record.state},
                        ensure_ascii=False,
                    ),
                ),
            )
        return record

    def has_active_operation(self) -> bool:
        with self._lock, self._read_connection() as connection:
            if connection is None:
                return False
            row = connection.execute(
                "SELECT 1 FROM operations WHERE state IN ('queued', 'running') LIMIT 1"
            ).fetchone()
        return row is not None

    def recover_interrupted_operations(self) -> int:
        """Mark work that was running when the service stopped as failed."""

        self.initialize()
        finished_at = utc_now()
        with self._lock, self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE operations
                SET state = 'failed', stage = 'interrupted', finished_at = ?,
                    error_code = 'PLUGIN_OPERATION_INTERRUPTED',
                    error_message = 'The service stopped while this operation was running.'
                WHERE state = 'running'
                """,
                (finished_at,),
            )
        return cursor.rowcount

    def list_managed_plugins(self) -> list[ManagedPluginRecord]:
        with self._lock, self._read_connection() as connection:
            if connection is None:
                return []
            rows = connection.execute(
                "SELECT * FROM managed_plugins ORDER BY plugin_id"
            ).fetchall()
        return [self._managed_plugin_from_row(row) for row in rows]

    def get_managed_plugin(self, plugin_id: str) -> ManagedPluginRecord | None:
        with self._lock, self._read_connection() as connection:
            if connection is None:
                return None
            row = connection.execute(
                "SELECT * FROM managed_plugins WHERE plugin_id = ?",
                (plugin_id,),
            ).fetchone()
        return self._managed_plugin_from_row(row) if row is not None else None

    def set_desired_state(
        self,
        plugin_id: str,
        desired_state: str,
    ) -> ManagedPluginRecord:
        if desired_state not in {"enabled", "disabled"}:
            raise ValueError("desired_state must be enabled or disabled")
        current = self.get_managed_plugin(plugin_id)
        if current is None:
            raise KeyError(plugin_id)
        if current.desired_state != desired_state:
            with self._lock, self._connection() as connection:
                connection.execute(
                    """
                    UPDATE managed_plugins
                    SET desired_state = ?, restart_required = 1
                    WHERE plugin_id = ?
                    """,
                    (desired_state, plugin_id),
                )
            self.record_audit(
                "plugin.desired_state.changed",
                plugin_id,
                details={
                    "from": current.desired_state,
                    "to": desired_state,
                },
            )
        updated = self.get_managed_plugin(plugin_id)
        if updated is None:  # pragma: no cover - guarded by the prior lookup
            raise KeyError(plugin_id)
        return updated

    def replace_managed_plugins(self, plugins: Iterable[ManagedPluginRecord]) -> None:
        records = list(plugins)
        self.initialize()
        with self._lock, self._connection() as connection:
            connection.execute("DELETE FROM managed_plugins")
            connection.executemany(
                """
                INSERT INTO managed_plugins (
                    plugin_id, distribution, version, source_type, source_ref,
                    artifact_path, artifact_sha256, desired_state, trust,
                    display_name, description, collectors_json, runtime_name,
                    artifacts_json, installed_at, restart_required
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.plugin_id,
                        item.distribution,
                        item.version,
                        item.source_type,
                        item.source_ref,
                        item.artifact_path,
                        item.artifact_sha256,
                        item.desired_state,
                        item.trust,
                        item.display_name,
                        item.description,
                        json.dumps(item.collectors, ensure_ascii=False),
                        item.runtime_name,
                        json.dumps(
                            [artifact.to_dict() for artifact in item.artifacts],
                            ensure_ascii=False,
                        ),
                        item.installed_at or utc_now(),
                        int(item.restart_required),
                    )
                    for item in records
                ],
            )

    def mark_runtime_reconciled(self, active_distributions: Iterable[str]) -> int:
        from src.plugin_manager.environment import get_plugin_manager_access
        from src.plugin_manager.package_reader import normalize_distribution

        if not get_plugin_manager_access()["mutable"]:
            return 0
        normalized = {normalize_distribution(item) for item in active_distributions}
        self.initialize()
        changed = 0
        with self._lock, self._connection() as connection:
            rows = connection.execute(
                """
                SELECT plugin_id, distribution, runtime_name, desired_state
                FROM managed_plugins
                WHERE restart_required = 1
                """
            ).fetchall()
            for row in rows:
                runtime_identity = row["runtime_name"] or row["distribution"]
                is_active = normalize_distribution(runtime_identity) in normalized
                reconciled = (
                    row["desired_state"] == "enabled" and is_active
                ) or (
                    row["desired_state"] == "disabled" and not is_active
                )
                if reconciled:
                    connection.execute(
                        "UPDATE managed_plugins SET restart_required = 0 WHERE plugin_id = ?",
                        (row["plugin_id"],),
                    )
                    changed += 1
        return changed

    def mark_generation_reconciled(self) -> str | None:
        """Record that this process started from the active generation pointer."""

        from src.plugin_manager.environment import get_plugin_manager_access

        if not get_plugin_manager_access()["mutable"]:
            return None

        active = self.get_active_generation()
        if active is None:
            return None
        generation_id = str(active["generation_id"])
        self._set_control_state("runtime_generation_id", generation_id)
        return generation_id

    def generation_restart_required(self) -> bool:
        active = self.get_active_generation()
        if active is None:
            return False
        return self._get_control_state("runtime_generation_id") != str(
            active["generation_id"]
        )

    def record_generation(
        self,
        *,
        generation_id: str,
        site_packages: str,
        previous_generation_id: str | None,
        plugins: Iterable[ManagedPluginRecord],
    ) -> None:
        self.initialize()
        lock_payload = [plugin.to_dict() for plugin in plugins]
        now = utc_now()
        with self._lock, self._connection() as connection:
            connection.execute(
                "UPDATE generations SET state = 'previous' WHERE state = 'active'"
            )
            connection.execute(
                """
                INSERT OR REPLACE INTO generations (
                    generation_id, site_packages, state, previous_generation_id,
                    lock_json, created_at, activated_at
                ) VALUES (?, ?, 'active', ?, ?, ?, ?)
                """,
                (
                    generation_id,
                    site_packages,
                    previous_generation_id,
                    json.dumps(lock_payload, ensure_ascii=False),
                    now,
                    now,
                ),
            )

    def commit_generation(
        self,
        *,
        generation_id: str,
        site_packages: str,
        previous_generation_id: str | None,
        plugins: Iterable[ManagedPluginRecord],
    ) -> None:
        """Atomically replace the lock set and record the activated generation."""

        records = list(plugins)
        self.initialize()
        now = utc_now()
        with self._lock, self._connection() as connection:
            connection.execute("DELETE FROM managed_plugins")
            connection.executemany(
                """
                INSERT INTO managed_plugins (
                    plugin_id, distribution, version, source_type, source_ref,
                    artifact_path, artifact_sha256, desired_state, trust,
                    display_name, description, collectors_json, runtime_name,
                    artifacts_json, installed_at, restart_required
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.plugin_id,
                        item.distribution,
                        item.version,
                        item.source_type,
                        item.source_ref,
                        item.artifact_path,
                        item.artifact_sha256,
                        item.desired_state,
                        item.trust,
                        item.display_name,
                        item.description,
                        json.dumps(item.collectors, ensure_ascii=False),
                        item.runtime_name,
                        json.dumps(
                            [artifact.to_dict() for artifact in item.artifacts],
                            ensure_ascii=False,
                        ),
                        item.installed_at or now,
                        int(item.restart_required),
                    )
                    for item in records
                ],
            )
            connection.execute(
                "UPDATE generations SET state = 'previous' WHERE state = 'active'"
            )
            connection.execute(
                """
                INSERT OR REPLACE INTO generations (
                    generation_id, site_packages, state, previous_generation_id,
                    lock_json, created_at, activated_at
                ) VALUES (?, ?, 'active', ?, ?, ?, ?)
                """,
                (
                    generation_id,
                    site_packages,
                    previous_generation_id,
                    json.dumps([item.to_dict() for item in records], ensure_ascii=False),
                    now,
                    now,
                ),
            )

    def list_generations(self, *, limit: int = 20) -> list[dict[str, Any]]:
        with self._lock, self._read_connection() as connection:
            if connection is None:
                return []
            rows = connection.execute(
                """
                SELECT * FROM generations
                ORDER BY COALESCE(activated_at, created_at) DESC
                LIMIT ?
                """,
                (max(1, min(100, int(limit))),),
            ).fetchall()
        return [self._generation_from_row(row) for row in rows]

    def get_generation(self, generation_id: str) -> dict[str, Any] | None:
        with self._lock, self._read_connection() as connection:
            if connection is None:
                return None
            row = connection.execute(
                "SELECT * FROM generations WHERE generation_id = ?",
                (generation_id,),
            ).fetchone()
        return self._generation_from_row(row) if row is not None else None

    def get_active_generation(self) -> dict[str, Any] | None:
        with self._lock, self._read_connection() as connection:
            if connection is None:
                return None
            row = connection.execute(
                """
                SELECT * FROM generations
                WHERE state = 'active'
                ORDER BY activated_at DESC
                LIMIT 1
                """
            ).fetchone()
        return self._generation_from_row(row) if row is not None else None

    def activate_existing_generation(
        self,
        generation_id: str,
        *,
        previous_generation_id: str | None,
        plugins: Iterable[ManagedPluginRecord],
    ) -> None:
        records = list(plugins)
        self.initialize()
        now = utc_now()
        with self._lock, self._connection() as connection:
            exists = connection.execute(
                "SELECT 1 FROM generations WHERE generation_id = ?",
                (generation_id,),
            ).fetchone()
            if exists is None:
                raise KeyError(generation_id)
            connection.execute("DELETE FROM managed_plugins")
            connection.executemany(
                """
                INSERT INTO managed_plugins (
                    plugin_id, distribution, version, source_type, source_ref,
                    artifact_path, artifact_sha256, desired_state, trust,
                    display_name, description, collectors_json, runtime_name,
                    artifacts_json, installed_at, restart_required
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.plugin_id,
                        item.distribution,
                        item.version,
                        item.source_type,
                        item.source_ref,
                        item.artifact_path,
                        item.artifact_sha256,
                        item.desired_state,
                        item.trust,
                        item.display_name,
                        item.description,
                        json.dumps(item.collectors, ensure_ascii=False),
                        item.runtime_name,
                        json.dumps(
                            [artifact.to_dict() for artifact in item.artifacts],
                            ensure_ascii=False,
                        ),
                        item.installed_at or now,
                        int(item.restart_required),
                    )
                    for item in records
                ],
            )
            connection.execute(
                "UPDATE generations SET state = 'previous' WHERE state = 'active'"
            )
            connection.execute(
                """
                UPDATE generations
                SET state = 'active', previous_generation_id = ?, activated_at = ?
                WHERE generation_id = ?
                """,
                (previous_generation_id, now, generation_id),
            )

    def _get_control_state(self, key: str) -> str | None:
        with self._lock, self._read_connection() as connection:
            if connection is None:
                return None
            row = connection.execute(
                "SELECT value FROM control_state WHERE key = ?",
                (key,),
            ).fetchone()
        return str(row["value"]) if row is not None else None

    def _set_control_state(self, key: str, value: str) -> None:
        self.initialize()
        with self._lock, self._connection() as connection:
            connection.execute(
                """
                INSERT INTO control_state (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, value, utc_now()),
            )

    def record_audit(
        self,
        action: str,
        target: str,
        *,
        operation_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.initialize()
        with self._lock, self._connection() as connection:
            connection.execute(
                """
                INSERT INTO audit_events (
                    created_at, action, target, operation_id, details_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    utc_now(),
                    action,
                    target,
                    operation_id,
                    json.dumps(details or {}, ensure_ascii=False),
                ),
            )

    def list_audit_events(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock, self._read_connection() as connection:
            if connection is None:
                return []
            rows = connection.execute(
                "SELECT * FROM audit_events ORDER BY event_id DESC LIMIT ?",
                (max(1, min(500, int(limit))),),
            ).fetchall()
        return [
            {
                "id": row["event_id"],
                "created_at": row["created_at"],
                "action": row["action"],
                "target": row["target"],
                "operation_id": row["operation_id"],
                "details": json.loads(row["details_json"] or "{}"),
            }
            for row in rows
        ]

    @staticmethod
    def _operation_from_row(row: sqlite3.Row) -> OperationRecord:
        return OperationRecord(
            operation_id=row["operation_id"],
            kind=row["kind"],
            plugin_id=row["plugin_id"],
            requested_version=row["requested_version"],
            state=row["state"],
            stage=row["stage"],
            progress=row["progress"],
            created_at=row["created_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            error_code=row["error_code"],
            error_message=row["error_message"],
            restart_required=bool(row["restart_required"]),
            request=json.loads(row["request_json"] or "{}"),
            logs=tuple(json.loads(row["logs_json"] or "[]")),
        )

    @staticmethod
    def _managed_plugin_from_row(row: sqlite3.Row) -> ManagedPluginRecord:
        keys = set(row.keys())
        artifacts_payload = (
            json.loads(row["artifacts_json"] or "[]")
            if "artifacts_json" in keys
            else []
        )
        return ManagedPluginRecord(
            plugin_id=row["plugin_id"],
            distribution=row["distribution"],
            version=row["version"],
            source_type=row["source_type"],
            source_ref=row["source_ref"],
            artifact_path=row["artifact_path"],
            artifact_sha256=row["artifact_sha256"],
            desired_state=row["desired_state"],
            trust=row["trust"],
            display_name=row["display_name"],
            description=row["description"],
            collectors=tuple(json.loads(row["collectors_json"] or "[]")),
            runtime_name=row["runtime_name"] if "runtime_name" in keys else "",
            artifacts=tuple(
                ManagedArtifactRecord.from_dict(item)
                for item in artifacts_payload
                if isinstance(item, dict)
            ),
            installed_at=row["installed_at"],
            restart_required=bool(row["restart_required"]),
        )

    @staticmethod
    def _managed_plugin_from_payload(payload: dict[str, Any]) -> ManagedPluginRecord:
        return ManagedPluginRecord(
            plugin_id=str(payload.get("plugin_id") or ""),
            distribution=str(payload.get("distribution") or ""),
            version=str(payload.get("version") or ""),
            source_type=str(payload.get("source_type") or ""),
            source_ref=str(payload.get("source_ref") or ""),
            artifact_path=str(payload.get("artifact_path") or ""),
            artifact_sha256=str(payload.get("artifact_sha256") or ""),
            desired_state=str(payload.get("desired_state") or "enabled"),
            trust=str(payload.get("trust") or "official"),
            display_name=str(payload.get("display_name") or ""),
            description=str(payload.get("description") or ""),
            collectors=tuple(payload.get("collectors") or ()),
            runtime_name=str(payload.get("runtime_name") or ""),
            artifacts=tuple(
                ManagedArtifactRecord.from_dict(item)
                for item in payload.get("artifacts") or ()
                if isinstance(item, dict)
            ),
            installed_at=str(payload.get("installed_at") or ""),
            restart_required=bool(payload.get("restart_required", True)),
        )

    @classmethod
    def _generation_from_row(cls, row: sqlite3.Row) -> dict[str, Any]:
        lock_payload = json.loads(row["lock_json"] or "[]")
        plugins = [
            cls._managed_plugin_from_payload(item)
            for item in lock_payload
            if isinstance(item, dict)
        ]
        return {
            "generation_id": row["generation_id"],
            "site_packages": row["site_packages"],
            "state": row["state"],
            "previous_generation_id": row["previous_generation_id"],
            "plugins": plugins,
            "created_at": row["created_at"],
            "activated_at": row["activated_at"],
        }


plugin_state_store = PluginStateStore()
