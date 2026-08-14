"""Serializable models used by the plugin manager API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Compatibility:
    """Whether a discovered plugin can run in the current environment."""

    compatible: bool = True
    reasons: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "compatible": self.compatible,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class PluginInventoryRecord:
    """One plugin's package, desired, and current runtime states."""

    plugin_id: str
    distribution: str
    display_name: str
    installed_version: str
    source_type: str
    source: str
    runtime_state: str
    collectors: tuple[str, ...] = ()
    capabilities: tuple[dict[str, str], ...] = ()
    description: str = ""
    trust: str = "unknown"
    install_state: str = "installed"
    desired_state: str = "enabled"
    restart_required: bool = False
    latest_version: str | None = None
    compatibility: Compatibility = field(default_factory=Compatibility)
    last_error: str | None = None
    managed: bool = False
    management_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        can_enable = self.managed and self.desired_state == "disabled"
        can_disable = self.managed and self.desired_state == "enabled"
        can_upgrade = bool(
            self.managed
            and self.latest_version
            and self.latest_version != self.installed_version
        )
        can_uninstall = bool(
            self.managed
            and self.desired_state == "disabled"
            and not self.restart_required
        )
        return {
            "id": self.plugin_id,
            "distribution": self.distribution,
            "display_name": self.display_name,
            "description": self.description,
            "installed_version": self.installed_version,
            "latest_version": self.latest_version,
            "source_type": self.source_type,
            "source": self.source,
            "trust": self.trust,
            "install_state": self.install_state,
            "desired_state": self.desired_state,
            "runtime_state": self.runtime_state,
            "restart_required": self.restart_required,
            "collectors": list(self.collectors),
            "capabilities": [dict(item) for item in self.capabilities],
            "compatibility": self.compatibility.to_dict(),
            "last_error": self.last_error,
            "management": {
                "managed": self.managed,
                "can_enable": can_enable,
                "can_disable": can_disable,
                "can_upgrade": can_upgrade,
                "can_uninstall": can_uninstall,
                "reason": self.management_reason,
            },
        }


@dataclass(frozen=True)
class ManagedArtifactRecord:
    """One immutable wheel participating in a managed plugin installation."""

    distribution: str
    version: str
    path: str
    sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "distribution": self.distribution,
            "version": self.version,
            "path": self.path,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ManagedArtifactRecord:
        return cls(
            distribution=str(payload.get("distribution") or ""),
            version=str(payload.get("version") or ""),
            path=str(payload.get("path") or ""),
            sha256=str(payload.get("sha256") or ""),
        )


@dataclass(frozen=True)
class ManagedPluginRecord:
    """One distribution pinned in the active managed generation."""

    plugin_id: str
    distribution: str
    version: str
    source_type: str
    source_ref: str
    artifact_path: str
    artifact_sha256: str
    desired_state: str = "enabled"
    trust: str = "official"
    display_name: str = ""
    description: str = ""
    collectors: tuple[str, ...] = ()
    runtime_name: str = ""
    artifacts: tuple[ManagedArtifactRecord, ...] = ()
    installed_at: str = ""
    restart_required: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "plugin_id": self.plugin_id,
            "distribution": self.distribution,
            "version": self.version,
            "source_type": self.source_type,
            "source_ref": self.source_ref,
            "artifact_path": self.artifact_path,
            "artifact_sha256": self.artifact_sha256,
            "desired_state": self.desired_state,
            "trust": self.trust,
            "display_name": self.display_name,
            "description": self.description,
            "collectors": list(self.collectors),
            "runtime_name": self.runtime_name,
            "artifacts": [item.to_dict() for item in self.artifacts],
            "installed_at": self.installed_at,
            "restart_required": self.restart_required,
        }


@dataclass(frozen=True)
class OperationRecord:
    """Persistent lifecycle-operation state returned by the management API."""

    operation_id: str
    kind: str
    plugin_id: str
    requested_version: str | None
    state: str
    stage: str
    progress: int
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    restart_required: bool = False
    request: dict[str, Any] = field(default_factory=dict)
    logs: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.operation_id,
            "kind": self.kind,
            "plugin_id": self.plugin_id,
            "requested_version": self.requested_version,
            "state": self.state,
            "stage": self.stage,
            "progress": self.progress,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "restart_required": self.restart_required,
            "logs": list(self.logs),
        }


@dataclass(frozen=True)
class CatalogPlugin:
    """Installable plugin advertised by a trusted catalog provider."""

    plugin_id: str
    distribution: str
    display_name: str
    version: str
    description: str
    publisher: str
    trust: str
    package_dir: str
    collectors: tuple[str, ...]
    runtime_capabilities: tuple[str, ...] = ()
    internal_dependencies: tuple[str, ...] = ()
    plugin_api: str = "1"
    core_specifier: str = ">=0.1,<0.3"
    python_specifier: str = ">=3.12"
    supported_systems: tuple[str, ...] = ()
    homepage: str = ""
    license: str = ""
    icon: str = ""

    def to_dict(
        self,
        *,
        available: bool,
        installed_version: str | None = None,
        compatibility: Compatibility | None = None,
    ) -> dict[str, Any]:
        compatibility = compatibility or Compatibility()
        return {
            "id": self.plugin_id,
            "distribution": self.distribution,
            "display_name": self.display_name,
            "version": self.version,
            "description": self.description,
            "publisher": self.publisher,
            "trust": self.trust,
            "collectors": list(self.collectors),
            "runtime_capabilities": list(self.runtime_capabilities),
            "plugin_api": self.plugin_api,
            "core_specifier": self.core_specifier,
            "python_specifier": self.python_specifier,
            "supported_systems": list(self.supported_systems),
            "homepage": self.homepage,
            "license": self.license,
            "icon": self.icon,
            "available": available,
            "installed": installed_version is not None,
            "installed_version": installed_version,
            "update_available": bool(
                installed_version is not None and installed_version != self.version
            ),
            "compatibility": compatibility.to_dict(),
        }


@dataclass(frozen=True)
class PluginReference:
    """One persisted or live object that depends on a plugin collector."""

    kind: str
    reference_id: str
    name: str
    collector: str
    state: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "kind": self.kind,
            "id": self.reference_id,
            "name": self.name,
            "collector": self.collector,
            "state": self.state,
        }
