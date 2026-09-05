"""Aggregate runtime plugin diagnostics into management-facing inventory."""

from __future__ import annotations

import importlib.metadata
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any, Iterable

from src.core.plugin_system import PLUGIN_ENTRY_POINT_GROUP, PluginStatus, plugin_manager
from src.plugin_manager.catalog import plugin_catalog
from src.plugin_manager.environment import get_managed_site_packages
from src.plugin_manager.models import PluginInventoryRecord
from src.plugin_manager.package_reader import normalize_distribution
from src.plugin_manager.store import PluginStateStore, plugin_state_store

_READ_ONLY_REASON = "This plugin source is outside the managed plugin environment."
_MANAGED_REASON = "Installed in the managed plugin environment."


def _runtime_state(state: str) -> str:
    return {
        "active": "active",
        "failed": "failed",
        "discovered": "not_loaded",
    }.get(state, "inactive")


def _managed_restart_required(
    *,
    desired_state: str,
    runtime_state: str,
    persisted: bool,
) -> bool:
    """Reconcile persisted intent with what the current process actually loaded."""

    if persisted:
        return True
    if desired_state == "disabled":
        return runtime_state == "active"
    return runtime_state in {"inactive", "not_loaded"}


def _humanize_distribution(name: str) -> str:
    value = name.strip()
    for prefix in ("autoflux-plugin-", "gamedata-autoflux-plugin-"):
        if value.lower().startswith(prefix):
            value = value[len(prefix) :]
            break
    return " ".join(part.capitalize() for part in value.replace("_", "-").split("-") if part)


def _entry_points() -> Iterable[Any]:
    discovered = importlib.metadata.entry_points()
    try:
        return discovered.select(group=PLUGIN_ENTRY_POINT_GROUP)
    except AttributeError:  # pragma: no cover - Python <3.10 compatibility
        return discovered.get(PLUGIN_ENTRY_POINT_GROUP, [])


def _entry_point_details() -> dict[str, dict[str, str]]:
    details: dict[str, dict[str, str]] = {}
    for entry_point in _entry_points():
        source = f"entrypoint:{entry_point.name}"
        distribution = getattr(entry_point, "dist", None)
        metadata = getattr(distribution, "metadata", None)
        name = str(metadata.get("Name", "") if metadata is not None else "").strip()
        summary = str(metadata.get("Summary", "") if metadata is not None else "").strip()
        version = str(getattr(distribution, "version", "") or "").strip()
        location = ""
        if distribution is not None:
            try:
                location = str(Path(distribution.locate_file("")).resolve())
            except (AttributeError, OSError, TypeError):
                location = ""
        details[source] = {
            "distribution": name,
            "description": summary,
            "version": version,
            "location": location,
        }
    return details


def _development_details(source: str) -> dict[str, str]:
    module_name = source.partition(":")[2]
    module = sys.modules.get(module_name)
    spec = getattr(module, "plugin", None) if module is not None else None
    if callable(spec) and not hasattr(spec, "name"):
        return {}
    return {
        "distribution": str(getattr(spec, "name", "") or ""),
        "description": str(getattr(spec, "description", "") or ""),
        "version": str(getattr(spec, "version", "") or ""),
    }


def _is_within(path: str, parent: Path | None) -> bool:
    if not path or parent is None:
        return False
    try:
        Path(path).resolve().relative_to(parent.resolve())
    except (OSError, ValueError):
        return False
    return True


def record_from_status(
    status: PluginStatus,
    *,
    entry_point_details: dict[str, dict[str, str]] | None = None,
    managed_site_packages: Path | None = None,
) -> PluginInventoryRecord:
    """Convert one runtime status without importing new plugin code."""

    details: dict[str, str] = {}
    if status.source.startswith("module:"):
        source_type = "development"
        trust = "local"
        details = _development_details(status.source)
    elif status.source.startswith("entrypoint:"):
        details = (entry_point_details or {}).get(status.source, {})
        source_type = (
            "managed"
            if _is_within(details.get("location", ""), managed_site_packages)
            else "external"
        )
        trust = "unknown"
    else:
        source_type = "external"
        trust = "unknown"

    distribution = details.get("distribution") or status.name
    version = status.version or details.get("version", "")
    plugin_id = status.name or distribution or status.source
    managed = source_type == "managed"

    return PluginInventoryRecord(
        plugin_id=plugin_id,
        distribution=distribution,
        display_name=_humanize_distribution(distribution or plugin_id) or plugin_id,
        description=details.get("description", ""),
        installed_version=version,
        source_type=source_type,
        source=status.source,
        trust=trust,
        runtime_state=_runtime_state(status.state),
        collectors=tuple(status.collectors),
        capabilities=tuple(item.to_dict() for item in status.capabilities),
        last_error=status.error or None,
        managed=managed,
        management_reason=_READ_ONLY_REASON,
    )


def build_plugin_inventory(
    statuses: Iterable[PluginStatus] | None = None,
    *,
    store: PluginStateStore = plugin_state_store,
) -> dict[str, Any]:
    """Return the management-facing inventory and summary counters."""

    runtime_statuses = list(plugin_manager.list_statuses() if statuses is None else statuses)
    entry_points = _entry_point_details()
    managed_site_packages = get_managed_site_packages()
    runtime_records = [
        record_from_status(
            status,
            entry_point_details=entry_points,
            managed_site_packages=managed_site_packages,
        )
        for status in runtime_statuses
    ]
    managed_records = store.list_managed_plugins()
    catalog_versions = {
        item.plugin_id: item.version for item in plugin_catalog.list_plugins()
    }
    managed_by_distribution = {
        normalize_distribution(item.distribution): item for item in managed_records
    }
    seen_managed: set[str] = set()
    records: list[PluginInventoryRecord] = []
    for record in runtime_records:
        managed = managed_by_distribution.get(
            normalize_distribution(record.distribution)
        )
        if managed is None or record.source_type != "managed":
            records.append(record)
            continue
        seen_managed.add(managed.plugin_id)
        restart_required = _managed_restart_required(
            desired_state=managed.desired_state,
            runtime_state=record.runtime_state,
            persisted=managed.restart_required,
        )
        records.append(
            replace(
                record,
                plugin_id=managed.plugin_id,
                distribution=managed.distribution,
                display_name=managed.display_name or record.display_name,
                description=managed.description or record.description,
                installed_version=managed.version,
                source_type="managed",
                source=managed.source_ref,
                trust=managed.trust,
                desired_state=managed.desired_state,
                restart_required=restart_required,
                latest_version=catalog_versions.get(managed.plugin_id),
                collectors=managed.collectors or record.collectors,
                managed=True,
                management_reason=_MANAGED_REASON,
            )
        )

    for managed in managed_records:
        if managed.plugin_id in seen_managed:
            continue
        runtime_state = (
            "inactive" if managed.desired_state == "disabled" else "not_loaded"
        )
        records.append(
            PluginInventoryRecord(
                plugin_id=managed.plugin_id,
                distribution=managed.distribution,
                display_name=managed.display_name
                or _humanize_distribution(managed.distribution),
                description=managed.description,
                installed_version=managed.version,
                source_type="managed",
                source=managed.source_ref,
                trust=managed.trust,
                install_state="installed",
                desired_state=managed.desired_state,
                runtime_state=runtime_state,
                restart_required=_managed_restart_required(
                    desired_state=managed.desired_state,
                    runtime_state=runtime_state,
                    persisted=managed.restart_required,
                ),
                latest_version=catalog_versions.get(managed.plugin_id),
                collectors=managed.collectors,
                managed=True,
                management_reason=_MANAGED_REASON,
            )
        )
    records.sort(key=lambda item: (item.display_name.lower(), item.plugin_id))

    source_counts: dict[str, int] = {}
    runtime_counts: dict[str, int] = {}
    for record in records:
        source_counts[record.source_type] = source_counts.get(record.source_type, 0) + 1
        runtime_counts[record.runtime_state] = runtime_counts.get(record.runtime_state, 0) + 1

    pending_count = sum(record.restart_required for record in records)
    if store.generation_restart_required() and pending_count == 0:
        pending_count = 1

    return {
        "plugins": [record.to_dict() for record in records],
        "summary": {
            "total": len(records),
            "active": runtime_counts.get("active", 0),
            "failed": runtime_counts.get("failed", 0),
            "restart_required": pending_count,
            "by_source": source_counts,
            "by_runtime_state": runtime_counts,
        },
    }
