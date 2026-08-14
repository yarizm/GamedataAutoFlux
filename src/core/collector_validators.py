"""Plugin-owned collector configuration validators."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable


@dataclass(frozen=True)
class CollectorConfigIssue:
    """One validation issue using a config-relative field path."""

    code: str
    message: str
    field: str = ""
    level: str = "error"
    suggested_action: str = ""


CollectorConfigValidator = Callable[[dict[str, Any]], Iterable[CollectorConfigIssue]]
_VALIDATORS: dict[str, list[tuple[CollectorConfigValidator, str]]] = {}


def register_collector_config_validator(
    collector_id: str,
    validator: CollectorConfigValidator,
    *,
    owner: str,
) -> None:
    """Register a config validator owned by one installed plugin."""

    normalized_id = str(collector_id or "").strip()
    normalized_owner = str(owner or "").strip()
    if not normalized_id or not normalized_owner:
        raise ValueError("collector_id and validator owner are required")
    entries = _VALIDATORS.setdefault(normalized_id, [])
    if any(current_owner == normalized_owner for _, current_owner in entries):
        entries[:] = [
            (current_validator, current_owner)
            for current_validator, current_owner in entries
            if current_owner != normalized_owner
        ]
    entries.append((validator, normalized_owner))


def validate_collector_config(
    collector_id: str,
    config: dict[str, Any],
) -> list[CollectorConfigIssue]:
    """Run every validator registered for an installed collector."""

    issues: list[CollectorConfigIssue] = []
    for validator, _ in _VALIDATORS.get(str(collector_id or "").strip(), []):
        issues.extend(validator(config))
    return issues


def snapshot_collector_config_validators() -> dict[str, list[tuple[CollectorConfigValidator, str]]]:
    return {collector_id: list(entries) for collector_id, entries in _VALIDATORS.items()}


def restore_collector_config_validators(
    snapshot: dict[str, list[tuple[CollectorConfigValidator, str]]],
) -> None:
    _VALIDATORS.clear()
    _VALIDATORS.update({collector_id: list(entries) for collector_id, entries in snapshot.items()})
