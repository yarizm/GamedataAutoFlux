"""Collector capability metadata registered by installed collector plugins.

The core owns the metadata contract and lookup helpers, but it deliberately
ships with an empty catalog.  Collector plugins register their own metadata
when the plugin manager activates them.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from src.core.config import get as get_config
from src.core.dag_nodes import DagOutputField


class TargetValidationRule(BaseModel):
    """A declarative target validation rule used by task precheck."""

    mode: str = Field(default="any", description="any/all field presence check")
    fields: list[str] = Field(default_factory=list)
    level: str = "error"
    code: str
    field: str = ""
    message: str
    skip_if_error: bool = True
    check: str = Field(
        default="presence",
        description="presence/regex/absolute_url/url_host validation",
    )
    pattern: str = ""
    allowed_hosts: list[str] = Field(default_factory=list)
    suggested_action: str = ""
    optional: bool = False


class CollectorTargetField(BaseModel):
    """One operator-facing field used to build a task target."""

    key: str
    label: str
    description: str = ""
    location: Literal["name", "params"] = "params"
    input_type: Literal[
        "text",
        "url",
        "number",
        "date",
        "boolean",
        "select",
        "textarea",
        "textarea_lines",
    ] = "text"
    required: bool = False
    placeholder: str = ""
    default: Any = None
    options: list[dict[str, Any]] = Field(default_factory=list)
    minimum: float | None = None
    maximum: float | None = None
    multiple: bool = False


class CollectorTargetSchema(BaseModel):
    """Target fields and rules declared by one collector plugin."""

    target_type: str = "game"
    fields: list[CollectorTargetField] = Field(default_factory=list)
    default_params: dict[str, Any] = Field(default_factory=dict)
    required_fields: list[str] = Field(default_factory=list)
    rules: list[TargetValidationRule] = Field(default_factory=list)


class CredentialRequirement(BaseModel):
    """Declarative runtime/credential requirement owned by a plugin."""

    requirement_id: str
    kind: str = Field(description="config_value/config_list/python_module/notice")
    config_key: str = ""
    module: str = ""
    status_key: str = ""
    level: str = "warning"
    code: str
    field: str = ""
    message: str
    suggested_action: str = ""
    when_all: dict[str, Any] = Field(default_factory=dict)
    allow_placeholder: bool = False


class SessionAccountSpec(BaseModel):
    """Declarative account/session locator exposed by a collector plugin."""

    account_id: str
    account_kind: str
    session_modes: list[str] = Field(default_factory=list)
    locator_config_key: str = ""
    default_locator: str = ""
    locator_label: str = ""
    readiness_check: str = ""
    worker_capability: str = ""
    when_all: dict[str, Any] = Field(default_factory=dict)


class SessionCheckSpec(BaseModel):
    """Declarative local runtime check owned by a collector plugin."""

    check_id: str
    kind: str = Field(
        description="path_directory/path_file/http_endpoint/notice runtime check"
    )
    session_modes: list[str] = Field(default_factory=list)
    config_key: str = ""
    default_value: Any = None
    endpoint_template: str = ""
    detail_key: str = ""
    level: str = "warning"
    required: bool = False
    required_config_key: str = ""
    message: str = ""
    ok_message: str = ""
    suggested_action: str = ""
    when_all: dict[str, Any] = Field(default_factory=dict)


class CollectorMetadata(BaseModel):
    """Collector capabilities shared by Web, Agent, precheck, and workers."""

    collector_id: str
    display_name: str
    description: str = ""
    capabilities: list[str] = Field(default_factory=list)
    requires_session: bool = False
    session_mode: str = "api_only"
    session_config_key: str = ""
    supports_checkpoint: bool = False
    recovery_level: str = "L0"
    target_schema: CollectorTargetSchema = Field(default_factory=CollectorTargetSchema)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    output_fields: list[DagOutputField] = Field(default_factory=list)
    credential_profiles: list[str] = Field(default_factory=list)
    credential_requirements: list[CredentialRequirement] = Field(default_factory=list)
    session_accounts: list[SessionAccountSpec] = Field(default_factory=list)
    session_checks: list[SessionCheckSpec] = Field(default_factory=list)
    supported_session_modes: list[str] = Field(default_factory=list)


_RECOVERY_GUIDANCE = {
    "L0": "This collector does not support checkpoint resume yet; failed tasks should be reviewed and rerun.",
    "L1": "This collector can record local checkpoints; use the latest checkpoint to plan targeted follow-up work.",
    "L2": "This collector can resume across workers when the required session is available.",
    "L3": "This collector is idempotent and can resume on any compatible worker.",
}

_SESSION_MODES = ("api_only", "local_profile", "managed_state")

# Empty by design: installed plugins populate this catalog during activation.
_COLLECTOR_METADATA: dict[str, CollectorMetadata] = {}
_COLLECTOR_METADATA_OWNERS: dict[str, str] = {}


def register_collector_metadata(
    metadata: CollectorMetadata,
    *,
    owner: str,
) -> None:
    """Register metadata for one installed collector plugin.

    Re-registering the same collector from the same plugin is idempotent.  A
    different owner is rejected so two installed distributions cannot silently
    replace each other's public contract.
    """

    normalized_owner = str(owner or "").strip()
    if not normalized_owner:
        raise ValueError("collector metadata owner is required")
    collector_id = str(metadata.collector_id or "").strip()
    if not collector_id:
        raise ValueError("collector_id is required")
    current_owner = _COLLECTOR_METADATA_OWNERS.get(collector_id)
    if current_owner and current_owner != normalized_owner:
        raise ValueError(
            f"collector metadata '{collector_id}' already belongs to plugin '{current_owner}'"
        )
    _COLLECTOR_METADATA[collector_id] = metadata.model_copy(deep=True)
    _COLLECTOR_METADATA_OWNERS[collector_id] = normalized_owner


def unregister_collector_metadata_owner(owner: str) -> None:
    """Remove every metadata entry owned by a plugin (rollback/tests)."""

    normalized_owner = str(owner or "").strip()
    owned = [
        collector_id
        for collector_id, current_owner in _COLLECTOR_METADATA_OWNERS.items()
        if current_owner == normalized_owner
    ]
    for collector_id in owned:
        _COLLECTOR_METADATA.pop(collector_id, None)
        _COLLECTOR_METADATA_OWNERS.pop(collector_id, None)


def snapshot_collector_metadata() -> tuple[dict[str, CollectorMetadata], dict[str, str]]:
    """Return a rollback-safe metadata catalog snapshot."""

    return (
        {key: value.model_copy(deep=True) for key, value in _COLLECTOR_METADATA.items()},
        dict(_COLLECTOR_METADATA_OWNERS),
    )


def restore_collector_metadata(
    snapshot: tuple[dict[str, CollectorMetadata], dict[str, str]],
) -> None:
    """Restore a snapshot created by :func:`snapshot_collector_metadata`."""

    metadata, owners = snapshot
    _COLLECTOR_METADATA.clear()
    _COLLECTOR_METADATA.update(
        {key: value.model_copy(deep=True) for key, value in metadata.items()}
    )
    _COLLECTOR_METADATA_OWNERS.clear()
    _COLLECTOR_METADATA_OWNERS.update(owners)


def get_collector_metadata(collector_id: str) -> CollectorMetadata | None:
    """Return metadata for an installed collector."""

    return _COLLECTOR_METADATA.get(collector_id)


def list_collector_metadata(
    collector_ids: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return public metadata, optionally constrained to collector ids."""

    ids = collector_ids or sorted(_COLLECTOR_METADATA)
    return {
        collector_id: collector_metadata_payload(collector_id)
        for collector_id in ids
        if get_collector_metadata(collector_id) is not None
    }


def list_session_sensitive_collectors() -> list[str]:
    """Return installed collectors with runtime/session requirements."""

    return sorted(
        collector_id
        for collector_id, metadata in _COLLECTOR_METADATA.items()
        if (
            metadata.requires_session
            or bool(metadata.credential_requirements)
            or bool(metadata.session_checks)
        )
    )


def build_collector_recovery_info(
    collector_id: str,
    *,
    latest_checkpoint: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a compact recovery guidance payload for task/precheck surfaces."""

    metadata = get_collector_metadata(collector_id) or fallback_collector_metadata(collector_id)
    session_contract = resolve_session_mode_contract(collector_id)
    recovery_level = metadata.recovery_level
    info: dict[str, Any] = {
        "collector_id": metadata.collector_id,
        "supports_checkpoint": metadata.supports_checkpoint,
        "recovery_level": recovery_level,
        "session_mode": session_contract["effective_mode"],
        "default_session_mode": session_contract["default_mode"],
        "configured_session_mode": session_contract["configured_mode"],
        "session_mode_source": session_contract["source"],
        "session_mode_override_status": session_contract["override_status"],
        "supported_session_modes": session_contract["supported_modes"],
        "requires_session": metadata.requires_session,
        "guidance": _RECOVERY_GUIDANCE.get(recovery_level, _RECOVERY_GUIDANCE["L0"]),
        "latest_checkpoint": latest_checkpoint,
    }
    if not metadata.supports_checkpoint:
        info["recommended_action"] = "rerun_task"
    elif latest_checkpoint:
        info["recommended_action"] = "review_checkpoint"
    else:
        info["recommended_action"] = "record_checkpoint"
    return info


def required_worker_capabilities(collector_id: str) -> set[str]:
    """Return extra worker capabilities required to execute a collector."""

    metadata = get_collector_metadata(collector_id)
    if metadata is None or not metadata.requires_session:
        return set()

    session_mode = resolve_session_mode(collector_id)
    required = {f"session_mode:{session_mode}"}
    account = resolve_session_account(metadata, session_mode=session_mode)
    if account and account.worker_capability:
        required.add(account.worker_capability)
    elif session_mode == "local_profile":
        required.add(f"session:{collector_id}_profile")
    return required


def resolve_session_account(
    metadata: CollectorMetadata,
    *,
    session_mode: str | None = None,
) -> SessionAccountSpec | None:
    """Return the first account declaration matching config and session mode."""

    effective_mode = session_mode or resolve_session_mode(metadata.collector_id)
    for account in metadata.session_accounts:
        if account.session_modes and effective_mode not in account.session_modes:
            continue
        if not _config_conditions_match(account.when_all):
            continue
        return account
    return None


def worker_binding_mode(collector_id: str) -> str:
    """Describe how strongly a task should stay bound to one worker."""

    metadata = get_collector_metadata(collector_id) or fallback_collector_metadata(collector_id)
    if not metadata.requires_session:
        return "flexible"
    session_mode = resolve_session_mode(collector_id)
    if session_mode == "local_profile":
        return "sticky"
    if session_mode == "managed_state":
        return "lease"
    return "flexible"


def collector_metadata_payload(collector_id: str) -> dict[str, Any]:
    """Return collector metadata augmented with effective session configuration."""

    metadata = get_collector_metadata(collector_id) or fallback_collector_metadata(collector_id)
    session_contract = resolve_session_mode_contract(collector_id)
    payload = metadata.model_dump(mode="json")
    payload["default_session_mode"] = session_contract["default_mode"]
    payload["session_mode"] = session_contract["effective_mode"]
    payload["configured_session_mode"] = session_contract["configured_mode"]
    payload["session_mode_source"] = session_contract["source"]
    payload["session_mode_override_status"] = session_contract["override_status"]
    payload["supported_session_modes"] = session_contract["supported_modes"]
    return payload


def resolve_session_mode(collector_id: str) -> str:
    """Return the effective session mode after supported config overrides."""

    return str(resolve_session_mode_contract(collector_id)["effective_mode"])


def resolve_session_mode_contract(collector_id: str) -> dict[str, Any]:
    """Resolve how a collector's effective session mode is derived."""

    metadata = get_collector_metadata(collector_id) or fallback_collector_metadata(collector_id)
    default_mode = _normalize_session_mode(metadata.session_mode)
    supported_modes = _supported_session_modes(metadata)
    configured_mode = _configured_session_mode(metadata)

    effective_mode = default_mode
    source = "metadata"
    override_status = "default"

    if configured_mode:
        if configured_mode not in _SESSION_MODES:
            override_status = "ignored_invalid"
        elif configured_mode not in supported_modes:
            override_status = "ignored_unsupported"
        else:
            effective_mode = configured_mode
            source = "config"
            override_status = "applied"

    return {
        "collector_id": metadata.collector_id,
        "default_mode": default_mode,
        "configured_mode": configured_mode,
        "effective_mode": effective_mode,
        "source": source,
        "override_status": override_status,
        "supported_modes": list(supported_modes),
        "config_key": _session_mode_config_key(metadata),
    }


def _configured_session_mode(metadata: CollectorMetadata) -> str:
    raw_value = get_config(_session_mode_config_key(metadata), "")
    return _normalize_session_mode(raw_value)


def _supported_session_modes(metadata: CollectorMetadata) -> tuple[str, ...]:
    configured = [
        _normalize_session_mode(mode)
        for mode in metadata.supported_session_modes
        if _normalize_session_mode(mode) in _SESSION_MODES
    ]
    ordered: list[str] = []
    seen: set[str] = set()
    for mode in [*configured, _normalize_session_mode(metadata.session_mode)]:
        if not mode or mode in seen:
            continue
        seen.add(mode)
        ordered.append(mode)
    return tuple(ordered or ["api_only"])


def _normalize_session_mode(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in _SESSION_MODES else ""


def _session_mode_config_key(metadata: CollectorMetadata) -> str:
    return metadata.session_config_key or f"{metadata.collector_id}.session_mode"


def _config_conditions_match(conditions: dict[str, Any]) -> bool:
    for config_key, expected in conditions.items():
        if get_config(config_key, expected) != expected:
            return False
    return True


def fallback_collector_metadata(collector_id: str) -> CollectorMetadata:
    """Build minimal metadata for custom/uninstalled collectors."""

    return CollectorMetadata(
        collector_id=collector_id,
        display_name=collector_id,
        capabilities=[],
        requires_session=False,
        session_mode="api_only",
        supports_checkpoint=False,
        recovery_level="L0",
        target_schema=CollectorTargetSchema(
            required_fields=["target.name or target.params"],
            rules=[],
        ),
    )
