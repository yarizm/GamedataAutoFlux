"""Collector capability metadata and target validation schema."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from src.core.config import get as get_config


class TargetValidationRule(BaseModel):
    """A declarative rule used by task precheck."""

    mode: str = Field(default="any", description="any/all field presence check")
    fields: list[str] = Field(default_factory=list)
    level: str = "error"
    code: str
    field: str = ""
    message: str
    skip_if_error: bool = True


class CollectorTargetSchema(BaseModel):
    """Target fields and validation rules for a collector."""

    required_fields: list[str] = Field(default_factory=list)
    rules: list[TargetValidationRule] = Field(default_factory=list)


class CollectorMetadata(BaseModel):
    """Collector capabilities shared by Web, Agent, and precheck."""

    collector_id: str
    display_name: str
    capabilities: list[str] = Field(default_factory=list)
    requires_session: bool = False
    session_mode: str = "api_only"
    supports_checkpoint: bool = False
    recovery_level: str = "L0"
    target_schema: CollectorTargetSchema = Field(default_factory=CollectorTargetSchema)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    credential_profiles: list[str] = Field(default_factory=list)
    supported_session_modes: list[str] = Field(default_factory=list)


_RECOVERY_GUIDANCE = {
    "L0": "This collector does not support checkpoint resume yet; failed tasks should be reviewed and rerun.",
    "L1": "This collector can record local checkpoints; use the latest checkpoint to plan targeted follow-up work.",
    "L2": "This collector can resume across workers when the required session is available.",
    "L3": "This collector is idempotent and can resume on any compatible worker.",
}

_SESSION_MODES = ("api_only", "local_profile", "managed_state")


_COLLECTOR_METADATA: dict[str, CollectorMetadata] = {
    "steam": CollectorMetadata(
        collector_id="steam",
        display_name="Steam",
        capabilities=[
            "steam_store",
            "steam_reviews",
            "steam_api",
            "steamdb_optional_browser",
        ],
        requires_session=False,
        session_mode="api_only",
        supports_checkpoint=True,
        recovery_level="L1",
        credential_profiles=["steam_api_key", "steamdb_optional_browser_session"],
        target_schema=CollectorTargetSchema(
            required_fields=["target.name", "target.params.app_id (recommended)"],
            rules=[
                TargetValidationRule(
                    mode="any",
                    fields=["target.name", "target.params.app_id"],
                    code="missing_steam_target",
                    field="targets[{index}]",
                    message="Steam target needs a game name or app_id.",
                    skip_if_error=False,
                ),
                TargetValidationRule(
                    mode="any",
                    fields=["target.params.app_id"],
                    level="warning",
                    code="missing_steam_app_id",
                    field="targets[{index}]",
                    message="Steam app_id is recommended to avoid wrong game matches.",
                    skip_if_error=True,
                ),
            ],
        ),
        config_schema={
            "type": "object",
            "properties": {
                "request_delay": {"type": "number", "minimum": 0},
                "collect_timeout_seconds": {"type": "number", "minimum": 0},
                "collect_retries": {"type": "integer", "minimum": 0},
            },
        },
    ),
    "steam_discussions": CollectorMetadata(
        collector_id="steam_discussions",
        display_name="Steam Community Discussions",
        capabilities=["steam_community", "forum_threads", "discussion_posts"],
        requires_session=False,
        session_mode="api_only",
        supports_checkpoint=True,
        recovery_level="L1",
        target_schema=CollectorTargetSchema(
            required_fields=["target.params.app_id or target.params.forum_url"],
            rules=[
                TargetValidationRule(
                    mode="any",
                    fields=["target.params.app_id", "target.params.forum_url"],
                    code="missing_discussion_target",
                    field="targets[{index}]",
                    message="Steam discussions need app_id or forum_url.",
                    skip_if_error=False,
                )
            ],
        ),
    ),
    "taptap": CollectorMetadata(
        collector_id="taptap",
        display_name="TapTap",
        capabilities=["public_game_page", "reviews", "updates", "browser_collection"],
        requires_session=False,
        session_mode="api_only",
        supports_checkpoint=False,
        recovery_level="L0",
        credential_profiles=["playwright_runtime"],
        target_schema=CollectorTargetSchema(
            required_fields=["target.params.app_id or target.params.url"],
            rules=[
                TargetValidationRule(
                    mode="any",
                    fields=["target.params.app_id", "target.params.url"],
                    code="missing_taptap_target",
                    field="targets[{index}]",
                    message="TapTap target needs app_id or url.",
                    skip_if_error=False,
                )
            ],
        ),
    ),
    "gtrends": CollectorMetadata(
        collector_id="gtrends",
        display_name="Google Trends",
        capabilities=["trend_timeseries", "related_queries"],
        requires_session=False,
        session_mode="api_only",
        supports_checkpoint=True,
        recovery_level="L1",
        target_schema=CollectorTargetSchema(
            required_fields=["target.name"],
            rules=[
                TargetValidationRule(
                    mode="any",
                    fields=["target.name"],
                    code="missing_keyword",
                    field="targets[{index}]",
                    message="Google Trends target needs a keyword name.",
                    skip_if_error=False,
                )
            ],
        ),
    ),
    "monitor": CollectorMetadata(
        collector_id="monitor",
        display_name="Smart Monitor",
        capabilities=["steam_metrics", "twitch_metrics", "site_monitoring"],
        requires_session=False,
        session_mode="api_only",
        supports_checkpoint=True,
        recovery_level="L1",
        target_schema=CollectorTargetSchema(
            required_fields=[
                "target.params.app_id or target.params.siteurl",
                "target.params.twitch_name (optional)",
            ],
            rules=[
                TargetValidationRule(
                    mode="any",
                    fields=["target.params.app_id", "target.params.siteurl"],
                    code="missing_monitor_app_id",
                    field="targets[{index}]",
                    message="Monitor target requires app_id or siteurl.",
                    skip_if_error=False,
                )
            ],
        ),
    ),
    "qimai": CollectorMetadata(
        collector_id="qimai",
        display_name="Qimai",
        capabilities=["app_store_rank", "ratings", "download_export", "browser_collection"],
        requires_session=True,
        session_mode="local_profile",
        supported_session_modes=["local_profile", "managed_state"],
        supports_checkpoint=False,
        recovery_level="L0",
        credential_profiles=["playwright_runtime", "local_browser_profile"],
        target_schema=CollectorTargetSchema(
            required_fields=["target.params.app_id"],
            rules=[
                TargetValidationRule(
                    mode="any",
                    fields=["target.params.app_id"],
                    code="missing_qimai_app_id",
                    field="targets[{index}]",
                    message="Qimai target needs app_id.",
                    skip_if_error=False,
                )
            ],
        ),
    ),
    "official_site": CollectorMetadata(
        collector_id="official_site",
        display_name="Official Site",
        capabilities=["official_news", "announcements", "events", "browser_collection"],
        requires_session=False,
        session_mode="api_only",
        supports_checkpoint=False,
        recovery_level="L0",
        credential_profiles=["playwright_runtime"],
        target_schema=CollectorTargetSchema(
            required_fields=["target.params.official_url"],
            rules=[
                TargetValidationRule(
                    mode="any",
                    fields=["target.params.official_url"],
                    code="missing_official_url",
                    field="targets[{index}]",
                    message="Official site target needs official_url.",
                    skip_if_error=False,
                )
            ],
        ),
    ),
    "dynamic_playwright": CollectorMetadata(
        collector_id="dynamic_playwright",
        display_name="Dynamic Playwright",
        capabilities=["browser_collection", "custom_selectors", "dynamic_pages"],
        requires_session=False,
        session_mode="api_only",
        supports_checkpoint=False,
        recovery_level="L0",
        credential_profiles=["playwright_runtime"],
        target_schema=CollectorTargetSchema(
            required_fields=["target.name"],
            rules=[
                TargetValidationRule(
                    mode="any",
                    fields=["target.name"],
                    level="warning",
                    code="missing_target_name",
                    field="targets[{index}]",
                    message="Dynamic Playwright target should have a game name.",
                    skip_if_error=False,
                )
            ],
        ),
        config_schema={
            "type": "object",
            "required": ["url"],
            "properties": {
                "url": {"type": "string", "format": "uri"},
                "fields": {"type": "object"},
                "wait_for": {"type": "string"},
            },
        },
    ),
}


def get_collector_metadata(collector_id: str) -> CollectorMetadata | None:
    """Return metadata for a known collector."""
    return _COLLECTOR_METADATA.get(collector_id)


def list_collector_metadata(
    collector_ids: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Return public metadata for known collectors, optionally constrained to ids."""
    ids = collector_ids or sorted(_COLLECTOR_METADATA)
    return {
        collector_id: collector_metadata_payload(collector_id)
        for collector_id in ids
        if get_collector_metadata(collector_id) is not None
    }


def list_session_sensitive_collectors() -> list[str]:
    """Return collectors with local runtime or session requirements."""
    return sorted(
        collector_id
        for collector_id, metadata in _COLLECTOR_METADATA.items()
        if metadata.requires_session or bool(metadata.credential_profiles)
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
    """Return extra worker capabilities required to execute the collector."""
    metadata = get_collector_metadata(collector_id)
    if metadata is None or not metadata.requires_session:
        return set()

    session_mode = resolve_session_mode(collector_id)
    required = {f"session_mode:{session_mode}"}
    if session_mode == "local_profile":
        required.add(f"session:{collector_id}_profile")
    return required


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
    """Return the effective session mode after applying supported config overrides."""
    return str(resolve_session_mode_contract(collector_id)["effective_mode"])


def resolve_session_mode_contract(collector_id: str) -> dict[str, Any]:
    """Resolve how a collector's effective session mode is derived."""
    metadata = get_collector_metadata(collector_id) or fallback_collector_metadata(collector_id)
    default_mode = _normalize_session_mode(metadata.session_mode)
    supported_modes = _supported_session_modes(metadata)
    configured_mode = _configured_session_mode(collector_id)

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
        "config_key": _session_mode_config_key(metadata.collector_id),
    }


def _configured_session_mode(collector_id: str) -> str:
    raw_value = get_config(_session_mode_config_key(collector_id), "")
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


def _session_mode_config_key(collector_id: str) -> str:
    if collector_id == "steam":
        return "steam.steamdb.session_mode"
    return f"{collector_id}.session_mode"


def fallback_collector_metadata(collector_id: str) -> CollectorMetadata:
    """Build minimal metadata for custom collectors that do not define schema yet."""
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
