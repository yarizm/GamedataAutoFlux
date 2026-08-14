"""Session/runtime modeling helpers for browser-backed collectors."""

from __future__ import annotations

from typing import Any

from src.core.collector_metadata import (
    SessionAccountSpec,
    collector_metadata_payload,
    fallback_collector_metadata,
    get_collector_metadata,
    required_worker_capabilities,
    resolve_session_account,
    resolve_session_mode,
    worker_binding_mode,
)
from src.core.config import get as get_config


def build_collector_session_runtime(
    collector_id: str,
    *,
    checks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a minimal session runtime model for one collector."""
    metadata = get_collector_metadata(collector_id) or fallback_collector_metadata(collector_id)
    normalized_checks = checks or []
    session_mode = resolve_session_mode(metadata.collector_id)
    account_spec = resolve_session_account(metadata, session_mode=session_mode)
    account = _session_account_summary(account_spec)
    binding = worker_binding_mode(metadata.collector_id)
    lease = _session_lease_summary(
        metadata.collector_id,
        session_mode=session_mode,
        worker_binding=binding,
    )
    state = _session_state_summary(
        checks=normalized_checks,
        requires_session=metadata.requires_session,
        session_mode=session_mode,
        account_spec=account_spec,
    )
    return {
        "account": account,
        "state": state,
        "lease": lease,
        "worker_binding": binding,
        "required_worker_capabilities": sorted(required_worker_capabilities(metadata.collector_id)),
        "collector_metadata": collector_metadata_payload(metadata.collector_id),
    }


def _session_account_summary(account: SessionAccountSpec | None) -> dict[str, Any]:
    if account is not None:
        locator = str(
            get_config(account.locator_config_key, account.default_locator)
            or account.default_locator
            or ""
        ).strip()
        return {
            "account_id": account.account_id,
            "account_kind": account.account_kind,
            "locator": locator,
            "locator_label": account.locator_label,
        }
    return {
        "account_id": "",
        "account_kind": "not_required",
        "locator": "",
        "locator_label": "",
    }


def _session_state_summary(
    *,
    checks: list[dict[str, Any]],
    requires_session: bool,
    session_mode: str,
    account_spec: SessionAccountSpec | None,
) -> dict[str, Any]:
    readiness_name = account_spec.readiness_check if account_spec else ""
    readiness_status = next(
        (
            str(check.get("status") or "")
            for check in checks
            if readiness_name and check.get("name") == readiness_name
        ),
        "not_configured",
    )
    endpoint_statuses = [
        str(check.get("status") or "")
        for check in checks
        if isinstance(check.get("details"), dict)
        and check["details"].get("session_role") == "endpoint"
    ]
    cdp_status = _worst_status(endpoint_statuses, default="not_configured")
    required_endpoint_error = any(
        check.get("status") == "error"
        and isinstance(check.get("details"), dict)
        and check["details"].get("session_role") == "endpoint"
        and bool(check["details"].get("required"))
        for check in checks
    )
    account_kind = account_spec.account_kind if account_spec else ""
    profile_ready = account_kind == "local_profile" and readiness_status == "ok"
    storage_state_ready = account_kind == "managed_state" and readiness_status == "ok"

    if not requires_session:
        health = "ready"
    elif readiness_status == "ok" and not required_endpoint_error:
        health = "ready"
    elif readiness_status in {"warning", "error"} or required_endpoint_error:
        health = "blocked"
    else:
        health = "degraded"

    return {
        "health": health,
        "required": requires_session,
        "mode": session_mode,
        "local_profile_ready": profile_ready,
        "storage_state_ready": storage_state_ready,
        "cdp_status": cdp_status,
    }


def _worst_status(statuses: list[str], *, default: str) -> str:
    priority = {"error": 3, "warning": 2, "ok": 1}
    return max(statuses, key=lambda item: priority.get(item, 0), default=default)


def _session_lease_summary(
    collector_id: str,
    *,
    session_mode: str,
    worker_binding: str,
) -> dict[str, Any]:
    if session_mode == "managed_state":
        return {
            "mode": "managed",
            "strategy": "exclusive_lease",
            "scope": collector_id,
            "transferable": True,
        }
    if worker_binding == "sticky":
        return {
            "mode": "local",
            "strategy": "sticky_worker",
            "scope": collector_id,
            "transferable": False,
        }
    return {
        "mode": "none",
        "strategy": "none",
        "scope": collector_id,
        "transferable": True,
    }
