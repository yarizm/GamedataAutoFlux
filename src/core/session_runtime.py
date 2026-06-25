"""Session/runtime modeling helpers for browser-backed collectors."""

from __future__ import annotations

from typing import Any

from src.core.collector_metadata import (
    collector_metadata_payload,
    fallback_collector_metadata,
    get_collector_metadata,
    required_worker_capabilities,
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
    account = _session_account_summary(metadata.collector_id)
    binding = worker_binding_mode(metadata.collector_id)
    lease = _session_lease_summary(
        metadata.collector_id,
        session_mode=session_mode,
        worker_binding=binding,
    )
    state = _session_state_summary(
        metadata.collector_id,
        checks=normalized_checks,
        requires_session=metadata.requires_session,
        session_mode=session_mode,
    )
    return {
        "account": account,
        "state": state,
        "lease": lease,
        "worker_binding": binding,
        "required_worker_capabilities": sorted(required_worker_capabilities(metadata.collector_id)),
        "collector_metadata": collector_metadata_payload(metadata.collector_id),
    }


def _session_account_summary(collector_id: str) -> dict[str, Any]:
    if collector_id == "qimai":
        configured_mode = str(get_config("qimai.session_mode", "") or "").strip().lower()
        profile_dir = str(get_config("qimai.user_data_dir", "") or "data/qimai_profile").strip()
        storage_state_path = str(
            get_config("qimai.storage_state_path", "") or "data/qimai_storage_state.json"
        ).strip()
        if configured_mode == "managed_state":
            return {
                "account_id": "managed:qimai_storage_state",
                "account_kind": "managed_state",
                "locator": storage_state_path,
                "locator_label": "storage_state_path",
            }
        return {
            "account_id": "local:qimai_profile",
            "account_kind": "local_profile",
            "locator": profile_dir,
            "locator_label": "user_data_dir",
        }

    if collector_id == "steam":
        enabled = bool(get_config("steam.steamdb.enabled", False))
        profile_dir = str(
            get_config("steam.steamdb.cdp_profile_dir", "") or "data/steamdb_profile"
        ).strip()
        return {
            "account_id": "local:steamdb_profile" if enabled else "",
            "account_kind": "local_profile" if enabled else "not_required",
            "locator": profile_dir if enabled else "",
            "locator_label": "cdp_profile_dir" if enabled else "",
        }

    return {
        "account_id": "",
        "account_kind": "not_required",
        "locator": "",
        "locator_label": "",
    }


def _session_state_summary(
    collector_id: str,
    *,
    checks: list[dict[str, Any]],
    requires_session: bool,
    session_mode: str,
) -> dict[str, Any]:
    profile_check_name = f"session:{collector_id}_profile"
    profile_ready = any(
        check.get("name") == profile_check_name and check.get("status") == "ok" for check in checks
    )
    profile_missing = any(
        check.get("name") == profile_check_name and check.get("status") in {"warning", "error"}
        for check in checks
    )
    cdp_status = next(
        (
            str(check.get("status") or "")
            for check in checks
            if str(check.get("name") or "").endswith("_cdp")
            or check.get("name") == "session:steamdb"
        ),
        "not_configured",
    )
    storage_state_ready = any(
        check.get("name") == f"session:{collector_id}_storage_state" and check.get("status") == "ok"
        for check in checks
    )
    storage_state_missing = any(
        check.get("name") == f"session:{collector_id}_storage_state"
        and check.get("status") in {"warning", "error"}
        for check in checks
    )

    if not requires_session:
        health = "ready"
    elif session_mode == "managed_state" and storage_state_ready:
        health = "ready"
    elif session_mode == "managed_state" and storage_state_missing:
        health = "blocked"
    elif profile_ready and cdp_status not in {"error"}:
        health = "ready"
    elif profile_missing or cdp_status == "error":
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
