"""Runtime health and configuration diagnostics."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from typing import Any

from src.core.config import get as get_config
from src.core.config import get_data_dir, get_raw_section, get_root_dir
from src.core.config import get_settings_validation

Status = str


def build_health_report(scheduler_stats: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build a compact health report for API consumers."""
    checks = [
        _settings_file_check(),
        _settings_schema_check(),
        _data_dir_check(),
        _dependency_check("fastapi", required=True),
        _dependency_check("uvicorn", required=True),
        _dependency_check("playwright", required=True),
        _llm_provider_check(),
    ]
    status = _overall_status(checks)
    return {
        "status": status,
        "checks": checks,
        "summary": {
            "errors": sum(1 for check in checks if check["status"] == "error"),
            "warnings": sum(1 for check in checks if check["status"] == "warning"),
            "scheduler": scheduler_stats or {},
        },
    }


def build_config_diagnostics() -> dict[str, Any]:
    """Build detailed local diagnostics without touching external services."""
    checks = [
        _settings_file_check(),
        _settings_schema_check(),
        _data_dir_check(),
        _dependency_check("pyyaml", import_name="yaml", required=True),
        _dependency_check("python-dotenv", import_name="dotenv", required=False),
        _dependency_check("playwright", required=True),
        _dependency_check("langchain-openai", import_name="langchain_openai", required=False),
        _llm_provider_check(),
        _steam_config_check(),
        _steamdb_config_check(),
        _scheduler_config_check(),
    ]
    return {
        "status": _overall_status(checks),
        "checks": checks,
        "paths": {
            "root_dir": str(get_root_dir()),
            "settings_file": str(get_root_dir() / "config" / "settings.yaml"),
            "data_dir": str(get_data_dir()),
            "logs_dir": str(get_root_dir() / "logs"),
        },
    }


def build_collector_session_diagnostics(collector_id: str) -> dict[str, Any]:
    """Build local session/runtime diagnostics for one collector."""
    from src.core.collector_metadata import (
        collector_metadata_payload,
        fallback_collector_metadata,
        get_collector_metadata,
        resolve_session_mode_contract,
    )
    from src.core.session_runtime import build_collector_session_runtime

    metadata = get_collector_metadata(collector_id) or fallback_collector_metadata(collector_id)
    session_contract = resolve_session_mode_contract(metadata.collector_id)
    checks: list[dict[str, Any]] = []
    profiles = set(metadata.credential_profiles)
    effective_mode = str(session_contract["effective_mode"])

    if "playwright_runtime" in profiles:
        checks.append(_dependency_check("playwright", required=True))
    if "steamdb_optional_browser_session" in profiles:
        checks.append(_steamdb_session_check(optional=True))
    if "local_browser_profile" in profiles and effective_mode == "local_profile":
        checks.extend(_local_profile_session_checks(metadata.collector_id))
    if metadata.collector_id == "qimai" and effective_mode == "managed_state":
        checks.append(_storage_state_check("session:qimai_storage_state", "qimai.storage_state_path"))
    if (
        "local_browser_profile" in profiles
        and effective_mode != "local_profile"
        and session_contract["override_status"] != "applied"
    ):
        checks.append(
            _check(
                "session_mode_override",
                "warning",
                "Collector declares a local browser profile, but the effective session mode is not local_profile.",
                configured_mode=session_contract["configured_mode"],
                effective_mode=effective_mode,
                override_status=session_contract["override_status"],
            )
        )

    if not checks:
        checks.append(
            _check(
                "session",
                "ok",
                "Collector does not require a local browser session",
            )
        )

    runtime = build_collector_session_runtime(metadata.collector_id, checks=checks)

    return {
        "collector_id": metadata.collector_id,
        "display_name": metadata.display_name,
        "requires_session": metadata.requires_session,
        "session_mode": effective_mode,
        "default_session_mode": session_contract["default_mode"],
        "configured_session_mode": session_contract["configured_mode"],
        "session_mode_source": session_contract["source"],
        "session_mode_override_status": session_contract["override_status"],
        "supported_session_modes": session_contract["supported_modes"],
        "worker_binding": runtime["worker_binding"],
        "required_worker_capabilities": runtime["required_worker_capabilities"],
        "credential_profiles": sorted(profiles),
        "status": _overall_status(checks),
        "collector_metadata": collector_metadata_payload(metadata.collector_id),
        "session_account": runtime["account"],
        "session_state": runtime["state"],
        "session_lease": runtime["lease"],
        "checks": checks,
    }


def build_session_diagnostics_overview(
    collector_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Build diagnostics for all collectors with local runtime/session dependencies."""
    from src.core.collector_metadata import list_session_sensitive_collectors

    ids = collector_ids or list_session_sensitive_collectors()
    collectors = [build_collector_session_diagnostics(collector_id) for collector_id in ids]
    checks = [check for collector in collectors for check in collector.get("checks", [])]
    return {
        "status": _overall_status(checks or [{"status": "ok"}]),
        "summary": {
            "collectors": len(collectors),
            "requires_session": sum(1 for item in collectors if item.get("requires_session")),
            "errors": sum(1 for item in collectors if item.get("status") == "error"),
            "warnings": sum(1 for item in collectors if item.get("status") == "warning"),
        },
        "collectors": collectors,
    }


def build_session_readiness_summary(diagnostics: dict[str, Any]) -> dict[str, Any]:
    """Build a compact task-facing readiness summary from session diagnostics."""
    if not isinstance(diagnostics, dict) or not diagnostics:
        return {}

    requires_session = bool(diagnostics.get("requires_session", False))
    mode = str(diagnostics.get("session_mode") or "api_only").strip() or "api_only"
    binding = str(diagnostics.get("worker_binding") or "flexible").strip() or "flexible"
    diagnostics_status = str(diagnostics.get("status") or "unknown").strip().lower() or "unknown"
    account = diagnostics.get("session_account", {}) if isinstance(diagnostics.get("session_account"), dict) else {}
    lease = diagnostics.get("session_lease", {}) if isinstance(diagnostics.get("session_lease"), dict) else {}
    state = diagnostics.get("session_state", {}) if isinstance(diagnostics.get("session_state"), dict) else {}
    health = str(state.get("health") or ("ready" if not requires_session else "unknown")).strip().lower()
    status = "not_required" if not requires_session else health or "unknown"
    relevant_checks = _session_attention_checks(diagnostics)
    precheck_status = _session_precheck_status(
        requires_session=requires_session,
        mode=mode,
        health=health,
        diagnostics_status=diagnostics_status,
        state=state,
        relevant_checks=relevant_checks,
    )
    summary, recommended_action = _session_readiness_message(
        requires_session=requires_session,
        mode=mode,
        health=health,
        state=state,
        relevant_checks=relevant_checks,
    )

    return {
        "required": requires_session,
        "status": status,
        "is_ready": precheck_status == "ok",
        "precheck_status": precheck_status,
        "diagnostics_status": diagnostics_status,
        "mode": mode,
        "binding": binding,
        "summary": summary,
        "recommended_action": recommended_action,
        "required_worker_capabilities": list(diagnostics.get("required_worker_capabilities", []) or []),
        "account_kind": str(account.get("account_kind") or "not_required"),
        "account_id": str(account.get("account_id") or ""),
        "locator": str(account.get("locator") or ""),
        "locator_label": str(account.get("locator_label") or ""),
        "lease_strategy": str(lease.get("strategy") or "none"),
        "blocking_reasons": relevant_checks if precheck_status == "error" else [],
        "attention_reasons": relevant_checks if precheck_status == "warning" else [],
    }


def _settings_file_check() -> dict[str, Any]:
    path = get_root_dir() / "config" / "settings.yaml"
    if path.exists():
        return _check("settings_file", "ok", "Settings file exists", path=str(path))
    return _check("settings_file", "error", "Missing config/settings.yaml", path=str(path))


def _settings_schema_check() -> dict[str, Any]:
    validation = get_settings_validation()
    issues = validation.get("issues", [])
    if validation.get("valid", False):
        return _check("settings_schema", "ok", "settings.yaml schema validation passed")
    return _check(
        "settings_schema",
        "error",
        "settings.yaml contains invalid values",
        issues=issues,
    )


def _data_dir_check() -> dict[str, Any]:
    data_dir = get_data_dir()
    if data_dir.exists() and os.access(data_dir, os.W_OK):
        return _check("data_dir", "ok", "Data directory is writable", path=str(data_dir))
    return _check(
        "data_dir",
        "error",
        "Data directory is missing or not writable",
        path=str(data_dir),
    )


def _dependency_check(
    name: str,
    *,
    import_name: str | None = None,
    required: bool,
) -> dict[str, Any]:
    module_name = import_name or name.replace("-", "_")
    installed = importlib.util.find_spec(module_name) is not None
    if installed:
        return _check(f"dependency:{name}", "ok", f"Dependency can be imported: {name}")
    status = "error" if required else "warning"
    return _check(
        f"dependency:{name}",
        status,
        f"Dependency cannot be imported: {name}",
        install_hint=f"pip install {name}",
    )


def _llm_provider_check() -> dict[str, Any]:
    provider = str(get_config("llm.provider", "") or "").strip()
    raw_llm = get_raw_section("llm")
    providers = [
        key for key, value in raw_llm.items() if key != "provider" and isinstance(value, dict)
    ]
    if not provider:
        return _check(
            "llm.provider",
            "warning",
            "Default LLM provider is not configured",
            providers=providers,
        )
    if provider not in providers:
        return _check(
            "llm.provider",
            "error",
            f"Default LLM provider does not exist: {provider}",
            provider=provider,
            providers=providers,
        )

    model = str(get_config(f"llm.{provider}.model", "") or "").strip()
    base_url = str(get_config(f"llm.{provider}.base_url", "") or "").strip()
    api_key = str(get_config(f"llm.{provider}.api_key", "") or "").strip()
    missing = []
    if not model:
        missing.append("model")
    if provider != "openai" and not base_url:
        missing.append("base_url")
    if provider != "local" and not api_key:
        missing.append("api_key")
    if missing:
        return _check(
            "llm.provider",
            "warning",
            f"Default LLM provider configuration is incomplete: {provider}",
            provider=provider,
            missing=missing,
        )
    return _check(
        "llm.provider",
        "ok",
        f"Default LLM provider is usable: {provider}",
        provider=provider,
    )


def _steam_config_check() -> dict[str, Any]:
    api_key = str(get_config("steam.api_key", "") or "").strip()
    if api_key:
        return _check("steam.api_key", "ok", "Steam API Key is configured")
    return _check(
        "steam.api_key",
        "warning",
        "Steam API Key is not configured; some official Steam APIs may be unavailable",
    )


def _steamdb_config_check() -> dict[str, Any]:
    enabled = bool(get_config("steam.steamdb.enabled", False))
    if not enabled:
        return _check("steam.steamdb", "ok", "SteamDB collection is disabled")
    cdp_enabled = bool(get_config("steam.steamdb.cdp_enabled", False))
    cdp_port = get_config("steam.steamdb.cdp_port", 9222)
    profile_dir = str(get_config("steam.steamdb.cdp_profile_dir", "") or "").strip()

    if not cdp_enabled:
        return _check(
            "steam.steamdb",
            "ok",
            "SteamDB collection is enabled without CDP. Expect captchas.",
            cdp_enabled=False,
        )

    if _is_cdp_endpoint_reachable(cdp_port):
        return _check(
            "steam.steamdb",
            "ok",
            "SteamDB CDP 浏览器已连接并就绪",
            cdp_enabled=True,
            cdp_port=cdp_port,
            profile_dir=profile_dir,
        )

    return _check(
        "steam.steamdb",
        "warning",
        "未检测到 SteamDB 浏览器运行。请先启动登录浏览器以开放 CDP 端口。",
        cdp_enabled=True,
        cdp_port=cdp_port,
        profile_dir=profile_dir,
        action="open_steamdb_browser",
    )


def _steamdb_session_check(*, optional: bool) -> dict[str, Any]:
    enabled = bool(get_config("steam.steamdb.enabled", False))
    cdp_enabled = bool(get_config("steam.steamdb.cdp_enabled", False))
    cdp_port = get_config("steam.steamdb.cdp_port", 9222)
    profile_dir = str(get_config("steam.steamdb.cdp_profile_dir", "") or "").strip()

    if not enabled:
        return _check(
            "session:steamdb",
            "ok",
            "SteamDB collection is disabled",
            optional=optional,
        )
    if not cdp_enabled:
        return _check(
            "session:steamdb",
            "warning",
            "SteamDB CDP session is disabled; SteamDB pages may hit captcha or rate limits",
            optional=optional,
            cdp_enabled=False,
        )
    if _is_cdp_endpoint_reachable(cdp_port):
        return _check(
            "session:steamdb",
            "ok",
            "SteamDB CDP browser is reachable",
            optional=optional,
            cdp_port=cdp_port,
            profile_dir=profile_dir,
        )
    return _check(
        "session:steamdb",
        "warning" if optional else "error",
        "SteamDB CDP browser is not reachable; launch and log in before SteamDB collection",
        optional=optional,
        cdp_port=cdp_port,
        profile_dir=profile_dir,
        action="open_steamdb_browser",
    )


def _local_profile_session_checks(collector_id: str) -> list[dict[str, Any]]:
    if collector_id == "qimai":
        profile_dir = _resolve_project_path(
            str(get_config("qimai.user_data_dir", "") or "data/qimai_profile")
        )
        cdp_enabled = bool(get_config("qimai.cdp_enabled", True))
        cdp_required = bool(get_config("qimai.cdp_required", False))
        cdp_port = get_config("qimai.cdp_port", 9222)

        checks = [_profile_dir_check("session:qimai_profile", profile_dir)]
        if cdp_enabled:
            checks.append(
                _cdp_session_check(
                    "session:qimai_cdp",
                    cdp_port=cdp_port,
                    required=cdp_required,
                    message_prefix="Qimai",
                )
            )
        return checks

    profile_dir = _resolve_project_path(f"data/{collector_id}_profile")
    return [_profile_dir_check(f"session:{collector_id}_profile", profile_dir)]


def _profile_dir_check(name: str, profile_dir: Path) -> dict[str, Any]:
    details = {
        "profile_dir": str(profile_dir),
        "exists": profile_dir.exists(),
    }
    if profile_dir.exists() and profile_dir.is_dir():
        return _check(name, "ok", "Browser profile directory exists", **details)
    return _check(
        name,
        "warning",
        "Browser profile directory is missing; run the login helper before collection",
        **details,
    )


def _storage_state_check(name: str, config_key: str) -> dict[str, Any]:
    storage_state_path = _resolve_project_path(str(get_config(config_key, "") or ""))
    details = {
        "storage_state_path": str(storage_state_path),
        "exists": storage_state_path.exists(),
    }
    if storage_state_path.exists() and storage_state_path.is_file():
        return _check(name, "ok", "Storage state file exists", **details)
    return _check(
        name,
        "warning",
        "Storage state file is missing; export a logged-in browser storage state before collection",
        **details,
    )


def _cdp_session_check(
    name: str,
    *,
    cdp_port: Any,
    required: bool,
    message_prefix: str,
) -> dict[str, Any]:
    if _is_cdp_endpoint_reachable(cdp_port):
        return _check(
            name,
            "ok",
            f"{message_prefix} CDP browser is reachable",
            cdp_port=cdp_port,
            required=required,
        )
    return _check(
        name,
        "error" if required else "warning",
        f"{message_prefix} CDP browser is not reachable",
        cdp_port=cdp_port,
        required=required,
    )


def _resolve_project_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return get_root_dir() / path


def _is_cdp_endpoint_reachable(port: Any, *, timeout: float = 0.25) -> bool:
    try:
        import urllib.request

        safe_port = int(port)
        url = f"http://127.0.0.1:{safe_port}/json/version"
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status == 200
    except Exception:
        return False


def _scheduler_config_check() -> dict[str, Any]:
    max_concurrent = get_config("scheduler.max_concurrent_tasks", 0)
    try:
        max_concurrent_int = int(max_concurrent)
    except (TypeError, ValueError):
        return _check(
            "scheduler.max_concurrent_tasks", "error", "Max concurrency is not an integer"
        )
    if max_concurrent_int <= 0:
        return _check(
            "scheduler.max_concurrent_tasks",
            "error",
            "Max concurrency must be greater than 0",
        )
    return _check(
        "scheduler.max_concurrent_tasks",
        "ok",
        "Scheduler concurrency configuration is valid",
        max_concurrent=max_concurrent_int,
    )


def _check(name: str, status: Status, message: str, **details: Any) -> dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "message": message,
        "details": details,
    }


def _overall_status(checks: list[dict[str, Any]]) -> str:
    statuses = {check["status"] for check in checks}
    if "error" in statuses:
        return "error"
    if "warning" in statuses:
        return "warning"
    return "ok"


def _session_attention_checks(diagnostics: dict[str, Any]) -> list[dict[str, Any]]:
    relevant: list[dict[str, Any]] = []
    for raw_check in diagnostics.get("checks", []) or []:
        if not isinstance(raw_check, dict):
            continue
        name = str(raw_check.get("name") or "session")
        if name.startswith("dependency:"):
            continue
        status = str(raw_check.get("status") or "").strip().lower()
        if status not in {"warning", "error"}:
            continue
        relevant.append(
            {
                "name": name,
                "status": status,
                "message": str(raw_check.get("message") or ""),
            }
        )
    return relevant


def _session_precheck_status(
    *,
    requires_session: bool,
    mode: str,
    health: str,
    diagnostics_status: str,
    state: dict[str, Any],
    relevant_checks: list[dict[str, Any]],
) -> str:
    if diagnostics_status == "error":
        return "error"
    if requires_session and health == "blocked":
        return "error"
    if requires_session and mode == "managed_state" and not bool(state.get("storage_state_ready")):
        return "error"
    if requires_session and mode == "local_profile" and not bool(state.get("local_profile_ready")):
        return "error"
    if requires_session and mode == "local_profile" and str(state.get("cdp_status") or "") == "error":
        return "error"
    if diagnostics_status == "warning":
        return "warning"
    if health == "degraded":
        return "warning"
    if any(check.get("status") == "error" for check in relevant_checks):
        return "error"
    if relevant_checks:
        return "warning"
    return "ok"


def _session_readiness_message(
    *,
    requires_session: bool,
    mode: str,
    health: str,
    state: dict[str, Any],
    relevant_checks: list[dict[str, Any]],
) -> tuple[str, str]:
    if not requires_session:
        if relevant_checks:
            return (
                "No required local session, but optional browser session attention is recommended.",
                "review_optional_session",
            )
        return ("No local session required for task submission.", "none")

    if mode == "managed_state":
        if bool(state.get("storage_state_ready")):
            return ("Managed browser state is ready for task submission.", "none")
        return (
            "Managed browser state is missing. Export a logged-in storage_state before submitting this task.",
            "export_storage_state",
        )

    if mode == "local_profile":
        if not bool(state.get("local_profile_ready")):
            return (
                "Local browser profile is missing. Complete the one-time browser login before submitting this task.",
                "prepare_local_profile",
            )
        cdp_status = str(state.get("cdp_status") or "not_configured")
        if cdp_status == "error":
            return (
                "Local browser profile exists, but the required CDP browser is not reachable.",
                "start_cdp_browser",
            )
        if cdp_status == "warning":
            return (
                "Local browser profile is ready, but the optional CDP browser is not reachable.",
                "start_cdp_browser",
            )
        return ("Local browser profile is ready for task submission.", "none")

    if health == "blocked":
        return (
            "Collector session is blocked and needs attention before task submission.",
            "review_session",
        )
    if health == "degraded" or relevant_checks:
        return (
            "Collector session is partially ready. Review the session warnings before task submission.",
            "review_session",
        )
    return ("Collector session is ready for task submission.", "none")


def build_steamdb_launch_command() -> list[str]:
    """构建启动 SteamDB 登录浏览器的 subprocess 命令列表"""
    import sys

    from src.core.config import get as get_config
    from src.core.config import get_root_dir

    cdp_port = get_config("steam.steamdb.cdp_port", 9222)
    profile_dir = str(get_config("steam.steamdb.cdp_profile_dir", "") or "").strip()

    cmd = [
        sys.executable,
        str(get_root_dir() / "scripts" / "steamdb_login.py"),
        "--no-wait",
    ]
    if cdp_port is not None:
        cmd.extend(["--port", str(cdp_port)])
    if profile_dir:
        cmd.extend(["--profile-dir", profile_dir])
    return cmd
