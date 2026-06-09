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
    from src.core.collector_metadata import fallback_collector_metadata, get_collector_metadata

    metadata = get_collector_metadata(collector_id) or fallback_collector_metadata(collector_id)
    checks: list[dict[str, Any]] = []
    profiles = set(metadata.credential_profiles)

    if "playwright_runtime" in profiles:
        checks.append(_dependency_check("playwright", required=True))
    if "steamdb_optional_browser_session" in profiles:
        checks.append(_steamdb_session_check(optional=True))
    if "local_browser_profile" in profiles:
        checks.extend(_local_profile_session_checks(metadata.collector_id))

    if not checks:
        checks.append(
            _check(
                "session",
                "ok",
                "Collector does not require a local browser session",
            )
        )

    return {
        "collector_id": metadata.collector_id,
        "requires_session": metadata.requires_session,
        "session_mode": metadata.session_mode,
        "status": _overall_status(checks),
        "checks": checks,
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
