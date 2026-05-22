"""Runtime health and configuration diagnostics."""

from __future__ import annotations

import importlib.util
import os
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
        _embedding_provider_check(),
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


def _embedding_provider_check() -> dict[str, Any]:
    provider = str(get_config("embedding.provider", "") or "").strip()
    if not provider:
        return _check("embedding.provider", "warning", "Embedding provider is not configured")
    if provider == "local":
        return _check("embedding.provider", "ok", "Embedding uses local model", provider=provider)
    api_key = str(get_config(f"embedding.{provider}.api_key", "") or "").strip()
    if not api_key:
        return _check(
            "embedding.provider",
            "warning",
            f"Embedding provider is missing api_key: {provider}",
            provider=provider,
        )
    return _check(
        "embedding.provider",
        "ok",
        f"Embedding provider is configured: {provider}",
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

    # Check if CDP port is actually listening
    import urllib.request
    try:
        url = f"http://127.0.0.1:{cdp_port}/json/version"
        with urllib.request.urlopen(url, timeout=1) as response:
            if response.status == 200:
                return _check(
                    "steam.steamdb",
                    "ok",
                    "SteamDB CDP 浏览器已连接并就绪",
                    cdp_enabled=True,
                    cdp_port=cdp_port,
                    profile_dir=profile_dir,
                )
    except Exception:
        pass

    return _check(
        "steam.steamdb",
        "warning",
        "未检测到 SteamDB 浏览器运行。请先启动登录浏览器以开放 CDP 端口。",
        cdp_enabled=True,
        cdp_port=cdp_port,
        profile_dir=profile_dir,
        action="open_steamdb_browser",
    )


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
