"""Plugin compatibility and runtime-capability checks."""

from __future__ import annotations

import importlib.metadata
import importlib.util
import json
import os
import platform
import subprocess
import sys
from functools import lru_cache
from pathlib import Path

from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.tags import sys_tags
from packaging.utils import InvalidWheelFilename, parse_wheel_filename

from src.plugin_manager.models import CatalogPlugin, Compatibility
from src.plugin_manager.package_reader import WheelMetadata, normalize_distribution
from src.plugin_manager.processes import managed_subprocess_options


PLUGIN_API_VERSION = "1"


def current_core_version() -> str:
    try:
        return importlib.metadata.version("gamedata-autoflux")
    except importlib.metadata.PackageNotFoundError:
        return "0.1.0"


@lru_cache(maxsize=1)
def detect_runtime_capabilities() -> tuple[str, ...]:
    """Detect optional process-level capabilities without importing plugins."""

    capabilities = {
        item.strip()
        for item in os.getenv("AUTOFLUX_RUNTIME_CAPABILITIES", "").split(",")
        if item.strip()
    }
    if _playwright_chromium_available():
        capabilities.add("playwright-chromium")
    disabled = {
        item.strip()
        for item in os.getenv("AUTOFLUX_DISABLED_RUNTIME_CAPABILITIES", "").split(",")
        if item.strip()
    }
    return tuple(sorted(capabilities - disabled))


def evaluate_catalog_plugin(
    plugin: CatalogPlugin,
    *,
    runtime_capabilities: tuple[str, ...] | None = None,
    core_version: str | None = None,
) -> Compatibility:
    reasons: list[str] = []
    if plugin.plugin_api != PLUGIN_API_VERSION:
        reasons.append(
            f"Plugin API {plugin.plugin_api} is not supported (expected {PLUGIN_API_VERSION})."
        )
    reasons.extend(
        _specifier_reasons(
            "core",
            core_version or current_core_version(),
            plugin.core_specifier,
        )
    )
    reasons.extend(
        _specifier_reasons(
            "Python",
            platform.python_version(),
            plugin.python_specifier,
        )
    )
    systems = {item.lower() for item in plugin.supported_systems}
    if systems and platform.system().lower() not in systems:
        reasons.append(
            f"Operating system {platform.system()} is not supported by this plugin."
        )
    available = set(
        detect_runtime_capabilities()
        if runtime_capabilities is None
        else runtime_capabilities
    )
    missing = sorted(set(plugin.runtime_capabilities) - available)
    if missing:
        reasons.append("Missing runtime capabilities: " + ", ".join(missing))
    return Compatibility(compatible=not reasons, reasons=tuple(reasons))


def evaluate_wheel(
    metadata: WheelMetadata,
    *,
    filename: str | None = None,
    core_version: str | None = None,
) -> Compatibility:
    reasons: list[str] = []
    reasons.extend(
        _specifier_reasons(
            "Python",
            platform.python_version(),
            metadata.requires_python,
        )
    )
    for raw in metadata.requires_dist:
        try:
            requirement = Requirement(raw)
        except InvalidRequirement:
            reasons.append(f"Invalid dependency requirement: {raw}")
            continue
        if requirement.marker and not requirement.marker.evaluate():
            continue
        if normalize_distribution(requirement.name) != "gamedata-autoflux":
            continue
        version = core_version or current_core_version()
        if requirement.specifier and not requirement.specifier.contains(
            version,
            prereleases=True,
        ):
            reasons.append(
                f"Core {version} does not satisfy {requirement.specifier}."
            )

    wheel_name = filename or metadata.path.name
    try:
        _name, _version, _build, tags = parse_wheel_filename(Path(wheel_name).name)
    except InvalidWheelFilename:
        reasons.append("Wheel filename is invalid.")
    else:
        if set(tags).isdisjoint(set(sys_tags())):
            reasons.append("Wheel platform tags are incompatible with this interpreter.")
    return Compatibility(compatible=not reasons, reasons=tuple(reasons))


def _specifier_reasons(label: str, version: str, raw_specifier: str) -> list[str]:
    if not raw_specifier:
        return []
    try:
        specifier = SpecifierSet(raw_specifier)
    except InvalidSpecifier:
        return [f"Invalid {label} version constraint: {raw_specifier}"]
    if specifier.contains(version, prereleases=True):
        return []
    return [f"{label} {version} does not satisfy {raw_specifier}."]


def _playwright_chromium_available() -> bool:
    if importlib.util.find_spec("playwright") is None:
        return False
    script = r"""
import json
from pathlib import Path
from playwright.sync_api import sync_playwright
with sync_playwright() as playwright:
    print(json.dumps({"available": Path(playwright.chromium.executable_path).is_file()}))
"""
    safe_environment = {
        key: value
        for key, value in os.environ.items()
        if key.upper()
        in {
            "PATH",
            "PATHEXT",
            "SYSTEMROOT",
            "WINDIR",
            "COMSPEC",
            "TEMP",
            "TMP",
            "TMPDIR",
            "HOME",
            "USERPROFILE",
            "LOCALAPPDATA",
            "APPDATA",
            "PLAYWRIGHT_BROWSERS_PATH",
        }
    }
    try:
        process = subprocess.run(
            [sys.executable, "-c", script],
            env=safe_environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
            **managed_subprocess_options(),
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    if process.returncode != 0:
        return False
    try:
        payload = json.loads(process.stdout.strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError):
        return False
    return payload.get("available") is True
