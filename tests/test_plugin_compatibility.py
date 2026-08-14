"""M4 compatibility, runtime capability, and deployment-mode tests."""

from __future__ import annotations

from pathlib import Path

from src.plugin_manager.compatibility import evaluate_catalog_plugin, evaluate_wheel
from src.plugin_manager.environment import get_plugin_manager_access
from src.plugin_manager.models import CatalogPlugin
from src.plugin_manager.package_reader import WheelMetadata


def _catalog_plugin(**changes) -> CatalogPlugin:
    values = {
        "plugin_id": "official.example",
        "distribution": "autoflux-plugin-example",
        "display_name": "Example",
        "version": "1.0.0",
        "description": "Example plugin",
        "publisher": "Autoflux",
        "trust": "official",
        "package_dir": "example",
        "collectors": ("example",),
    }
    values.update(changes)
    return CatalogPlugin(**values)


def test_catalog_compatibility_reports_every_blocking_reason() -> None:
    plugin = _catalog_plugin(
        plugin_api="99",
        core_specifier=">=99",
        python_specifier=">=99",
        supported_systems=("unsupported-os",),
        runtime_capabilities=("browser-x",),
    )

    result = evaluate_catalog_plugin(
        plugin,
        runtime_capabilities=(),
        core_version="0.1.0",
    )

    assert result.compatible is False
    assert len(result.reasons) == 5
    assert any("Plugin API" in reason for reason in result.reasons)
    assert any("Missing runtime capabilities: browser-x" in reason for reason in result.reasons)


def test_wheel_compatibility_checks_python_core_and_platform_tags(tmp_path: Path) -> None:
    metadata = WheelMetadata(
        path=tmp_path / "example.whl",
        distribution="autoflux-plugin-example",
        version="1.0.0",
        summary="",
        requires_dist=("gamedata-autoflux>=99",),
        entry_points=(("example", "autoflux_plugin_example:plugin"),),
        sha256="sha",
        size=1,
        requires_python=">=99",
    )

    result = evaluate_wheel(
        metadata,
        filename="autoflux_plugin_example-1.0.0-cp39-cp39-win32.whl",
        core_version="0.1.0",
    )

    assert result.compatible is False
    assert any("Python" in reason for reason in result.reasons)
    assert any("Core" in reason for reason in result.reasons)
    assert any("platform tags" in reason for reason in result.reasons)


def test_explicit_and_multi_worker_modes_are_read_only(monkeypatch) -> None:
    monkeypatch.setenv("AUTOFLUX_PLUGIN_MANAGER_MODE", "read_only")
    explicit = get_plugin_manager_access()
    assert explicit["mutable"] is False
    assert "AUTOFLUX_PLUGIN_MANAGER_MODE" in explicit["reason"]

    monkeypatch.setenv("AUTOFLUX_PLUGIN_MANAGER_MODE", "mutable")
    monkeypatch.setenv("WEB_CONCURRENCY", "2")
    multi_worker = get_plugin_manager_access()
    assert multi_worker["mutable"] is False
    assert multi_worker["multi_process"] is True
    assert "single service worker" in multi_worker["reason"]


def test_nonexistent_manager_path_checks_its_actual_parent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    blocking_file = tmp_path / "not-a-directory"
    blocking_file.write_text("blocked", encoding="utf-8")
    monkeypatch.setenv(
        "AUTOFLUX_PLUGIN_MANAGER_DIR",
        str(blocking_file / "plugin-manager"),
    )
    monkeypatch.setenv("AUTOFLUX_PLUGIN_MANAGER_MODE", "auto")
    monkeypatch.delenv("WEB_CONCURRENCY", raising=False)

    access = get_plugin_manager_access()

    assert access["mutable"] is False
    assert access["writable"] is False
    assert "not writable" in access["reason"]
