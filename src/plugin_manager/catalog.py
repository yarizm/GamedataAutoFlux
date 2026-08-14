"""Bundled first-party plugin catalog."""

from __future__ import annotations

import json
import os
from pathlib import Path

from src.plugin_manager.compatibility import evaluate_catalog_plugin
from src.plugin_manager.models import CatalogPlugin
from src.plugin_manager.store import PluginStateStore, plugin_state_store


CATALOG_PATH = Path(__file__).with_name("catalog.json")
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def resolve_catalog_project_root() -> Path:
    """Locate repository-owned plugin sources without assuming editable install."""

    configured = os.getenv("AUTOFLUX_PROJECT_ROOT", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    working_directory = Path.cwd().resolve()
    if (working_directory / "plugins").is_dir():
        return working_directory
    if (PROJECT_ROOT / "plugins").is_dir():
        return PROJECT_ROOT
    return working_directory


class PluginCatalog:
    def __init__(
        self,
        *,
        catalog_path: Path = CATALOG_PATH,
        project_root: Path | None = None,
        store: PluginStateStore = plugin_state_store,
    ) -> None:
        self.catalog_path = catalog_path
        self.project_root = (project_root or resolve_catalog_project_root()).resolve()
        self.store = store

    def list_plugins(self) -> list[CatalogPlugin]:
        payload = json.loads(self.catalog_path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != 1:
            raise ValueError("unsupported bundled catalog schema")
        plugins = [self._coerce_plugin(item) for item in payload.get("plugins", [])]
        ids = [item.plugin_id for item in plugins]
        if len(ids) != len(set(ids)):
            raise ValueError("bundled catalog contains duplicate plugin ids")
        return sorted(plugins, key=lambda item: item.display_name.lower())

    def get(self, plugin_id: str) -> CatalogPlugin | None:
        return next((item for item in self.list_plugins() if item.plugin_id == plugin_id), None)

    def package_path(self, plugin: CatalogPlugin, package_dir: str | None = None) -> Path:
        directory = package_dir or plugin.package_dir
        candidate = (self.project_root / "plugins" / directory).resolve()
        plugins_root = (self.project_root / "plugins").resolve()
        try:
            candidate.relative_to(plugins_root)
        except ValueError as exc:  # pragma: no cover - catalog is repository-owned
            raise ValueError("catalog package path escapes plugins root") from exc
        return candidate

    def payload(self) -> dict:
        installed = {
            item.plugin_id: item.version for item in self.store.list_managed_plugins()
        }
        plugins = self.list_plugins()
        plugin_payloads = []
        for item in plugins:
            compatibility = evaluate_catalog_plugin(item)
            source_available = (self.package_path(item) / "pyproject.toml").is_file()
            plugin_payloads.append(
                item.to_dict(
                    available=source_available and compatibility.compatible,
                    installed_version=installed.get(item.plugin_id),
                    compatibility=compatibility,
                )
            )
        return {
            "catalogs": [{"id": "official", "type": "bundled", "trust": "official"}],
            "plugins": plugin_payloads,
            "total": len(plugins),
        }

    @staticmethod
    def _coerce_plugin(item: dict) -> CatalogPlugin:
        required = {
            "id",
            "distribution",
            "display_name",
            "version",
            "description",
            "publisher",
            "trust",
            "package_dir",
            "collectors",
        }
        missing = sorted(required - set(item))
        if missing:
            raise ValueError(f"catalog plugin is missing: {', '.join(missing)}")
        return CatalogPlugin(
            plugin_id=str(item["id"]),
            distribution=str(item["distribution"]),
            display_name=str(item["display_name"]),
            version=str(item["version"]),
            description=str(item["description"]),
            publisher=str(item["publisher"]),
            trust=str(item["trust"]),
            package_dir=str(item["package_dir"]),
            collectors=tuple(str(value) for value in item["collectors"]),
            runtime_capabilities=tuple(
                str(value) for value in item.get("runtime_capabilities", [])
            ),
            internal_dependencies=tuple(
                str(value) for value in item.get("internal_dependencies", [])
            ),
            plugin_api=str(item.get("plugin_api", "1")),
            core_specifier=str(item.get("core_specifier", ">=0.1,<0.3")),
            python_specifier=str(item.get("python_specifier", ">=3.12")),
            supported_systems=tuple(
                str(value) for value in item.get("supported_systems", [])
            ),
            homepage=str(item.get("homepage", "")),
            license=str(item.get("license", "")),
            icon=str(item.get("icon", "")),
        )


plugin_catalog = PluginCatalog()
