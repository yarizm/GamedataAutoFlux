"""Collector distribution discovery and isolation tests."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

from src.core.plugin_system import PluginManager


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run_probe(
    *,
    modules: str,
    extra_paths: list[Path] | None = None,
    preload: bool = False,
) -> dict:
    env = os.environ.copy()
    env["AUTOFLUX_PLUGIN_MODULES"] = modules
    env["AUTOFLUX_TEST_PRELOAD_PLUGINS"] = "1" if preload else ""
    python_paths = [str(PROJECT_ROOT), *(str(path) for path in (extra_paths or []))]
    if env.get("PYTHONPATH"):
        python_paths.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(python_paths)
    code = """
import importlib
import json
import os
from src.core.plugin_system import plugin_manager
from src.core.pipeline_templates import PIPELINE_TEMPLATES
from src.core.registry import registry
from src.core.collector_validators import validate_collector_config
from src.core.identifier_resolvers import list_identifier_resolvers
from src.core.dag_nodes import dag_node_catalog_payload
from src.core.collector_metadata import list_collector_metadata
if os.getenv("AUTOFLUX_TEST_PRELOAD_PLUGINS"):
    for plugin_module in os.getenv("AUTOFLUX_PLUGIN_MODULES", "").split(","):
        if not plugin_module:
            continue
        spec = importlib.import_module(plugin_module).plugin
        for module_name in spec.modules:
            importlib.import_module(module_name)
plugin_manager.load_installed()
print(json.dumps({
    "collectors": registry.list_components("collector").get("collector", []),
    "templates": [item["id"] for item in PIPELINE_TEMPLATES],
    "broken_validator_issues": len(validate_collector_config("broken", {})),
    "identifier_resolvers": [spec.platform for spec in list_identifier_resolvers()],
    "dag_nodes": dag_node_catalog_payload(),
    "collector_metadata": list_collector_metadata(),
    "status": plugin_manager.payload(),
}))
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(completed.stdout.strip().splitlines()[-1])


def test_core_only_does_not_load_any_collector() -> None:
    payload = _run_probe(modules="")
    assert payload["collectors"] == []
    assert payload["templates"] == []
    assert payload["identifier_resolvers"] == []


def test_explicit_youtube_plugin_loads_only_youtube() -> None:
    payload = _run_probe(
        modules="autoflux_plugin_youtube",
        extra_paths=[PROJECT_ROOT / "plugins" / "youtube" / "src"],
    )
    assert set(payload["collectors"]) == {"youtube_profiles", "youtube_comments"}
    assert set(payload["templates"]) == {
        "youtube_profiles_pipeline",
        "youtube_comments_pipeline",
    }
    assert payload["status"]["active"] == 1
    assert payload["status"]["failed"] == 0
    assert payload["identifier_resolvers"] == []
    collector_node_items = [
        item
        for item in payload["dag_nodes"]
        if item["type"] == "collector"
    ]
    assert {item["component"] for item in collector_node_items} == {
        "youtube_profiles",
        "youtube_comments",
    }
    assert all(item["description"].strip() for item in collector_node_items)
    assert all(item["owner"] == "autoflux-plugin-youtube" for item in collector_node_items)
    assert {
        item["key"]
        for item in payload["collector_metadata"]["youtube_profiles"]["target_schema"]["fields"]
    } == {"channel_url"}
    assert payload["collector_metadata"]["youtube_profiles"]["target_schema"]["fields"][0][
        "input_type"
    ] == "textarea_lines"
    capability_kinds = {
        item["kind"] for item in payload["status"]["plugins"][0]["capabilities"]
    }
    assert {
        "component",
        "collector_metadata",
        "dag_node",
        "collector_probe",
        "pipeline_template",
    }.issubset(capability_kinds)


def test_preimported_plugin_modules_still_receive_full_contract_validation() -> None:
    payload = _run_probe(
        modules="autoflux_plugin_youtube",
        extra_paths=[PROJECT_ROOT / "plugins" / "youtube" / "src"],
        preload=True,
    )

    assert set(payload["collectors"]) == {"youtube_profiles", "youtube_comments"}
    assert payload["status"]["active"] == 1
    assert payload["status"]["failed"] == 0
    assert len(payload["status"]["plugins"][0]["capabilities"]) >= 10


def test_explicit_steam_plugin_loads_only_steam_identifier_resolver() -> None:
    payload = _run_probe(
        modules="autoflux_plugin_steam",
        extra_paths=[
            PROJECT_ROOT / "plugins" / "steam" / "src",
            PROJECT_ROOT / "plugins" / "smart_web" / "src",
        ],
    )

    assert payload["identifier_resolvers"] == ["steam"]
    assert set(payload["collectors"]) == {"steam", "steam_discussions"}


def test_broken_plugin_is_rolled_back(tmp_path: Path) -> None:
    module = tmp_path / "broken_autoflux_plugin.py"
    module.write_text(
        """
from src.core.collector_metadata import CollectorMetadata
from src.core.plugin_system import PluginSpec
plugin = PluginSpec(
    name="broken-plugin",
    version="0.1.0",
    modules=("module_that_does_not_exist",),
    collectors=("broken",),
    metadata=(CollectorMetadata(
        collector_id="broken",
        display_name="Broken",
        description="Broken test collector.",
    ),),
)
""",
        encoding="utf-8",
    )
    payload = _run_probe(modules="broken_autoflux_plugin", extra_paths=[tmp_path])
    assert payload["collectors"] == []
    assert payload["templates"] == []
    assert payload["broken_validator_issues"] == 0
    assert payload["status"]["active"] == 0
    assert payload["status"]["failed"] == 1


def test_broken_plugin_rolls_back_validators_registered_during_activation(tmp_path: Path) -> None:
    registrar = tmp_path / "broken_validator_registrar.py"
    registrar.write_text(
        """
from src.core.collector_validators import CollectorConfigIssue, register_collector_config_validator
from src.core.identifier_resolvers import IdentifierResolverSpec, register_identifier_resolver

def validate(config):
    return [CollectorConfigIssue(code="leaked", message="must be rolled back")]

async def resolve(service, game_name):
    return None

register_collector_config_validator("broken", validate, owner="broken-validator-plugin")
register_identifier_resolver(
    IdentifierResolverSpec(platform="broken", collector_ids=("broken",), resolve=resolve),
    owner="broken-validator-plugin",
)
""",
        encoding="utf-8",
    )
    module = tmp_path / "broken_validator_plugin.py"
    module.write_text(
        """
from src.core.collector_metadata import CollectorMetadata
from src.core.plugin_system import PluginSpec
plugin = PluginSpec(
    name="broken-validator-plugin",
    version="0.1.0",
    modules=("broken_validator_registrar", "module_that_does_not_exist"),
    collectors=("broken",),
    metadata=(CollectorMetadata(
        collector_id="broken",
        display_name="Broken",
        description="Broken validator test collector.",
    ),),
)
""",
        encoding="utf-8",
    )

    payload = _run_probe(modules="broken_validator_plugin", extra_paths=[tmp_path])

    assert payload["broken_validator_issues"] == 0
    assert "broken" not in payload["identifier_resolvers"]
    assert payload["status"]["active"] == 0
    assert payload["status"]["failed"] == 1


def test_plugin_rejects_and_rolls_back_undeclared_collector(tmp_path: Path) -> None:
    registrar = tmp_path / "undeclared_collector_registrar.py"
    registrar.write_text(
        """
from src.collectors.base import BaseCollector, CollectResult
from src.core.registry import registry

@registry.register("collector", "actual")
class ActualCollector(BaseCollector):
    async def collect(self, target):
        return CollectResult(target=target, data={})
""",
        encoding="utf-8",
    )
    module = tmp_path / "undeclared_collector_plugin.py"
    module.write_text(
        """
from src.core.collector_metadata import CollectorMetadata
from src.core.plugin_system import PluginSpec
plugin = PluginSpec(
    name="undeclared-collector-plugin",
    version="0.1.0",
    modules=("undeclared_collector_registrar",),
    collectors=("declared",),
    metadata=(CollectorMetadata(
        collector_id="declared",
        display_name="Declared",
        description="Declared test collector.",
    ),),
)
""",
        encoding="utf-8",
    )

    payload = _run_probe(modules="undeclared_collector_plugin", extra_paths=[tmp_path])

    assert "actual" not in payload["collectors"]
    assert "declared" not in payload["collectors"]
    assert payload["status"]["failed"] == 1
    assert "not registered" in payload["status"]["plugins"][0]["error"]


def test_plugin_validates_unused_resolver_ownership(tmp_path: Path) -> None:
    registrar = tmp_path / "invalid_resolver_registrar.py"
    registrar.write_text(
        """
from src.collectors.base import BaseCollector, CollectResult
from src.core.identifier_resolvers import IdentifierResolverSpec, register_identifier_resolver
from src.core.registry import registry

@registry.register("collector", "owned")
class OwnedCollector(BaseCollector):
    async def collect(self, target):
        return CollectResult(target=target, data={})

async def resolve(service, name):
    return None

register_identifier_resolver(
    IdentifierResolverSpec(platform="unused", collector_ids=("owned",), resolve=resolve),
    owner="wrong-owner",
)
""",
        encoding="utf-8",
    )
    module = tmp_path / "invalid_resolver_plugin.py"
    module.write_text(
        """
from src.core.collector_metadata import CollectorMetadata
from src.core.plugin_system import PluginSpec
plugin = PluginSpec(
    name="resolver-owner-plugin",
    version="0.1.0",
    modules=("invalid_resolver_registrar",),
    collectors=("owned",),
    metadata=(CollectorMetadata(
        collector_id="owned",
        display_name="Owned",
        description="Owned test collector.",
    ),),
)
""",
        encoding="utf-8",
    )

    payload = _run_probe(modules="invalid_resolver_plugin", extra_paths=[tmp_path])

    assert "owned" not in payload["collectors"]
    assert "unused" not in payload["identifier_resolvers"]
    assert payload["status"]["failed"] == 1
    assert "owner must be" in payload["status"]["plugins"][0]["error"]


def test_plugin_rejects_collector_without_node_description(tmp_path: Path) -> None:
    registrar = tmp_path / "missing_description_registrar.py"
    registrar.write_text(
        """
from src.collectors.base import BaseCollector, CollectResult
from src.core.registry import registry

@registry.register("collector", "undocumented")
class UndocumentedCollector(BaseCollector):
    async def collect(self, target):
        return CollectResult(target=target, data={})
""",
        encoding="utf-8",
    )
    module = tmp_path / "missing_description_plugin.py"
    module.write_text(
        """
from src.core.collector_metadata import CollectorMetadata
from src.core.plugin_system import PluginSpec
plugin = PluginSpec(
    name="missing-description-plugin",
    version="0.1.0",
    modules=("missing_description_registrar",),
    collectors=("undocumented",),
    metadata=(CollectorMetadata(
        collector_id="undocumented",
        display_name="Undocumented",
    ),),
)
""",
        encoding="utf-8",
    )

    payload = _run_probe(modules="missing_description_plugin", extra_paths=[tmp_path])

    assert "undocumented" not in payload["collectors"]
    assert payload["status"]["failed"] == 1
    assert "description is required" in payload["status"]["plugins"][0]["error"]


def test_plugin_rejects_undocumented_target_field(tmp_path: Path) -> None:
    registrar = tmp_path / "undocumented_target_registrar.py"
    registrar.write_text(
        """
from src.collectors.base import BaseCollector, CollectResult
from src.core.registry import registry

@registry.register("collector", "undocumented_target")
class UndocumentedTargetCollector(BaseCollector):
    async def collect(self, target):
        return CollectResult(target=target, data={})
""",
        encoding="utf-8",
    )
    module = tmp_path / "undocumented_target_plugin.py"
    module.write_text(
        """
from src.core.collector_metadata import (
    CollectorMetadata,
    CollectorTargetField,
    CollectorTargetSchema,
)
from src.core.plugin_system import PluginSpec

plugin = PluginSpec(
    name="undocumented-target-plugin",
    version="0.1.0",
    modules=("undocumented_target_registrar",),
    collectors=("undocumented_target",),
    metadata=(CollectorMetadata(
        collector_id="undocumented_target",
        display_name="Undocumented Target",
        description="Collector with an invalid public target contract.",
        target_schema=CollectorTargetSchema(fields=[
            CollectorTargetField(
                key="url",
                label="URL",
                description="",
                input_type="url",
            )
        ]),
    ),),
)
""",
        encoding="utf-8",
    )

    payload = _run_probe(modules="undocumented_target_plugin", extra_paths=[tmp_path])

    assert payload["collectors"] == []
    assert payload["status"]["failed"] == 1
    assert "target field 'url' description is required" in payload["status"]["plugins"][0]["error"]


def test_same_name_and_version_with_different_contract_is_rejected(
    tmp_path: Path,
) -> None:
    for suffix in ("first", "second"):
        (tmp_path / f"shared_{suffix}_registrar.py").write_text(
            f"""
from src.collectors.base import BaseCollector, CollectResult
from src.core.registry import registry

@registry.register("collector", "{suffix}_collector")
class SharedCollector(BaseCollector):
    async def collect(self, target):
        return CollectResult(target=target, data={{}})
""",
            encoding="utf-8",
        )
        (tmp_path / f"shared_{suffix}_plugin.py").write_text(
            f"""
from src.core.collector_metadata import CollectorMetadata
from src.core.plugin_system import PluginSpec
plugin = PluginSpec(
    name="shared-plugin",
    version="0.1.0",
    modules=("shared_{suffix}_registrar",),
    collectors=("{suffix}_collector",),
    metadata=(CollectorMetadata(
        collector_id="{suffix}_collector",
        display_name="{suffix.title()}",
        description="{suffix.title()} shared-identity collector.",
    ),),
)
""",
            encoding="utf-8",
        )

    payload = _run_probe(
        modules="shared_first_plugin,shared_second_plugin",
        extra_paths=[tmp_path],
    )

    assert payload["collectors"] == ["first_collector"]
    assert payload["status"]["active"] == 1
    assert payload["status"]["failed"] == 1
    failed = next(
        item for item in payload["status"]["plugins"] if item["state"] == "failed"
    )
    assert "PLUGIN_IDENTITY_CONFLICT" in failed["error"]


def test_disabled_distribution_is_not_imported(monkeypatch) -> None:
    monkeypatch.setenv("AUTOFLUX_PLUGIN_MODULES", "")

    class FakeDistribution:
        metadata = {"Name": "autoflux-plugin-youtube"}

    class FakeEntryPoint:
        name = "youtube"
        dist = FakeDistribution()

        def load(self):
            raise AssertionError("disabled entry point must not be imported")

    class FakeEntryPoints:
        def select(self, *, group):
            assert group == "gamedata_autoflux.plugins"
            return [FakeEntryPoint()]

    monkeypatch.setattr(
        "src.core.plugin_system.importlib.metadata.entry_points",
        lambda: FakeEntryPoints(),
    )
    manager = PluginManager()

    statuses = manager.load_installed(
        disabled_distributions={"autoflux_plugin_youtube"}
    )

    assert statuses == []
