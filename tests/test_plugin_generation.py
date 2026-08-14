"""Managed generation build, pointer switch, and restart discovery tests."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import zipfile
from types import SimpleNamespace
from pathlib import Path

import pytest

from src.plugin_manager.environment import (
    GenerationBuildError,
    GenerationBuilder,
    get_managed_site_packages,
)
from src.plugin_manager.models import ManagedPluginRecord
from src.plugin_manager.store import PluginStateStore


REPO = Path(__file__).resolve().parents[1]


def _build_youtube_wheel(wheel_dir: Path) -> Path:
    wheel_dir.mkdir(parents=True, exist_ok=True)
    build_source = wheel_dir.parent / "youtube-build-source"
    shutil.copytree(
        REPO / "plugins" / "youtube",
        build_source,
        ignore=shutil.ignore_patterns(
            "build",
            "dist",
            "*.egg-info",
            "__pycache__",
            "*.pyc",
        ),
    )
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--disable-pip-version-check",
            "--no-deps",
            "--no-build-isolation",
            "--wheel-dir",
            str(wheel_dir),
            str(build_source),
        ],
        cwd=REPO,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )
    assert process.returncode == 0, process.stdout + process.stderr
    wheels = list(wheel_dir.glob("*.whl"))
    assert len(wheels) == 1
    return wheels[0]


def test_generation_switch_and_core_only_restart_discovers_youtube(
    tmp_path: Path,
    monkeypatch,
) -> None:
    manager_dir = tmp_path / "plugin-manager"
    monkeypatch.setenv("AUTOFLUX_PLUGIN_MANAGER_DIR", str(manager_dir))
    wheel = _build_youtube_wheel(tmp_path / "wheelhouse")
    stages: list[str] = []
    builder = GenerationBuilder(
        manager_dir=manager_dir,
        project_root=REPO,
        python_executable=sys.executable,
    )

    result = builder.build(
        [wheel],
        progress=lambda stage, _progress, _message: stages.append(stage),
    )
    pointer = builder.activate(result)

    assert result.site_packages.is_dir()
    assert pointer["generation_id"] == result.generation_id
    assert get_managed_site_packages() == result.site_packages
    assert stages == [
        "resolving_dependencies",
        "installing_wheels",
        "verifying_generation",
        "generation_ready",
    ]

    script = """
import json
from src.core.plugin_system import plugin_manager
from src.core.registry import registry
from src.web.app import _auto_discover_plugins

_auto_discover_plugins()
print(json.dumps({
    "plugins": plugin_manager.payload(),
    "components": registry.list_components(),
}))
"""
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(REPO)
    environment["AUTOFLUX_PLUGIN_MANAGER_DIR"] = str(manager_dir)
    environment["AUTOFLUX_PLUGIN_MODULES"] = ""
    process = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPO,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )
    assert process.returncode == 0, process.stdout + process.stderr
    payload = json.loads(process.stdout.strip().splitlines()[-1])
    assert payload["plugins"]["active"] == 1
    assert payload["plugins"]["failed"] == 0
    assert payload["plugins"]["plugins"][0]["name"] == "autoflux-plugin-youtube"
    assert payload["components"]["collector"] == [
        "youtube_profiles",
        "youtube_comments",
    ]

    PluginStateStore(manager_dir).replace_managed_plugins(
        [
            ManagedPluginRecord(
                plugin_id="official.youtube",
                distribution="autoflux-plugin-youtube",
                version="0.1.0",
                source_type="catalog",
                source_ref="official:official.youtube@0.1.0",
                artifact_path=str(wheel),
                artifact_sha256="test",
                desired_state="disabled",
                collectors=("youtube_profiles", "youtube_comments"),
                restart_required=True,
            )
        ]
    )
    disabled_process = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPO,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )
    assert disabled_process.returncode == 0, (
        disabled_process.stdout + disabled_process.stderr
    )
    disabled_payload = json.loads(disabled_process.stdout.strip().splitlines()[-1])
    assert disabled_payload["plugins"]["active"] == 0
    assert disabled_payload["components"].get("collector", []) == []
    assert (
        PluginStateStore(manager_dir).get_managed_plugin("official.youtube").restart_required
        is False
    )


def test_import_failure_never_switches_current_generation(tmp_path: Path) -> None:
    manager_dir = tmp_path / "manager"
    manager_dir.mkdir()
    pointer = manager_dir / "current.json"
    pointer.write_text('{"sentinel":"active"}', encoding="utf-8")
    wheel = tmp_path / "autoflux_plugin_broken-1.0.0-py3-none-any.whl"
    dist_info = "autoflux_plugin_broken-1.0.0.dist-info"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            f"{dist_info}/METADATA",
            "Metadata-Version: 2.4\nName: autoflux-plugin-broken\n"
            "Version: 1.0.0\nRequires-Python: >=3.12\n",
        )
        archive.writestr(
            f"{dist_info}/entry_points.txt",
            "[gamedata_autoflux.plugins]\nbroken = missing_plugin_module:plugin\n",
        )
        archive.writestr(
            f"{dist_info}/WHEEL",
            "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\n"
            "Tag: py3-none-any\n",
        )
        archive.writestr(f"{dist_info}/RECORD", "")

    builder = GenerationBuilder(manager_dir=manager_dir, project_root=REPO)
    with pytest.raises(GenerationBuildError) as error:
        builder.build([wheel])

    assert error.value.code == "PLUGIN_IMPORT_VALIDATION_FAILED"
    assert pointer.read_text(encoding="utf-8") == '{"sentinel":"active"}'


def test_generation_rejects_undeclared_plugin_capability(tmp_path: Path) -> None:
    manager_dir = tmp_path / "manager"
    wheel = tmp_path / "autoflux_plugin_contract_bad-1.0.0-py3-none-any.whl"
    dist_info = "autoflux_plugin_contract_bad-1.0.0.dist-info"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            "contract_bad_plugin/__init__.py",
            """from src.core.collector_metadata import CollectorMetadata
from src.core.plugin_system import PluginSpec
plugin = PluginSpec(
    name="autoflux-plugin-contract-bad",
    version="1.0.0",
    modules=("contract_bad_plugin.components",),
    collectors=("declared",),
    metadata=(CollectorMetadata(
        collector_id="declared",
        display_name="Declared",
        description="Declared contract-test collector.",
    ),),
)
""",
        )
        archive.writestr(
            "contract_bad_plugin/components.py",
            """from src.collectors.base import BaseCollector, CollectResult
from src.core.registry import registry

@registry.register("collector", "declared")
class DeclaredCollector(BaseCollector):
    async def collect(self, target):
        return CollectResult(target=target, data={})

@registry.register("collector", "hidden")
class HiddenCollector(BaseCollector):
    async def collect(self, target):
        return CollectResult(target=target, data={})
""",
        )
        archive.writestr(
            f"{dist_info}/METADATA",
            "Metadata-Version: 2.4\nName: autoflux-plugin-contract-bad\n"
            "Version: 1.0.0\nRequires-Python: >=3.12\n",
        )
        archive.writestr(
            f"{dist_info}/entry_points.txt",
            "[gamedata_autoflux.plugins]\ncontract-bad = contract_bad_plugin:plugin\n",
        )
        archive.writestr(
            f"{dist_info}/WHEEL",
            "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\n"
            "Tag: py3-none-any\n",
        )
        archive.writestr(f"{dist_info}/RECORD", "")

    builder = GenerationBuilder(manager_dir=manager_dir, project_root=REPO)
    with pytest.raises(GenerationBuildError) as error:
        builder.build([wheel])

    assert error.value.code == "PLUGIN_IMPORT_VALIDATION_FAILED"
    assert "undeclared collector components" in str(error.value)


def test_generation_resolver_preserves_conflicting_requirements_for_pip(
    tmp_path: Path,
) -> None:
    builder = GenerationBuilder(manager_dir=tmp_path / "manager", project_root=REPO)
    metadata = [
        SimpleNamespace(
            distribution="autoflux-plugin-one",
            requires_dist=("autoflux-impossible-dependency<1",),
        ),
        SimpleNamespace(
            distribution="autoflux-plugin-two",
            requires_dist=("autoflux-impossible-dependency>=2",),
        ),
    ]

    requirements = builder._external_requirements(metadata)

    assert requirements == [
        "autoflux-impossible-dependency<1",
        "autoflux-impossible-dependency>=2",
    ]


def test_generation_rejects_plugin_wheels_with_overlapping_import_files(
    tmp_path: Path,
) -> None:
    builder = GenerationBuilder(manager_dir=tmp_path / "manager", project_root=REPO)

    with pytest.raises(GenerationBuildError) as error:
        builder._validate_wheel_conflicts(
            [
                SimpleNamespace(
                    distribution="autoflux-plugin-one",
                    install_paths=("shared_namespace/__init__.py", "plugin_one.py"),
                ),
                SimpleNamespace(
                    distribution="autoflux-plugin-two",
                    install_paths=("SHARED_NAMESPACE/__init__.py", "plugin_two.py"),
                ),
            ]
        )

    assert error.value.code == "PLUGIN_WHEEL_CONFLICT"
    assert "both install" in str(error.value)


def test_generation_rejects_plugin_wheel_that_shadows_platform_package(
    tmp_path: Path,
) -> None:
    builder = GenerationBuilder(manager_dir=tmp_path / "manager", project_root=REPO)

    with pytest.raises(GenerationBuildError) as error:
        builder._validate_wheel_conflicts(
            [
                SimpleNamespace(
                    distribution="autoflux-plugin-host-shadow",
                    install_paths=("src/core/plugin_system.py",),
                )
            ]
        )

    assert error.value.code == "PLUGIN_WHEEL_CONFLICT"
    assert "reserved install path" in str(error.value)
