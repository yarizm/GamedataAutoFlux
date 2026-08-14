"""M3 plugin lifecycle, rollback, and dependency protection tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.core.dag import DAG, NodeSpec
from src.core.pipeline import Pipeline
from src.core.task import Task
from src.plugin_manager.environment import GenerationBuilder, read_current_pointer
from src.plugin_manager.installer import PluginInstaller
from src.plugin_manager.models import ManagedPluginRecord
from src.plugin_manager.references import PluginReferenceScanner
from src.plugin_manager.store import PluginStateStore


REPO = Path(__file__).resolve().parents[1]


def _plugin(
    *,
    version: str = "0.1.0",
    desired_state: str = "enabled",
    restart_required: bool = True,
) -> ManagedPluginRecord:
    return ManagedPluginRecord(
        plugin_id="official.youtube",
        distribution="autoflux-plugin-youtube",
        version=version,
        source_type="catalog",
        source_ref=f"official:official.youtube@{version}",
        artifact_path=f"cache/youtube-{version}.whl",
        artifact_sha256=f"sha-{version}",
        desired_state=desired_state,
        trust="official",
        display_name="YouTube",
        collectors=("youtube_profiles", "youtube_comments"),
        installed_at="2026-07-21T00:00:00+00:00",
        restart_required=restart_required,
    )


def test_desired_state_reconciles_after_disabled_restart(tmp_path: Path) -> None:
    store = PluginStateStore(tmp_path / "manager")
    store.replace_managed_plugins([_plugin(restart_required=False)])

    changed = store.set_desired_state("official.youtube", "disabled")
    assert changed.desired_state == "disabled"
    assert changed.restart_required is True
    assert store.mark_runtime_reconciled([]) == 1

    reconciled = store.get_managed_plugin("official.youtube")
    assert reconciled is not None
    assert reconciled.desired_state == "disabled"
    assert reconciled.restart_required is False


def test_rollback_restores_previous_generation_lock(tmp_path: Path) -> None:
    manager_dir = tmp_path / "manager"
    store = PluginStateStore(manager_dir)
    version_one = _plugin(version="0.1.0", restart_required=False)
    version_two = _plugin(version="0.2.0", restart_required=False)
    for generation_id in ("gen-one", "gen-two"):
        (manager_dir / "generations" / generation_id / "site-packages").mkdir(
            parents=True
        )
    store.record_generation(
        generation_id="gen-one",
        site_packages="generations/gen-one/site-packages",
        previous_generation_id=None,
        plugins=[version_one],
    )
    store.record_generation(
        generation_id="gen-two",
        site_packages="generations/gen-two/site-packages",
        previous_generation_id="gen-one",
        plugins=[version_two],
    )
    store.replace_managed_plugins([version_two])
    GenerationBuilder(manager_dir=manager_dir).activate_existing(
        generation_id="gen-two",
        site_packages="generations/gen-two/site-packages",
        previous_generation_id="gen-one",
    )
    store.mark_generation_reconciled()
    assert store.generation_restart_required() is False

    installer = PluginInstaller(store=store, manager_dir=manager_dir, project_root=REPO)
    result = installer.rollback()

    assert result["generation_id"] == "gen-one"
    assert result["restart_required"] is True
    assert read_current_pointer(manager_dir)["generation_id"] == "gen-one"
    restored = store.get_managed_plugin("official.youtube")
    assert restored is not None
    assert restored.version == "0.1.0"
    assert restored.restart_required is True
    assert store.get_active_generation()["previous_generation_id"] == "gen-two"
    assert store.generation_restart_required() is True


def test_uninstall_last_disabled_plugin_builds_empty_generation(tmp_path: Path) -> None:
    manager_dir = tmp_path / "manager"
    store = PluginStateStore(manager_dir)
    current = _plugin(desired_state="disabled", restart_required=False)
    (manager_dir / "generations" / "gen-current" / "site-packages").mkdir(
        parents=True
    )
    store.record_generation(
        generation_id="gen-current",
        site_packages="generations/gen-current/site-packages",
        previous_generation_id=None,
        plugins=[current],
    )
    store.replace_managed_plugins([current])
    GenerationBuilder(manager_dir=manager_dir).activate_existing(
        generation_id="gen-current",
        site_packages="generations/gen-current/site-packages",
        previous_generation_id=None,
    )
    store.mark_generation_reconciled()

    installer = PluginInstaller(store=store, manager_dir=manager_dir, project_root=REPO)
    result = installer.uninstall("official.youtube")

    assert result["restart_required"] is True
    assert result["plugins"] == []
    assert store.list_managed_plugins() == []
    assert store.get_active_generation()["generation_id"] != "gen-current"
    assert store.generation_restart_required() is True


def test_catalog_upgrade_rebuilds_generation_and_preserves_desired_state(
    tmp_path: Path,
) -> None:
    manager_dir = tmp_path / "manager"
    store = PluginStateStore(manager_dir)
    old = _plugin(
        version="0.0.9",
        desired_state="disabled",
        restart_required=False,
    )
    store.replace_managed_plugins([old])
    installer = PluginInstaller(store=store, manager_dir=manager_dir, project_root=REPO)

    result = installer.upgrade_catalog(
        "official.youtube",
        requested_version="0.1.0",
    )

    assert result["restart_required"] is True
    upgraded = store.get_managed_plugin("official.youtube")
    assert upgraded is not None
    assert upgraded.version == "0.1.0"
    assert upgraded.desired_state == "disabled"
    assert upgraded.restart_required is True


class _DagRepository:
    async def list_all(self):
        return [
            DAG(
                name="youtube-dag",
                nodes=[NodeSpec("collect", "collector", "youtube_comments")],
                edges=[],
            )
        ]


class _Scheduler:
    def __init__(self) -> None:
        self.pipeline = Pipeline("youtube-pipeline").add_collector("youtube_profiles")
        running = Task(
            id="task-running",
            name="YouTube running",
            pipeline_name="youtube-pipeline",
        )
        running.start()
        completed = Task(
            id="task-completed",
            name="Completed",
            pipeline_name="youtube-pipeline",
        )
        completed.start()
        completed.complete()
        self.tasks = [running, completed]

    def get_all_pipelines(self):
        return [self.pipeline]

    def list_cron_jobs(self):
        return [
            {
                "id": "cron-youtube",
                "name": "YouTube daily",
                "pipeline_name": "youtube-pipeline",
                "task_template": {},
                "enabled": True,
            }
        ]

    def get_all_tasks(self):
        return self.tasks


@pytest.mark.asyncio
async def test_reference_scanner_covers_pipeline_dag_cron_and_active_task() -> None:
    scanner = PluginReferenceScanner(
        scheduler=_Scheduler(),
        dag_repository=_DagRepository(),
    )

    references = await scanner.scan(("youtube_profiles", "youtube_comments"))

    assert {item.kind for item in references} == {"pipeline", "dag", "cron", "task"}
    assert all(item.reference_id != "task-completed" for item in references)
