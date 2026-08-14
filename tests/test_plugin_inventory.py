"""Management-facing plugin inventory tests."""

from pathlib import Path

from fastapi.testclient import TestClient

from src.core.plugin_system import PluginCapability, PluginStatus
from src.plugin_manager.inventory import build_plugin_inventory, record_from_status
from src.plugin_manager.models import ManagedPluginRecord
from src.plugin_manager.store import PluginStateStore
from src.web.app import app


def test_development_module_is_read_only() -> None:
    status = PluginStatus(
        name="autoflux-plugin-example",
        version="1.2.3",
        source="module:example_plugin",
        state="active",
        collectors=["example"],
        capabilities=[PluginCapability("dag_node", "collector:example", "Example")],
    )

    record = record_from_status(status)
    payload = record.to_dict()

    assert payload["source_type"] == "development"
    assert payload["runtime_state"] == "active"
    assert payload["install_state"] == "installed"
    assert payload["desired_state"] == "enabled"
    assert payload["collectors"] == ["example"]
    assert payload["capabilities"] == [
        {"kind": "dag_node", "name": "collector:example", "target": "Example"}
    ]
    assert payload["management"]["managed"] is False
    assert payload["management"]["can_uninstall"] is False


def test_entry_point_source_classifies_managed_path(tmp_path: Path) -> None:
    managed_path = tmp_path / "generation" / "site-packages"
    status = PluginStatus(
        name="autoflux-plugin-managed",
        version="2.0.0",
        source="entrypoint:managed",
        state="failed",
        error="broken import",
    )

    record = record_from_status(
        status,
        entry_point_details={
            "entrypoint:managed": {
                "distribution": "autoflux-plugin-managed",
                "description": "Managed test plugin",
                "version": "2.0.0",
                "location": str(managed_path / "package"),
            }
        },
        managed_site_packages=managed_path,
    )

    assert record.source_type == "managed"
    assert record.managed is True
    assert record.runtime_state == "failed"
    assert record.last_error == "broken import"


def test_inventory_summary_uses_runtime_and_source_dimensions() -> None:
    payload = build_plugin_inventory(
        [
            PluginStatus(
                name="autoflux-plugin-one",
                source="module:one",
                state="active",
            ),
            PluginStatus(
                name="autoflux-plugin-two",
                source="module:two",
                state="failed",
                error="failure",
            ),
        ]
    )

    assert payload["summary"] == {
        "total": 2,
        "active": 1,
        "failed": 1,
        "restart_required": 0,
        "by_source": {"development": 2},
        "by_runtime_state": {"active": 1, "failed": 1},
    }


def test_enabled_managed_plugin_not_loaded_by_process_requires_restart(
    tmp_path: Path,
) -> None:
    store = PluginStateStore(tmp_path / "manager")
    store.replace_managed_plugins(
        [
            ManagedPluginRecord(
                plugin_id="official.steam",
                distribution="autoflux-plugin-steam",
                version="0.1.0",
                source_type="catalog",
                source_ref="official:official.steam@0.1.0",
                artifact_path="cache/steam.whl",
                artifact_sha256="sha-steam",
                display_name="Steam",
                collectors=("steam", "steam_discussions"),
                installed_at="2026-07-21T00:00:00+00:00",
                restart_required=False,
            )
        ]
    )

    payload = build_plugin_inventory([], store=store)

    steam = payload["plugins"][0]
    assert steam["runtime_state"] == "not_loaded"
    assert steam["restart_required"] is True
    assert payload["summary"]["restart_required"] == 1


def test_plugin_manager_api_matches_runtime_diagnostics() -> None:
    with TestClient(app) as client:
        runtime = client.get("/api/plugins")
        inventory = client.get("/api/plugin-manager/plugins")
        environment = client.get("/api/plugin-manager/environment")

    assert runtime.status_code == 200
    assert inventory.status_code == 200
    assert environment.status_code == 200

    runtime_payload = runtime.json()
    inventory_payload = inventory.json()
    environment_payload = environment.json()
    assert inventory_payload["summary"]["total"] == len(runtime_payload["plugins"])
    assert inventory_payload["summary"]["active"] == runtime_payload["active"]
    assert inventory_payload["summary"]["failed"] == runtime_payload["failed"]
    assert inventory_payload["summary"]["by_source"] == {
        "development": len(runtime_payload["plugins"])
    }
    assert all(
        plugin["management"]["managed"] is False
        for plugin in inventory_payload["plugins"]
    )
    assert environment_payload["mode"] == "mutable"
    assert environment_payload["mutable"] is True
    assert environment_payload["entry_point_group"] == "gamedata_autoflux.plugins"


def test_plugin_center_page_shell_is_rendered() -> None:
    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert 'data-tab="plugins"' in response.text
    assert 'id="tab-plugins"' in response.text
    assert 'data-i18n="plugins.title"' in response.text
