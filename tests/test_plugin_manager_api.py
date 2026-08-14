"""Managed plugin catalog, operation, and upload API tests."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from fastapi.testclient import TestClient

from src.core.pipeline import Pipeline
from src.plugin_manager.environment import get_plugin_manager_dir
from src.plugin_manager.compatibility import detect_runtime_capabilities
from src.plugin_manager.models import ManagedPluginRecord
from src.plugin_manager.store import PluginStateStore
from src.web.app import app


REPO = Path(__file__).resolve().parents[1]


def _wait_for_operation(client: TestClient, operation_id: str) -> dict:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        response = client.get(f"/api/plugin-manager/operations/{operation_id}")
        assert response.status_code == 200
        operation = response.json()
        if operation["state"] in {"succeeded", "failed"}:
            return operation
        time.sleep(0.05)
    raise AssertionError("plugin operation did not finish")


def _build_youtube_wheel(tmp_path: Path) -> Path:
    build_source = tmp_path / "youtube-source"
    wheel_dir = tmp_path / "wheelhouse"
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
    wheel_dir.mkdir()
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


def test_catalog_install_api_builds_managed_generation() -> None:
    with TestClient(app) as client:
        catalog = client.get("/api/plugin-manager/catalog")
        assert catalog.status_code == 200
        catalog_payload = catalog.json()
        assert catalog_payload["total"] == 8
        assert all(item["trust"] == "official" for item in catalog_payload["plugins"])

        missing_confirmation = client.post(
            "/api/plugin-manager/operations/install",
            json={"plugin_id": "official.youtube"},
        )
        assert missing_confirmation.status_code == 400

        accepted = client.post(
            "/api/plugin-manager/operations/install?confirm=true",
            json={"plugin_id": "official.youtube", "version": "0.1.0"},
        )
        assert accepted.status_code == 202
        operation_id = accepted.json()["id"]

        conflict = client.post(
            "/api/plugin-manager/operations/install?confirm=true",
            json={"plugin_id": "official.steam"},
        )
        assert conflict.status_code == 409
        assert conflict.json()["detail"]["code"] == "PLUGIN_OPERATION_CONFLICT"

        operation = _wait_for_operation(client, operation_id)
        assert operation["state"] == "succeeded", operation
        assert operation["restart_required"] is True
        assert operation["progress"] == 100

        history = client.get("/api/plugin-manager/operations").json()
        assert history["total"] == 1
        assert history["active"] is None

        inventory = client.get("/api/plugin-manager/plugins").json()
        managed = next(
            item for item in inventory["plugins"] if item["id"] == "official.youtube"
        )
        assert managed["source_type"] == "managed"
        assert managed["runtime_state"] == "not_loaded"
        assert managed["restart_required"] is True
        assert managed["collectors"] == ["youtube_profiles", "youtube_comments"]

        refreshed_catalog = client.get("/api/plugin-manager/catalog").json()
        youtube = next(
            item
            for item in refreshed_catalog["plugins"]
            if item["id"] == "official.youtube"
        )
        assert youtube["installed"] is True
        assert youtube["installed_version"] == "0.1.0"

    manager_dir = get_plugin_manager_dir()
    assert (manager_dir / "current.json").is_file()

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
    restart_payload = json.loads(process.stdout.strip().splitlines()[-1])
    assert restart_payload["plugins"]["active"] == 1
    assert restart_payload["plugins"]["failed"] == 0
    assert restart_payload["plugins"]["plugins"][0]["name"] == (
        "autoflux-plugin-youtube"
    )
    assert restart_payload["components"]["collector"] == [
        "youtube_profiles",
        "youtube_comments",
    ]
    assert PluginStateStore(manager_dir).list_managed_plugins()[0].restart_required is False


def test_upload_rejects_invalid_wheel_before_queuing_operation() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/api/plugin-manager/operations/upload?confirm=true",
            files={"file": ("malicious.whl", b"not-a-wheel", "application/octet-stream")},
        )
        assert response.status_code == 400
        assert response.json()["detail"]["code"] == "PLUGIN_PACKAGE_INVALID"
        assert client.get("/api/plugin-manager/operations").json()["total"] == 0


def test_valid_local_wheel_upload_is_installed(tmp_path: Path) -> None:
    wheel = _build_youtube_wheel(tmp_path)
    with TestClient(app) as client:
        with wheel.open("rb") as handle:
            accepted = client.post(
                "/api/plugin-manager/operations/upload?confirm=true",
                files={"file": (wheel.name, handle, "application/octet-stream")},
            )
        assert accepted.status_code == 202
        operation = _wait_for_operation(client, accepted.json()["id"])
        assert operation["state"] == "succeeded", operation
        assert operation["restart_required"] is True

        inventory = client.get("/api/plugin-manager/plugins").json()
        managed = next(
            item
            for item in inventory["plugins"]
            if item["id"] == "local.autoflux-plugin-youtube"
        )
        assert managed["source_type"] == "managed"
        assert managed["trust"] == "local"

    records = PluginStateStore(get_plugin_manager_dir()).list_managed_plugins()
    assert records[0].source_type == "upload"
    assert records[0].artifact_path.startswith("cache")
    assert list((get_plugin_manager_dir() / "uploads").rglob("*.whl")) == []


def test_operation_lookup_returns_stable_not_found_error() -> None:
    with TestClient(app) as client:
        response = client.get("/api/plugin-manager/operations/op_missing")
        assert response.status_code == 404
        assert response.json()["detail"]["code"] == "PLUGIN_OPERATION_NOT_FOUND"


def test_terminal_operation_history_can_be_deleted() -> None:
    with TestClient(app) as client:
        store = PluginStateStore(get_plugin_manager_dir())
        store.create_operation(
            operation_id="op_delete_me",
            kind="install",
            plugin_id="official.steam",
            requested_version="0.1.0",
            request={},
        )
        store.update_operation(
            "op_delete_me",
            state="failed",
            stage="failed",
            error_code="PLUGIN_WHEEL_BUILD_FAILED",
        )

        unconfirmed = client.delete(
            "/api/plugin-manager/operations/op_delete_me"
        )
        assert unconfirmed.status_code == 400

        deleted = client.delete(
            "/api/plugin-manager/operations/op_delete_me?confirm=true"
        )
        assert deleted.status_code == 200
        assert deleted.json()["operation"]["state"] == "failed"
        assert client.get(
            "/api/plugin-manager/operations/op_delete_me"
        ).status_code == 404
        audit = store.list_audit_events()
        assert audit[0]["action"] == "operation.history.deleted"


def test_active_operation_history_cannot_be_deleted() -> None:
    with TestClient(app) as client:
        store = PluginStateStore(get_plugin_manager_dir())
        store.create_operation(
            operation_id="op_still_active",
            kind="install",
            plugin_id="official.steam",
            requested_version=None,
            request={},
        )

        response = client.delete(
            "/api/plugin-manager/operations/op_still_active?confirm=true"
        )

        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "PLUGIN_OPERATION_ACTIVE"


def _seed_managed_youtube(*, desired_state: str, restart_required: bool) -> None:
    PluginStateStore(get_plugin_manager_dir()).replace_managed_plugins(
        [
            ManagedPluginRecord(
                plugin_id="official.youtube",
                distribution="autoflux-plugin-youtube",
                version="0.1.0",
                source_type="catalog",
                source_ref="official:official.youtube@0.1.0",
                artifact_path="cache/youtube.whl",
                artifact_sha256="sha-youtube",
                desired_state=desired_state,
                trust="official",
                display_name="YouTube",
                collectors=("youtube_profiles", "youtube_comments"),
                installed_at="2026-07-21T00:00:00+00:00",
                restart_required=restart_required,
            )
        ]
    )


def test_desired_state_api_requires_confirmation_and_marks_restart() -> None:
    with TestClient(app) as client:
        _seed_managed_youtube(desired_state="enabled", restart_required=False)
        missing_confirmation = client.put(
            "/api/plugin-manager/plugins/official.youtube/desired-state",
            json={"desired_state": "disabled"},
        )
        assert missing_confirmation.status_code == 400

        response = client.put(
            "/api/plugin-manager/plugins/official.youtube/desired-state?confirm=true",
            json={"desired_state": "disabled"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["desired_state"] == "disabled"
        assert payload["restart_required"] is True
        assert payload["management"]["can_enable"] is True
        assert payload["management"]["can_uninstall"] is False


def test_referenced_plugin_cannot_be_uninstalled() -> None:
    import src.web.app as web_app

    with TestClient(app) as client:
        _seed_managed_youtube(desired_state="disabled", restart_required=False)
        pipeline = Pipeline("youtube-reference").add_collector("youtube_profiles")
        assert web_app.scheduler is not None
        web_app.scheduler._pipelines[pipeline.name] = pipeline
        try:
            detail = client.get("/api/plugin-manager/plugins/official.youtube")
            assert detail.status_code == 200
            assert any(
                item["kind"] == "pipeline" and item["id"] == pipeline.name
                for item in detail.json()["references"]
            )

            response = client.post(
                "/api/plugin-manager/operations/uninstall?confirm=true",
                json={"plugin_id": "official.youtube"},
            )
            assert response.status_code == 409
            payload = response.json()["detail"]
            assert payload["code"] == "PLUGIN_REFERENCED"
            assert payload["references"][0]["kind"] in {"pipeline", "dag", "cron"}
        finally:
            web_app.scheduler._pipelines.pop(pipeline.name, None)


def test_manual_restart_and_missing_rollback_return_stable_errors() -> None:
    with TestClient(app) as client:
        restart = client.post("/api/plugin-manager/apply-restart?confirm=true")
        assert restart.status_code == 409
        assert restart.json()["detail"]["code"] == "PLUGIN_RESTART_UNAVAILABLE"

        rollback = client.post("/api/plugin-manager/operations/rollback?confirm=true")
        assert rollback.status_code == 409
        assert rollback.json()["detail"]["code"] == "PLUGIN_ROLLBACK_UNAVAILABLE"


def test_read_only_deployment_rejects_mutation_without_touching_pointer(
    monkeypatch,
) -> None:
    manager_dir = get_plugin_manager_dir()
    manager_dir.mkdir(parents=True, exist_ok=True)
    pointer = manager_dir / "current.json"
    pointer.write_text('{"sentinel":"unchanged"}', encoding="utf-8")
    before = pointer.read_bytes()
    monkeypatch.setenv("AUTOFLUX_PLUGIN_MANAGER_MODE", "read_only")

    with TestClient(app) as client:
        environment = client.get("/api/plugin-manager/environment")
        assert environment.status_code == 200
        assert environment.json()["mode"] == "read_only"
        response = client.post(
            "/api/plugin-manager/operations/install?confirm=true",
            json={"plugin_id": "official.youtube"},
        )
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "PLUGIN_ENV_READ_ONLY"

    assert pointer.read_bytes() == before


def test_missing_runtime_capability_rejects_catalog_install(monkeypatch) -> None:
    monkeypatch.setenv(
        "AUTOFLUX_DISABLED_RUNTIME_CAPABILITIES",
        "playwright-chromium",
    )
    detect_runtime_capabilities.cache_clear()
    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/plugin-manager/operations/install?confirm=true",
                json={"plugin_id": "official.steam"},
            )
    finally:
        detect_runtime_capabilities.cache_clear()

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["code"] == "PLUGIN_INCOMPATIBLE"
    assert "playwright-chromium" in " ".join(detail["reasons"])
