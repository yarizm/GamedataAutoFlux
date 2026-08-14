"""Managed plugin subprocess isolation tests."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from src.plugin_manager.catalog import PluginCatalog
from src.plugin_manager.installer import PluginInstallError, PluginInstaller
from src.plugin_manager.locking import PluginManagerProcessLock
from src.plugin_manager.processes import managed_subprocess_options
from src.plugin_manager.store import PluginStateStore


REPO = Path(__file__).resolve().parents[1]


def test_managed_subprocesses_never_inherit_interactive_stdin() -> None:
    options = managed_subprocess_options()

    assert options["stdin"] == subprocess.DEVNULL
    if sys.platform == "win32":
        assert options["creationflags"] & subprocess.CREATE_NO_WINDOW
    else:
        assert "creationflags" not in options


def test_catalog_build_reports_console_interrupt_with_stable_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict = {}

    def interrupted(command, **options):
        captured.update(options)
        return subprocess.CompletedProcess(
            command,
            returncode=130,
            stdout="",
            stderr="Traceback\nKeyboardInterrupt\n",
        )

    monkeypatch.setattr("src.plugin_manager.installer.subprocess.run", interrupted)
    manager_dir = tmp_path / "manager"
    store = PluginStateStore(manager_dir)
    catalog = PluginCatalog(project_root=REPO, store=store)
    installer = PluginInstaller(
        store=store,
        catalog=catalog,
        manager_dir=manager_dir,
        project_root=REPO,
    )
    steam = catalog.get("official.steam")
    assert steam is not None

    with pytest.raises(PluginInstallError) as error:
        installer._build_catalog_artifacts(steam)

    assert error.value.code == "PLUGIN_WHEEL_BUILD_INTERRUPTED"
    assert "Retry" in str(error.value)
    assert captured["stdin"] == subprocess.DEVNULL
    if sys.platform == "win32":
        assert captured["creationflags"] & subprocess.CREATE_NO_WINDOW


def test_process_lock_forces_other_service_process_read_only(tmp_path: Path) -> None:
    manager_dir = tmp_path / "manager"
    lock = PluginManagerProcessLock(manager_dir)
    assert lock.acquire() is True
    try:
        environment = os.environ.copy()
        environment["AUTOFLUX_PLUGIN_MANAGER_DIR"] = str(manager_dir)
        environment["AUTOFLUX_PLUGIN_MANAGER_MODE"] = "auto"
        environment["AUTOFLUX_PLUGIN_MODULES"] = ""
        script = """
import json
from src.plugin_manager.environment import get_plugin_manager_access
print(json.dumps(get_plugin_manager_access()))
"""
        process = subprocess.run(
            [sys.executable, "-c", script],
            cwd=REPO,
            env=environment,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
        assert process.returncode == 0, process.stdout + process.stderr
        access = json.loads(process.stdout.strip().splitlines()[-1])
        assert access["mutable"] is False
        assert access["lock_available"] is False
        assert "another service process" in access["reason"]
    finally:
        lock.release()
