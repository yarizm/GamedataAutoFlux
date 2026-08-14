"""Node-level smoke tests for the Plugin Center page model and i18n."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parents[1]
SELFTEST = (
    REPO
    / "src"
    / "web"
    / "src"
    / "pages"
    / "plugins"
    / "plugin-center.selftest.mjs"
)


@pytest.mark.skipif(shutil.which("node") is None, reason="node not on PATH")
def test_plugin_center_selftest_passes() -> None:
    assert SELFTEST.is_file()
    process = subprocess.run(
        ["node", str(SELFTEST)],
        cwd=str(REPO),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    output = (process.stdout or "") + (process.stderr or "")
    assert process.returncode == 0, output
    assert "PLUGIN_CENTER_SELFTEST_OK" in output
