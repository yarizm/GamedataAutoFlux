from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SELFTEST = REPO / "src" / "web" / "src" / "core" / "theme.selftest.mjs"


@pytest.mark.skipif(shutil.which("node") is None, reason="node not on PATH")
def test_theme_selftest_passes():
    assert SELFTEST.is_file()
    proc = subprocess.run(
        ["node", str(SELFTEST)],
        cwd=str(REPO),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    out = (proc.stdout or "") + (proc.stderr or "")
    assert proc.returncode == 0, out
    assert "THEME_SELFTEST_OK" in out
