"""Drive the shipped frontend validateEditor via Node (real ES module path)."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SELFTEST = REPO / "src" / "web" / "src" / "pages" / "dag" / "validate.selftest.mjs"
VALIDATE_JS = REPO / "src" / "web" / "src" / "pages" / "dag" / "validate.js"


@pytest.mark.skipif(shutil.which("node") is None, reason="node not on PATH")
def test_shipped_validate_editor_selftest_passes():
    """Runs validate.selftest.mjs which imports validate.js (not a reimplementation)."""
    assert VALIDATE_JS.is_file(), f"missing shipped module {VALIDATE_JS}"
    assert SELFTEST.is_file(), f"missing selftest {SELFTEST}"
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
    assert proc.returncode == 0, f"validate.selftest failed:\n{out}"
    assert "VALIDATE_EDITOR_SELFTEST_OK" in out
    # must have exercised invalid graphs, not only happy path
    assert "FAIL_AS_EXPECTED" in out
    assert "upstream_no_edge" in out or "case upstream_no_edge" in out
    assert "dangling_port" in out or "case dangling_port" in out
    assert "cycle" in out or "case cycle" in out
