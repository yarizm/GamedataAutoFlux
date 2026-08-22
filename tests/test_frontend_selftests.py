"""Run every frontend `*.selftest.mjs` under node.

Discovery-based on purpose: the previous convention was one hand-written
pytest wrapper per selftest, which meant a new selftest silently ran nowhere
until somebody remembered to add its wrapper. Globbing removes that step.

Each selftest is expected to be standalone (`node <file>`, no arguments, repo
root as cwd) and to print a `*_SELFTEST_OK` marker on success.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SELFTEST_ROOT = REPO / "src" / "web" / "src"

SELFTESTS = sorted(SELFTEST_ROOT.rglob("*.selftest.mjs"))


def _test_id(path: Path) -> str:
    return path.relative_to(SELFTEST_ROOT).as_posix()


def test_selftests_are_discovered():
    """Fail loudly if the glob stops matching, instead of vacuously passing."""
    assert SELFTESTS, f"no *.selftest.mjs found under {SELFTEST_ROOT}"


@pytest.mark.skipif(shutil.which("node") is None, reason="node not on PATH")
@pytest.mark.parametrize("selftest", SELFTESTS, ids=_test_id)
def test_frontend_selftest_passes(selftest: Path):
    proc = subprocess.run(
        ["node", str(selftest)],
        cwd=str(REPO),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )
    out = (proc.stdout or "") + (proc.stderr or "")
    assert proc.returncode == 0, out
    assert "SELFTEST_OK" in out, out
