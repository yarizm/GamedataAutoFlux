from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parents[1]
HELP_DIR = REPO / "src" / "web" / "src" / "core" / "help"
SELFTESTS = {
    "HELP_STORAGE_SELFTEST_OK": HELP_DIR / "storage.selftest.mjs",
    "HELP_CONTENT_SELFTEST_OK": HELP_DIR / "content.selftest.mjs",
    "HELP_SPOTLIGHT_SELFTEST_OK": HELP_DIR / "spotlight.selftest.mjs",
}


@pytest.mark.skipif(shutil.which("node") is None, reason="node not on PATH")
@pytest.mark.parametrize(("marker", "selftest"), SELFTESTS.items())
def test_help_selftest_passes(marker: str, selftest: Path) -> None:
    proc = subprocess.run(
        ["node", str(selftest)],
        cwd=str(REPO),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    output = (proc.stdout or "") + (proc.stderr or "")
    assert proc.returncode == 0, output
    assert marker in output


def test_core_guides_use_stable_targets_and_versioned_inline_state() -> None:
    storage = (HELP_DIR / "storage.js").read_text(encoding="utf-8")
    content = (HELP_DIR / "content.js").read_text(encoding="utf-8")
    templates = "\n".join(
        [
            (REPO / "src/web/templates/index.html").read_text(encoding="utf-8"),
            (REPO / "src/web/templates/pages/pipelines.html").read_text(encoding="utf-8"),
            (REPO / "src/web/templates/pages/dag.html").read_text(encoding="utf-8"),
        ]
    )

    assert "gamedata-autoflux.help.inline." in storage
    assert "isInlineGuideCollapsed" in storage
    assert "setInlineGuideCollapsed" in storage
    assert "page-pipelines" in content
    assert "page-tasks" in content
    assert "page-dag" in content
    assert "action:task-tour-close" in content
    assert 'data-inline-guide-version="v1"' in templates
    assert 'data-tour-id="task-target-fields"' in templates
    assert 'data-tour-id="dag-inspector"' in templates
