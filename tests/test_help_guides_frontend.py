"""Structural assertions for the in-app help guides.

The node-level `*.selftest.mjs` runs used to live here too; they are now
covered generically by `tests/test_frontend_selftests.py`, which discovers
every selftest by glob. What remains below is the coverage that file cannot
provide: assertions tying the guide modules to the tour targets and inline
guide version markers embedded in the HTML templates.
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
HELP_DIR = REPO / "src" / "web" / "src" / "core" / "help"


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
