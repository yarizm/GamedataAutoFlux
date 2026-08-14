"""First-party release metadata must agree with the actual PluginSpec."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from scripts.check_plugin_release import ReleaseValidationError, validate_release_set


REPO = Path(__file__).resolve().parents[1]


def test_release_check_rejects_catalog_collector_drift(tmp_path: Path) -> None:
    shutil.copytree(
        REPO / "plugins",
        tmp_path / "plugins",
        ignore=shutil.ignore_patterns(
            "build",
            "dist",
            "*.egg-info",
            "__pycache__",
            "*.pyc",
        ),
    )
    catalog_path = tmp_path / "src" / "plugin_manager" / "catalog.json"
    catalog_path.parent.mkdir(parents=True)
    payload = json.loads(
        (REPO / "src" / "plugin_manager" / "catalog.json").read_text(
            encoding="utf-8"
        )
    )
    payload["plugins"][0]["collectors"] = ["catalog-drift"]
    catalog_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ReleaseValidationError, match="collectors do not match"):
        validate_release_set(tmp_path)
