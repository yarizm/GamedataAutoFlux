"""Build the core wheel in isolation and verify required runtime resources."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_MEMBERS = {
    "src/plugin_manager/catalog.json",
    "src/web/templates/index.html",
    "src/web/templates/pages/plugins.html",
    "src/web/static/favicon.svg",
    "src/web/static/style.css",
    "src/web/static/core/api.js",
    "src/web/static/dist/.vite/manifest.json",
}


def build_and_check(root: Path = ROOT) -> Path:
    manifest = root / "src" / "web" / "static" / "dist" / ".vite" / "manifest.json"
    if not manifest.is_file():
        raise RuntimeError("frontend production assets must be built before the core wheel")

    temporary = tempfile.TemporaryDirectory(prefix="autoflux-core-wheel-")
    temp_root = Path(temporary.name)
    source = temp_root / "source"
    source.mkdir()
    shutil.copy2(root / "pyproject.toml", source / "pyproject.toml")
    shutil.copytree(
        root / "src",
        source / "src",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.egg-info"),
    )
    wheelhouse = temp_root / "wheelhouse"
    wheelhouse.mkdir()
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--disable-pip-version-check",
            "--no-input",
            "--no-deps",
            "--no-build-isolation",
            "--wheel-dir",
            str(wheelhouse),
            str(source),
        ],
        cwd=temp_root,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=180,
    )
    if process.returncode != 0:
        temporary.cleanup()
        raise RuntimeError(process.stderr.strip() or process.stdout.strip() or "wheel build failed")
    wheels = list(wheelhouse.glob("gamedata_autoflux-*.whl"))
    if len(wheels) != 1:
        temporary.cleanup()
        raise RuntimeError("core build did not produce exactly one wheel")
    with zipfile.ZipFile(wheels[0]) as archive:
        names = set(archive.namelist())
        missing = sorted(REQUIRED_MEMBERS - names)
        if missing:
            temporary.cleanup()
            raise RuntimeError("core wheel is missing runtime resources: " + ", ".join(missing))
        if not any(name.startswith("src/web/static/dist/assets/") for name in names):
            temporary.cleanup()
            raise RuntimeError("core wheel contains no frontend production assets")

    installed = temp_root / "installed"
    install = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-input",
            "--no-deps",
            "--target",
            str(installed),
            str(wheels[0]),
        ],
        cwd=temp_root,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=180,
    )
    if install.returncode != 0:
        temporary.cleanup()
        raise RuntimeError(install.stderr.strip() or install.stdout.strip())
    runtime_script = r"""
import os
import sys
from pathlib import Path

installed, deployment_root = sys.argv[1:3]
os.chdir(deployment_root)
os.environ["AUTOFLUX_PROJECT_ROOT"] = deployment_root
os.environ["AUTOFLUX_PLUGIN_MODULES"] = ""
sys.path.insert(0, installed)
from src.plugin_manager.catalog import plugin_catalog
from src.web.app import _WEB_DIR

assert Path(__import__("src").__file__).resolve().is_relative_to(Path(installed).resolve())
assert len(plugin_catalog.list_plugins()) == 8
assert (_WEB_DIR / "templates" / "pages" / "plugins.html").is_file()
assert (_WEB_DIR / "static" / "dist" / ".vite" / "manifest.json").is_file()
"""
    deployment_root = temp_root / "deployment"
    deployment_root.mkdir()
    runtime = subprocess.run(
        [sys.executable, "-c", runtime_script, str(installed), str(deployment_root)],
        cwd=temp_root,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )
    if runtime.returncode != 0:
        temporary.cleanup()
        raise RuntimeError(runtime.stderr.strip() or runtime.stdout.strip())

    output = root / "tmp" / "core-package-check" / wheels[0].name
    output.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(wheels[0], output)
    temporary.cleanup()
    return output


def main() -> int:
    output = build_and_check()
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
