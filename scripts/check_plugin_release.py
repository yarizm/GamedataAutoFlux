"""Validate and optionally build the first-party plugin release set."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "src" / "plugin_manager" / "catalog.json"
ENTRY_POINT_GROUP = "gamedata_autoflux.plugins"
REPOSITORY_URL = "https://github.com/yarizm/GamedataAutoFlux"
_NORMALIZE_PATTERN = re.compile(r"[-_.]+")


class ReleaseValidationError(RuntimeError):
    """Raised when release metadata is internally inconsistent."""


def _normalize_distribution(value: str) -> str:
    return _NORMALIZE_PATTERN.sub("-", value).lower().strip("-")


def _inspect_plugin_specs(root: Path, requests: list[dict[str, str]]) -> dict[str, dict]:
    plugin_src_paths = sorted(
        str(path.resolve())
        for path in (root / "plugins").glob("*/src")
        if path.is_dir()
    )
    script = r"""
import importlib
import json
import sys

from src.core.plugin_system import PluginManager

root, src_paths_json, requests_json = sys.argv[1:4]
sys.path[:0] = [root, *json.loads(src_paths_json)]
result = {}
manager = PluginManager()
class TargetLoader:
    def __init__(self, module_name, attribute):
        self.module_name = module_name
        self.attribute = attribute

    def load(self):
        return getattr(importlib.import_module(self.module_name), self.attribute)

for request in json.loads(requests_json):
    module_name, attribute = request["target"].split(":", 1)
    status = manager.load_candidate(
        f"release:{request['plugin_id']}",
        TargetLoader(module_name, attribute),
    )
    if status.state != "active":
        raise RuntimeError(
            f"{request['plugin_id']} failed plugin contract validation: {status.error}"
        )
    result[request["plugin_id"]] = {
        "name": str(status.name),
        "version": str(status.version),
        "collectors": [str(item) for item in status.collectors],
        "capabilities": [item.to_dict() for item in status.capabilities],
    }
print(json.dumps(result))
"""
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(root),
            json.dumps(plugin_src_paths),
            json.dumps(requests),
        ],
        cwd=root,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )
    if process.returncode != 0:
        message = process.stderr.strip() or process.stdout.strip()
        raise ReleaseValidationError(
            "PluginSpec inspection failed: " + (message or "no subprocess output")
        )
    try:
        payload = json.loads(process.stdout.strip().splitlines()[-1])
    except (IndexError, TypeError, json.JSONDecodeError) as exc:
        raise ReleaseValidationError("PluginSpec inspection returned malformed output") from exc
    if not isinstance(payload, dict):
        raise ReleaseValidationError("PluginSpec inspection returned malformed output")
    return payload


def _load_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_release_set(root: Path = ROOT) -> dict[str, Any]:
    """Cross-check catalog entries, package metadata, and source layout."""

    catalog_path = root / CATALOG_PATH.relative_to(ROOT)
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    if catalog.get("schema_version") != 1:
        raise ReleaseValidationError("catalog schema_version must be 1")
    entries = catalog.get("plugins")
    if not isinstance(entries, list) or not entries:
        raise ReleaseValidationError("catalog must contain at least one plugin")

    errors: list[str] = []
    spec_requests: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    seen_distributions: set[str] = set()
    package_dirs = {
        str(entry.get("package_dir", "")) for entry in entries if isinstance(entry, dict)
    }
    for entry in entries:
        plugin_id = str(entry.get("id", ""))
        distribution = str(entry.get("distribution", ""))
        package_dir = str(entry.get("package_dir", ""))
        if plugin_id in seen_ids:
            errors.append(f"duplicate catalog id: {plugin_id}")
        if distribution in seen_distributions:
            errors.append(f"duplicate catalog distribution: {distribution}")
        seen_ids.add(plugin_id)
        seen_distributions.add(distribution)
        package_path = root / "plugins" / package_dir
        pyproject_path = package_path / "pyproject.toml"
        if not pyproject_path.is_file():
            errors.append(f"{plugin_id}: missing {pyproject_path.relative_to(root)}")
            continue
        payload = _load_toml(pyproject_path)
        project = payload.get("project", {})
        if project.get("name") != distribution:
            errors.append(f"{plugin_id}: distribution does not match pyproject")
        if project.get("version") != entry.get("version"):
            errors.append(f"{plugin_id}: version does not match pyproject")
        if entry.get("plugin_api") != "1":
            errors.append(f"{plugin_id}: plugin_api must be explicit and equal to 1")
        if not entry.get("core_specifier") or not entry.get("python_specifier"):
            errors.append(f"{plugin_id}: compatibility ranges must be explicit")
        if not isinstance(entry.get("supported_systems"), list):
            errors.append(f"{plugin_id}: supported_systems must be a list")
        if entry.get("license") != "MIT" or not entry.get("homepage"):
            errors.append(f"{plugin_id}: catalog license and homepage are required")
        if not entry.get("icon"):
            errors.append(f"{plugin_id}: catalog icon is required")
        if project.get("license") != "MIT":
            errors.append(f"{plugin_id}: license must be MIT")
        if not project.get("authors"):
            errors.append(f"{plugin_id}: authors are required")
        urls = project.get("urls", {})
        if urls.get("Repository") != REPOSITORY_URL:
            errors.append(f"{plugin_id}: Repository URL is missing or incorrect")
        entry_points = project.get("entry-points", {}).get(ENTRY_POINT_GROUP, {})
        expected_names = {plugin_id.removeprefix("official.")}
        if set(entry_points) != expected_names:
            errors.append(f"{plugin_id}: plugin entry point does not match catalog id")
        elif entry_points:
            spec_requests.append(
                {
                    "plugin_id": plugin_id,
                    "target": str(next(iter(entry_points.values()))),
                }
            )
        if not list((package_path / "src").glob("autoflux_plugin_*")):
            errors.append(f"{plugin_id}: no plugin source package was found")
        for dependency in entry.get("internal_dependencies", []):
            if dependency not in package_dirs and not (root / "plugins" / dependency).is_dir():
                errors.append(f"{plugin_id}: missing internal dependency {dependency}")

    internal_dirs = sorted(
        {
            str(dependency)
            for entry in entries
            for dependency in entry.get("internal_dependencies", [])
        }
    )
    for package_dir in internal_dirs:
        project = _load_toml(root / "plugins" / package_dir / "pyproject.toml").get(
            "project",
            {},
        )
        if project.get("license") != "MIT" or not project.get("authors"):
            errors.append(f"{package_dir}: internal package release metadata is incomplete")
        if project.get("urls", {}).get("Repository") != REPOSITORY_URL:
            errors.append(f"{package_dir}: Repository URL is missing or incorrect")

    try:
        inspected_specs = _inspect_plugin_specs(root, spec_requests)
    except ReleaseValidationError as exc:
        errors.append(str(exc))
        inspected_specs = {}
    for entry in entries:
        plugin_id = str(entry.get("id", ""))
        spec = inspected_specs.get(plugin_id)
        if not isinstance(spec, dict):
            if any(item["plugin_id"] == plugin_id for item in spec_requests):
                errors.append(f"{plugin_id}: PluginSpec inspection result is missing")
            continue
        if _normalize_distribution(str(spec.get("name", ""))) != _normalize_distribution(
            str(entry.get("distribution", ""))
        ):
            errors.append(f"{plugin_id}: PluginSpec name does not match distribution")
        if str(spec.get("version", "")) != str(entry.get("version", "")):
            errors.append(f"{plugin_id}: PluginSpec version does not match catalog")
        if tuple(spec.get("collectors") or ()) != tuple(entry.get("collectors") or ()):
            errors.append(f"{plugin_id}: PluginSpec collectors do not match catalog")

    if errors:
        raise ReleaseValidationError("\n".join(errors))
    return {
        "catalog_plugins": len(entries),
        "package_dirs": sorted(package_dirs | set(internal_dirs)),
    }


def build_release_set(output_dir: Path, root: Path = ROOT) -> dict[str, Any]:
    """Build every validated first-party wheel and write a SHA-256 manifest."""

    report = validate_release_set(root)
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    built: list[dict[str, Any]] = []
    for package_dir in report["package_dirs"]:
        with tempfile.TemporaryDirectory(prefix=f"autoflux-release-{package_dir}-") as temp:
            build_source = Path(temp) / package_dir
            shutil.copytree(
                root / "plugins" / package_dir,
                build_source,
                ignore=shutil.ignore_patterns(
                    "build",
                    "dist",
                    "*.egg-info",
                    "__pycache__",
                    "*.pyc",
                ),
            )
            command = [
                sys.executable,
                "-m",
                "pip",
                "wheel",
                "--disable-pip-version-check",
                "--no-input",
                "--no-deps",
                "--no-build-isolation",
                "--wheel-dir",
                str(output_dir),
                str(build_source),
            ]
            process = subprocess.run(
                command,
                cwd=root,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=300,
            )
        if process.returncode != 0:
            output = "\n".join(
                line for line in (process.stdout + process.stderr).splitlines() if line.strip()
            )
            raise ReleaseValidationError(
                f"wheel build failed for {package_dir}:\n{output[-4000:]}"
            )
        project = _load_toml(root / "plugins" / package_dir / "pyproject.toml")["project"]
        prefix = str(project["name"]).replace("-", "_") + "-" + str(project["version"])
        wheels = sorted(output_dir.glob(f"{prefix}-*.whl"))
        if len(wheels) != 1:
            raise ReleaseValidationError(
                f"expected one built wheel for {package_dir}, found {len(wheels)}"
            )
        wheel = wheels[0]
        built.append(
            {
                "package": project["name"],
                "version": project["version"],
                "filename": wheel.name,
                "sha256": _sha256(wheel),
                "size": wheel.stat().st_size,
            }
        )
    manifest = {
        "schema_version": 1,
        "core_compatibility": ">=0.1,<0.3",
        "artifacts": built,
    }
    (output_dir / "plugin-release-manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--build-dir",
        type=Path,
        help="Optionally build all wheels and write a SHA-256 manifest here.",
    )
    arguments = parser.parse_args()
    try:
        report = (
            build_release_set(arguments.build_dir)
            if arguments.build_dir
            else validate_release_set()
        )
    except ReleaseValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
