"""Read-only inspection of the future managed plugin environment."""

from __future__ import annotations

import importlib.metadata
import importlib
import json
import os
import platform
import shutil
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from packaging.requirements import InvalidRequirement, Requirement

from src.core.config import get_data_dir
from src.plugin_manager.processes import managed_subprocess_options


MANAGER_DIRNAME = "plugin-manager"
CURRENT_POINTER = "current.json"
_prepared_managed_path: str | None = None


class GenerationBuildError(RuntimeError):
    """A safe error code plus operator-facing message for generation failures."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ValidatedPluginSpec:
    distribution: str
    name: str
    version: str
    collectors: tuple[str, ...]
    capabilities: tuple[dict[str, str], ...] = ()


@dataclass(frozen=True)
class GenerationResult:
    generation_id: str
    generation_dir: Path
    site_packages: Path
    previous_generation_id: str | None
    previous_pointer: dict[str, Any] | None
    plugin_specs: tuple[ValidatedPluginSpec, ...] = ()


ProgressCallback = Callable[[str, int, str], None]


def get_plugin_manager_dir() -> Path:
    """Return the managed environment root without creating it."""

    configured = os.getenv("AUTOFLUX_PLUGIN_MANAGER_DIR", "").strip()
    if not configured:
        return get_data_dir() / MANAGER_DIRNAME
    candidate = Path(configured)
    if not candidate.is_absolute():
        candidate = get_data_dir().parent / candidate
    return candidate


def get_managed_site_packages() -> Path | None:
    """Resolve the active managed site-packages path from a safe local pointer.

    M1 does not create this pointer.  Supporting the format now makes source
    classification stable when the generation builder arrives in M2.
    """

    manager_dir = get_plugin_manager_dir().resolve()
    pointer_path = manager_dir / CURRENT_POINTER
    if not pointer_path.is_file():
        return None

    try:
        payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None

    raw_path = payload.get("site_packages") if isinstance(payload, dict) else None
    if not isinstance(raw_path, str) or not raw_path.strip():
        return None

    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = manager_dir / candidate
    candidate = candidate.resolve()
    try:
        candidate.relative_to(manager_dir)
    except ValueError:
        return None
    return candidate


def read_current_pointer(manager_dir: Path | None = None) -> dict[str, Any] | None:
    root = (manager_dir or get_plugin_manager_dir()).resolve()
    pointer_path = root / CURRENT_POINTER
    if not pointer_path.is_file():
        return None
    try:
        payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    return payload if isinstance(payload, dict) else None


def prepare_managed_environment() -> Path | None:
    """Put only the active generation on ``sys.path`` before entry-point discovery."""

    global _prepared_managed_path
    site_packages = get_managed_site_packages()
    selected = str(site_packages) if site_packages and site_packages.is_dir() else None
    if _prepared_managed_path and _prepared_managed_path != selected:
        while _prepared_managed_path in sys.path:
            sys.path.remove(_prepared_managed_path)
    if selected and selected not in sys.path:
        sys.path.insert(0, selected)
    _prepared_managed_path = selected
    importlib.invalidate_caches()
    return site_packages if selected else None


class GenerationBuilder:
    """Build and validate immutable site-packages generations."""

    def __init__(
        self,
        *,
        manager_dir: Path | None = None,
        project_root: Path | None = None,
        python_executable: str | None = None,
        keep_generations: int = 2,
    ) -> None:
        self._manager_dir = manager_dir
        self.project_root = (project_root or Path(__file__).resolve().parents[2]).resolve()
        self.python_executable = python_executable or sys.executable
        self.keep_generations = max(1, keep_generations)

    @property
    def manager_dir(self) -> Path:
        return (self._manager_dir or get_plugin_manager_dir()).resolve()

    def build(
        self,
        wheel_paths: Iterable[Path],
        *,
        progress: ProgressCallback | None = None,
        allow_empty: bool = False,
    ) -> GenerationResult:
        from src.plugin_manager.package_reader import (
            normalize_distribution,
            read_wheel_metadata,
        )

        report = progress or (lambda _stage, _progress, _message: None)
        wheels = [Path(path).resolve() for path in wheel_paths]
        if not wheels and not allow_empty:
            raise GenerationBuildError("PLUGIN_PACKAGE_MISSING", "No plugin wheels were provided.")
        metadata = [
            read_wheel_metadata(path, require_plugin_entry_point=False) for path in wheels
        ]
        if metadata and not any(item.entry_points for item in metadata):
            raise GenerationBuildError(
                "PLUGIN_ENTRY_POINT_MISSING",
                "The generation does not contain an Autoflux plugin entry point.",
            )
        self._validate_wheel_conflicts(metadata)

        manager_dir = self.manager_dir
        generations_dir = manager_dir / "generations"
        generations_dir.mkdir(parents=True, exist_ok=True)
        generation_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
        building_dir = generations_dir / f".building-{generation_id}"
        final_dir = generations_dir / generation_id
        site_packages = building_dir / "site-packages"
        site_packages.mkdir(parents=True, exist_ok=False)

        try:
            report("resolving_dependencies", 35, "Resolving plugin dependencies")
            requirements = self._external_requirements(metadata)
            if requirements:
                self._run_pip(
                    [
                        "install",
                        "--target",
                        str(site_packages),
                        *requirements,
                    ],
                    error_code="PLUGIN_DEPENDENCY_INSTALL_FAILED",
                )

            report("installing_wheels", 55, "Installing plugin wheels")
            if wheels:
                self._run_pip(
                    [
                        "install",
                        "--no-deps",
                        "--target",
                        str(site_packages),
                        *(str(path) for path in wheels),
                    ],
                    error_code="PLUGIN_WHEEL_INSTALL_FAILED",
                )

            report("verifying_generation", 75, "Validating plugin imports in a child process")
            expected_distributions = {
                normalize_distribution(item.distribution)
                for item in metadata
                if item.entry_points
            }
            plugin_specs = self._smoke_test(site_packages, expected_distributions)

            building_dir.replace(final_dir)
            final_site_packages = final_dir / "site-packages"
            pointer = read_current_pointer(manager_dir)
            previous_id = str(pointer.get("generation_id") or "") if pointer else ""
            report("generation_ready", 85, "Generation is ready for activation")
            return GenerationResult(
                generation_id=generation_id,
                generation_dir=final_dir,
                site_packages=final_site_packages,
                previous_generation_id=previous_id or None,
                previous_pointer=pointer,
                plugin_specs=plugin_specs,
            )
        except Exception:
            if building_dir.exists() and self._is_within_manager(building_dir):
                shutil.rmtree(building_dir)
            raise

    def activate(self, result: GenerationResult) -> dict[str, Any]:
        pointer_payload = {
            "schema_version": 1,
            "generation_id": result.generation_id,
            "site_packages": str(result.site_packages.relative_to(self.manager_dir)),
            "previous_generation_id": result.previous_generation_id,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        self._write_pointer(pointer_payload)
        return pointer_payload

    def restore_pointer(self, pointer: dict[str, Any] | None) -> None:
        pointer_path = self.manager_dir / CURRENT_POINTER
        if pointer is None:
            if pointer_path.exists() and self._is_within_manager(pointer_path):
                pointer_path.unlink()
            return
        self._write_pointer(pointer)

    def activate_existing(
        self,
        *,
        generation_id: str,
        site_packages: str,
        previous_generation_id: str | None,
    ) -> dict[str, Any] | None:
        """Atomically point at an already validated retained generation."""

        expected = (
            self.manager_dir / "generations" / generation_id / "site-packages"
        ).resolve()
        candidate = (self.manager_dir / site_packages).resolve()
        if candidate != expected or not candidate.is_dir() or not self._is_within_manager(candidate):
            raise GenerationBuildError(
                "PLUGIN_ROLLBACK_UNAVAILABLE",
                "The retained rollback generation is missing or invalid.",
            )
        previous_pointer = read_current_pointer(self.manager_dir)
        self._write_pointer(
            {
                "schema_version": 1,
                "generation_id": generation_id,
                "site_packages": str(candidate.relative_to(self.manager_dir)),
                "previous_generation_id": previous_generation_id,
                "updated_at": datetime.now(UTC).isoformat(),
            }
        )
        return previous_pointer

    def cleanup_old_generations(self, active_generation_id: str) -> list[str]:
        generations_dir = self.manager_dir / "generations"
        if not generations_dir.is_dir():
            return []
        candidates = sorted(
            (
                path
                for path in generations_dir.iterdir()
                if path.is_dir() and not path.name.startswith(".building-")
            ),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        keep_names = {active_generation_id}
        for path in candidates:
            if len(keep_names) >= self.keep_generations:
                break
            keep_names.add(path.name)
        removed: list[str] = []
        for path in candidates:
            if path.name in keep_names:
                continue
            if self._is_within_manager(path):
                shutil.rmtree(path)
                removed.append(path.name)
        return removed

    def _write_pointer(self, payload: dict[str, Any]) -> None:
        self.manager_dir.mkdir(parents=True, exist_ok=True)
        pointer_path = self.manager_dir / CURRENT_POINTER
        temporary = self.manager_dir / f".{CURRENT_POINTER}.{uuid.uuid4().hex}.tmp"
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary, pointer_path)

    def _external_requirements(self, wheel_metadata: Iterable[Any]) -> list[str]:
        from src.plugin_manager.package_reader import normalize_distribution

        items = list(wheel_metadata)
        supplied = {normalize_distribution(item.distribution) for item in items}
        supplied.add("gamedata-autoflux")
        requirements: dict[str, list[Requirement]] = {}
        for metadata in items:
            for raw in metadata.requires_dist:
                try:
                    requirement = Requirement(raw)
                except InvalidRequirement as exc:
                    raise GenerationBuildError(
                        "PLUGIN_REQUIREMENT_INVALID",
                        f"Invalid dependency declared by {metadata.distribution}: {raw}",
                    ) from exc
                if requirement.marker and not requirement.marker.evaluate():
                    continue
                normalized = normalize_distribution(requirement.name)
                if normalized in supplied:
                    continue
                requirements.setdefault(normalized, []).append(requirement)
        unresolved: list[str] = []
        for normalized in sorted(requirements):
            group = requirements[normalized]
            try:
                installed_version = importlib.metadata.version(group[0].name)
            except importlib.metadata.PackageNotFoundError:
                installed_version = None
            if installed_version and all(
                not item.specifier
                or item.specifier.contains(installed_version, prereleases=True)
                for item in group
            ):
                continue
            unresolved.extend(sorted({str(item) for item in group}))
        return unresolved

    @staticmethod
    def _validate_wheel_conflicts(wheel_metadata: Iterable[Any]) -> None:
        """Reject wheels that would overwrite another plugin or the host package."""

        from src.plugin_manager.package_reader import normalize_distribution

        distributions: dict[str, Any] = {}
        installed_files: dict[str, tuple[str, str]] = {}
        for metadata in wheel_metadata:
            distribution = str(getattr(metadata, "distribution", "") or "")
            normalized_distribution = normalize_distribution(distribution)
            if normalized_distribution in distributions:
                raise GenerationBuildError(
                    "PLUGIN_WHEEL_CONFLICT",
                    f"Multiple wheels declare distribution '{distribution}'.",
                )
            distributions[normalized_distribution] = metadata

            for raw_path in getattr(metadata, "install_paths", ()):
                path = str(raw_path or "").replace("\\", "/").strip("/")
                if not path:
                    continue
                normalized_path = path.casefold()
                top_level = normalized_path.split("/", 1)[0]
                if (
                    top_level == "src"
                    or normalized_path in {"sitecustomize.py", "usercustomize.py"}
                    or normalized_path.endswith(".pth")
                ):
                    raise GenerationBuildError(
                        "PLUGIN_WHEEL_CONFLICT",
                        f"Plugin wheel '{distribution}' contains reserved install path "
                        f"'{path}'.",
                    )
                previous = installed_files.get(normalized_path)
                if previous is not None:
                    previous_distribution, previous_path = previous
                    raise GenerationBuildError(
                        "PLUGIN_WHEEL_CONFLICT",
                        f"Plugin wheels '{previous_distribution}' and '{distribution}' "
                        f"both install '{previous_path}'.",
                    )
                installed_files[normalized_path] = (distribution, path)

    def _run_pip(self, arguments: list[str], *, error_code: str) -> None:
        command = [
            self.python_executable,
            "-m",
            "pip",
            "--disable-pip-version-check",
            "--no-input",
            *arguments,
        ]
        process = subprocess.run(
            command,
            cwd=self.project_root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=600,
            **managed_subprocess_options(),
        )
        if process.returncode != 0:
            message = self._last_output_lines(process.stdout, process.stderr)
            raise GenerationBuildError(error_code, message or "pip failed without output")

    def _smoke_test(
        self,
        site_packages: Path,
        expected_distributions: set[str],
    ) -> tuple[ValidatedPluginSpec, ...]:
        script = r"""
import importlib
import importlib.metadata
import json
import sys

from src.core.plugin_system import PluginManager

site_packages, project_root, expected_json = sys.argv[1:4]
sys.path[:] = [site_packages, project_root, *[item for item in sys.path if item not in {site_packages, project_root}]]
expected = set(json.loads(expected_json))
loaded = {}
manager = PluginManager()
for distribution in importlib.metadata.distributions(path=[site_packages]):
    name = (distribution.metadata.get("Name") or "").lower().replace("_", "-").replace(".", "-")
    for entry_point in distribution.entry_points:
        if entry_point.group != "gamedata_autoflux.plugins":
            continue
        status = manager.load_candidate(
            f"wheel:{name}:{entry_point.name}",
            entry_point,
        )
        if status.state != "active":
            raise RuntimeError(
                f"invalid PluginSpec from {entry_point.name}: {status.error}"
            )
        details = {
            "distribution": name,
            "name": str(status.name),
            "version": str(status.version),
            "collectors": [str(item) for item in status.collectors],
            "capabilities": [item.to_dict() for item in status.capabilities],
        }
        previous = loaded.get(name)
        if previous is not None and previous != details:
            raise RuntimeError(f"conflicting PluginSpec values for {name}")
        loaded[name] = details
missing = sorted(expected - set(loaded))
if missing:
    raise RuntimeError("missing plugin entry points: " + ", ".join(missing))
print(json.dumps({"plugins": [loaded[name] for name in sorted(loaded)]}))
"""
        safe_environment = {
            key: value
            for key, value in os.environ.items()
            if key.upper()
            in {
                "PATH",
                "PATHEXT",
                "SYSTEMROOT",
                "WINDIR",
                "COMSPEC",
                "TEMP",
                "TMP",
                "TMPDIR",
                "HOME",
                "USERPROFILE",
                "LOCALAPPDATA",
                "APPDATA",
                "PROGRAMDATA",
                "SSL_CERT_FILE",
                "SSL_CERT_DIR",
            }
        }
        safe_environment["PYTHONPATH"] = str(self.project_root)
        safe_environment["AUTOFLUX_PLUGIN_MODULES"] = ""
        process = subprocess.run(
            [
                self.python_executable,
                "-c",
                script,
                str(site_packages),
                str(self.project_root),
                json.dumps(sorted(expected_distributions)),
            ],
            cwd=self.project_root,
            env=safe_environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=120,
            **managed_subprocess_options(),
        )
        if process.returncode != 0:
            message = self._last_output_lines(process.stdout, process.stderr)
            raise GenerationBuildError(
                "PLUGIN_IMPORT_VALIDATION_FAILED",
                message or "plugin import validation failed without output",
            )
        try:
            output = json.loads(process.stdout.strip().splitlines()[-1])
            plugins = output["plugins"]
            validated = tuple(
                ValidatedPluginSpec(
                    distribution=str(item["distribution"]),
                    name=str(item["name"]),
                    version=str(item["version"]),
                    collectors=tuple(str(value) for value in item["collectors"]),
                    capabilities=tuple(
                        {
                            "kind": str(value.get("kind") or ""),
                            "name": str(value.get("name") or ""),
                            "target": str(value.get("target") or ""),
                        }
                        for value in item.get("capabilities", [])
                    ),
                )
                for item in plugins
            )
        except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise GenerationBuildError(
                "PLUGIN_IMPORT_VALIDATION_FAILED",
                "plugin import validation returned malformed metadata",
            ) from exc
        if {item.distribution for item in validated} != expected_distributions:
            raise GenerationBuildError(
                "PLUGIN_IMPORT_VALIDATION_FAILED",
                "plugin import validation returned an unexpected distribution set",
            )
        return validated

    def _is_within_manager(self, path: Path) -> bool:
        try:
            path.resolve().relative_to(self.manager_dir)
        except ValueError:
            return False
        return True

    @staticmethod
    def _last_output_lines(*outputs: str, limit: int = 20) -> str:
        lines = [
            line.strip()
            for output in outputs
            for line in (output or "").splitlines()
            if line.strip()
        ]
        return "\n".join(lines[-limit:])


def _core_version() -> str:
    try:
        return importlib.metadata.version("gamedata-autoflux")
    except importlib.metadata.PackageNotFoundError:
        return "0.1.0"


def _nearest_existing_parent(path: Path) -> Path:
    candidate = path
    while not candidate.exists() and candidate.parent != candidate:
        candidate = candidate.parent
    return candidate


def get_plugin_manager_access() -> dict[str, Any]:
    """Resolve whether this process may mutate the managed environment."""

    manager_dir = get_plugin_manager_dir().resolve()
    writable_parent = _nearest_existing_parent(manager_dir)
    requested_mode = os.getenv("AUTOFLUX_PLUGIN_MANAGER_MODE", "auto").strip().lower()
    worker_variables = ("WEB_CONCURRENCY", "UVICORN_WORKERS", "GUNICORN_WORKERS")
    multi_process = any(
        os.getenv(name, "").strip().isdigit() and int(os.environ[name]) > 1
        for name in worker_variables
    )
    writable = writable_parent.is_dir() and os.access(writable_parent, os.W_OK)
    from src.plugin_manager.locking import plugin_manager_lock_available

    lock_available = plugin_manager_lock_available(manager_dir)
    if requested_mode not in {"auto", "mutable", "read_only"}:
        mutable = False
        reason = "AUTOFLUX_PLUGIN_MANAGER_MODE must be auto, mutable, or read_only."
    elif requested_mode == "read_only":
        mutable = False
        reason = "Plugin mutations are disabled by AUTOFLUX_PLUGIN_MANAGER_MODE."
    elif multi_process:
        mutable = False
        reason = "Plugin mutations require a single service worker."
    elif not lock_available:
        mutable = False
        reason = "Plugin mutations are owned by another service process."
    else:
        mutable = writable
        reason = "" if writable else "The managed plugin directory is not writable."
    return {
        "mutable": mutable,
        "writable": writable,
        "reason": reason,
        "configured_mode": requested_mode,
        "multi_process": multi_process,
        "lock_available": lock_available,
    }


def get_environment_inventory() -> dict[str, Any]:
    """Return deployment facts needed by the plugin center."""

    from src.plugin_manager.restart import restart_controller
    from src.plugin_manager.store import plugin_state_store
    from src.plugin_manager.compatibility import (
        current_core_version,
        detect_runtime_capabilities,
    )

    data_dir = get_data_dir().resolve()
    manager_dir = get_plugin_manager_dir().resolve()
    site_packages = get_managed_site_packages()
    current_pointer = manager_dir / CURRENT_POINTER
    access = get_plugin_manager_access()
    mutable = bool(access["mutable"])
    writable = bool(access["writable"])
    pointer = read_current_pointer(manager_dir)
    active_generation = plugin_state_store.get_active_generation()
    previous_generation_id = (
        str(active_generation.get("previous_generation_id") or "")
        if active_generation
        else ""
    )
    previous_generation = (
        plugin_state_store.get_generation(previous_generation_id)
        if previous_generation_id
        else None
    )

    return {
        "mode": "mutable" if mutable else "read_only",
        "mutable": mutable,
        "reason": access["reason"],
        "data_dir": str(data_dir),
        "runtime_dir": str(manager_dir),
        "runtime_dir_exists": manager_dir.exists(),
        "runtime_dir_writable": writable,
        "current_pointer": str(current_pointer),
        "current_generation": (
            {
                "id": pointer.get("generation_id") if pointer else None,
                "site_packages": str(site_packages),
            }
            if site_packages is not None
            else None
        ),
        "python_version": platform.python_version(),
        "core_version": current_core_version(),
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "entry_point_group": "gamedata_autoflux.plugins",
        "restart_required": plugin_state_store.generation_restart_required(),
        "restart_controller": restart_controller.payload(),
        "rollback": {
            "available": previous_generation is not None,
            "target_generation_id": previous_generation_id or None,
        },
        "runtime_capabilities": list(detect_runtime_capabilities()),
        "configured_mode": access["configured_mode"],
        "multi_process": access["multi_process"],
        "process": {
            "executable": sys.executable,
        },
    }
