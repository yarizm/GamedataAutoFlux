"""Trusted catalog and local-wheel installation into managed generations."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

from src.plugin_manager.catalog import PluginCatalog, plugin_catalog
from src.plugin_manager.compatibility import evaluate_catalog_plugin, evaluate_wheel
from src.plugin_manager.environment import (
    GenerationBuildError,
    GenerationBuilder,
    ProgressCallback,
    ValidatedPluginSpec,
)
from src.plugin_manager.models import (
    CatalogPlugin,
    ManagedArtifactRecord,
    ManagedPluginRecord,
)
from src.plugin_manager.package_reader import (
    WheelMetadata,
    normalize_distribution,
    read_wheel_metadata,
)
from src.plugin_manager.processes import managed_subprocess_options
from src.plugin_manager.store import PluginStateStore, plugin_state_store


class PluginInstallError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class PluginInstaller:
    def __init__(
        self,
        *,
        store: PluginStateStore = plugin_state_store,
        catalog: PluginCatalog = plugin_catalog,
        manager_dir: Path | None = None,
        project_root: Path | None = None,
        python_executable: str | None = None,
    ) -> None:
        self.store = store
        self.catalog = catalog
        self._manager_dir = manager_dir
        self.project_root = (project_root or Path(__file__).resolve().parents[2]).resolve()
        self.python_executable = python_executable or sys.executable

    @property
    def manager_dir(self) -> Path:
        return (self._manager_dir or self.store.manager_dir).resolve()

    def install_catalog(
        self,
        plugin_id: str,
        *,
        requested_version: str | None = None,
        progress: ProgressCallback | None = None,
        operation_id: str | None = None,
    ) -> dict:
        report = progress or (lambda _stage, _progress, _message: None)
        plugin = self.catalog.get(plugin_id)
        if plugin is None:
            raise PluginInstallError("PLUGIN_NOT_FOUND", f"Unknown catalog plugin: {plugin_id}")
        if requested_version and requested_version != plugin.version:
            raise PluginInstallError(
                "PLUGIN_VERSION_NOT_FOUND",
                f"Catalog does not provide {plugin_id} version {requested_version}.",
            )
        self._require_compatible(evaluate_catalog_plugin(plugin))
        package_path = self.catalog.package_path(plugin)
        if not (package_path / "pyproject.toml").is_file():
            raise PluginInstallError(
                "PLUGIN_PACKAGE_UNAVAILABLE",
                f"Bundled package source is unavailable for {plugin_id}.",
            )

        existing = self.store.list_managed_plugins()
        current = next((item for item in existing if item.plugin_id == plugin_id), None)
        if current and current.version == plugin.version:
            raise PluginInstallError(
                "PLUGIN_ALREADY_INSTALLED",
                f"{plugin.display_name} {plugin.version} is already installed.",
            )

        report("building_wheel", 10, f"Building trusted wheel for {plugin.display_name}")
        artifact_map: dict[str, list[tuple[Path, WheelMetadata]]] = {}
        artifact_map[plugin_id] = self._build_catalog_artifacts(plugin)
        main_artifact, main_metadata = self._main_artifact(plugin, artifact_map[plugin_id])
        now = datetime.now(UTC).isoformat()
        candidate = ManagedPluginRecord(
            plugin_id=plugin.plugin_id,
            distribution=main_metadata.distribution,
            version=main_metadata.version,
            source_type="catalog",
            source_ref=f"official:{plugin.plugin_id}@{main_metadata.version}",
            artifact_path=self._relative_artifact_path(main_artifact),
            artifact_sha256=main_metadata.sha256,
            trust=plugin.trust,
            display_name=plugin.display_name,
            description=plugin.description,
            collectors=plugin.collectors,
            artifacts=self._artifact_records(artifact_map[plugin_id]),
            installed_at=current.installed_at if current else now,
            desired_state=current.desired_state if current else "enabled",
            restart_required=True,
        )
        desired = [item for item in existing if item.plugin_id != plugin_id] + [candidate]
        return self._build_and_commit(
            desired,
            artifact_map=artifact_map,
            report=report,
            operation_id=operation_id,
        )

    def prepare_uploaded_wheel(self, source_path: Path) -> tuple[Path, WheelMetadata]:
        metadata = read_wheel_metadata(source_path)
        self._require_compatible(evaluate_wheel(metadata, filename=source_path.name))
        cache_dir = self.manager_dir / "cache" / "wheels"
        artifact_dir = cache_dir / metadata.sha256[:16]
        artifact_dir.mkdir(parents=True, exist_ok=True)
        destination = artifact_dir / source_path.name
        if not destination.exists():
            shutil.copy2(source_path, destination)
        cached = read_wheel_metadata(destination)
        if cached.sha256 != metadata.sha256:
            raise PluginInstallError(
                "PLUGIN_ARTIFACT_HASH_MISMATCH",
                "The cached wheel does not match the uploaded artifact.",
            )
        return destination, cached

    def install_uploaded(
        self,
        artifact_path: Path,
        *,
        progress: ProgressCallback | None = None,
        operation_id: str | None = None,
    ) -> dict:
        report = progress or (lambda _stage, _progress, _message: None)
        artifact = self._resolve_artifact(artifact_path)
        metadata = read_wheel_metadata(artifact)
        self._require_compatible(evaluate_wheel(metadata, filename=artifact.name))
        plugin_id = f"local.{normalize_distribution(metadata.distribution)}"
        existing = self.store.list_managed_plugins()
        current = next((item for item in existing if item.plugin_id == plugin_id), None)
        if current and current.version == metadata.version:
            raise PluginInstallError(
                "PLUGIN_ALREADY_INSTALLED",
                f"{metadata.distribution} {metadata.version} is already installed.",
            )
        candidate = ManagedPluginRecord(
            plugin_id=plugin_id,
            distribution=metadata.distribution,
            version=metadata.version,
            source_type="upload",
            source_ref=f"local-wheel:{metadata.sha256}",
            artifact_path=self._relative_artifact_path(artifact),
            artifact_sha256=metadata.sha256,
            trust="local",
            display_name=metadata.distribution,
            description=metadata.summary,
            artifacts=self._artifact_records([(artifact, metadata)]),
            installed_at=(
                current.installed_at if current else datetime.now(UTC).isoformat()
            ),
            desired_state=current.desired_state if current else "enabled",
            restart_required=True,
        )
        desired = [item for item in existing if item.plugin_id != plugin_id] + [candidate]
        return self._build_and_commit(
            desired,
            artifact_map={plugin_id: [(artifact, metadata)]},
            report=report,
            operation_id=operation_id,
        )

    def upgrade_catalog(
        self,
        plugin_id: str,
        *,
        requested_version: str | None = None,
        progress: ProgressCallback | None = None,
        operation_id: str | None = None,
    ) -> dict:
        current = self.store.get_managed_plugin(plugin_id)
        if current is None:
            raise PluginInstallError(
                "PLUGIN_NOT_INSTALLED",
                f"Managed plugin is not installed: {plugin_id}",
            )
        if current.source_type != "catalog":
            raise PluginInstallError(
                "PLUGIN_UPGRADE_UNAVAILABLE",
                "Only trusted catalog plugins can use catalog upgrade.",
            )
        available = self.catalog.get(plugin_id)
        if available is None:
            raise PluginInstallError(
                "PLUGIN_CATALOG_ENTRY_MISSING",
                f"Catalog entry disappeared: {plugin_id}",
            )
        target_version = requested_version or available.version
        if current.version == target_version:
            raise PluginInstallError(
                "PLUGIN_ALREADY_CURRENT",
                f"{available.display_name} is already at version {target_version}.",
            )
        return self.install_catalog(
            plugin_id,
            requested_version=target_version,
            progress=progress,
            operation_id=operation_id,
        )

    def uninstall(
        self,
        plugin_id: str,
        *,
        progress: ProgressCallback | None = None,
        operation_id: str | None = None,
    ) -> dict:
        current = self.store.get_managed_plugin(plugin_id)
        if current is None:
            raise PluginInstallError(
                "PLUGIN_NOT_INSTALLED",
                f"Managed plugin is not installed: {plugin_id}",
            )
        if current.desired_state != "disabled":
            raise PluginInstallError(
                "PLUGIN_MUST_BE_DISABLED",
                "Disable the plugin before uninstalling it.",
            )
        if current.restart_required:
            raise PluginInstallError(
                "PLUGIN_RESTART_REQUIRED",
                "Restart the service to finish disabling the plugin before uninstalling it.",
            )
        report = progress or (lambda _stage, _progress, _message: None)
        desired = [
            item
            for item in self.store.list_managed_plugins()
            if item.plugin_id != plugin_id
        ]
        return self._build_and_commit(
            desired,
            artifact_map={},
            report=report,
            operation_id=operation_id,
        )

    def rollback(
        self,
        *,
        progress: ProgressCallback | None = None,
        operation_id: str | None = None,
    ) -> dict:
        report = progress or (lambda _stage, _progress, _message: None)
        active = self.store.get_active_generation()
        target_id = str(active.get("previous_generation_id") or "") if active else ""
        if not active or not target_id:
            raise PluginInstallError(
                "PLUGIN_ROLLBACK_UNAVAILABLE",
                "No retained previous generation is available.",
            )
        target = self.store.get_generation(target_id)
        if target is None:
            raise PluginInstallError(
                "PLUGIN_ROLLBACK_UNAVAILABLE",
                "The previous generation record is unavailable.",
            )
        desired_by_id = {
            item.plugin_id: item.desired_state
            for item in self.store.list_managed_plugins()
        }
        restored = [
            replace(
                item,
                desired_state=desired_by_id.get(item.plugin_id, item.desired_state),
                restart_required=True,
            )
            for item in target["plugins"]
        ]
        builder = GenerationBuilder(
            manager_dir=self.manager_dir,
            project_root=self.project_root,
            python_executable=self.python_executable,
        )
        report("switching_generation", 70, "Switching to the previous generation")
        try:
            previous_pointer = builder.activate_existing(
                generation_id=target_id,
                site_packages=str(target["site_packages"]),
                previous_generation_id=str(active["generation_id"]),
            )
        except GenerationBuildError as exc:
            raise PluginInstallError(exc.code, str(exc)) from exc
        try:
            self.store.activate_existing_generation(
                target_id,
                previous_generation_id=str(active["generation_id"]),
                plugins=restored,
            )
        except Exception:
            builder.restore_pointer(previous_pointer)
            raise
        self.store.record_audit(
            "generation.rolled_back",
            target_id,
            operation_id=operation_id,
            details={"from": active["generation_id"], "plugins": [p.plugin_id for p in restored]},
        )
        report("completed", 100, "Previous generation selected; restart required")
        return {
            "generation_id": target_id,
            "previous_generation_id": active["generation_id"],
            "restart_required": True,
            "plugins": [item.plugin_id for item in restored],
        }

    def _build_and_commit(
        self,
        desired: list[ManagedPluginRecord],
        *,
        artifact_map: dict[str, list[tuple[Path, WheelMetadata]]],
        report: ProgressCallback,
        operation_id: str | None,
        restart_required: bool = True,
    ) -> dict:
        all_artifacts: dict[str, Path] = {}
        locked_desired: list[ManagedPluginRecord] = []
        for record in desired:
            artifacts = artifact_map.get(record.plugin_id)
            if artifacts is None:
                artifacts = self._resolve_locked_artifacts(record)
                artifact_map[record.plugin_id] = artifacts
            if not record.artifacts:
                record = replace(record, artifacts=self._artifact_records(artifacts))
            locked_desired.append(record)
            for artifact_path, metadata in artifacts:
                if (
                    normalize_distribution(metadata.distribution)
                    == normalize_distribution(record.distribution)
                    and record.source_type == "upload"
                    and metadata.sha256 != record.artifact_sha256
                ):
                    raise PluginInstallError(
                        "PLUGIN_ARTIFACT_HASH_MISMATCH",
                        f"Stored artifact for {record.plugin_id} failed integrity validation.",
                    )
                all_artifacts[normalize_distribution(metadata.distribution)] = artifact_path
        desired = locked_desired

        builder = GenerationBuilder(
            manager_dir=self.manager_dir,
            project_root=self.project_root,
            python_executable=self.python_executable,
        )
        try:
            result = builder.build(
                all_artifacts.values(),
                progress=report,
                allow_empty=not all_artifacts,
            )
        except GenerationBuildError as exc:
            raise PluginInstallError(exc.code, str(exc)) from exc

        try:
            desired = self._reconcile_plugin_specs(desired, result.plugin_specs)
        except Exception:
            if result.generation_dir.exists() and self._is_within_manager(
                result.generation_dir
            ):
                shutil.rmtree(result.generation_dir)
            raise

        report("switching_generation", 90, "Activating the validated generation")
        builder.activate(result)
        try:
            self.store.commit_generation(
                generation_id=result.generation_id,
                site_packages=str(result.site_packages.relative_to(self.manager_dir)),
                previous_generation_id=result.previous_generation_id,
                plugins=desired,
            )
        except Exception:
            builder.restore_pointer(result.previous_pointer)
            raise
        self.store.record_audit(
            "generation.activated",
            result.generation_id,
            operation_id=operation_id,
            details={"plugins": [item.plugin_id for item in desired]},
        )
        removed = builder.cleanup_old_generations(result.generation_id)
        message = (
            "Plugin environment is ready; restart required"
            if restart_required
            else "Plugin environment is ready"
        )
        report("completed", 100, message)
        return {
            "generation_id": result.generation_id,
            "previous_generation_id": result.previous_generation_id,
            "restart_required": restart_required,
            "plugins": [item.plugin_id for item in desired],
            "removed_generations": removed,
        }

    def _build_catalog_artifacts(
        self,
        plugin: CatalogPlugin,
    ) -> list[tuple[Path, WheelMetadata]]:
        build_root = self.manager_dir / "cache" / "builds"
        wheel_cache = self.manager_dir / "cache" / "wheels"
        build_root.mkdir(parents=True, exist_ok=True)
        wheel_cache.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix="wheel-build-", dir=build_root))
        source_package_paths = [
            self.catalog.package_path(plugin, dependency)
            for dependency in plugin.internal_dependencies
        ] + [self.catalog.package_path(plugin)]
        for package_path in source_package_paths:
            if not (package_path / "pyproject.toml").is_file():
                raise PluginInstallError(
                    "PLUGIN_PACKAGE_UNAVAILABLE",
                    f"Bundled package source is unavailable: {package_path.name}",
                )
        try:
            sources_dir = temporary / "sources"
            sources_dir.mkdir()
            package_paths: list[Path] = []
            for index, source_path in enumerate(source_package_paths):
                build_source = sources_dir / f"{index:02d}-{source_path.name}"
                shutil.copytree(
                    source_path,
                    build_source,
                    ignore=shutil.ignore_patterns(
                        "build",
                        "dist",
                        "*.egg-info",
                        "__pycache__",
                        "*.pyc",
                    ),
                )
                package_paths.append(build_source)
            command = [
                self.python_executable,
                "-m",
                "pip",
                "wheel",
                "--disable-pip-version-check",
                "--no-input",
                "--no-deps",
                "--no-build-isolation",
                "--wheel-dir",
                str(temporary),
                *(str(path) for path in package_paths),
            ]
            process = subprocess.run(
                command,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=300,
                **managed_subprocess_options(),
            )
            if process.returncode != 0:
                message = GenerationBuilder._last_output_lines(
                    process.stdout,
                    process.stderr,
                )
                if "KeyboardInterrupt" in message:
                    raise PluginInstallError(
                        "PLUGIN_WHEEL_BUILD_INTERRUPTED",
                        "The wheel build was interrupted before plugin code ran. Retry the operation.",
                    )
                raise PluginInstallError(
                    "PLUGIN_WHEEL_BUILD_FAILED",
                    message or "wheel build failed without output",
                )
            built: list[tuple[Path, WheelMetadata]] = []
            for wheel in sorted(temporary.glob("*.whl")):
                metadata = read_wheel_metadata(
                    wheel,
                    require_plugin_entry_point=False,
                )
                artifact_dir = wheel_cache / metadata.sha256[:16]
                artifact_dir.mkdir(parents=True, exist_ok=True)
                destination = artifact_dir / wheel.name
                if not destination.exists():
                    shutil.copy2(wheel, destination)
                cached_metadata = read_wheel_metadata(
                    destination,
                    require_plugin_entry_point=False,
                )
                built.append((destination, cached_metadata))
            if len(built) != len(package_paths):
                raise PluginInstallError(
                    "PLUGIN_WHEEL_BUILD_INCOMPLETE",
                    "Not every bundled package produced a wheel.",
                )
            return built
        finally:
            if temporary.exists() and self._is_within_manager(temporary):
                shutil.rmtree(temporary)

    def _resolve_locked_artifacts(
        self,
        record: ManagedPluginRecord,
    ) -> list[tuple[Path, WheelMetadata]]:
        locked = record.artifacts
        if not locked:
            catalog_plugin = (
                self.catalog.get(record.plugin_id)
                if record.source_type == "catalog"
                else None
            )
            if catalog_plugin is not None and catalog_plugin.internal_dependencies:
                raise PluginInstallError(
                    "PLUGIN_ARTIFACT_LOCK_INCOMPLETE",
                    f"Stored artifact lock for {record.plugin_id} is incomplete; "
                    "upgrade or reinstall the plugin before rebuilding its generation.",
                )
            locked = (
                ManagedArtifactRecord(
                    distribution=record.distribution,
                    version=record.version,
                    path=record.artifact_path,
                    sha256=record.artifact_sha256,
                ),
            )

        artifacts: list[tuple[Path, WheelMetadata]] = []
        for expected in locked:
            is_main = normalize_distribution(
                expected.distribution
            ) == normalize_distribution(record.distribution)
            artifact = self._resolve_artifact(Path(expected.path))
            metadata = read_wheel_metadata(
                artifact,
                require_plugin_entry_point=is_main,
            )
            if (
                metadata.sha256 != expected.sha256
                or normalize_distribution(metadata.distribution)
                != normalize_distribution(expected.distribution)
                or metadata.version != expected.version
            ):
                raise PluginInstallError(
                    "PLUGIN_ARTIFACT_HASH_MISMATCH",
                    f"Stored artifact for {record.plugin_id} failed lock validation.",
                )
            self._require_compatible(evaluate_wheel(metadata, filename=artifact.name))
            artifacts.append((artifact, metadata))

        main = [
            item
            for item in artifacts
            if normalize_distribution(item[1].distribution)
            == normalize_distribution(record.distribution)
        ]
        if (
            len(main) != 1
            or main[0][1].version != record.version
            or main[0][1].sha256 != record.artifact_sha256
        ):
            raise PluginInstallError(
                "PLUGIN_ARTIFACT_LOCK_INCOMPLETE",
                f"Stored artifact lock for {record.plugin_id} has no matching main wheel.",
            )
        return artifacts

    def _reconcile_plugin_specs(
        self,
        desired: list[ManagedPluginRecord],
        specs: tuple[ValidatedPluginSpec, ...],
    ) -> list[ManagedPluginRecord]:
        by_distribution = {
            normalize_distribution(item.distribution): item for item in specs
        }
        reconciled: list[ManagedPluginRecord] = []
        for record in desired:
            spec = by_distribution.get(normalize_distribution(record.distribution))
            if spec is None:
                raise PluginInstallError(
                    "PLUGIN_SPEC_MISSING",
                    f"Validated PluginSpec is missing for {record.distribution}.",
                )
            if spec.version != record.version:
                raise PluginInstallError(
                    "PLUGIN_VERSION_MISMATCH",
                    f"Managed version {record.version} does not match PluginSpec "
                    f"{spec.version} for {record.distribution}.",
                )
            if record.source_type == "catalog":
                if normalize_distribution(spec.name) != normalize_distribution(
                    record.distribution
                ):
                    raise PluginInstallError(
                        "PLUGIN_DISTRIBUTION_MISMATCH",
                        f"PluginSpec name {spec.name} does not match {record.distribution}.",
                    )
                if tuple(spec.collectors) != tuple(record.collectors):
                    raise PluginInstallError(
                        "PLUGIN_COLLECTOR_MISMATCH",
                        f"Catalog collectors do not match PluginSpec for {record.plugin_id}.",
                    )
            reconciled.append(
                replace(
                    record,
                    runtime_name=spec.name,
                    collectors=tuple(spec.collectors),
                )
            )
        return reconciled

    def _artifact_records(
        self,
        artifacts: Iterable[tuple[Path, WheelMetadata]],
    ) -> tuple[ManagedArtifactRecord, ...]:
        return tuple(
            ManagedArtifactRecord(
                distribution=metadata.distribution,
                version=metadata.version,
                path=self._relative_artifact_path(path),
                sha256=metadata.sha256,
            )
            for path, metadata in artifacts
        )

    @staticmethod
    def _main_artifact(
        plugin: CatalogPlugin,
        artifacts: Iterable[tuple[Path, WheelMetadata]],
    ) -> tuple[Path, WheelMetadata]:
        expected = normalize_distribution(plugin.distribution)
        matches = [
            item
            for item in artifacts
            if normalize_distribution(item[1].distribution) == expected
        ]
        if len(matches) != 1:
            raise PluginInstallError(
                "PLUGIN_DISTRIBUTION_MISMATCH",
                f"Expected exactly one {plugin.distribution} wheel.",
            )
        artifact, metadata = matches[0]
        if metadata.version != plugin.version:
            raise PluginInstallError(
                "PLUGIN_VERSION_MISMATCH",
                f"Catalog version {plugin.version} does not match wheel {metadata.version}.",
            )
        if not metadata.entry_points:
            raise PluginInstallError(
                "PLUGIN_ENTRY_POINT_MISSING",
                f"{plugin.distribution} does not declare an Autoflux plugin entry point.",
            )
        return artifact, metadata

    def _resolve_artifact(self, path: Path) -> Path:
        candidate = path if path.is_absolute() else self.manager_dir / path
        candidate = candidate.resolve()
        try:
            candidate.relative_to(self.manager_dir)
        except ValueError as exc:
            raise PluginInstallError(
                "PLUGIN_ARTIFACT_PATH_INVALID",
                "Managed plugin artifact is outside the plugin-manager directory.",
            ) from exc
        return candidate

    def _relative_artifact_path(self, path: Path) -> str:
        resolved = path.resolve()
        try:
            return str(resolved.relative_to(self.manager_dir))
        except ValueError as exc:
            raise PluginInstallError(
                "PLUGIN_ARTIFACT_PATH_INVALID",
                "Plugin artifact is outside the plugin-manager directory.",
            ) from exc

    def _is_within_manager(self, path: Path) -> bool:
        try:
            path.resolve().relative_to(self.manager_dir)
        except ValueError:
            return False
        return True

    @staticmethod
    def _require_compatible(compatibility) -> None:
        if compatibility.compatible:
            return
        raise PluginInstallError(
            "PLUGIN_INCOMPATIBLE",
            " ".join(compatibility.reasons) or "Plugin is incompatible with this runtime.",
        )


plugin_installer = PluginInstaller()
