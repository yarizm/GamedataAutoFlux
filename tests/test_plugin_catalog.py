"""Bundled catalog and wheel metadata validation tests."""

from __future__ import annotations

import zipfile
import stat
from pathlib import Path

import pytest

from src.plugin_manager.catalog import PluginCatalog
from src.plugin_manager.compatibility import detect_runtime_capabilities
from src.plugin_manager.installer import PluginInstaller, PluginInstallError
from src.plugin_manager.models import ManagedArtifactRecord, ManagedPluginRecord
from src.plugin_manager.package_reader import normalize_distribution, read_wheel_metadata
from src.plugin_manager.store import PluginStateStore


REPO = Path(__file__).resolve().parents[1]


def _write_test_wheel(
    path: Path,
    *,
    entry_point_target: str = "autoflux_plugin_youtube:plugin",
    requires_python: str = ">=3.12",
) -> None:
    dist_info = "autoflux_plugin_youtube-0.1.0.dist-info"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("autoflux_plugin_youtube/__init__.py", "plugin = object()\n")
        archive.writestr(
            f"{dist_info}/METADATA",
            "\n".join(
                [
                    "Metadata-Version: 2.4",
                    "Name: autoflux-plugin-youtube",
                    "Version: 0.1.0",
                    "Summary: YouTube collectors for GamedataAutoFlux",
                    f"Requires-Python: {requires_python}",
                    "Requires-Dist: gamedata-autoflux>=0.1.0",
                    "Requires-Dist: httpx>=0.27.0",
                    "",
                ]
            ),
        )
        archive.writestr(
            f"{dist_info}/entry_points.txt",
            f"[gamedata_autoflux.plugins]\nyoutube = {entry_point_target}\n",
        )
        archive.writestr(
            f"{dist_info}/WHEEL",
            "Wheel-Version: 1.0\nTag: py3-none-any\n",
        )


def _write_runtime_test_wheel(path: Path) -> None:
    dist_info = "autoflux_plugin_local_example-0.1.0.dist-info"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "autoflux_plugin_local_example/__init__.py",
            """from src.core.collector_metadata import CollectorMetadata
from src.core.plugin_system import PluginSpec

plugin = PluginSpec(
    name="runtime-local-example",
    version="0.1.0",
    modules=("autoflux_plugin_local_example.collector",),
    collectors=("local_example",),
    metadata=(CollectorMetadata(
        collector_id="local_example",
        display_name="Local",
        description="Local catalog-test collector.",
    ),),
)
""",
        )
        archive.writestr(
            "autoflux_plugin_local_example/collector.py",
            """from src.collectors.base import BaseCollector
from src.core.registry import registry

@registry.register("collector", "local_example")
class LocalExampleCollector(BaseCollector):
    async def collect(self, target):
        raise NotImplementedError
""",
        )
        archive.writestr(
            f"{dist_info}/METADATA",
            "\n".join(
                [
                    "Metadata-Version: 2.4",
                    "Name: autoflux-plugin-local-example",
                    "Version: 0.1.0",
                    "Summary: Local example",
                    "Requires-Python: >=3.12",
                    "Requires-Dist: gamedata-autoflux>=0.1.0",
                    "",
                ]
            ),
        )
        archive.writestr(
            f"{dist_info}/entry_points.txt",
            "[gamedata_autoflux.plugins]\nlocal = autoflux_plugin_local_example:plugin\n",
        )
        archive.writestr(
            f"{dist_info}/WHEEL",
            "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )
        archive.writestr(f"{dist_info}/RECORD", "")


def test_bundled_catalog_lists_first_party_plugins(tmp_path: Path) -> None:
    catalog = PluginCatalog(
        project_root=REPO,
        store=PluginStateStore(tmp_path / "manager"),
    )

    payload = catalog.payload()

    assert payload["total"] == 8
    assert payload["catalogs"] == [
        {"id": "official", "type": "bundled", "trust": "official"}
    ]
    assert all(
        item["available"] == item["compatibility"]["compatible"]
        for item in payload["plugins"]
    )
    youtube = next(item for item in payload["plugins"] if item["id"] == "official.youtube")
    assert youtube["distribution"] == "autoflux-plugin-youtube"
    assert youtube["collectors"] == ["youtube_profiles", "youtube_comments"]
    assert youtube["installed"] is False
    assert youtube["available"] is True
    assert youtube["plugin_api"] == "1"


def test_reader_validates_built_youtube_wheel(tmp_path: Path) -> None:
    wheel_path = tmp_path / "autoflux_plugin_youtube-0.1.0-py3-none-any.whl"
    _write_test_wheel(wheel_path)

    metadata = read_wheel_metadata(wheel_path)

    assert normalize_distribution(metadata.distribution) == "autoflux-plugin-youtube"
    assert metadata.version == "0.1.0"
    assert metadata.entry_points == (("youtube", "autoflux_plugin_youtube:plugin"),)
    assert metadata.sha256
    assert metadata.size > 0
    assert metadata.requires_python == ">=3.12"
    assert metadata.install_paths == ("autoflux_plugin_youtube/__init__.py",)


def test_reader_rejects_non_wheel(tmp_path: Path) -> None:
    invalid = tmp_path / "not-a-plugin.whl"
    invalid.write_text("not a zip", encoding="utf-8")

    with pytest.raises(ValueError, match="valid wheel"):
        read_wheel_metadata(invalid)


def test_catalog_marks_missing_runtime_capability_unavailable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "AUTOFLUX_DISABLED_RUNTIME_CAPABILITIES",
        "playwright-chromium",
    )
    detect_runtime_capabilities.cache_clear()
    try:
        catalog = PluginCatalog(
            project_root=REPO,
            store=PluginStateStore(tmp_path / "manager"),
        )
        payload = catalog.payload()
    finally:
        detect_runtime_capabilities.cache_clear()

    steam = next(item for item in payload["plugins"] if item["id"] == "official.steam")
    youtube = next(
        item for item in payload["plugins"] if item["id"] == "official.youtube"
    )
    assert steam["available"] is False
    assert steam["compatibility"]["compatible"] is False
    assert "playwright-chromium" in " ".join(steam["compatibility"]["reasons"])
    assert youtube["available"] is True


def test_reader_rejects_unsafe_entry_point_target(tmp_path: Path) -> None:
    wheel_path = tmp_path / "autoflux_plugin_youtube-0.1.0-py3-none-any.whl"
    _write_test_wheel(wheel_path, entry_point_target="os:system [danger]")

    with pytest.raises(ValueError, match="unsafe plugin entry-point target"):
        read_wheel_metadata(wheel_path)


def test_reader_rejects_symbolic_links_and_path_traversal(tmp_path: Path) -> None:
    for filename, member_name, link in (
        ("symlink.whl", "autoflux_plugin_youtube/link", True),
        ("traversal.whl", "../outside.py", False),
    ):
        wheel_path = tmp_path / filename
        _write_test_wheel(wheel_path)
        with zipfile.ZipFile(wheel_path, "a") as archive:
            if link:
                info = zipfile.ZipInfo(member_name)
                info.external_attr = (stat.S_IFLNK | 0o777) << 16
                archive.writestr(info, "target")
            else:
                archive.writestr(member_name, "unsafe")

        with pytest.raises(ValueError, match="symbolic link|unsafe archive path"):
            read_wheel_metadata(wheel_path)


def test_reader_rejects_suspicious_compression_ratio(tmp_path: Path) -> None:
    wheel_path = tmp_path / "autoflux_plugin_youtube-0.1.0-py3-none-any.whl"
    _write_test_wheel(wheel_path)
    with zipfile.ZipFile(wheel_path, "a", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("autoflux_plugin_youtube/padding.bin", b"0" * (2 * 1024 * 1024))

    with pytest.raises(ValueError, match="suspicious compression ratio"):
        read_wheel_metadata(wheel_path)


def test_cached_upload_hash_mismatch_preserves_generation_pointer(tmp_path: Path) -> None:
    manager_dir = tmp_path / "manager"
    source = tmp_path / "autoflux_plugin_youtube-0.1.0-py3-none-any.whl"
    _write_test_wheel(source)
    source_metadata = read_wheel_metadata(source)
    destination = (
        manager_dir
        / "cache"
        / "wheels"
        / source_metadata.sha256[:16]
        / source.name
    )
    destination.parent.mkdir(parents=True)
    _write_test_wheel(destination)
    with zipfile.ZipFile(destination, "a") as archive:
        archive.writestr("autoflux_plugin_youtube/tampered.txt", "tampered")
    pointer = manager_dir / "current.json"
    pointer.write_text('{"sentinel":"active"}', encoding="utf-8")
    installer = PluginInstaller(
        store=PluginStateStore(manager_dir),
        manager_dir=manager_dir,
        project_root=REPO,
    )

    with pytest.raises(PluginInstallError) as error:
        installer.prepare_uploaded_wheel(source)

    assert error.value.code == "PLUGIN_ARTIFACT_HASH_MISMATCH"
    assert pointer.read_text(encoding="utf-8") == '{"sentinel":"active"}'


def test_uploaded_wheel_persists_runtime_identity_collectors_and_lock(
    tmp_path: Path,
) -> None:
    manager_dir = tmp_path / "manager"
    source = tmp_path / "autoflux_plugin_local_example-0.1.0-py3-none-any.whl"
    _write_runtime_test_wheel(source)
    store = PluginStateStore(manager_dir)
    installer = PluginInstaller(
        store=store,
        manager_dir=manager_dir,
        project_root=REPO,
    )
    cached, _metadata = installer.prepare_uploaded_wheel(source)

    installer.install_uploaded(cached)

    managed = store.get_managed_plugin("local.autoflux-plugin-local-example")
    assert managed is not None
    assert managed.runtime_name == "runtime-local-example"
    assert managed.collectors == ("local_example",)
    assert len(managed.artifacts) == 1
    assert managed.artifacts[0].sha256 == managed.artifact_sha256
    assert store.mark_runtime_reconciled(["runtime-local-example"]) == 1
    assert store.get_managed_plugin(managed.plugin_id).restart_required is False


def test_existing_catalog_plugin_uses_its_locked_wheel(tmp_path: Path) -> None:
    manager_dir = tmp_path / "manager"
    source = tmp_path / "autoflux_plugin_youtube-0.1.0-py3-none-any.whl"
    _write_test_wheel(source)
    installer = PluginInstaller(
        store=PluginStateStore(manager_dir),
        manager_dir=manager_dir,
        project_root=REPO,
    )
    cached, metadata = installer.prepare_uploaded_wheel(source)
    relative_path = str(cached.relative_to(manager_dir))
    record = ManagedPluginRecord(
        plugin_id="official.youtube",
        distribution=metadata.distribution,
        version=metadata.version,
        source_type="catalog",
        source_ref="official:official.youtube@0.1.0",
        artifact_path=relative_path,
        artifact_sha256=metadata.sha256,
        collectors=("youtube_profiles", "youtube_comments"),
        artifacts=(
            ManagedArtifactRecord(
                distribution=metadata.distribution,
                version=metadata.version,
                path=relative_path,
                sha256=metadata.sha256,
            ),
        ),
    )

    artifacts = installer._resolve_locked_artifacts(record)

    assert artifacts == [(cached, metadata)]
