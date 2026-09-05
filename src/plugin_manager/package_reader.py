"""Safe, non-importing inspection of Python wheel metadata."""

from __future__ import annotations

import configparser
import hashlib
import re
import stat
import zipfile
from dataclasses import dataclass
from email.parser import BytesParser
from pathlib import Path, PurePosixPath

from src.core.plugin_system import PLUGIN_ENTRY_POINT_GROUP

_NORMALIZE_PATTERN = re.compile(r"[-_.]+")
_ENTRY_POINT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$")
_ENTRY_POINT_TARGET = re.compile(
    r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$"
)
MAX_ARCHIVE_MEMBERS = 10_000
MAX_UNCOMPRESSED_SIZE = 512 * 1024 * 1024
MAX_COMPRESSION_RATIO = 200
MAX_METADATA_SIZE = 1024 * 1024


def normalize_distribution(name: str) -> str:
    return _NORMALIZE_PATTERN.sub("-", name).lower().strip("-")


@dataclass(frozen=True)
class WheelMetadata:
    path: Path
    distribution: str
    version: str
    summary: str
    requires_dist: tuple[str, ...]
    entry_points: tuple[tuple[str, str], ...]
    sha256: str
    size: int
    requires_python: str = ""
    install_paths: tuple[str, ...] = ()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_wheel_metadata(
    path: Path,
    *,
    require_plugin_entry_point: bool = True,
    max_size: int = 200 * 1024 * 1024,
) -> WheelMetadata:
    """Read metadata without importing or extracting plugin code."""

    path = path.resolve()
    if path.suffix.lower() != ".whl" or not path.is_file():
        raise ValueError("plugin package must be an existing .whl file")
    size = path.stat().st_size
    if size <= 0:
        raise ValueError("wheel file is empty")
    if size > max_size:
        raise ValueError("wheel exceeds the configured package size limit")

    try:
        with zipfile.ZipFile(path) as archive:
            members = archive.infolist()
            if len(members) > MAX_ARCHIVE_MEMBERS:
                raise ValueError("wheel contains too many archive members")
            names = [member.filename for member in members]
            if len(names) != len(set(names)):
                raise ValueError("wheel contains duplicate archive members")
            if len(names) != len({name.casefold() for name in names}):
                raise ValueError("wheel contains case-colliding archive members")
            total_uncompressed = 0
            for info in members:
                name = info.filename
                member = PurePosixPath(name)
                if (
                    not name
                    or "\\" in name
                    or re.match(r"^[A-Za-z]:", name)
                    or any(ord(character) < 32 for character in name)
                    or member.is_absolute()
                    or ".." in member.parts
                    or any(part.rstrip(" .") != part for part in member.parts)
                ):
                    raise ValueError("wheel contains an unsafe archive path")
                if info.flag_bits & 0x1:
                    raise ValueError("wheel contains an encrypted archive member")
                file_type = (info.external_attr >> 16) & 0o170000
                if file_type == stat.S_IFLNK:
                    raise ValueError("wheel contains a symbolic link")
                total_uncompressed += info.file_size
                if total_uncompressed > MAX_UNCOMPRESSED_SIZE:
                    raise ValueError("wheel expands beyond the configured size limit")
                if (
                    info.file_size > MAX_METADATA_SIZE
                    and info.compress_size == 0
                ):
                    raise ValueError("wheel contains an invalid compressed member")
                if (
                    info.compress_size > 0
                    and info.file_size / info.compress_size > MAX_COMPRESSION_RATIO
                ):
                    raise ValueError("wheel contains a suspicious compression ratio")

            metadata_names = [
                name for name in names if name.endswith(".dist-info/METADATA")
            ]
            if len(metadata_names) != 1:
                raise ValueError("wheel must contain exactly one dist-info/METADATA")
            metadata_info = archive.getinfo(metadata_names[0])
            if metadata_info.file_size > MAX_METADATA_SIZE:
                raise ValueError("wheel metadata exceeds the configured size limit")
            metadata = BytesParser().parsebytes(archive.read(metadata_names[0]))

            dist_info_dir = metadata_names[0].rsplit("/", 1)[0]
            install_paths = tuple(
                sorted(
                    {
                        installed_path
                        for name in names
                        if (
                            installed_path := _site_packages_path(
                                name,
                                dist_info_dir=dist_info_dir,
                            )
                        )
                    }
                )
            )
            entry_point_name = f"{dist_info_dir}/entry_points.txt"
            entry_points: list[tuple[str, str]] = []
            if entry_point_name in names:
                if archive.getinfo(entry_point_name).file_size > MAX_METADATA_SIZE:
                    raise ValueError("wheel entry points exceed the configured size limit")
                parser = configparser.ConfigParser(interpolation=None)
                parser.optionxform = str
                parser.read_string(archive.read(entry_point_name).decode("utf-8"))
                if parser.has_section(PLUGIN_ENTRY_POINT_GROUP):
                    entry_points = list(parser.items(PLUGIN_ENTRY_POINT_GROUP))
    except zipfile.BadZipFile as exc:
        raise ValueError("plugin package is not a valid wheel archive") from exc
    except (configparser.Error, UnicodeError, KeyError) as exc:
        raise ValueError("wheel contains malformed plugin metadata") from exc

    distribution = str(metadata.get("Name", "")).strip()
    version = str(metadata.get("Version", "")).strip()
    if not distribution or not version:
        raise ValueError("wheel metadata requires Name and Version")
    if require_plugin_entry_point and not entry_points:
        raise ValueError(
            f"wheel does not declare a {PLUGIN_ENTRY_POINT_GROUP} entry point"
        )
    for entry_point_name, target in entry_points:
        if not _ENTRY_POINT_NAME.fullmatch(entry_point_name):
            raise ValueError("wheel declares an invalid plugin entry-point name")
        if target != target.strip() or not _ENTRY_POINT_TARGET.fullmatch(target):
            raise ValueError("wheel declares an unsafe plugin entry-point target")

    return WheelMetadata(
        path=path,
        distribution=distribution,
        version=version,
        summary=str(metadata.get("Summary", "") or "").strip(),
        requires_dist=tuple(str(value) for value in metadata.get_all("Requires-Dist", [])),
        entry_points=tuple(entry_points),
        sha256=sha256_file(path),
        size=size,
        requires_python=str(metadata.get("Requires-Python", "") or "").strip(),
        install_paths=install_paths,
    )


def _site_packages_path(name: str, *, dist_info_dir: str) -> str:
    """Map a wheel member to its final relative site-packages path."""

    if name.endswith("/") or name.startswith(f"{dist_info_dir}/"):
        return ""
    parts = PurePosixPath(name).parts
    if not parts:
        return ""
    if parts[0].endswith(".data"):
        if len(parts) < 3 or parts[1] not in {"purelib", "platlib"}:
            return ""
        parts = parts[2:]
    return "/".join(parts)
