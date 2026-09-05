"""Plugin-owned identifier discovery contracts.

Collectors may optionally register a resolver for turning a human-readable
target name into platform-specific identifiers.  The core orchestrates these
callbacks without knowing any platform names.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

ResolveIdentifier = Callable[[Any, str], Awaitable[Any]]
VerifyIdentifier = Callable[[Any, str, str], Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class IdentifierResolverSpec:
    """One plugin-provided identifier resolver and optional target binding."""

    platform: str
    collector_ids: tuple[str, ...]
    resolve: ResolveIdentifier
    verify: VerifyIdentifier | None = None
    requires_shared_browser: bool = False
    cdp_config_key: str = ""
    cdp_default_port: int = 9222
    pipeline_prefixes: tuple[str, ...] = ()
    existing_target_params: tuple[str, ...] = ()
    output_target_param: str = ""
    output_value_type: str = "string"


_RESOLVERS: dict[str, tuple[IdentifierResolverSpec, str]] = {}


def register_identifier_resolver(spec: IdentifierResolverSpec, *, owner: str) -> None:
    """Register a resolver while preventing cross-plugin replacement."""

    platform = str(spec.platform or "").strip()
    normalized_owner = str(owner or "").strip()
    if not platform or not normalized_owner:
        raise ValueError("identifier resolver platform and owner are required")
    if not spec.collector_ids:
        raise ValueError(f"identifier resolver '{platform}' must declare collector_ids")
    current = _RESOLVERS.get(platform)
    if current and current[1] != normalized_owner:
        raise ValueError(
            f"identifier resolver '{platform}' already belongs to plugin '{current[1]}'"
        )
    _RESOLVERS[platform] = (spec, normalized_owner)


def get_identifier_resolver(platform: str) -> IdentifierResolverSpec | None:
    entry = _RESOLVERS.get(str(platform or "").strip())
    return entry[0] if entry else None


def list_identifier_resolvers() -> list[IdentifierResolverSpec]:
    return [_RESOLVERS[key][0] for key in sorted(_RESOLVERS)]


def snapshot_identifier_resolvers() -> dict[str, tuple[IdentifierResolverSpec, str]]:
    return dict(_RESOLVERS)


def restore_identifier_resolvers(
    snapshot: dict[str, tuple[IdentifierResolverSpec, str]],
) -> None:
    _RESOLVERS.clear()
    _RESOLVERS.update(snapshot)
