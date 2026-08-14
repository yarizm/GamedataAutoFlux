"""Pipeline templates contributed by installed plugins.

The core intentionally starts with an empty catalog. Collector plugins add
their templates when they are activated by :mod:`src.core.plugin_system`.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any


# Kept as a stable list object because routes and older integrations import it.
PIPELINE_TEMPLATES: list[dict[str, Any]] = []
_TEMPLATE_OWNERS: dict[str, str] = {}


def register_pipeline_template(template: dict[str, Any], *, owner: str) -> None:
    """Register one plugin-owned template.

    Re-registering a template from the same plugin is idempotent. A different
    plugin cannot silently replace an existing template id.
    """

    template_id = str(template.get("id") or "").strip()
    normalized_owner = str(owner or "").strip()
    if not template_id:
        raise ValueError("pipeline template id is required")
    if not normalized_owner:
        raise ValueError("pipeline template owner is required")

    current_owner = _TEMPLATE_OWNERS.get(template_id)
    if current_owner and current_owner != normalized_owner:
        raise ValueError(
            f"pipeline template '{template_id}' already belongs to plugin '{current_owner}'"
        )

    payload = deepcopy(template)
    for index, current in enumerate(PIPELINE_TEMPLATES):
        if current.get("id") == template_id:
            PIPELINE_TEMPLATES[index] = payload
            break
    else:
        PIPELINE_TEMPLATES.append(payload)
    _TEMPLATE_OWNERS[template_id] = normalized_owner


def unregister_pipeline_template_owner(owner: str) -> None:
    """Remove all templates contributed by ``owner``."""

    normalized_owner = str(owner or "").strip()
    owned_ids = {
        template_id
        for template_id, current_owner in _TEMPLATE_OWNERS.items()
        if current_owner == normalized_owner
    }
    if not owned_ids:
        return
    PIPELINE_TEMPLATES[:] = [
        template for template in PIPELINE_TEMPLATES if template.get("id") not in owned_ids
    ]
    for template_id in owned_ids:
        _TEMPLATE_OWNERS.pop(template_id, None)


def snapshot_pipeline_templates() -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Return a rollback-safe snapshot of the template catalog."""

    return deepcopy(PIPELINE_TEMPLATES), dict(_TEMPLATE_OWNERS)


def restore_pipeline_templates(
    snapshot: tuple[list[dict[str, Any]], dict[str, str]],
) -> None:
    """Restore a snapshot created by :func:`snapshot_pipeline_templates`."""

    templates, owners = snapshot
    PIPELINE_TEMPLATES[:] = deepcopy(templates)
    _TEMPLATE_OWNERS.clear()
    _TEMPLATE_OWNERS.update(owners)
