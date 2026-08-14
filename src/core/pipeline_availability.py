"""Determine whether a Pipeline/DAG can run with the active component registry."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from src.core.registry import registry


EXECUTABLE_COMPONENT_TYPES = frozenset({"collector", "processor", "storage"})


@dataclass(frozen=True)
class PipelineComponentReference:
    """One executable component referenced by a Pipeline or DAG."""

    component_type: str
    name: str

    def label(self) -> str:
        return f"{self.component_type}:{self.name}"


@dataclass(frozen=True)
class PipelineAvailability:
    """Availability result against the currently active plugin components."""

    missing_components: tuple[PipelineComponentReference, ...] = ()

    @property
    def available(self) -> bool:
        return not self.missing_components

    def missing_labels(self) -> list[str]:
        return [item.label() for item in self.missing_components]


def inspect_pipeline_availability(definition: Any) -> PipelineAvailability:
    """Inspect a Pipeline, DAG, stored payload, or plugin template.

    Historical definitions remain persisted, but are unavailable for new task
    submission until every executable component is registered again.
    """

    components = registry.list_components()
    missing: set[tuple[str, str]] = set()
    for reference in _iter_component_references(definition):
        registered = components.get(reference.component_type, [])
        if reference.name not in registered:
            missing.add((reference.component_type, reference.name))

    return PipelineAvailability(
        missing_components=tuple(
            PipelineComponentReference(component_type, name)
            for component_type, name in sorted(missing)
        )
    )


def _iter_component_references(definition: Any) -> Iterator[PipelineComponentReference]:
    if isinstance(definition, Mapping):
        raw_steps = definition.get("steps")
        raw_nodes = definition.get("nodes")
        if isinstance(raw_steps, list):
            for step in raw_steps:
                if isinstance(step, Mapping):
                    yield from _mapping_reference(step, component_key="name")
            return
        if isinstance(raw_nodes, list):
            for node in raw_nodes:
                if isinstance(node, Mapping):
                    yield from _mapping_reference(node, component_key="component")
            return

    steps = getattr(definition, "steps", None)
    if isinstance(steps, list):
        for step in steps:
            raw_type = getattr(step, "step_type", "")
            component_type = str(getattr(raw_type, "value", raw_type) or "")
            name = str(getattr(step, "component_name", "") or "")
            reference = _normalize_reference(component_type, name)
            if reference is not None:
                yield reference
        return

    nodes = getattr(definition, "nodes", None)
    if isinstance(nodes, list):
        for node in nodes:
            component_type = str(getattr(node, "type", "") or "")
            name = str(getattr(node, "component", "") or "")
            reference = _normalize_reference(component_type, name)
            if reference is not None:
                yield reference


def _mapping_reference(
    item: Mapping[str, Any],
    *,
    component_key: str,
) -> Iterator[PipelineComponentReference]:
    component_type = str(item.get("type") or "")
    name = str(item.get(component_key) or item.get("component_name") or "")
    reference = _normalize_reference(component_type, name)
    if reference is not None:
        yield reference


def _normalize_reference(
    component_type: str,
    name: str,
) -> PipelineComponentReference | None:
    normalized_type = component_type.strip().lower()
    if normalized_type not in EXECUTABLE_COMPONENT_TYPES:
        return None

    normalized_name = name.strip()
    if normalized_type == "storage" and normalized_name:
        from src.storage.factory import normalize_storage_name

        normalized_name = normalize_storage_name(normalized_name)
    return PipelineComponentReference(normalized_type, normalized_name or "<missing>")
