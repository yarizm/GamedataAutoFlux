"""Discovery, validation, and activation of installed Autoflux plugins.

Distributions expose one :class:`PluginSpec` through the
``gamedata_autoflux.plugins`` entry-point group.  Loading is transactional: all
declared modules and every contribution they register are checked before the
plugin becomes active.  A failed candidate restores every shared registry.

For repository development, ``AUTOFLUX_PLUGIN_MODULES`` may contain an
explicit comma-separated list of plugin modules.  Production never scans the
collector source tree.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import inspect
import os
import pkgutil
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from loguru import logger

from src.collectors.base import BaseCollector
from src.core.collector_metadata import (
    CollectorMetadata,
    register_collector_metadata,
    restore_collector_metadata,
    snapshot_collector_metadata,
)
from src.core.collector_probes import restore_collector_probes, snapshot_collector_probes
from src.core.collector_validators import (
    CollectorConfigIssue,
    restore_collector_config_validators,
    snapshot_collector_config_validators,
)
from src.core.dag_nodes import (
    DagNodeDefinition,
    default_dag_node,
    register_dag_node,
    restore_dag_nodes,
    snapshot_dag_nodes,
)
from src.core.identifier_resolvers import (
    restore_identifier_resolvers,
    snapshot_identifier_resolvers,
)
from src.core.pipeline_templates import (
    register_pipeline_template,
    restore_pipeline_templates,
    snapshot_pipeline_templates,
)
from src.core.registry import registry
from src.processors.base import BaseProcessor
from src.storage.base import BaseStorage


PLUGIN_ENTRY_POINT_GROUP = "gamedata_autoflux.plugins"
PLUGIN_MODULES_ENV = "AUTOFLUX_PLUGIN_MODULES"
PLUGIN_API_VERSION = "1"
_SUPPORTED_COMPONENT_TYPES = ("collector", "processor", "storage")
_COMPONENT_BASES = {
    "collector": BaseCollector,
    "processor": BaseProcessor,
    "storage": BaseStorage,
}


@dataclass(frozen=True)
class PluginSpec:
    """Public, declarative contract implemented by one plugin distribution.

    Every declared component is made available to DAG orchestration.  A
    conventional node definition is generated automatically unless the plugin
    supplies an override in ``dag_nodes``.
    """

    name: str
    version: str
    modules: tuple[str, ...]
    collectors: tuple[str, ...]
    metadata: tuple[CollectorMetadata, ...]
    pipeline_templates: tuple[dict[str, Any], ...] = ()
    description: str = ""
    processors: tuple[str, ...] = ()
    storages: tuple[str, ...] = ()
    dag_nodes: tuple[DagNodeDefinition, ...] = ()
    api_version: str = PLUGIN_API_VERSION


@dataclass(frozen=True)
class PluginCapability:
    """One contribution that was validated during plugin activation."""

    kind: str
    name: str
    target: str = ""

    def to_dict(self) -> dict[str, str]:
        return {"kind": self.kind, "name": self.name, "target": self.target}


@dataclass
class PluginStatus:
    """Serializable activation result for diagnostics and the Web API."""

    name: str
    version: str = ""
    source: str = ""
    state: str = "discovered"
    collectors: list[str] = field(default_factory=list)
    components: dict[str, list[str]] = field(default_factory=dict)
    capabilities: list[PluginCapability] = field(default_factory=list)
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "source": self.source,
            "state": self.state,
            "collectors": list(self.collectors),
            "components": {
                key: list(values) for key, values in self.components.items()
            },
            "capabilities": [item.to_dict() for item in self.capabilities],
            "error": self.error,
        }


@dataclass(frozen=True)
class _ActivationSnapshots:
    components: dict[str, dict[str, type]]
    metadata: Any
    templates: Any
    probes: Any
    validators: Any
    identifier_resolvers: Any
    dag_nodes: Any


class PluginManager:
    """Discover and activate installed plugins with per-plugin rollback."""

    def __init__(self) -> None:
        self._statuses: dict[str, PluginStatus] = {}
        self._activated_sources: set[str] = set()
        self._activated_names: dict[str, str] = {}
        self._activated_specs: dict[str, PluginSpec] = {}
        self._activated_name_sources: dict[str, str] = {}

    def load_installed(
        self,
        *,
        disabled_distributions: set[str] | None = None,
    ) -> list[PluginStatus]:
        """Activate installed entry points plus explicitly configured dev modules."""

        load_core_components()
        candidates: list[tuple[str, Any]] = []
        disabled = {
            self._normalize_distribution(item)
            for item in (disabled_distributions or set())
        }
        try:
            entry_points = importlib.metadata.entry_points()
            selected = entry_points.select(group=PLUGIN_ENTRY_POINT_GROUP)
        except AttributeError:  # pragma: no cover - Python <3.10 compatibility
            selected = importlib.metadata.entry_points().get(PLUGIN_ENTRY_POINT_GROUP, [])

        for entry_point in selected:
            distribution = getattr(entry_point, "dist", None)
            metadata = getattr(distribution, "metadata", None)
            distribution_name = str(
                metadata.get("Name", "") if metadata is not None else ""
            )
            if self._normalize_distribution(distribution_name) in disabled:
                logger.info("Autoflux plugin disabled: {}", distribution_name)
                continue
            candidates.append((f"entrypoint:{entry_point.name}", entry_point))

        raw_modules = os.getenv(PLUGIN_MODULES_ENV, "")
        for module_name in dict.fromkeys(
            part.strip() for part in raw_modules.split(",") if part.strip()
        ):
            candidates.append((f"module:{module_name}", module_name))

        for source, loader in candidates:
            if source in self._activated_sources:
                continue
            self.load_candidate(source, loader)
        return self.list_statuses()

    def load_candidate(self, source: str, loader: Any) -> PluginStatus:
        """Validate and activate one explicit candidate.

        This public entry point is also used by the isolated wheel validator and
        the first-party release checker, ensuring install-time checks are the
        same checks used at runtime.
        """

        load_core_components()
        snapshots = _take_snapshots()
        status = PluginStatus(name=source, source=source)
        try:
            if hasattr(loader, "load"):
                loaded = loader.load()
            elif isinstance(loader, str):
                loaded = importlib.import_module(loader)
            else:
                loaded = loader
            spec = self._coerce_spec(loaded)
            status = PluginStatus(
                name=spec.name,
                version=spec.version,
                source=source,
                collectors=list(spec.collectors),
                components=_declared_components(spec),
            )
            active_version = self._activated_names.get(spec.name)
            if active_version is not None:
                if active_version != spec.version:
                    raise RuntimeError(
                        "PLUGIN_IDENTITY_CONFLICT: "
                        f"plugin '{spec.name}' is already active at version {active_version}; "
                        f"cannot also load version {spec.version}"
                    )
                active_spec = self._activated_specs[spec.name]
                if active_spec != spec:
                    active_source = self._activated_name_sources.get(spec.name, "unknown")
                    raise RuntimeError(
                        "PLUGIN_IDENTITY_CONFLICT: "
                        f"plugin '{spec.name}' version {spec.version} from '{source}' "
                        f"does not match the already active contract from '{active_source}'"
                    )
                # Loading a duplicate source may have imported modules with
                # registration side effects. Preserve the first activation as
                # the authoritative registry state.
                _restore_snapshots(snapshots)
                active_source = self._activated_name_sources.get(spec.name, "")
                active_status = self._statuses.get(active_source)
                status.state = "active"
                status.capabilities = (
                    list(active_status.capabilities) if active_status else []
                )
                self._activated_sources.add(source)
                self._statuses[source] = status
                logger.debug("Duplicate Autoflux plugin source ignored: {}", source)
                return status

            capabilities = _activate_and_validate_spec(spec, snapshots)
            status.capabilities = capabilities
            status.state = "active"
            self._activated_sources.add(source)
            self._activated_names[spec.name] = spec.version
            self._activated_specs[spec.name] = deepcopy(spec)
            self._activated_name_sources[spec.name] = source
            logger.info(
                "Autoflux plugin activated: {} {} ({} validated capabilities)",
                spec.name,
                spec.version,
                len(capabilities),
            )
        except Exception as exc:  # noqa: BLE001 - one bad plugin must not block startup
            _restore_snapshots(snapshots)
            status.state = "failed"
            status.error = str(exc)
            logger.warning("Autoflux plugin failed: {} - {}", source, exc)

        self._statuses[source] = status
        return status

    @staticmethod
    def _coerce_spec(loaded: Any) -> PluginSpec:
        candidate = getattr(loaded, "plugin", loaded)
        if callable(candidate) and not isinstance(candidate, PluginSpec):
            candidate = candidate()
        if not isinstance(candidate, PluginSpec):
            raise TypeError(
                "plugin entry point must expose a PluginSpec or a factory returning one"
            )
        if not candidate.name.strip():
            raise ValueError("plugin name is required")
        if not candidate.version.strip():
            raise ValueError(f"plugin '{candidate.name}' version is required")
        if candidate.api_version != PLUGIN_API_VERSION:
            raise ValueError(
                f"plugin '{candidate.name}' uses unsupported API version "
                f"'{candidate.api_version}'"
            )
        if not candidate.modules:
            raise ValueError(f"plugin '{candidate.name}' does not declare activation modules")
        if not any(_declared_components(candidate).values()):
            raise ValueError(f"plugin '{candidate.name}' does not declare any components")
        _require_unique(candidate.modules, "activation module")
        for component_type, names in _declared_components(candidate).items():
            _require_unique(names, f"{component_type} component")
        return candidate

    @staticmethod
    def _normalize_distribution(value: str) -> str:
        return value.strip().lower().replace("_", "-").replace(".", "-")

    def list_statuses(self) -> list[PluginStatus]:
        return sorted(self._statuses.values(), key=lambda item: (item.name, item.source))

    def payload(self) -> dict[str, Any]:
        statuses = self.list_statuses()
        return {
            "entry_point_group": PLUGIN_ENTRY_POINT_GROUP,
            "plugin_api": PLUGIN_API_VERSION,
            "plugins": [status.to_dict() for status in statuses],
            "active": sum(status.state == "active" for status in statuses),
            "failed": sum(status.state == "failed" for status in statuses),
        }

    def reset_for_tests(self) -> None:
        """Forget activation bookkeeping; registry isolation remains caller-owned."""

        self._statuses.clear()
        self._activated_sources.clear()
        self._activated_names.clear()
        self._activated_specs.clear()
        self._activated_name_sources.clear()


def load_core_components() -> None:
    """Load core processor/storage implementations before plugin validation."""

    for package_name in ("src.processors", "src.storage"):
        package = importlib.import_module(package_name)
        package_file = getattr(package, "__file__", None)
        if not package_file:
            continue
        package_path = Path(package_file).parent
        for _, module_name, _ in pkgutil.iter_modules([str(package_path)]):
            if module_name == "base":
                continue
            importlib.import_module(f"{package_name}.{module_name}")


def _take_snapshots() -> _ActivationSnapshots:
    return _ActivationSnapshots(
        components=registry.snapshot(),
        metadata=snapshot_collector_metadata(),
        templates=snapshot_pipeline_templates(),
        probes=snapshot_collector_probes(),
        validators=snapshot_collector_config_validators(),
        identifier_resolvers=snapshot_identifier_resolvers(),
        dag_nodes=snapshot_dag_nodes(),
    )


def _restore_snapshots(snapshots: _ActivationSnapshots) -> None:
    registry.restore(snapshots.components)
    restore_collector_metadata(snapshots.metadata)
    restore_pipeline_templates(snapshots.templates)
    restore_collector_probes(snapshots.probes)
    restore_collector_config_validators(snapshots.validators)
    restore_identifier_resolvers(snapshots.identifier_resolvers)
    restore_dag_nodes(snapshots.dag_nodes)


def _activate_and_validate_spec(
    spec: PluginSpec,
    snapshots: _ActivationSnapshots,
) -> list[PluginCapability]:
    capabilities = [
        PluginCapability("module", module_name) for module_name in spec.modules
    ]
    for module_name in spec.modules:
        importlib.import_module(module_name)

    components = _validate_component_contributions(spec, snapshots.components)
    for component_type, names in components.items():
        capabilities.extend(
            PluginCapability("component", name, component_type) for name in names
        )

    metadata_by_id = _validate_metadata(spec)
    for metadata in spec.metadata:
        register_collector_metadata(metadata, owner=spec.name)
        capabilities.append(
            PluginCapability("collector_metadata", metadata.collector_id, metadata.display_name)
        )

    dag_nodes = _resolve_dag_nodes(spec, metadata_by_id)
    for definition in dag_nodes:
        register_dag_node(definition, owner=spec.name)
        capabilities.append(
            PluginCapability(
                "dag_node",
                f"{definition.node_type}:{definition.component}",
                definition.display_name,
            )
        )

    capabilities.extend(_validate_registered_functions(spec))
    capabilities.extend(_validate_and_register_templates(spec, components))
    return sorted(capabilities, key=lambda item: (item.kind, item.name, item.target))


def _declared_components(spec: PluginSpec) -> dict[str, list[str]]:
    return {
        "collector": list(spec.collectors),
        "processor": list(spec.processors),
        "storage": list(spec.storages),
    }


def _validate_component_contributions(
    spec: PluginSpec,
    before: dict[str, dict[str, type]],
) -> dict[str, list[str]]:
    after = registry.snapshot()
    declared = _declared_components(spec)
    unsupported = sorted(
        f"{component_type}:{name}"
        for component_type, implementations in after.items()
        if component_type not in _SUPPORTED_COMPONENT_TYPES
        for name, implementation in implementations.items()
        if _implementation_from_spec(implementation, spec)
    )
    if unsupported:
        raise ValueError(
            "plugin registered unsupported component types: " + ", ".join(unsupported)
        )

    for component_type in _SUPPORTED_COMPONENT_TYPES:
        previous = before.get(component_type, {})
        current = after.get(component_type, {})
        overwritten = sorted(
            name
            for name, implementation in current.items()
            if name in previous
            and previous[name] is not implementation
            and _implementation_from_spec(implementation, spec)
            and not _implementation_from_spec(previous[name], spec)
        )
        if overwritten:
            raise ValueError(
                "PLUGIN_COMPONENT_CONFLICT: "
                f"plugin attempted to replace existing {component_type} components: "
                + ", ".join(overwritten)
            )
        actual = {
            name
            for name, implementation in current.items()
            if _implementation_from_spec(implementation, spec)
        }
        expected = set(declared[component_type])
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        if missing:
            raise ValueError(
                f"declared {component_type} components were not registered: "
                + ", ".join(missing)
            )
        if unexpected:
            raise ValueError(
                f"undeclared {component_type} components were registered: "
                + ", ".join(unexpected)
            )
        for name in declared[component_type]:
            implementation = current[name]
            base = _COMPONENT_BASES[component_type]
            if not inspect.isclass(implementation) or not issubclass(implementation, base):
                raise TypeError(
                    f"{component_type} '{name}' must inherit {base.__name__}"
                )
            if inspect.isabstract(implementation):
                raise TypeError(f"{component_type} '{name}' is still abstract")
            if not _implementation_from_spec(implementation, spec):
                raise ValueError(
                    f"{component_type} '{name}' is implemented by undeclared module "
                    f"'{implementation.__module__}'"
                )
    return declared


def _implementation_from_spec(implementation: Any, spec: PluginSpec) -> bool:
    module_name = str(getattr(implementation, "__module__", "") or "")
    return any(
        module_name == declared_module or module_name.startswith(declared_module + ".")
        for declared_module in spec.modules
    )


def _validate_metadata(spec: PluginSpec) -> dict[str, CollectorMetadata]:
    metadata_ids = [str(item.collector_id or "").strip() for item in spec.metadata]
    _require_unique(metadata_ids, "collector metadata")
    missing = sorted(set(spec.collectors) - set(metadata_ids))
    unexpected = sorted(set(metadata_ids) - set(spec.collectors))
    if missing:
        raise ValueError("collector metadata missing: " + ", ".join(missing))
    if unexpected:
        raise ValueError("metadata declared for unknown collectors: " + ", ".join(unexpected))

    for metadata in spec.metadata:
        if not metadata.display_name.strip():
            raise ValueError(
                f"collector '{metadata.collector_id}' display_name is required"
            )
        if not metadata.description.strip() and not spec.description.strip():
            raise ValueError(
                f"collector '{metadata.collector_id}' description is required "
                "when the plugin has no fallback description"
            )
        if metadata.recovery_level not in {"L0", "L1", "L2", "L3"}:
            raise ValueError(
                f"collector '{metadata.collector_id}' has invalid recovery_level"
            )
        _require_unique(metadata.capabilities, f"{metadata.collector_id} capability")
        _require_unique(
            (item.requirement_id for item in metadata.credential_requirements),
            f"{metadata.collector_id} credential requirement",
        )
        _require_unique(
            (item.account_id for item in metadata.session_accounts),
            f"{metadata.collector_id} session account",
        )
        if any(not item.check_id.strip() for item in metadata.session_checks):
            raise ValueError(
                f"{metadata.collector_id} session check ids must be non-empty"
            )
        _require_unique(
            (item.code for item in metadata.target_schema.rules),
            f"{metadata.collector_id} target rule",
        )
        target_field_keys = [
            f"{field.location}:{field.key.strip()}"
            for field in metadata.target_schema.fields
        ]
        _require_unique(
            target_field_keys,
            f"{metadata.collector_id} target field",
        )
        for target_field in metadata.target_schema.fields:
            if not target_field.key.strip():
                raise ValueError(
                    f"collector '{metadata.collector_id}' target field key is required"
                )
            if not target_field.label.strip():
                raise ValueError(
                    f"collector '{metadata.collector_id}' target field "
                    f"'{target_field.key}' label is required"
                )
            if not target_field.description.strip():
                raise ValueError(
                    f"collector '{metadata.collector_id}' target field "
                    f"'{target_field.key}' description is required"
                )
            if target_field.input_type == "select" and not target_field.options:
                raise ValueError(
                    f"collector '{metadata.collector_id}' select target field "
                    f"'{target_field.key}' requires options"
                )
        # Building the conventional node validates config_schema and output fields.
        default_dag_node(
            "collector",
            metadata.collector_id,
            display_name=metadata.display_name,
            description=metadata.description,
            config_schema=metadata.config_schema,
            output_fields=metadata.output_fields,
        )
    return {item.collector_id: item for item in spec.metadata}


def _resolve_dag_nodes(
    spec: PluginSpec,
    metadata_by_id: dict[str, CollectorMetadata],
) -> list[DagNodeDefinition]:
    explicit: dict[tuple[str, str], DagNodeDefinition] = {}
    for definition in spec.dag_nodes:
        key = (definition.node_type, definition.component)
        if key in explicit:
            raise ValueError(f"duplicate DAG node definition: {key[0]}:{key[1]}")
        explicit[key] = definition.model_copy(deep=True)

    declared = _declared_components(spec)
    expected = {
        (component_type, component)
        for component_type, names in declared.items()
        for component in names
    }
    unknown = sorted(set(explicit) - expected)
    if unknown:
        raise ValueError(
            "DAG nodes reference undeclared components: "
            + ", ".join(f"{item[0]}:{item[1]}" for item in unknown)
        )

    resolved: list[DagNodeDefinition] = []
    for component_type in _SUPPORTED_COMPONENT_TYPES:
        for component in declared[component_type]:
            key = (component_type, component)
            definition = explicit.get(key)
            metadata = metadata_by_id.get(component)
            if definition is None:
                definition = default_dag_node(
                    component_type,
                    component,
                    display_name=metadata.display_name if metadata else component,
                    description=(
                        (metadata.description or spec.description) if metadata else spec.description
                    ),
                    config_schema=metadata.config_schema if metadata else {},
                    output_fields=metadata.output_fields if metadata else [],
                )
            elif metadata is not None:
                if definition.config_schema and (
                    definition.config_schema != metadata.config_schema
                ):
                    raise ValueError(
                        f"collector DAG node '{component}' config_schema does not match metadata"
                    )
                definition = definition.model_copy(
                    update={
                        "display_name": definition.display_name or metadata.display_name,
                        "description": (
                            definition.description
                            or metadata.description
                            or spec.description
                        ),
                        "config_schema": definition.config_schema or metadata.config_schema,
                        "output_fields": definition.output_fields or metadata.output_fields,
                    },
                    deep=True,
                )
                # model_copy does not re-run model validators.
                definition = DagNodeDefinition.model_validate(
                    definition.model_dump(mode="python", by_alias=True)
                )
            resolved.append(definition)
    return resolved


def _validate_registered_functions(
    spec: PluginSpec,
) -> list[PluginCapability]:
    capabilities: list[PluginCapability] = []
    declared_collectors = set(spec.collectors)

    current_probes = snapshot_collector_probes()
    for collector_id, entries in current_probes.items():
        for entry in entries:
            name, runner, owner = entry
            if owner != spec.name and not _implementation_from_spec(runner, spec):
                continue
            _validate_owned_function(
                owner=owner,
                expected_owner=spec.name,
                collector_id=collector_id,
                declared_collectors=declared_collectors,
                function=runner,
                label=f"probe '{collector_id}:{name}'",
                require_async=True,
            )
            capabilities.append(
                PluginCapability("collector_probe", name, collector_id)
            )

    current_validators = snapshot_collector_config_validators()
    for collector_id, entries in current_validators.items():
        for entry in entries:
            validator, owner = entry
            if owner != spec.name and not _implementation_from_spec(validator, spec):
                continue
            label = (
                f"{getattr(validator, '__module__', '')}."
                f"{getattr(validator, '__qualname__', getattr(validator, '__name__', 'validator'))}"
            ).strip(".")
            _validate_owned_function(
                owner=owner,
                expected_owner=spec.name,
                collector_id=collector_id,
                declared_collectors=declared_collectors,
                function=validator,
                label=f"config validator '{label}'",
                require_async=False,
            )
            try:
                issues = list(validator({}))
            except Exception as exc:  # noqa: BLE001
                raise ValueError(
                    f"config validator '{label}' failed its empty-config contract check: {exc}"
                ) from exc
            if any(not isinstance(item, CollectorConfigIssue) for item in issues):
                raise TypeError(
                    f"config validator '{label}' must return CollectorConfigIssue values"
                )
            capabilities.append(
                PluginCapability("config_validator", label, collector_id)
            )

    current_resolvers = snapshot_identifier_resolvers()
    for platform, entry in current_resolvers.items():
        resolver, owner = entry
        if owner != spec.name and not _implementation_from_spec(resolver.resolve, spec):
            continue
        if owner != spec.name:
            raise ValueError(
                f"identifier resolver '{platform}' owner must be '{spec.name}'"
            )
        if not set(resolver.collector_ids).issubset(declared_collectors):
            raise ValueError(
                f"identifier resolver '{platform}' references collectors outside its plugin"
            )
        if not inspect.iscoroutinefunction(resolver.resolve):
            raise TypeError(f"identifier resolver '{platform}' resolve must be async")
        if resolver.verify is not None and not inspect.iscoroutinefunction(resolver.verify):
            raise TypeError(f"identifier resolver '{platform}' verify must be async")
        capabilities.append(
            PluginCapability(
                "identifier_resolver",
                platform,
                ",".join(resolver.collector_ids),
            )
        )
    return capabilities


def _validate_owned_function(
    *,
    owner: str,
    expected_owner: str,
    collector_id: str,
    declared_collectors: set[str],
    function: Any,
    label: str,
    require_async: bool,
) -> None:
    if owner != expected_owner:
        raise ValueError(f"{label} owner must be '{expected_owner}'")
    if collector_id not in declared_collectors:
        raise ValueError(f"{label} targets undeclared collector '{collector_id}'")
    if not callable(function):
        raise TypeError(f"{label} is not callable")
    if require_async and not inspect.iscoroutinefunction(function):
        raise TypeError(f"{label} must be async")


def _validate_and_register_templates(
    spec: PluginSpec,
    components: dict[str, list[str]],
) -> list[PluginCapability]:
    capabilities: list[PluginCapability] = []
    template_ids = [str(item.get("id") or "").strip() for item in spec.pipeline_templates]
    _require_unique(template_ids, "pipeline template")
    for template in spec.pipeline_templates:
        template_id = str(template.get("id") or "").strip()
        if not template_id:
            raise ValueError("pipeline template id is required")
        if not str(template.get("name") or "").strip():
            raise ValueError(f"pipeline template '{template_id}' name is required")
        steps = template.get("steps")
        if not isinstance(steps, list) or not steps:
            raise ValueError(f"pipeline template '{template_id}' must contain steps")
        owns_component = False
        for index, step in enumerate(steps):
            if not isinstance(step, dict):
                raise TypeError(
                    f"pipeline template '{template_id}' step {index} must be an object"
                )
            component_type = str(step.get("type") or "").strip()
            component_name = str(step.get("name") or "").strip()
            if component_type not in _SUPPORTED_COMPONENT_TYPES or not component_name:
                raise ValueError(
                    f"pipeline template '{template_id}' step {index} has invalid component"
                )
            try:
                registry.get(component_type, component_name)
            except KeyError as exc:
                raise ValueError(
                    f"pipeline template '{template_id}' references unavailable "
                    f"{component_type} '{component_name}'"
                ) from exc
            if component_name in components[component_type]:
                owns_component = True
            if component_type == "collector" and component_name not in components["collector"]:
                raise ValueError(
                    f"pipeline template '{template_id}' references collector "
                    f"'{component_name}' owned by another plugin"
                )
            config = step.get("config", {})
            if not isinstance(config, dict):
                raise TypeError(
                    f"pipeline template '{template_id}' step {index} config must be an object"
                )
        if not owns_component:
            raise ValueError(
                f"pipeline template '{template_id}' does not use a component from its plugin"
            )
        register_pipeline_template(template, owner=spec.name)
        capabilities.append(PluginCapability("pipeline_template", template_id))
    return capabilities


def _require_unique(values: Iterable[Any], label: str) -> None:
    normalized = [str(value or "").strip() for value in values]
    if any(not value for value in normalized):
        raise ValueError(f"{label} values must be non-empty")
    duplicates = sorted({value for value in normalized if normalized.count(value) > 1})
    if duplicates:
        raise ValueError(f"duplicate {label} values: {', '.join(duplicates)}")


plugin_manager = PluginManager()


def make_template(
    template_id: str,
    name: str,
    description: str,
    collector: str,
    *,
    collector_config: dict[str, Any] | None = None,
    clean: bool = True,
) -> dict[str, Any]:
    """Small helper used by plugins to create conventional persistence pipelines."""

    steps: list[dict[str, Any]] = [
        {"type": "collector", "name": collector, "config": collector_config or {}}
    ]
    if clean:
        steps.append({"type": "processor", "name": "cleaner", "config": {}})
    steps.append({"type": "storage", "name": "sqlalchemy", "config": {}})
    return {
        "id": template_id,
        "name": name,
        "description": description,
        "steps": steps,
    }
