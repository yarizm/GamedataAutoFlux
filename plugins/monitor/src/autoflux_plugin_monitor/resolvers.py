"""Identifier discovery registration for the monitoring plugin."""

from src.core.identifier_resolvers import IdentifierResolverSpec, register_identifier_resolver


async def _resolve(service, game_name: str):
    return await service.resolve_monitor_name(game_name)


async def _verify(service, identifier: str, game_name: str):
    return await service._verify_monitor(identifier, game_name)


register_identifier_resolver(
    IdentifierResolverSpec(
        platform="monitor",
        collector_ids=("monitor",),
        resolve=_resolve,
        verify=_verify,
        pipeline_prefixes=("monitor",),
        existing_target_params=("siteurl",),
        output_target_param="siteurl",
    ),
    owner="autoflux-plugin-monitor",
)
