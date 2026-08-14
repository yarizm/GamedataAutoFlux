"""Identifier discovery registration for the TapTap plugin."""

from src.core.identifier_resolvers import IdentifierResolverSpec, register_identifier_resolver


async def _resolve(service, game_name: str):
    return await service.resolve_taptap(game_name)


async def _verify(service, identifier: str, game_name: str):
    return await service._verify_taptap(identifier, game_name)


register_identifier_resolver(
    IdentifierResolverSpec(
        platform="taptap",
        collector_ids=("taptap",),
        resolve=_resolve,
        verify=_verify,
        requires_shared_browser=True,
        pipeline_prefixes=("taptap",),
        existing_target_params=("app_id", "url"),
        output_target_param="app_id",
    ),
    owner="autoflux-plugin-taptap",
)
