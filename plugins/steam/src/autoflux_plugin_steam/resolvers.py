"""Identifier discovery registration for the Steam plugin."""

from src.core.identifier_resolvers import IdentifierResolverSpec, register_identifier_resolver


async def _resolve(service, game_name: str):
    return await service.resolve_steam(game_name)


async def _verify(service, identifier: str, game_name: str):
    return await service._verify_steam(identifier, game_name)


register_identifier_resolver(
    IdentifierResolverSpec(
        platform="steam",
        collector_ids=("steam", "steam_discussions"),
        resolve=_resolve,
        verify=_verify,
        pipeline_prefixes=("steam",),
        existing_target_params=("app_id",),
        output_target_param="app_id",
        output_value_type="integer",
    ),
    owner="autoflux-plugin-steam",
)
