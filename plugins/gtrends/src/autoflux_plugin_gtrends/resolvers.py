"""Identifier discovery registration for the Google Trends plugin."""

from src.core.identifier_resolvers import IdentifierResolverSpec, register_identifier_resolver


async def _resolve(service, game_name: str):
    return await service.resolve_gtrends(game_name)


register_identifier_resolver(
    IdentifierResolverSpec(
        platform="gtrends",
        collector_ids=("gtrends",),
        resolve=_resolve,
    ),
    owner="autoflux-plugin-gtrends",
)
