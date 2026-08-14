"""Identifier discovery registration for the Qimai plugin."""

from src.core.identifier_resolvers import IdentifierResolverSpec, register_identifier_resolver


async def _resolve(service, game_name: str):
    return await service.resolve_qimai(game_name)


async def _verify(service, identifier: str, game_name: str):
    return await service._verify_qimai(identifier, game_name)


register_identifier_resolver(
    IdentifierResolverSpec(
        platform="qimai",
        collector_ids=("qimai",),
        resolve=_resolve,
        verify=_verify,
        requires_shared_browser=True,
        cdp_config_key="qimai.cdp_port",
        pipeline_prefixes=("qimai",),
        existing_target_params=("app_id", "qimai_app_id"),
        output_target_param="qimai_app_id",
    ),
    owner="autoflux-plugin-qimai",
)
