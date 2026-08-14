"""Identifier resolver extension tests for third-party collector plugins."""

import pytest

from src.agent.schemas import IdentifierConfidence, IdentifierResult
from src.agent.tools.identifiers import _auto_fill_identifiers
from src.core.identifier_resolvers import (
    IdentifierResolverSpec,
    register_identifier_resolver,
    restore_identifier_resolvers,
    snapshot_identifier_resolvers,
)
from src.core.registry import registry
from src.services.game_resolver import GameIdentifierResolver


@pytest.mark.asyncio
async def test_third_party_identifier_resolver_needs_no_core_platform_branch() -> None:
    registry_snapshot = registry.snapshot()
    resolver_snapshot = snapshot_identifier_resolvers()

    class CustomCollector:
        pass

    async def resolve(service, target_name: str):
        return IdentifierResult(
            platform="custom_catalog",
            identifier="asset-42",
            identifier_type="asset_id",
            game_name=target_name,
            confidence=IdentifierConfidence.HIGH,
            source="plugin",
        )

    async def verify(service, identifier: str, target_name: str):
        return {
            "valid": identifier == "asset-42",
            "platform": "custom_catalog",
            "name": target_name,
        }

    registry.register("collector", "custom_catalog")(CustomCollector)
    register_identifier_resolver(
        IdentifierResolverSpec(
            platform="custom_catalog",
            collector_ids=("custom_catalog",),
            resolve=resolve,
            verify=verify,
            pipeline_prefixes=("custom_catalog",),
            existing_target_params=("asset_id",),
            output_target_param="asset_id",
        ),
        owner="test-custom-catalog",
    )

    service = GameIdentifierResolver()
    try:
        result = await service.resolve_all("Example Asset", ["custom_catalog"])
        verification = await service.verify_identifier(
            "custom_catalog", "asset-42", "Example Asset"
        )
        targets = await _auto_fill_identifiers(
            [{"name": "Example Asset", "params": {}}],
            "custom_catalog_basic",
        )
    finally:
        await service.teardown()
        restore_identifier_resolvers(resolver_snapshot)
        registry.restore(registry_snapshot)

    assert result.platforms["custom_catalog"].identifier == "asset-42"
    assert result.found_platforms() == ["custom_catalog"]
    assert result.high_confidence() == ["custom_catalog"]
    assert verification["valid"] is True
    assert targets[0]["params"]["asset_id"] == "asset-42"
