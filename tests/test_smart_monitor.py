import json
from unittest.mock import AsyncMock, patch

import pytest

from src.collectors.monitor_collector import (
    _resolve_mode,
    _choose_best_sully_siteurl,
)


def test_resolve_mode_default():
    assert _resolve_mode({}) == "fast"


def test_resolve_mode_explicit():
    assert _resolve_mode({"mode": "smart"}) == "smart"
    assert _resolve_mode({"mode": "auto"}) == "auto"
    assert _resolve_mode({"mode": "fast"}) == "fast"


def test_resolve_mode_invalid():
    assert _resolve_mode({"mode": "invalid"}) == "fast"


@pytest.mark.asyncio
async def test_smart_mode_uses_llm_verification():
    mock_llm = AsyncMock()
    mock_llm.model_name = "test-model"
    mock_response = AsyncMock()
    mock_response.content = json.dumps(
        {"matched_index": 0, "confidence": 0.95, "reason": "Exact match"}
    )
    mock_llm.ainvoke = AsyncMock(return_value=mock_response)

    candidates = [
        {"displaytext": "Delta Force", "siteurl": "delta_force_hawk_ops"},
    ]

    with patch("src.collectors.llm_extractor._get_extraction_llms", return_value=[mock_llm]):
        result = await _choose_best_sully_siteurl(
            candidates, ["Delta Force"], mode="smart", game_name="Delta Force", app_id=2507950
        )

    assert result == "delta_force_hawk_ops"
    mock_llm.ainvoke.assert_called_once()


@pytest.mark.asyncio
async def test_smart_mode_low_confidence_returns_none():
    mock_llm = AsyncMock()
    mock_llm.model_name = "test-model"
    mock_response = AsyncMock()
    mock_response.content = json.dumps(
        {"matched_index": 0, "confidence": 0.2, "reason": "Uncertain"}
    )
    mock_llm.ainvoke = AsyncMock(return_value=mock_response)

    candidates = [
        {"displaytext": "Some Other Game", "siteurl": "some_other"},
    ]

    with patch("src.collectors.llm_extractor._get_extraction_llms", return_value=[mock_llm]):
        result = await _choose_best_sully_siteurl(
            candidates,
            ["Delta Force"],
            mode="smart",
            game_name="Delta Force",
            app_id=2507950,
            confidence_threshold=0.5,
        )

    assert result is None


@pytest.mark.asyncio
async def test_fast_mode_uses_sequence_matcher():
    candidates = [
        {"displaytext": "Counter-Strike 2", "siteurl": "counter-strike_2"},
        {"displaytext": "Delta Force: Hawk Ops", "siteurl": "delta_force_hawk_ops"},
    ]
    result = await _choose_best_sully_siteurl(
        candidates, ["Delta Force"], mode="fast", game_name="Delta Force", app_id=2507950
    )
    assert result == "delta_force_hawk_ops"
