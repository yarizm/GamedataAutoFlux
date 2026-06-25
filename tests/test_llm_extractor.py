import json
from unittest.mock import AsyncMock, patch

import pytest

from src.collectors.llm_extractor import (
    extract_items_from_html,
    verify_game_candidate,
    _parse_llm_json,
    _is_truncated_json,
)


def test_parse_llm_json_valid():
    text = '[{"title": "Test", "date": "2026-01-01"}]'
    result = _parse_llm_json(text)
    assert len(result) == 1
    assert result[0]["title"] == "Test"


def test_parse_llm_json_with_markdown_fence():
    text = '```json\n[{"title": "Test"}]\n```'
    result = _parse_llm_json(text)
    assert len(result) == 1


def test_parse_llm_json_invalid():
    result = _parse_llm_json("not json at all")
    assert result is None


def test_parse_llm_json_empty_array():
    result = _parse_llm_json("[]")
    assert result == []


def test_is_truncated_json():
    assert _is_truncated_json('[{"title": "test"') is True
    assert _is_truncated_json('[{"title": "test"}]') is False
    assert _is_truncated_json('{"key": "val"') is True
    assert _is_truncated_json('{"key": "val"}') is False
    assert _is_truncated_json("not json") is False


def _make_mock_llm(response_content: str) -> AsyncMock:
    mock_llm = AsyncMock()
    mock_llm.model_name = "test-model"
    mock_response = AsyncMock()
    mock_response.content = response_content
    mock_llm.ainvoke = AsyncMock(return_value=mock_response)
    return mock_llm


@pytest.mark.asyncio
async def test_extract_items_from_html_success():
    mock_llm = _make_mock_llm(
        json.dumps(
            [
                {
                    "title": "Patch 1.2",
                    "date": "2026-05-01",
                    "url": "/news/1",
                    "category": "patch",
                    "summary": "Big update",
                }
            ]
        )
    )

    with patch("src.collectors.llm_extractor._get_extraction_llms", return_value=[mock_llm]):
        items = await extract_items_from_html(
            "<html><body>test</body></html>", "https://example.com"
        )

    assert len(items) == 1
    assert items[0]["title"] == "Patch 1.2"
    assert items[0]["url"] == "https://example.com/news/1"


@pytest.mark.asyncio
async def test_extract_items_from_html_llm_returns_invalid():
    mock_llm = _make_mock_llm("I can't extract that")

    with patch("src.collectors.llm_extractor._get_extraction_llms", return_value=[mock_llm]):
        items = await extract_items_from_html(
            "<html><body>test</body></html>", "https://example.com"
        )

    assert items == []


@pytest.mark.asyncio
async def test_extract_items_from_html_fallback_on_truncation():
    """主模型返回截断 JSON 时应自动回退到下一个模型。"""
    mock_bad = _make_mock_llm('[{"title": "Truncated')
    mock_good = _make_mock_llm(
        json.dumps(
            [
                {
                    "title": "Fallback Result",
                    "date": "2026-01-01",
                    "url": "/x",
                    "category": "news",
                    "summary": "ok",
                }
            ]
        )
    )

    with patch(
        "src.collectors.llm_extractor._get_extraction_llms", return_value=[mock_bad, mock_good]
    ):
        items = await extract_items_from_html(
            "<html><body>test</body></html>", "https://example.com"
        )

    assert len(items) == 1
    assert items[0]["title"] == "Fallback Result"
    mock_bad.ainvoke.assert_called_once()
    mock_good.ainvoke.assert_called_once()


@pytest.mark.asyncio
async def test_verify_game_candidate_match():
    mock_llm = _make_mock_llm(
        json.dumps({"matched_index": 1, "confidence": 0.9, "reason": "App ID matches"})
    )

    candidates = [
        {"displaytext": "Counter-Strike 2", "siteurl": "counter-strike_2"},
        {"displaytext": "Delta Force", "siteurl": "delta_force"},
    ]

    with patch("src.collectors.llm_extractor._get_extraction_llms", return_value=[mock_llm]):
        result = await verify_game_candidate(candidates, "Delta Force", 2507950, None)

    assert result["matched_index"] == 1
    assert result["confidence"] == 0.9


@pytest.mark.asyncio
async def test_verify_game_candidate_no_match():
    mock_llm = _make_mock_llm(
        json.dumps({"matched_index": -1, "confidence": 0.0, "reason": "No match found"})
    )

    with patch("src.collectors.llm_extractor._get_extraction_llms", return_value=[mock_llm]):
        result = await verify_game_candidate([], "Unknown Game", 999999, None)

    assert result["matched_index"] == -1
