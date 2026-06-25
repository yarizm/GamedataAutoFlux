import json
from unittest.mock import AsyncMock, patch

import pytest

from src.collectors.official_site_collector import (
    _resolve_collector_mode,
    _should_fallback_to_smart,
)


def test_resolve_collector_mode_default():
    assert _resolve_collector_mode({}) == "fast"


def test_resolve_collector_mode_explicit():
    assert _resolve_collector_mode({"mode": "smart"}) == "smart"
    assert _resolve_collector_mode({"mode": "auto"}) == "auto"


def test_should_fallback_empty_items():
    assert _should_fallback_to_smart([]) is True


def test_should_fallback_nonempty_items():
    items = [{"title": "News", "url": "https://example.com/1"}]
    assert _should_fallback_to_smart(items) is False


@pytest.mark.asyncio
async def test_smart_mode_uses_llm_extraction():
    mock_llm = AsyncMock()
    mock_llm.model_name = "test-model"
    mock_response = AsyncMock()
    mock_response.content = json.dumps(
        [
            {
                "title": "Patch 2.0",
                "date": "2026-06-01",
                "url": "/patch/2.0",
                "category": "patch",
                "summary": "Big update",
            }
        ]
    )
    mock_llm.ainvoke = AsyncMock(return_value=mock_response)

    html = "<html><body><article><h1>Patch 2.0</h1><p>2026-06-01</p></article></body></html>"

    with patch("src.collectors.llm_extractor._get_extraction_llms", return_value=[mock_llm]):
        from src.collectors.llm_extractor import extract_items_from_html

        items = await extract_items_from_html(html, "https://example.com")

    assert len(items) == 1
    assert items[0]["title"] == "Patch 2.0"
