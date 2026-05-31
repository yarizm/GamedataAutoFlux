"""Smart Collector 集成测试 — 实际调用 LLM 验证提取效果。

运行: pytest tests/test_smart_collector_integration.py -v -m integration
"""

import pytest

from src.collectors.base import CollectTarget
from src.collectors.official_site_collector import OfficialSiteCollector


@pytest.mark.integration
@pytest.mark.asyncio
async def test_smart_mode_extracts_from_lol_news():
    """用 LLM 从 League of Legends 新闻页提取。"""
    from src.collectors.llm_extractor import extract_items_from_html
    import httpx

    url = "https://www.leagueoflegends.com/en-us/news/game-updates/"
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            pytest.skip(f"Cannot fetch {url}: HTTP {resp.status_code}")
        html = resp.text

    items = await extract_items_from_html(html, url)
    print(f"\n[LLM 提取结果] 从 {url} 提取到 {len(items)} 条:")
    for i, item in enumerate(items[:5]):
        print(f"  {i+1}. [{item.get('category', '?')}] {item.get('title', '?')} ({item.get('date', '?')})")

    assert len(items) > 0, "LLM 应至少提取到 1 条新闻"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_smart_mode_extracts_from_game_official_site():
    """用 smart 模式从游戏官网采集（Playwright 渲染）。"""
    collector = OfficialSiteCollector({"mode": "smart", "max_pages": 2})
    await collector.setup()

    target = CollectTarget(
        name="Genshin Impact",
        params={"official_url": "https://genshin.hoyoverse.com/en/news", "mode": "smart"},
    )

    try:
        result = await collector.collect(target)
        meta = result.data.get("source_meta", {}) if result.success else {}
        print(f"\n[原神官网] success={result.success}")
        print(f"  pages_discovered={meta.get('pages_discovered')}, pages_crawled={meta.get('pages_crawled')}")
        if result.success:
            data = result.data
            news = data.get("news", {}).get("items", [])
            patches = data.get("patch_notes", {}).get("items", [])
            print(f"  news={len(news)}, patches={len(patches)}")
            for item in (news + patches)[:5]:
                print(f"  - [{item.get('category')}] {item.get('title')} ({item.get('date')})")
        else:
            print(f"  error: {result.error}")
    finally:
        await collector.teardown()
