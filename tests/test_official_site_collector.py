import asyncio

from src.collectors.base import CollectTarget
from src.collectors.official_site_collector import (
    FetchResult,
    OfficialSiteCollector,
    _classify_category,
    _parse_html,
)
from src.core.registry import registry
from src.reporting.data_extractor import _detect_collector, extract_from_records


HOME_HTML = """
<html>
  <head>
    <title>Example Game Official Site</title>
    <meta property="og:site_name" content="Example Game">
    <meta property="og:description" content="Official website for Example Game.">
  </head>
  <body>
    <main>
      <a href="/news/launch">Launch News</a>
      <a href="/updates/patch-1-2">Patch Notes</a>
      <a href="/events/summer-festival">Summer Event</a>
      <a href="/privacy">Privacy Policy</a>
      <a href="/login">Login</a>
    </main>
  </body>
</html>
"""

NEWS_HTML = """
<html>
  <head>
    <title>Launch News</title>
    <meta property="article:published_time" content="2026-04-20">
  </head>
  <body>
    <article>
      <h1>Launch News</h1>
      <p>2026-04-20</p>
      <p>Example Game launches a new season with rewards, missions, and balance changes for all players.</p>
      <p>This official news article has enough body text to be considered meaningful content by the collector.</p>
    </article>
  </body>
</html>
"""

PATCH_HTML = """
<html><head><title>Patch 1.2 Update</title></head>
<body><article><p>2026/04/18</p><p>Patch notes and update details include fixes, balance changes, and new version content for the game.</p><p>Additional patch text keeps the article long enough for extraction.</p></article></body></html>
"""

EVENT_HTML = """
<html><head><title>Summer Festival Event</title></head>
<body><main><p>2026.04.22</p><p>The summer event campaign adds limited quests, login rewards, and themed challenges.</p><p>More event details are included on the official website for players.</p></main></body></html>
"""


def test_official_site_collector_registered():
    assert registry.get("collector", "official_site") is OfficialSiteCollector


def test_parse_html_extracts_title_date_and_content():
    parsed = _parse_html("https://example.com/news/launch", NEWS_HTML)

    assert parsed.title == "Launch News"
    assert parsed.published_at == "2026-04-20"
    assert "new season" in parsed.content


def test_category_classification():
    assert _classify_category("https://x.test/patch-notes", "Patch 1.2", "") == "patch"
    assert _classify_category("https://x.test/events/summer", "Festival", "") == "event"
    assert _classify_category("https://x.test/news/hello", "Hello", "") == "news"


def test_discovery_filters_include_and_exclude_patterns(monkeypatch):
    pages = {
        "https://example.com": HOME_HTML,
        "https://example.com/sitemap.xml": "",
    }
    collector = OfficialSiteCollector({"playwright_enabled": False})

    async def fake_fetch(url: str, *, use_playwright: str = "auto"):
        return FetchResult(url, 200 if url in pages else 404, pages.get(url, ""))

    monkeypatch.setattr(collector, "_fetch_page", fake_fetch)
    home = _parse_html("https://example.com", HOME_HTML)

    candidates = asyncio.run(
        collector._discover_pages(
            entry_url="https://example.com",
            home=home,
            max_pages=10,
            max_depth=1,
            include_patterns=("news", "update", "patch", "event"),
            exclude_patterns=("privacy", "terms", "login"),
            only_same_domain=True,
            use_playwright="never",
        )
    )
    urls = [item.url for item in candidates]

    assert "https://example.com/news/launch" in urls
    assert "https://example.com/updates/patch-1-2" in urls
    assert "https://example.com/events/summer-festival" in urls
    assert "https://example.com/privacy" not in urls
    assert "https://example.com/login" not in urls


def test_collect_returns_standard_structure(monkeypatch):
    pages = {
        "https://example.com": HOME_HTML,
        "https://example.com/news/launch": NEWS_HTML,
        "https://example.com/updates/patch-1-2": PATCH_HTML,
        "https://example.com/events/summer-festival": EVENT_HTML,
        "https://example.com/sitemap.xml": "",
    }
    collector = OfficialSiteCollector({"playwright_enabled": False})

    async def fake_fetch(url: str, *, use_playwright: str = "auto"):
        clean = url.rstrip("/")
        return FetchResult(clean, 200 if clean in pages else 404, pages.get(clean, ""))

    monkeypatch.setattr(collector, "_fetch_page", fake_fetch)
    result = asyncio.run(
        collector.collect(
            CollectTarget(
                name="Example Game",
                params={"official_url": "https://example.com", "max_pages": 20, "max_depth": 1, "use_playwright": "never"},
            )
        )
    )

    assert result.success is True
    data = result.data
    assert data["collector"] == "official_site"
    assert data["game_name"] == "Example Game"
    assert data["official_url"] == "https://example.com"
    assert len(data["news"]["items"]) == 1
    assert len(data["patch_notes"]["items"]) == 1
    assert len(data["events"]["items"]) == 1
    assert data["snapshot"]["news_count"] == 1
    assert "social_links" not in data
    assert "store_links" not in data
    assert "game" not in data


def test_collect_uses_config_recipe_when_url_is_missing(monkeypatch):
    pages = {
        "https://arcraiders.com/news": HOME_HTML,
        "https://arcraiders.com/news/launch": NEWS_HTML,
        "https://arcraiders.com/updates/patch-1-2": PATCH_HTML,
        "https://arcraiders.com/events/summer-festival": EVENT_HTML,
        "https://arcraiders.com/sitemap.xml": "",
    }
    collector = OfficialSiteCollector({"playwright_enabled": False})

    async def fake_fetch(url: str, *, use_playwright: str = "auto"):
        clean = url.rstrip("/")
        return FetchResult(clean, 200 if clean in pages else 404, pages.get(clean, ""))

    monkeypatch.setattr(collector, "_fetch_page", fake_fetch)
    result = asyncio.run(
        collector.collect(
            CollectTarget(
                name="Arc Raiders",
                params={"max_pages": 20, "max_depth": 1, "use_playwright": "never"},
            )
        )
    )

    assert result.success is True
    assert result.data["official_url"] == "https://arcraiders.com/news"
    assert result.data["source_meta"]["recipe"] == "Arc Raiders"
    assert len(result.data["news"]["items"]) == 1


def test_data_extractor_detects_and_extracts_official_site():
    record = {
        "collector": "official_site",
        "game_name": "Example Game",
        "official_url": "https://example.com",
        "source_meta": {"collected_at": "2026-04-29T12:00:00Z"},
        "news": {"items": [{"title": "News", "date": "2026-04-20", "summary": "Summary", "url": "https://example.com/news"}]},
        "patch_notes": {"items": [{"title": "Patch", "date": "2026-04-21", "summary": "Patch summary", "url": "https://example.com/patch"}]},
        "events": {"items": [{"title": "Event", "date": "2026-04-22", "summary": "Event summary", "url": "https://example.com/event"}]},
        "snapshot": {"name": "Example Game", "latest_news_title": "News", "latest_news_date": "2026-04-20"},
    }

    data = extract_from_records([record])

    assert _detect_collector(record) == "official_site"
    assert data.overview[0]["数据来源"] == "官方网站"
    assert len(data.events) == 3
    assert {item["标题"] for item in data.events} == {"News", "Patch", "Event"}
