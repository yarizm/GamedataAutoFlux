import httpx
import pytest

from src.collectors.base import CollectTarget
from src.collectors.official_site_collector import (
    FetchResult,
    OfficialSiteCollector,
    _safe_log_text as official_site_safe_log_text,
)
from src.collectors.steam_discussions_collector import (
    SteamDiscussionsCollector,
    _safe_log_text as steam_discussions_safe_log_text,
)
from src.collectors.steam_collector import (
    SteamCollector,
    _safe_log_text as steam_collector_safe_log_text,
)
from src.collectors.steam.steam_api_client import (
    _safe_log_text as steam_api_safe_log_text,
)
from src.collectors.steam.steamdb_scraper import (
    SteamDBScraper,
    _safe_log_text as steamdb_safe_log_text,
)
from src.collectors.gtrends.firecrawl_fallback import (
    _safe_log_text as gtrends_firecrawl_safe_log_text,
)
from src.collectors.monitor_collector import (
    MonitorCollector,
    _safe_log_text as monitor_safe_log_text,
)
from src.collectors.qimai_collector import (
    QimaiCollector,
    _safe_log_text as qimai_safe_log_text,
)
from src.collectors.steam.firecrawl_fallback import (
    _safe_log_text as steam_firecrawl_safe_log_text,
)
from src.collectors.taptap_collector import (
    TapTapCollector,
    _safe_log_text as taptap_safe_log_text,
)
from src.collectors.taptap.firecrawl_fallback import (
    _safe_log_text as taptap_firecrawl_safe_log_text,
)
from src.collectors.taptap.playwright_scraper import (
    TapTapPlaywrightFailed,
    TapTapPlaywrightScraper,
    _safe_log_text as taptap_playwright_safe_log_text,
)
from src.core.sensitive import redact_sensitive_text


def test_redact_sensitive_text_handles_compound_query_keys() -> None:
    safe = redact_sensitive_text(
        "https://example.com/path?access_token=secret-token"
        "&firecrawl_api_key=secret-key&ok=1"
    )

    assert "secret-token" not in safe
    assert "secret-key" not in safe
    assert "access_token=[REDACTED]" in safe
    assert "firecrawl_api_key=[REDACTED]" in safe
    assert "&ok=1" in safe


def test_collector_safe_log_helpers_redact_url_query_secrets() -> None:
    text = "https://example.com/?access_token=secret-token&api_key=secret-key"

    for safe_log_text in (
        taptap_safe_log_text,
        steam_discussions_safe_log_text,
        monitor_safe_log_text,
        taptap_firecrawl_safe_log_text,
        steam_firecrawl_safe_log_text,
        steam_collector_safe_log_text,
        steam_api_safe_log_text,
        steamdb_safe_log_text,
        taptap_playwright_safe_log_text,
        gtrends_firecrawl_safe_log_text,
        official_site_safe_log_text,
        qimai_safe_log_text,
    ):
        safe = safe_log_text(text)
        assert "secret-token" not in safe
        assert "secret-key" not in safe
        assert "access_token=[REDACTED]" in safe
        assert "api_key=[REDACTED]" in safe


@pytest.mark.asyncio
async def test_taptap_retry_log_redacts_url_and_error(monkeypatch) -> None:
    captured: list[str] = []

    def capture_warning(message: str, *args) -> None:
        captured.append(str(message).format(*args))

    class FailingClient:
        async def get(self, url: str, **kwargs):
            request = httpx.Request("GET", url)
            raise httpx.ConnectError(
                "connect failed access_token=error-secret",
                request=request,
            )

    monkeypatch.setattr("src.collectors.taptap_collector.logger.warning", capture_warning)
    collector = TapTapCollector({"request_retries": 2, "request_delay": 0})
    collector._client = FailingClient()

    with pytest.raises(httpx.HTTPError):
        await collector._fetch_with_retry(
            "https://example.com/app/1?access_token=url-secret"
        )

    rendered = " ".join(captured)
    assert "url-secret" not in rendered
    assert "error-secret" not in rendered
    assert "access_token=[REDACTED]" in rendered


@pytest.mark.asyncio
async def test_steam_firecrawl_fallback_error_is_redacted(monkeypatch) -> None:
    captured: list[str] = []

    def capture_error(message: str, *args) -> None:
        captured.append(str(message).format(*args))

    class FailingFirecrawl:
        async def scrape(self, *args, **kwargs):
            raise RuntimeError("firecrawl failed api_key=firecrawl-secret")

    monkeypatch.setattr("src.collectors.steam_collector.logger.error", capture_error)
    collector = SteamCollector({})
    collector._firecrawl = FailingFirecrawl()

    result = await collector._run_firecrawl_fallback(
        "730",
        "SteamDB failed",
        cookie="cookie-secret",
    )

    rendered = " ".join(captured)
    assert "firecrawl-secret" not in rendered
    assert "firecrawl-secret" not in result["error"]
    assert "api_key=[REDACTED]" in rendered
    assert "api_key=[REDACTED]" in result["error"]


@pytest.mark.asyncio
async def test_official_site_httpx_log_and_error_are_redacted(monkeypatch) -> None:
    captured: list[str] = []

    def capture_debug(message: str, *args) -> None:
        captured.append(str(message).format(*args))

    class FailingClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url: str):
            request = httpx.Request("GET", url)
            raise httpx.ConnectError(
                "connect failed token=error-secret",
                request=request,
            )

    monkeypatch.setattr("src.collectors.official_site_collector.logger.debug", capture_debug)
    monkeypatch.setattr("src.collectors.official_site_collector.httpx.AsyncClient", FailingClient)

    collector = OfficialSiteCollector({"playwright_enabled": False})
    result = await collector._fetch_with_httpx(
        "https://example.com/news?access_token=url-secret"
    )

    rendered = " ".join(captured)
    assert result.error == "connect failed token=[REDACTED]"
    assert "url-secret" not in rendered
    assert "error-secret" not in rendered
    assert "access_token=[REDACTED]" in rendered
    assert "token=[REDACTED]" in rendered


@pytest.mark.asyncio
async def test_official_site_collect_redacts_returned_urls_and_warnings(monkeypatch) -> None:
    home_html = """
    <html><body>
      <a href="/news?access_token=candidate-secret">News</a>
    </body></html>
    """
    collector = OfficialSiteCollector({"playwright_enabled": False})

    async def fake_fetch(
        url: str,
        *,
        use_playwright: str = "auto",
        wait_for_networkidle: bool = False,
    ) -> FetchResult:
        if "sitemap.xml" in url:
            return FetchResult(url, 404, "")
        if "news" in url:
            return FetchResult(url, 500, "", error="failed token=page-secret")
        return FetchResult(url, 200, home_html)

    monkeypatch.setattr(collector, "_fetch_page", fake_fetch)

    result = await collector.collect(
        CollectTarget(
            name="Official Game",
            params={
                "official_url": "https://example.com?access_token=entry-secret",
                "max_depth": 1,
                "use_playwright": "never",
                "validation_status": "token=status-secret",
                "validation_notes": "api_key=notes-secret",
            },
        )
    )

    rendered = str(result.data)
    assert result.success is True
    assert "entry-secret" not in rendered
    assert "candidate-secret" not in rendered
    assert "status-secret" not in rendered
    assert "notes-secret" not in rendered
    assert "access_token=[REDACTED]" in result.data["official_url"]
    assert "access_token=[REDACTED]" in result.data["source_meta"]["warnings"][0]
    assert result.data["source_meta"]["validation_status"] == "token=[REDACTED]"
    assert result.data["source_meta"]["validation_notes"] == "api_key=[REDACTED]"


@pytest.mark.asyncio
async def test_steamdb_sales_navigation_error_is_redacted(monkeypatch) -> None:
    captured: list[str] = []

    def capture_info(message: str, *args) -> None:
        captured.append(str(message).format(*args))

    async def fail_navigation(*args, **kwargs):
        raise RuntimeError("navigation failed access_token=nav-secret")

    monkeypatch.setattr("src.collectors.steam.steamdb_scraper.logger.info", capture_info)
    monkeypatch.setattr(
        "src.collectors.steam.steamdb_scraper._navigate_by_click_async",
        fail_navigation,
    )

    scraper = SteamDBScraper()
    result = await scraper._scrape_sales(
        object(),
        "730?access_token=url-secret",
    )

    rendered = " ".join(captured)
    assert "url-secret" not in rendered
    assert "nav-secret" not in result["error"]
    assert "access_token=[REDACTED]" in rendered
    assert "access_token=[REDACTED]" in result["error"]


@pytest.mark.asyncio
async def test_taptap_playwright_visit_log_and_error_are_redacted(monkeypatch) -> None:
    captured: list[str] = []

    def capture_info(message: str, *args) -> None:
        captured.append(str(message).format(*args))

    class FailingPage:
        async def goto(self, url: str, **kwargs):
            raise RuntimeError("page failed token=page-secret")

    monkeypatch.setattr(
        "src.collectors.taptap.playwright_scraper.logger.info",
        capture_info,
    )

    scraper = TapTapPlaywrightScraper()
    with pytest.raises(TapTapPlaywrightFailed) as exc_info:
        await scraper._load_page_html(
            FailingPage(),
            "https://www.taptap.cn/app/1?access_token=url-secret",
            kind="detail",
        )

    rendered = " ".join(captured)
    error = str(exc_info.value)
    assert "url-secret" not in rendered
    assert "page-secret" not in error
    assert "access_token=[REDACTED]" in rendered
    assert "token=[REDACTED]" in error


@pytest.mark.asyncio
async def test_monitor_retry_log_redacts_url_and_error(monkeypatch) -> None:
    captured: list[str] = []

    def capture_warning(message: str, *args) -> None:
        captured.append(str(message).format(*args))

    class FailingClient:
        async def get(self, url: str, **kwargs):
            request = httpx.Request("GET", url)
            raise httpx.ConnectError(
                "connect failed access_token=error-secret",
                request=request,
            )

    monkeypatch.setattr("src.collectors.monitor_collector.logger.warning", capture_warning)
    collector = MonitorCollector({"request_delay": 0})
    collector._client = FailingClient()

    with pytest.raises(httpx.HTTPError):
        await collector._fetch_text("https://example.com/?access_token=url-secret")

    rendered = " ".join(captured)
    assert "url-secret" not in rendered
    assert "error-secret" not in rendered
    assert "access_token=[REDACTED]" in rendered


@pytest.mark.asyncio
async def test_monitor_metric_failure_redacts_warning_and_raw_error(monkeypatch) -> None:
    captured: list[str] = []

    def capture_warning(message: str, *args) -> None:
        captured.append(str(message).format(*args))

    async def fail_metric(**kwargs):
        raise RuntimeError("metric failed token=metric-secret")

    monkeypatch.setattr("src.collectors.monitor_collector.logger.warning", capture_warning)
    collector = MonitorCollector()
    collector._metric_concurrency = 1
    monkeypatch.setattr(collector, "_collect_twitch_metric", fail_metric)
    warnings: list[str] = []

    result = await collector._collect_metrics_concurrently(
        metrics=["twitch_viewer_trend"],
        app_id=730,
        target_name="api_key=target-secret",
        days=30,
        tz_name="Asia/Shanghai",
        twitch_name=None,
        siteurl="https://example.com?api_key=url-secret",
        warnings=warnings,
    )
    rendered = " ".join(captured) + str(warnings) + str(result)

    assert "metric-secret" not in rendered
    assert "target-secret" not in rendered
    assert "token=[REDACTED]" in rendered
    assert result["twitch_viewer_trend"]["error"] == "metric failed token=[REDACTED]"
    assert warnings == ["twitch_viewer_trend: metric failed token=[REDACTED]"]


@pytest.mark.asyncio
async def test_qimai_collect_failure_redacts_error_and_logs(monkeypatch) -> None:
    captured: list[str] = []

    def capture_error(message: str, *args) -> None:
        captured.append(str(message).format(*args))

    def capture_info(message: str, *args) -> None:
        captured.append(str(message).format(*args))

    async def fail_scrape(app_id: str, country: str):
        raise RuntimeError("qimai failed token=qimai-secret")

    monkeypatch.setattr("src.collectors.qimai_collector.logger.error", capture_error)
    monkeypatch.setattr("src.collectors.qimai_collector.logger.info", capture_info)
    collector = QimaiCollector()
    monkeypatch.setattr(collector, "_should_use_threaded_playwright", lambda: False)
    monkeypatch.setattr(collector, "_scrape_async", fail_scrape)

    result = await collector.collect(
        CollectTarget(name="api_key=target-secret", params={"qimai_app_id": "123"})
    )
    rendered = " ".join(captured) + str(result.to_summary())

    assert result.success is False
    assert result.error == "Qimai collection failed: qimai failed token=[REDACTED]"
    assert "qimai-secret" not in rendered
    assert "target-secret" not in rendered
    assert "token=[REDACTED]" in rendered
    assert "api_key=[REDACTED]" in rendered


@pytest.mark.asyncio
async def test_steam_discussions_retry_log_redacts_url_and_error(monkeypatch) -> None:
    captured: list[str] = []

    def capture_warning(message: str, *args) -> None:
        captured.append(str(message).format(*args))

    class FailingClient:
        async def get(self, url: str):
            request = httpx.Request("GET", url)
            raise httpx.ConnectError(
                "connect failed access_token=error-secret",
                request=request,
            )

    monkeypatch.setattr(
        "src.collectors.steam_discussions_collector.logger.warning",
        capture_warning,
    )
    collector = SteamDiscussionsCollector({"request_delay": 0})
    collector._client = FailingClient()

    with pytest.raises(httpx.HTTPError):
        await collector._fetch_text(
            "https://steamcommunity.com/app/730/discussions/?access_token=url-secret"
        )

    rendered = " ".join(captured)
    assert "url-secret" not in rendered
    assert "error-secret" not in rendered
    assert "access_token=[REDACTED]" in rendered
