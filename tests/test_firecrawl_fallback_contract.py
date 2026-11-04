"""Firecrawl 回退与 SDK 的契约测试：真实 firecrawl-py 客户端 + 本地 HTTP 桩，不访问外网。

背景：回退代码曾调用 `scrape_url(url, params=...)`，这个写法只有 firecrawl-py 1.x 支持。
锁定版本升到 2.x 后每次调用都抛 ValueError，又被 except 吞成"返回 None"，三个插件的回退全部失效。
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from autoflux_plugin_gtrends.gtrends.firecrawl_fallback import GtrendsFirecrawlFallback
from autoflux_plugin_steam.steam.firecrawl_fallback import FirecrawlFallback
from autoflux_plugin_taptap.taptap.firecrawl_fallback import TapTapFirecrawlFallback
from firecrawl import FirecrawlApp

_SERIES = [{"title": "Players", "series": [{"name": "Players", "data": [[1, 2]]}]}]


class _FirecrawlStub:
    """记录请求体并返回 Firecrawl v2 形状的响应。"""

    def __init__(self) -> None:
        self.bodies: list[dict[str, Any]] = []
        self.status = 200
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - http.server 约定
                length = int(self.headers.get("Content-Length") or 0)
                stub.bodies.append(json.loads(self.rfile.read(length)))
                payload = {
                    "success": stub.status == 200,
                    "data": {
                        "markdown": "# scraped",
                        "metadata": {"sourceURL": "https://example.com", "statusCode": 200},
                        "actions": {
                            "javascriptReturns": [
                                {"type": "string", "value": json.dumps(_SERIES)},
                            ],
                            "screenshots": [],
                            "scrapes": [],
                            "pdfs": [],
                        },
                    },
                }
                encoded = json.dumps(payload).encode("utf-8")
                self.send_response(stub.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def log_message(self, *args: Any) -> None:
                return

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


@pytest.fixture
def stub(monkeypatch: pytest.MonkeyPatch) -> Iterator[_FirecrawlStub]:
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    server = _FirecrawlStub()
    try:
        yield server
    finally:
        server.close()


async def _ready(fallback: Any, stub: _FirecrawlStub) -> Any:
    await fallback.setup()
    # setup 必须用锁定的 SDK 建出客户端；这里只把地址指向本地桩
    assert isinstance(fallback._app, FirecrawlApp)
    fallback._app = FirecrawlApp(api_key="fc-test", api_url=stub.url)
    return fallback


async def test_steam_charts_scrape_sends_actions_and_reads_js_returns(
    stub: _FirecrawlStub,
) -> None:
    fallback = await _ready(FirecrawlFallback(api_key="fc-test"), stub)

    markdown, series = await fallback._scrape_charts_url(
        "https://steamdb.info/app/730/charts/", headers={"X-Test": "1"}
    )

    assert markdown == "# scraped"
    # JS 返回值是 Highcharts 序列的唯一来源，SDK 丢掉它就只剩 Markdown
    assert series == _SERIES
    body = stub.bodies[0]
    assert body["url"] == "https://steamdb.info/app/730/charts/"
    assert body["headers"] == {"X-Test": "1"}
    assert [action["type"] for action in body["actions"]] == ["wait", "executeJavascript"]
    assert "Highcharts" in body["actions"][1]["script"]


async def test_steam_scrape_without_actions(stub: _FirecrawlStub) -> None:
    fallback = await _ready(FirecrawlFallback(api_key="fc-test"), stub)

    assert await fallback._scrape_url("https://steamdb.info/app/730/info/") == "# scraped"
    assert "actions" not in stub.bodies[0]


async def test_taptap_scrape_returns_markdown(stub: _FirecrawlStub) -> None:
    fallback = await _ready(TapTapFirecrawlFallback(api_key="fc-test"), stub)

    assert await fallback._scrape_url("https://www.taptap.cn/app/1") == "# scraped"
    assert len(stub.bodies) == 1


async def test_gtrends_scrape_forwards_headers(stub: _FirecrawlStub) -> None:
    fallback = await _ready(
        GtrendsFirecrawlFallback(api_key="fc-test", headers={"Accept-Language": "en"}), stub
    )

    assert await fallback._scrape_url("https://trends.google.com/trends/explore") == "# scraped"
    assert stub.bodies[0]["headers"] == {"Accept-Language": "en"}


async def test_scrape_failure_returns_none_instead_of_raising(stub: _FirecrawlStub) -> None:
    fallback = await _ready(TapTapFirecrawlFallback(api_key="fc-test"), stub)
    stub.status = 402

    assert await fallback._scrape_url("https://www.taptap.cn/app/1") is None


@pytest.mark.parametrize(
    "fallback_cls", [FirecrawlFallback, TapTapFirecrawlFallback, GtrendsFirecrawlFallback]
)
async def test_scrape_without_setup_returns_none(fallback_cls: Any) -> None:
    # 没配 api_key 时 setup 不建客户端，回退必须安静地返回空结果
    fallback = fallback_cls(api_key="")
    await fallback.setup()

    assert await fallback._scrape_url("https://example.com") is None
