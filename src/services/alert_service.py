import httpx
from loguru import logger
from src.core.config import get_settings


class AlertService:
    _instance = None
    import threading
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "AlertService":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._client = None

    def _get_client(self) -> httpx.AsyncClient:
        # 安全说明：此方法无 await，在 asyncio 单线程事件循环中是原子的。
        # 如果未来添加 await，需引入 asyncio.Lock 防止并发创建。
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=10.0)
        return self._client

    async def send_alert(self, title: str, content: str, level: str = "error", **kwargs):
        """
        Send an alert based on global configuration.
        level: "info", "warning", "error", "critical"
        """
        settings = get_settings()
        alert_cfg = settings.get("alerts", {})

        if not alert_cfg.get("enabled", False):
            return

        webhook_type = alert_cfg.get("type", "dingtalk").lower()
        webhook_url = alert_cfg.get("webhook_url", "")

        if not webhook_url:
            logger.warning("Alerting is enabled but no webhook_url is configured.")
            return

        try:
            if webhook_type == "dingtalk":
                await self._send_dingtalk(webhook_url, title, content, level)
            elif webhook_type == "discord":
                await self._send_discord(webhook_url, title, content, level)
            else:
                await self._send_generic(webhook_url, title, content, level)
        except Exception as e:
            logger.error(f"Failed to send alert via {webhook_type}: {e}")

    async def _send_dingtalk(self, url: str, title: str, content: str, level: str):
        color = "#ff0000" if level in ("error", "critical") else "#000000"
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": f"### <font color='{color}'>{title}</font>\n\n{content}",
            },
        }
        client = self._get_client()
        response = await client.post(url, json=payload)
        response.raise_for_status()

    async def _send_discord(self, url: str, title: str, content: str, level: str):
        color_map = {"info": 3447003, "warning": 16776960, "error": 15158332, "critical": 10038562}
        payload = {
            "embeds": [
                {"title": title, "description": content, "color": color_map.get(level, 15158332)}
            ]
        }
        client = self._get_client()
        response = await client.post(url, json=payload)
        response.raise_for_status()

    async def _send_generic(self, url: str, title: str, content: str, level: str):
        payload = {"title": title, "content": content, "level": level}
        client = self._get_client()
        response = await client.post(url, json=payload)
        response.raise_for_status()

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
