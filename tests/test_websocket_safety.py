import pytest

from src.web.routes.ws import websocket_endpoint


@pytest.mark.asyncio
async def test_websocket_exception_log_is_redacted(monkeypatch) -> None:
    captured: list[str] = []

    def capture_warning(message: str, *args) -> None:
        captured.append(str(message).format(*args))

    class FailingWebSocket:
        async def accept(self) -> None:
            pass

        async def receive_text(self) -> str:
            raise RuntimeError("socket failed access_token=socket-secret")

    monkeypatch.setattr("src.web.routes.ws.logger.warning", capture_warning)

    await websocket_endpoint(FailingWebSocket())

    rendered = " ".join(captured)
    assert "socket-secret" not in rendered
    assert "access_token=[REDACTED]" in rendered
