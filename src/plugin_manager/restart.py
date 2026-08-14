"""Deployment-aware restart controller contract."""

from __future__ import annotations

import os
from typing import Protocol


class RestartUnavailableError(RuntimeError):
    code = "PLUGIN_RESTART_UNAVAILABLE"


class RestartController(Protocol):
    name: str

    def available(self) -> bool: ...

    def payload(self) -> dict[str, object]: ...

    async def request_restart(self) -> None: ...


class ManualRestartController:
    """Safe default: explain the deployment action without stopping the process."""

    name = "manual"

    def available(self) -> bool:
        return False

    def payload(self) -> dict[str, object]:
        instructions = os.getenv("AUTOFLUX_RESTART_INSTRUCTIONS", "").strip()
        if not instructions:
            instructions = (
                "Restart the GamedataAutoFlux service with your process manager "
                "or recreate the application container."
            )
        return {
            "name": self.name,
            "available": False,
            "instructions": instructions,
        }

    async def request_restart(self) -> None:
        raise RestartUnavailableError(str(self.payload()["instructions"]))


restart_controller: RestartController = ManualRestartController()
