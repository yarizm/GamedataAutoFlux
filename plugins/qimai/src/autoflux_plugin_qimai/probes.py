"""Local browser-session probes owned by the Qimai plugin."""

from pathlib import Path
from typing import Any

from src.core import collector_probes as core_probes
from src.core.collector_probes import ProbeResult, register_collector_probe
from src.core.config import get_root_dir
from src.core.errors import ErrorCode


async def _probe_session(collector_id: str, targets: list[dict[str, Any]]) -> ProbeResult:
    del targets
    mode = str(core_probes.get_config("qimai.session_mode", "local_profile") or "").strip()
    if mode == "managed_state":
        state_path = str(core_probes.get_config("qimai.storage_state_path", "") or "").strip()
        if not state_path:
            return ProbeResult(
                collector_id,
                "qimai.session_asset",
                "error",
                "Managed-state mode requires qimai.storage_state_path",
                ErrorCode.missing_credentials.value,
            )
        path = Path(state_path)
        if not path.is_absolute():
            path = get_root_dir() / path
        if not path.is_file():
            return ProbeResult(
                collector_id,
                "qimai.session_asset",
                "error",
                f"Qimai storage state does not exist: {path}",
                ErrorCode.missing_credentials.value,
            )
        return ProbeResult(
            collector_id,
            "qimai.session_asset",
            "ok",
            "Qimai managed storage state is available",
        )

    profile = str(core_probes.get_config("qimai.user_data_dir", "") or "").strip()
    if not profile:
        return ProbeResult(
            collector_id,
            "qimai.session_asset",
            "warning",
            "Qimai local-profile mode uses the default browser profile path",
        )
    path = Path(profile)
    if not path.is_absolute():
        path = get_root_dir() / path
    status = "ok" if path.exists() else "warning"
    return ProbeResult(
        collector_id,
        "qimai.session_asset",
        status,
        f"Qimai browser profile {'exists' if path.exists() else 'does not exist yet'}: {path}",
    )


register_collector_probe(
    "qimai",
    "qimai.session_asset",
    _probe_session,
    owner="autoflux-plugin-qimai",
)
