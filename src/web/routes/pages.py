"""
页面路由 — 渲染 WebUI 前端页面
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from fastapi import APIRouter, Request

router = APIRouter(tags=["pages"])

_MANIFEST_CACHE: dict | None = None


def _get_templates():
    from src.web.app import templates
    return templates


def _is_vite_dev() -> bool:
    return os.environ.get("VITE_DEV", "").lower() in ("1", "true", "yes")


def _read_manifest() -> dict:
    manifest_path = Path(__file__).parent.parent / "static" / "dist" / ".vite" / "manifest.json"
    if manifest_path.exists():
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    return {}


def _vite_assets() -> dict[str, str]:
    manifest = _read_manifest()
    entry = manifest.get("src/main.js", {})
    js_file = entry.get("file", "")
    css_files = entry.get("css", [])
    return {
        "js": js_file,
        "css": css_files[0] if css_files else "",
    }


@router.get("/")
async def index(request: Request):
    """主页"""
    assets = _vite_assets()
    return _get_templates().TemplateResponse(
        request=request,
        name="index.html",
        context={
            "request": request,
            "vite_dev": _is_vite_dev(),
            "vite_js": assets.get("js", ""),
            "vite_css": assets.get("css", ""),
        },
    )
