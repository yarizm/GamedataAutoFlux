"""
页面路由 — 渲染 WebUI 前端页面
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from fastapi import APIRouter, Request, status

from src.web.safety import admin_rejection, is_api_key_auth_enabled

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


def _get_llm_providers() -> list[dict]:
    """读取 LLM provider 列表，用于前端下拉框"""
    from src.reporting.generator import ReportGenerator

    return ReportGenerator.get_providers()


def _vite_assets() -> dict[str, str]:
    manifest = _read_manifest()
    entry = manifest.get("src/main.js", {})
    js_file = entry.get("file", "")
    css_files = entry.get("css", [])
    return {
        "js": js_file,
        "css": css_files[0] if css_files else "",
    }


def _auth_gate_page(request: Request):
    """未通过管理端鉴权时的页面：配了 api_key 给登录表单，免 key 模式说明只允许本机访问。"""
    login_enabled = is_api_key_auth_enabled()
    return _get_templates().TemplateResponse(
        request=request,
        name="login.html",
        context={"mode": "login" if login_enabled else "remote_disabled"},
        status_code=status.HTTP_401_UNAUTHORIZED if login_enabled else status.HTTP_403_FORBIDDEN,
        # 登录后要立刻重新请求本页拿 SPA，不能让浏览器缓存这个门禁页。
        headers={"Cache-Control": "no-store"},
    )


@router.get("/")
async def index(request: Request):
    """主页。

    登录门禁在服务端做：未通过鉴权时不下发 SPA，三条前端加载路径
    （Vite dev / 构建产物 / 旧版 static 脚本）因此都有登录入口。
    顶层导航不是 CSRF——从其他站点点链接打开时 Sec-Fetch-Site 为 cross-site，
    不能因此拒绝，所以不做同源校验。
    """
    if admin_rejection(request, None, enforce_same_origin=False) is not None:
        return _auth_gate_page(request)

    assets = _vite_assets()
    providers = _get_llm_providers()
    return _get_templates().TemplateResponse(
        request=request,
        name="index.html",
        context={
            "request": request,
            "vite_dev": _is_vite_dev(),
            "vite_js": assets.get("js", ""),
            "vite_css": assets.get("css", ""),
            "providers_json": json.dumps(providers, ensure_ascii=False),
        },
    )
