"""
页面路由 — 渲染 WebUI 前端页面
"""

from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter(tags=["pages"])


def _get_templates():
    """Lazy import to avoid circular dependency with app.py."""
    from src.web.app import templates
    return templates


@router.get("/")
async def index(request: Request):
    """主页"""
    return _get_templates().TemplateResponse(
        request=request,
        name="index.html",
        context={"request": request}
    )
