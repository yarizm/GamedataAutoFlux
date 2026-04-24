"""
页面路由 — 渲染 WebUI 前端页面
"""

from __future__ import annotations

from pathlib import Path

from typing import Annotated
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

router = APIRouter(tags=["pages"])

# 直接引用模板目录，避免从 app.py 循环导入
_TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


@router.get("/")
async def index(request: Request):
    """主页"""
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={}
    )
