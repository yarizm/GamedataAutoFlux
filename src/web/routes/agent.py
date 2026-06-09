"""Agent 聊天 API 路由 —— SSE 流式对话"""

from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from loguru import logger

from src.agent.schemas import ChatRequest, SetProviderRequest, UpdateProviderConfigRequest
from src.core.sensitive import redact_sensitive_text

router = APIRouter(tags=["agent"])

_TIMEOUT_SECONDS = 300


def _safe_provider_api_key(value: str) -> tuple[str, bool]:
    value = str(value or "")
    if not value:
        return "", False
    if value.startswith("${") and value.endswith("}"):
        return value, False
    return "", True


@router.post("/agent/chat")
async def agent_chat(req: ChatRequest):
    """与 AI 助手对话（SSE 流式响应）"""
    from src.web.app import get_agent_service

    agent_service = get_agent_service()
    if not agent_service:
        raise HTTPException(503, "Agent 服务未启用")

    async def event_stream():
        try:
            async with asyncio.timeout(_TIMEOUT_SECONDS):
                async for event in agent_service.ainvoke(req.message, req.session_id):
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except asyncio.TimeoutError:
            yield f"data: {json.dumps({'type': 'error', 'content': '响应超时，请稍后重试'}, ensure_ascii=False)}\n\n"
        except Exception as e:
            logger.error(f"Agent SSE 流出错: {redact_sensitive_text(str(e))}")
            yield f"data: {json.dumps({'type': 'error', 'content': '内部服务错误，请查看日志'}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/agent/history")
async def get_agent_history(session_id: str = "default"):
    """获取指定会话的消息历史"""
    from src.web.app import get_agent_service

    agent_service = get_agent_service()
    if not agent_service:
        raise HTTPException(503, "Agent 服务未启用")
    messages = agent_service.get_session_history(session_id)
    return {"session_id": session_id, "messages": messages}


@router.delete("/agent/history")
async def clear_agent_history(session_id: str = "default"):
    """清除指定会话的对话历史"""
    from src.web.app import get_agent_service

    agent_service = get_agent_service()
    if not agent_service:
        raise HTTPException(503, "Agent 服务未启用")

    await agent_service.clear_history(session_id)
    return {"message": "对话历史已清空"}


@router.get("/agent/sessions")
async def list_agent_sessions():
    """列出所有活跃的 Agent 会话 ID"""
    from src.web.app import get_agent_service

    agent_service = get_agent_service()
    if not agent_service:
        raise HTTPException(503, "Agent 服务未启用")
    return {"sessions": agent_service.list_sessions()}


@router.get("/agent/status")
async def get_agent_status():
    """获取 Agent 当前模型、工具、MCP 和会话状态。"""
    from src.web.app import get_agent_service

    agent_service = get_agent_service()
    if not agent_service:
        raise HTTPException(503, "Agent 服务未启用")
    return agent_service.get_status_summary()


@router.get("/agent/providers")
async def list_llm_providers():
    """列出可用的 LLM provider 及当前激活的 provider"""
    from src.agent.agent import AgentService

    providers = AgentService.get_available_providers()
    from src.web.app import get_agent_service

    agent_service = get_agent_service()
    active = (
        agent_service.get_active_provider()
        if agent_service
        else (providers[0]["key"] if providers else "")
    )
    return {"providers": providers, "active": active}


@router.post("/agent/providers")
async def set_llm_provider(req: SetProviderRequest):
    """切换 LLM provider"""
    from src.web.app import get_agent_service

    agent_service = get_agent_service()
    if not agent_service:
        raise HTTPException(503, "Agent 服务未启用")
    try:
        agent_service.set_provider(req.provider)
        return {"message": f"Provider switched to {req.provider}", "active": req.provider}
    except ValueError as e:
        raise HTTPException(400, redact_sensitive_text(str(e)))


@router.get("/agent/providers/config")
async def get_llm_providers_config():
    """获取完整的 LLM provider 原始配置（保留 ${ENV_VAR} 占位符）"""
    from src.agent.schemas import ProviderConfigItem
    from src.core.config import get as get_config, get_raw_section

    llm_config = get_raw_section("llm")
    items: list[dict] = []
    for key, cfg in llm_config.items():
        if key == "provider" or not isinstance(cfg, dict):
            continue
        safe_api_key, has_api_key = _safe_provider_api_key(str(cfg.get("api_key", "")))
        items.append(
            ProviderConfigItem(
                key=key,
                model=str(cfg.get("model", "")),
                base_url=str(cfg.get("base_url", "")),
                api_key=safe_api_key,
                has_api_key=has_api_key,
                temperature=_coerce_float(cfg.get("temperature"), default=0.3),
                max_tokens=_coerce_int(cfg.get("max_tokens"), default=2000),
            ).model_dump()
        )

    return {
        "providers": items,
        "active": get_config("llm.provider", ""),
    }


@router.put("/agent/providers/config")
async def update_llm_providers_config(req: UpdateProviderConfigRequest):
    """批量保存 LLM provider 配置到 settings.yaml"""
    from src.core.config import save_section
    from src.core.config import get as get_config, get_raw_section

    # 构建 llm section 字典
    raw_llm = get_raw_section("llm")
    llm_section: dict = {"provider": req.provider}
    for item in req.items:
        cfg: dict = {"model": item.model}
        if item.base_url:
            cfg["base_url"] = item.base_url
        if item.api_key:
            cfg["api_key"] = item.api_key
        elif isinstance(raw_llm.get(item.key), dict) and raw_llm[item.key].get("api_key"):
            cfg["api_key"] = raw_llm[item.key]["api_key"]
        cfg["temperature"] = item.temperature
        cfg["max_tokens"] = item.max_tokens
        # 保留已有的其他字段（fallback_to_stub, retry_count 等）
        existing = get_config(f"llm.{item.key}", {})
        if isinstance(existing, dict):
            for extra_key in (
                "fallback_to_stub",
                "retry_count",
                "retry_delay",
                "timeout",
                "max_input_chars",
            ):
                if extra_key in existing:
                    cfg[extra_key] = existing[extra_key]
        llm_section[item.key] = cfg

    save_section("llm", llm_section)

    from src.web.app import get_agent_service

    agent_service = get_agent_service()
    if agent_service:
        try:
            agent_service.reload_config(req.provider)
        except ValueError as e:
            raise HTTPException(400, redact_sensitive_text(str(e)))
    return {"message": "配置已保存", "providers_count": len(req.items)}


def _coerce_float(value, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_int(value, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
