"""LLM 驱动的内容提取和候选验证。"""

from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib.parse import urljoin

from langchain_openai import ChatOpenAI
from loguru import logger

from src.collectors.html_trimmer import trim_html


def _build_llm_from_config(provider: str, provider_cfg: dict[str, Any]) -> ChatOpenAI:
    """从 provider 配置构建 ChatOpenAI 实例。"""
    model = provider_cfg.get("model", "qwen-turbo")
    api_key = provider_cfg.get("api_key", "")
    base_url = provider_cfg.get("base_url", "")
    temperature = provider_cfg.get("temperature", 0.1)
    max_tokens = provider_cfg.get("max_tokens", 4000)
    timeout = provider_cfg.get("timeout", 60)

    if api_key and api_key.startswith("${") and api_key.endswith("}"):
        env_var = api_key[2:-1]
        api_key = os.environ.get(env_var, "")

    return ChatOpenAI(
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        api_key=api_key or "not-set",
        base_url=base_url if base_url else None,
        timeout=timeout,
        streaming=False,
    )


def _get_extraction_llms() -> list[ChatOpenAI]:
    """返回用于内容提取的 LLM 实例列表（主模型 + 回退模型）。

    优先使用 smart_collector.llm，否则复用 llm.provider。
    回退链：smart_collector.llm → llm.provider → 其他已配置的 provider。
    """
    from src.core.config import get as get_config, get_settings

    llms: list[ChatOpenAI] = []
    seen_providers: set[str] = set()

    # 1. smart_collector 专用 LLM
    sc_llm = get_config("smart_collector.llm", {})
    if isinstance(sc_llm, dict) and sc_llm.get("provider"):
        provider = sc_llm["provider"]
        provider_cfg = sc_llm.get(provider, {})
        if isinstance(provider_cfg, dict):
            llms.append(_build_llm_from_config(provider, provider_cfg))
            seen_providers.add(provider)

    # 2. llm.provider 主模型
    main_provider = get_config("llm.provider", "qwen")
    if main_provider not in seen_providers:
        provider_cfg = get_config(f"llm.{main_provider}", {})
        if isinstance(provider_cfg, dict):
            llms.append(_build_llm_from_config(main_provider, provider_cfg))
            seen_providers.add(main_provider)

    # 3. 其他已配置的 provider 作为回退
    settings = get_settings()
    llm_section = settings.get("llm", {})
    if isinstance(llm_section, dict):
        for key, cfg in llm_section.items():
            if key in ("provider",) or key in seen_providers:
                continue
            if not isinstance(cfg, dict):
                continue
            if not cfg.get("api_key") and not cfg.get("base_url"):
                continue
            llms.append(_build_llm_from_config(key, cfg))
            seen_providers.add(key)

    return llms


def _get_extraction_llm() -> ChatOpenAI:
    """返回主 LLM 实例（兼容旧调用）。"""
    llms = _get_extraction_llms()
    return llms[0] if llms else ChatOpenAI(model="qwen-turbo", api_key="not-set", streaming=False)


def _parse_llm_json(text: str) -> list | dict | None:
    """从 LLM 返回文本中提取 JSON，处理 markdown 围栏。"""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
        text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _is_truncated_json(text: str) -> bool:
    """检测 JSON 是否被截断。"""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
        text = text.strip()
    # JSON 数组/对象未闭合
    if text.startswith("[") and not text.endswith("]"):
        return True
    if text.startswith("{") and not text.endswith("}"):
        return True
    return False


async def _invoke_with_fallback(
    prompt: str,
    *,
    expect_type: type = list,
) -> list | dict | None:
    """调用 LLM 并回退到其他模型。

    依次尝试每个 LLM，直到返回有效 JSON。
    """
    llms = _get_extraction_llms()
    if not llms:
        logger.warning("[LLM Extractor] No LLM configured")
        return None

    for i, llm in enumerate(llms):
        try:
            response = await llm.ainvoke(prompt)
            content = response.content if hasattr(response, "content") else str(response)
        except Exception as e:
            logger.warning(f"[LLM Extractor] LLM #{i} ({llm.model_name}) invoke failed: {e}")
            continue

        parsed = _parse_llm_json(content)

        # 检查截断
        if parsed is None and _is_truncated_json(content):
            logger.warning(
                f"[LLM Extractor] LLM #{i} ({llm.model_name}) returned truncated JSON, "
                f"trying next model"
            )
            continue

        if parsed is None:
            logger.warning(
                f"[LLM Extractor] LLM #{i} ({llm.model_name}) returned invalid JSON, "
                f"trying next model"
            )
            continue

        if not isinstance(parsed, expect_type):
            logger.warning(
                f"[LLM Extractor] LLM #{i} ({llm.model_name}) returned {type(parsed).__name__} "
                f"expected {expect_type.__name__}, trying next model"
            )
            continue

        if i > 0:
            logger.info(f"[LLM Extractor] Fallback to LLM #{i} ({llm.model_name}) succeeded")
        return parsed

    logger.warning("[LLM Extractor] All LLMs failed")
    return None


def _fill_item_defaults(item: dict[str, Any], base_url: str) -> dict[str, Any]:
    """填充缺失字段，补全相对 URL。"""
    url = str(item.get("url", "") or "")
    if url and not url.startswith(("http://", "https://")):
        url = urljoin(base_url, url)
    return {
        "title": str(item.get("title", "") or ""),
        "date": str(item.get("date", "") or ""),
        "url": url,
        "category": str(item.get("category", "news") or "news"),
        "summary": str(item.get("summary", "") or ""),
    }


async def extract_items_from_html(html: str, base_url: str) -> list[dict[str, Any]]:
    """从 HTML 中提取新闻/公告/更新列表。"""
    trimmed = trim_html(html)
    if not trimmed:
        return []

    prompt = (
        "你是一个网页内容提取专家。从以下 HTML 文本中提取新闻/公告/更新/活动列表。\n\n"
        "输出 JSON 数组，每个元素包含：\n"
        "- title: 标题\n"
        "- date: 发布日期（YYYY-MM-DD 格式，无则空字符串）\n"
        "- url: 链接（相对路径补全为绝对路径）\n"
        "- category: 分类（news/patch/event/announcement 之一）\n"
        "- summary: 摘要（50字以内）\n\n"
        "只输出 JSON，不要解释。如果没有找到任何条目，输出空数组 []。\n\n"
        f"Base URL: {base_url}\n"
        f"HTML 内容：\n{trimmed}"
    )

    parsed = await _invoke_with_fallback(prompt, expect_type=list)
    if not isinstance(parsed, list):
        return []

    return [_fill_item_defaults(item, base_url) for item in parsed if isinstance(item, dict)]


async def verify_game_candidate(
    candidates: list[dict[str, Any]],
    game_name: str,
    app_id: int,
    twitch_name: str | None,
) -> dict[str, Any]:
    """验证 SullyGnome 搜索候选，返回匹配结果。"""
    candidates_text = json.dumps(
        [{"index": i, "displaytext": c.get("displaytext", ""), "siteurl": c.get("siteurl", "")}
         for i, c in enumerate(candidates)],
        ensure_ascii=False,
    )

    prompt = (
        f"你要从以下 SullyGnome 搜索结果中找到游戏「{game_name}」对应的条目。\n\n"
        f"搜索结果：\n{candidates_text}\n\n"
        f"目标游戏信息：\n"
        f"- 游戏名: {game_name}\n"
        f"- Steam App ID: {app_id}\n"
        f"- Twitch 名称: {twitch_name or '未知'}\n\n"
        "输出格式（JSON）：\n"
        "- matched_index: 匹配的候选索引（从0开始），无匹配则为 -1\n"
        "- confidence: 置信度（0-1）\n"
        "- reason: 判断理由\n\n"
        "只输出 JSON。"
    )

    default_result = {"matched_index": -1, "confidence": 0.0, "reason": "LLM verification failed"}

    parsed = await _invoke_with_fallback(prompt, expect_type=dict)
    if not isinstance(parsed, dict):
        return default_result

    return {
        "matched_index": int(parsed.get("matched_index", -1)),
        "confidence": float(parsed.get("confidence", 0.0)),
        "reason": str(parsed.get("reason", "")),
    }
