"""LangChain Agent 服务 —— 自然语言驱动的数据采集助手"""

from __future__ import annotations

import asyncio
import json
import time
import os
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    ChatMessage,
    FunctionMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langchain_openai import ChatOpenAI

try:
    from langchain_classic.agents import (
        AgentExecutor,
        create_openai_tools_agent,
        create_structured_chat_agent,
    )
except ImportError:
    from langchain.agents import (
        AgentExecutor,
        create_openai_tools_agent,
        create_structured_chat_agent,
    )
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import BaseTool

from loguru import logger

from src.agent.stream_parser import (
    StreamState,
    flush_buffer,
    parse_react_final_answer,
    process_react_chunk,
    process_text_chunk,
)
from src.agent.tools import ALL_TOOLS
from src.core.config import get as get_config, get_data_dir


class AgentService:
    """管理 LangChain Agent 会话和流式调用"""

    def __init__(self, session_service: Any = None) -> None:
        self._llm: ChatOpenAI | None = None
        self._agent_executor: Any = None
        self._mcp_manager: Any | None = None
        self._histories: dict[str, list[BaseMessage]] = {}
        self._sessions_timestamps: dict[str, float] = {}
        self._initialized: bool = False
        self._provider_override: str | None = None
        self._lock = asyncio.Lock()
        self._last_save_time: float = 0
        self._session_service = session_service  # AgentSessionService | None

        self._max_iterations: int = get_config("agent.max_iterations", 10)
        self._session_timeout: int = get_config("agent.session_timeout_minutes", 60) * 60
        self._old_persist_path: Path = get_data_dir() / "agent_sessions.json"
        self._histories_loaded: bool = False
        self._agent_engine: Any = None
        self._agent_session_factory: Any = None

        self._system_prompt: str = get_config(
            "agent.system_prompt",
            _default_system_prompt(),
        )

    def _build_prompt_with_tools(self, tools: list[BaseTool]) -> ChatPromptTemplate:
        """根据当前加载的工具集合，动态生成包含工具使用规范的系统提示词"""
        tool_desc = "\n".join([f"- {t.name}: {t.description}" for t in tools])
        system_content = (
            f"{self._system_prompt}\n\n"
            f"====== 非常重要：工具使用规范 ======\n"
            f"你必须优先使用以下工具来获取事实信息或执行操作，而不能仅凭记忆臆断。\n"
            f"**绝对禁止伪造或虚构工具调用结果！绝对禁止在没有实际发起函数调用的情况下回复用户你已经完成了操作！**\n"
            f"所有的查询、页面导航、数据采集动作都必须通过实际的工具调用（Tool Call/Function Call）来执行。\n"
            f"切勿在正文中手写类似 `⚙ xxx` 或 `### Result` 的伪造日志！必须使用原生工具调用。\n"
            f"当你需要查询数据、管理任务或进行网络请求时，必须主动且直接地调用对应的工具：\n"
            f"{tool_desc}\n"
            f"===================================\n"
        )
        return ChatPromptTemplate.from_messages(
            [
                SystemMessage(content=system_content),
                MessagesPlaceholder("chat_history", optional=True),
                ("human", "{input}"),
                MessagesPlaceholder("agent_scratchpad"),
            ]
        )

    def _build_react_prompt_with_tools(self, tools: list[BaseTool]) -> ChatPromptTemplate:
        """为 ReAct 范式动态生成包含工具使用规范的系统提示词"""
        prefix = f"""Respond to the human as helpfully and accurately as possible. You have access to the following tools:

{{tools}}

Use a json blob to specify a tool by providing an action key (tool name) and an action_input key (tool input).

Valid "action" values: "Final Answer" or {{tool_names}}

Provide only ONE action per $JSON_BLOB, as shown:

```
{{{{
  "action": $TOOL_NAME,
  "action_input": $INPUT
}}}}
```

Follow this format:

Question: input question to answer
Thought: consider previous and subsequent steps
Action:
```
$JSON_BLOB
```
Observation: action result
... (repeat Thought/Action/Observation N times)
Thought: I know what to respond
Action:
```
{{{{
  "action": "Final Answer",
  "action_input": "Final response to human"
}}}}
```

Additional Rules:
{self._system_prompt}
"""
        suffix = """Begin! Reminder to ALWAYS respond with a valid json blob of a single action. Use tools if necessary. Respond directly if appropriate. Format is Action:```$JSON_BLOB```then Observation"""

        return ChatPromptTemplate.from_messages(
            [
                ("system", prefix),
                MessagesPlaceholder("chat_history", optional=True),
                ("human", "{input}\n\n" + suffix + "\n\n{agent_scratchpad}"),
            ]
        )

    def _ensure_initialized(self) -> None:
        """延迟初始化 LLM 和 AgentExecutor"""
        if self._initialized:
            return

        provider = self._provider_override or get_config("llm.provider", "qwen")
        api_key = get_config(f"llm.{provider}.api_key", "")
        base_url = get_config(f"llm.{provider}.base_url", "")
        model = get_config(f"llm.{provider}.model", "qwen-max")
        temperature = get_config(f"llm.{provider}.temperature", 0.3)
        max_tokens = get_config(f"llm.{provider}.max_tokens", 2000)

        if not api_key:
            if provider == "local":
                api_key = "local"
            else:
                raise ValueError(
                    f"LLM provider '{provider}' 的 api_key 为空，请检查系统环境变量或 .env 文件中是否设置了对应的 key"
                )

        self._llm = ChatOpenAI(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            api_key=api_key,
            base_url=base_url if base_url else None,
            streaming=True,
        )

        all_tools = list(ALL_TOOLS)
        agent_type = get_config("agent.agent_type", "openai_tools")

        if agent_type == "react":
            prompt = self._build_react_prompt_with_tools(all_tools)
            agent = create_structured_chat_agent(self._llm, all_tools, prompt)
        else:
            prompt = self._build_prompt_with_tools(all_tools)
            agent = create_openai_tools_agent(self._llm, all_tools, prompt)
        self._agent_executor = AgentExecutor(
            agent=agent,
            tools=all_tools,
            max_iterations=self._max_iterations,
            handle_parsing_errors=True,
            verbose=False,
        )

        self._initialized = True
        logger.info(f"Agent 初始化完成 (provider={provider}, model={model})")

    def _create_mcp_manager(self) -> Any:
        try:
            from src.agent.mcp_client import PlaywrightMcpManager
        except ImportError as exc:
            raise RuntimeError(
                "Playwright MCP 已启用，但缺少 mcp 依赖。请安装项目依赖，"
                "或将 agent.playwright_mcp.enabled 设置为 false。"
            ) from exc
        return PlaywrightMcpManager()

    async def _async_ensure_initialized(self) -> None:
        """异步延迟初始化 LLM 和 AgentExecutor，处理 MCP 启动"""
        if not self._histories_loaded:
            await self._load_histories()
            self._histories_loaded = True

        if not self._initialized:
            self._ensure_initialized()

        # MCP 重连逻辑：独立于 _initialized 检查。
        # 当 MCP 进程崩溃后（_is_running=False），下次 ainvoke 会尝试重新启动。
        if get_config("agent.playwright_mcp.enabled", False):
            if not self._mcp_manager:
                self._mcp_manager = self._create_mcp_manager()

            if not self._mcp_manager._is_running:
                logger.info("MCP 未运行，尝试启动/重连 Playwright MCP Server...")
                await self._mcp_manager.start()

            # 如果 MCP 有工具且尚未注入（或需要重建），重建 AgentExecutor
            mcp_tools = self._mcp_manager.get_langchain_tools()
            current_tool_names = {
                t.name for t in (self._agent_executor.tools if self._agent_executor else [])
            }
            mcp_tool_names = {t.name for t in mcp_tools}

            if mcp_tools and not mcp_tool_names.issubset(current_tool_names):
                all_tools = list(ALL_TOOLS) + list(mcp_tools)
                agent_type = get_config("agent.agent_type", "openai_tools")
                if agent_type == "react":
                    prompt = self._build_react_prompt_with_tools(all_tools)
                    agent = create_structured_chat_agent(self._llm, all_tools, prompt)
                else:
                    prompt = self._build_prompt_with_tools(all_tools)
                    agent = create_openai_tools_agent(self._llm, all_tools, prompt)
                self._agent_executor = AgentExecutor(
                    agent=agent,
                    tools=all_tools,
                    max_iterations=self._max_iterations,
                    handle_parsing_errors=True,
                    verbose=False,
                )
                logger.info(f"成功将 {len(mcp_tools)} 个 MCP 工具注入 Agent")

    async def _get_history(self, session_id: str) -> list[BaseMessage]:
        """获取指定会话的聊天历史"""
        async with self._lock:
            if session_id not in self._histories:
                self._histories[session_id] = []
            self._sessions_timestamps[session_id] = time.time()
            return self._histories[session_id]

    async def ainvoke(self, user_input: str, session_id: str = "default") -> AsyncIterator[dict]:
        """流式执行 Agent，逐步 yield 事件

        事件类型:
          {"type": "thinking", "content": "..."}         — 思考过程
          {"type": "tool_call", "name": "...", "args": {...}} — 工具调用开始
          {"type": "tool_result", "name": "...", "content": "..."} — 工具执行结果
          {"type": "final", "content": "..."}            — 最终文本回复
          {"type": "error", "content": "..."}            — 错误信息
        """
        history = await self._get_history(session_id)

        # 清理超时会话
        await self._cleanup_stale_sessions()

        final_output: str = ""
        saved = False

        async def _save_history_helper():
            nonlocal saved
            if saved:
                return
            saved = True
            async with self._lock:
                history.append(HumanMessage(content=user_input))
                history.append(AIMessage(content=final_output or "已停止"))
                if len(history) > 40:
                    self._histories[session_id] = history[-20:]
                await self._save_histories()

        try:
            await self._async_ensure_initialized()

            state = StreamState()
            suppress_final_stream = get_config("agent.agent_type", "openai_tools") == "react"

            try:
                async for event in self._agent_executor.astream_events(
                    {
                        "input": user_input,
                        "chat_history": history,
                    },
                    config={"configurable": {"session_id": session_id}},
                    version="v2",
                ):
                    kind = event.get("event")

                    if kind == "on_chat_model_start":
                        state.in_react_action = False
                        state.react_emitted_len = 0
                        state.content_buffer = ""
                        if not suppress_final_stream:
                            yield {"type": "thinking", "content": "正在分析您的请求..."}

                    elif kind == "on_chat_model_stream":
                        chunk = event.get("data", {}).get("chunk")
                        if chunk is None:
                            continue

                        # 提取 reasoning/thinking 内容
                        reasoning = None
                        if hasattr(chunk, "additional_kwargs"):
                            ak = chunk.additional_kwargs
                            if isinstance(ak, dict):
                                reasoning = (
                                    ak.get("reasoning_content")
                                    or ak.get("thinking")
                                    or ak.get("thoughts")
                                )
                        if reasoning:
                            yield {"type": "thinking", "content": str(reasoning)}

                        # 提取正文内容
                        if hasattr(chunk, "content") and chunk.content:
                            if isinstance(chunk.content, str):
                                if suppress_final_stream:
                                    events, state = process_react_chunk(chunk.content, state)
                                    for e in events:
                                        yield e
                                else:
                                    events, state = process_text_chunk(
                                        chunk.content, state, suppress_final_stream
                                    )
                                    for e in events:
                                        yield e
                            elif isinstance(chunk.content, list):
                                for item in chunk.content:
                                    if isinstance(item, dict):
                                        if item.get("type") == "text":
                                            if suppress_final_stream:
                                                events, state = process_react_chunk(
                                                    item["text"], state
                                                )
                                            else:
                                                events, state = process_text_chunk(
                                                    item["text"], state, suppress_final_stream
                                                )
                                            for e in events:
                                                yield e
                                        elif (
                                            item.get("type") == "reasoning"
                                            or item.get("type") == "thinking"
                                        ):
                                            yield {
                                                "type": "thinking",
                                                "content": item.get("text", ""),
                                            }

                    elif kind == "on_tool_start":
                        tool_name = event.get("name", "unknown")
                        tool_input = event.get("data", {}).get("input", {})
                        args = tool_input if isinstance(tool_input, dict) else {}
                        thinking_desc = _describe_tool_action(tool_name, args)
                        if thinking_desc:
                            yield {"type": "thinking", "content": thinking_desc}
                        yield {
                            "type": "tool_call",
                            "name": tool_name,
                            "args": args,
                        }

                    elif kind == "on_tool_end":
                        tool_name = event.get("name", "unknown")
                        tool_output = event.get("data", {}).get("output", "")
                        output_str = str(tool_output)
                        if len(output_str) > 4000:
                            output_str = output_str[:4000] + "...(已截断)"
                        yield {"type": "tool_result", "name": tool_name, "content": output_str}

                    elif kind == "on_chain_end":
                        if event.get("name") == "AgentExecutor":
                            out = event.get("data", {}).get("output", {})
                            if isinstance(out, dict) and "output" in out:
                                if suppress_final_stream:
                                    final_ans = parse_react_final_answer(out["output"])
                                    if not final_ans:
                                        final_ans = out["output"]
                                    if not final_ans:
                                        final_ans = "(无文本输出)"
                                    yield {"type": "final", "content": final_ans}
                                    final_output += final_ans
                            break

                # 流结束，刷新剩余缓冲区
                events, state = flush_buffer(state, suppress_final_stream)
                for e in events:
                    yield e
                final_output += state.final_output

                # 正常结束，保存历史
                await _save_history_helper()

            except asyncio.CancelledError:
                logger.info(f"Agent stream cancelled for session {session_id}.")
                await _save_history_helper()
                raise
            except Exception:
                await _save_history_helper()
                raise

        except Exception as e:
            logger.opt(exception=True).error(f"Agent 执行出错: {e}")
            content = f"执行出错: {e}"
            if get_config("debug", False):
                content += f"\n\n(异常类型: {type(e).__name__})"
            yield {"type": "error", "content": content}

    async def clear_history(self, session_id: str = "default") -> None:
        """清除指定会话的对话记忆"""
        async with self._lock:
            self._histories.pop(session_id, None)
            self._sessions_timestamps.pop(session_id, None)
            await self._save_histories(force=True)

    MAX_PERSISTED_SESSIONS: int = 50

    async def _ensure_agent_db(self) -> None:
        """延迟初始化 Agent 会话的 SQLAlchemy 引擎和表"""
        if self._agent_engine is not None:
            return
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
        from src.core.config import get_settings
        from src.storage.models import Base

        settings = get_settings()
        url = settings.get("database", {}).get(
            "sqlalchemy_url",
            "postgresql+asyncpg://postgres:postgres@localhost:5432/autoflux",
        )
        self._agent_engine = create_async_engine(url, echo=False)
        self._agent_session_factory = async_sessionmaker(
            self._agent_engine, expire_on_commit=False, class_=AsyncSession
        )
        async with self._agent_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def _save_histories(self, force: bool = False) -> None:
        """持久化会话历史到数据库（最多保留 MAX_PERSISTED_SESSIONS 个会话）

        调用者必须持有 self._lock。
        """
        # 优先使用注入的 session_service
        if self._session_service is not None:
            self._last_save_time = await self._session_service.save_histories(
                self._histories,
                self._sessions_timestamps,
                last_save_time=self._last_save_time,
                force=force,
            )
            return

        # 旧路径：自建 DB engine
        now = time.time()
        if not force and now - self._last_save_time < 5:
            return

        if len(self._histories) > self.MAX_PERSISTED_SESSIONS:
            sorted_sids = sorted(
                self._histories.keys(),
                key=lambda sid: self._sessions_timestamps.get(sid, 0),
                reverse=True,
            )
            stale = sorted_sids[self.MAX_PERSISTED_SESSIONS :]
            for sid in stale:
                self._histories.pop(sid, None)
                self._sessions_timestamps.pop(sid, None)

        await self._ensure_agent_db()

        try:
            from src.storage.models import AgentSessionModel
            from sqlalchemy import select, delete

            async with self._agent_session_factory() as session:
                for sid, msgs in self._histories.items():
                    messages_json = json.dumps(
                        [msg.model_dump(mode="json") for msg in msgs], ensure_ascii=False
                    )
                    last_active = self._sessions_timestamps.get(sid, now)
                    result = await session.execute(
                        select(AgentSessionModel).where(AgentSessionModel.session_id == sid)
                    )
                    existing = result.scalars().first()
                    if existing:
                        existing.messages = messages_json
                        existing.last_active_at = datetime.fromtimestamp(
                            last_active, tz=timezone.utc
                        )
                    else:
                        session.add(
                            AgentSessionModel(
                                session_id=sid,
                                messages=messages_json,
                                last_active_at=datetime.fromtimestamp(last_active, tz=timezone.utc),
                            )
                        )

                active_sids = list(self._histories.keys())
                if active_sids:
                    await session.execute(
                        delete(AgentSessionModel).where(
                            ~AgentSessionModel.session_id.in_(active_sids)
                        )
                    )

                await session.commit()
            self._last_save_time = now
        except Exception as exc:
            logger.warning(f"保存 Agent 会话历史失败: {exc}")

    async def _load_histories(self) -> None:
        """从数据库恢复 Agent 会话历史，包含兼容旧版 JSON 迁移"""
        # 优先使用注入的 session_service
        if self._session_service is not None:
            try:
                histories, timestamps = await self._session_service.load_histories()
                self._histories = histories
                self._sessions_timestamps = timestamps
                if self._histories:
                    logger.info(f"已恢复 {len(self._histories)} 个 Agent 会话历史")
            except Exception as e:
                logger.warning(f"加载 Agent 会话历史失败: {e}")
            return

        # 旧路径：自建 DB engine
        from src.storage.models import AgentSessionModel
        from sqlalchemy import select

        await self._ensure_agent_db()

        try:
            # JSON 迁移：如果旧版 JSON 文件存在，先迁入数据库
            if self._old_persist_path.exists():
                logger.info("发现旧版 JSON 会话数据，正在迁移到数据库...")
                try:
                    data = json.loads(self._old_persist_path.read_text(encoding="utf-8"))
                    async with self._agent_session_factory() as session:
                        for sid, blob in data.items():
                            if isinstance(blob, dict) and "messages" in blob:
                                raw_msgs = blob.get("messages", [])
                                last_active = blob.get("last_active_at", time.time())
                            elif isinstance(blob, list):
                                raw_msgs = blob
                                last_active = time.time()
                            else:
                                continue

                            result = await session.execute(
                                select(AgentSessionModel).where(AgentSessionModel.session_id == sid)
                            )
                            if result.scalars().first() is None:
                                session.add(
                                    AgentSessionModel(
                                        session_id=sid,
                                        messages=raw_msgs,
                                        last_active_at=datetime.fromtimestamp(
                                            last_active, tz=timezone.utc
                                        ),
                                    )
                                )
                        await session.commit()
                    os.replace(
                        str(self._old_persist_path),
                        str(self._old_persist_path.with_suffix(".json.bak")),
                    )
                    logger.info("JSON 数据迁移完成，已重命名为 .bak")
                except Exception as e:
                    logger.warning(f"迁移 JSON 会话历史失败: {e}")

            async with self._agent_session_factory() as session:
                result = await session.execute(select(AgentSessionModel))
                rows = result.scalars().all()

                _MSG_CLASSES: dict[str, type[BaseMessage]] = {
                    "human": HumanMessage,
                    "ai": AIMessage,
                    "system": SystemMessage,
                    "tool": ToolMessage,
                    "function": FunctionMessage,
                    "chat": ChatMessage,
                }

                for row in rows:
                    sid = row.session_id
                    try:
                        raw_msgs = (
                            row.messages
                            if isinstance(row.messages, list)
                            else json.loads(row.messages)
                        )
                    except (json.JSONDecodeError, TypeError):
                        continue

                    last_active = row.last_active_at
                    if hasattr(last_active, "timestamp"):
                        last_active = last_active.timestamp()

                    msgs: list[BaseMessage] = []
                    for raw in raw_msgs:
                        msg_type = raw.get("type", "")
                        cls = _MSG_CLASSES.get(msg_type)
                        if cls is not None:
                            msgs.append(cls.model_validate(raw))
                        else:
                            logger.warning(f"恢复会话时跳过未知消息类型: {msg_type!r}")

                    if msgs:
                        self._histories[sid] = msgs
                        self._sessions_timestamps[sid] = last_active

                if self._histories:
                    logger.info(f"已恢复 {len(self._histories)} 个 Agent 会话历史")
        except Exception as e:
            logger.warning(f"加载 Agent 会话历史失败: {e}")

    async def _cleanup_stale_sessions(self) -> None:
        """清理超时的会话记忆"""
        if not self._session_timeout:
            return

        # 优先使用注入的 session_service
        if self._session_service is not None:
            async with self._lock:
                await self._session_service.cleanup_stale(
                    self._histories, self._sessions_timestamps
                )
            return

        # 旧路径：自建 DB engine
        async with self._lock:
            now = time.time()
            stale = [
                sid
                for sid, ts in list(self._sessions_timestamps.items())
                if now - ts > self._session_timeout
            ]
            for sid in stale:
                self._histories.pop(sid, None)
                self._sessions_timestamps.pop(sid, None)
            if stale:
                try:
                    from src.storage.models import AgentSessionModel
                    from sqlalchemy import delete

                    await self._ensure_agent_db()
                    async with self._agent_session_factory() as session:
                        await session.execute(
                            delete(AgentSessionModel).where(AgentSessionModel.session_id.in_(stale))
                        )
                        await session.commit()
                except Exception as e:
                    logger.warning(f"清理过期数据库会话失败: {e}")
                logger.debug(f"清理了 {len(stale)} 个超时会话")

    def list_sessions(self) -> list[str]:
        """返回当前有聊天历史的 session ID 列表"""
        return list(self._histories.keys())

    def get_active_provider(self) -> str:
        """返回当前生效的 LLM provider 名称"""
        return self._provider_override or get_config("llm.provider", "qwen")

    def set_provider(self, provider_name: str) -> None:
        """运行时切换 LLM provider，下次 ainvoke 生效"""
        available = self.get_available_providers()
        if provider_name not in {p["key"] for p in available}:
            raise ValueError(f"未知的 provider: {provider_name}")
        self._provider_override = provider_name
        self.reset_runtime()
        logger.info(f"Agent LLM provider 已切换: {provider_name}")

    def reload_config(self, provider_name: str | None = None) -> None:
        """重新读取配置并丢弃已初始化的 LLM/AgentExecutor。"""
        if provider_name:
            available = self.get_available_providers()
            if provider_name not in {p["key"] for p in available}:
                raise ValueError(f"未知的 provider: {provider_name}")
            self._provider_override = provider_name
        else:
            self._provider_override = None

        self._max_iterations = get_config("agent.max_iterations", 10)
        self._session_timeout = get_config("agent.session_timeout_minutes", 60) * 60
        self._system_prompt = get_config("agent.system_prompt", _default_system_prompt())
        self.reset_runtime()
        logger.info(f"Agent 配置已重新加载 (provider={self.get_active_provider()})")

    def reset_runtime(self) -> None:
        """丢弃运行时 LLM 实例，让下一次调用按最新配置重新初始化。"""
        self._initialized = False
        self._llm = None
        self._agent_executor = None

    @staticmethod
    def get_available_providers() -> list[dict]:
        """从 settings.yaml 动态扫描所有可用的 LLM provider"""
        llm_config = get_config("llm", {})
        providers: list[dict] = []
        for key, cfg in llm_config.items():
            if key == "provider" or not isinstance(cfg, dict):
                continue
            model = cfg.get("model")
            if model:
                providers.append(
                    {
                        "key": key,
                        "label": key.capitalize(),
                        "model": str(model),
                    }
                )
        return providers


def _describe_tool_action(tool_name: str, args: dict) -> str:
    """根据工具名和参数生成描述性的思考内容"""
    descriptions = {
        "generate_report": f"决定生成报告，分析: {args.get('prompt', '')[:80]}",
        "get_report_content": f"获取报告 {args.get('report_id', '')} 的详细内容",
        "list_tasks": "查看当前任务列表",
        "get_task_detail": f"查看任务 {args.get('task_id', '')} 的详情",
        "create_task": f"创建采集任务: {args.get('name', '')}",
        "cancel_task": f"取消任务 {args.get('task_id', '')}",
        "list_pipeline_templates": "查看可用的 Pipeline 模板",
        "list_pipelines": "查看已创建的 Pipeline",
        "create_pipeline": f"创建 Pipeline: {args.get('name', '')}",
        "delete_pipeline": f"删除 Pipeline: {args.get('name', '')}",
        "list_cron_jobs": "查看定时任务列表",
        "create_cron_job": f"创建定时任务: {args.get('name', '')}",
        "delete_cron_job": f"删除定时任务: {args.get('name', '')}",
        "list_data_games": "浏览已采集的游戏数据",
        "search_data": f"搜索数据: {args.get('query', '')}",
        "get_system_stats": "查看系统运行状态",
        "resolve_steam_app_id": f"搜索 Steam App ID: {args.get('game_name', '')}",
        "verify_steam_app_id": f"验证 Steam App ID: {args.get('app_id', '')}",
        "search_game_identifiers": f"自动搜索游戏平台标识符: {args.get('game_name', '')}",
        "verify_game_identifier": f"验证 {args.get('platform', '')} 标识符: {args.get('identifier', '')}",
        "review_collection_results": f"复查采集结果: {args.get('task_id', '')}",
        "browser_navigate": f"正在浏览器中打开 URL: {args.get('url', '')}",
        "browser_snapshot": "正在获取页面的快照数据",
        "browser_evaluate": "正在页面中执行提取脚本",
    }
    return descriptions.get(tool_name, f"调用工具 {tool_name}")


def _default_system_prompt() -> str:
    """Minimal fallback prompt — detailed rules live in settings.yaml."""
    return (
        "你是一个游戏数据采集与分析系统的 AI 助手。"
        "你可以帮助用户查看和管理任务、配置 Pipeline、设置定时采集、"
        "浏览数据以及生成报告。"
        "请根据用户的自然语言指令，选择合适的工具完成任务。"
        "如果意图不明确，请主动询问细节。"
        "所有响应请使用中文。"
    )
