from typing import Any, Sequence

import pytest
from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from langchain_core.tools import BaseTool, tool

from src.agent.checkpointer import JsonFileCheckpointSaver, create_agent_checkpointer
from src.agent.runtime import LangGraphAgentRuntime
from src.agent.agent import AgentService


class BindToolsFakeModel(FakeMessagesListChatModel):
    def bind_tools(
        self,
        tools: Sequence[BaseTool | dict | type | Any],
        *,
        tool_choice: Any = None,
        **kwargs: Any,
    ):
        return self


class PersistingSessionService:
    def __init__(self) -> None:
        self._max_sessions = 50
        self.saved_histories = None
        self.deleted_sessions = []

    async def save_histories(self, histories, timestamps, last_save_time, force=False):
        self.saved_histories = histories
        return 0

    async def delete_sessions(self, session_ids):
        self.deleted_sessions.extend(session_ids)


@pytest.mark.asyncio
async def test_memory_checkpointer_reuses_same_thread_state(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.agent.checkpointer.get_config",
        lambda key, default=None: "memory" if key == "agent.langgraph_checkpointer.backend" else default,
    )
    bundle = create_agent_checkpointer()
    model = BindToolsFakeModel(responses=[AIMessage(content="first"), AIMessage(content="second")])
    agent = create_agent(model=model, tools=None, system_prompt="You are helpful", checkpointer=bundle.checkpointer)

    first = await agent.ainvoke({"messages": [("human", "hello")]}, config={"configurable": {"thread_id": "thread-a"}})
    second = await agent.ainvoke({"messages": [("human", "follow up")]}, config={"configurable": {"thread_id": "thread-a"}})

    assert [msg.content for msg in first["messages"]] == ["hello", "first"]
    assert [msg.content for msg in second["messages"]][:3] == ["hello", "first", "follow up"]


@pytest.mark.asyncio
async def test_file_checkpointer_reloads_state_across_instances(monkeypatch, tmp_path) -> None:
    checkpoint_path = tmp_path / "agent_checkpoints.json"

    def fake_get_config(key, default=None):
        if key == "agent.langgraph_checkpointer.backend":
            return "file"
        if key == "agent.langgraph_checkpointer.file_path":
            return str(checkpoint_path)
        return default

    monkeypatch.setattr("src.agent.checkpointer.get_config", fake_get_config)

    bundle1 = create_agent_checkpointer()
    assert bundle1.backend_name == "file"
    assert bundle1.storage_path == str(checkpoint_path)
    assert isinstance(bundle1.checkpointer, JsonFileCheckpointSaver)

    first_agent = create_agent(
        model=BindToolsFakeModel(responses=[AIMessage(content="first")]),
        tools=None,
        system_prompt="You are helpful",
        checkpointer=bundle1.checkpointer,
    )
    first = await first_agent.ainvoke(
        {"messages": [("human", "hello")]},
        config={"configurable": {"thread_id": "thread-file"}},
    )
    assert [msg.content for msg in first["messages"]] == ["hello", "first"]
    assert checkpoint_path.exists()

    bundle2 = create_agent_checkpointer()
    assert isinstance(bundle2.checkpointer, JsonFileCheckpointSaver)
    second_agent = create_agent(
        model=BindToolsFakeModel(responses=[AIMessage(content="second")]),
        tools=None,
        system_prompt="You are helpful",
        checkpointer=bundle2.checkpointer,
    )
    second = await second_agent.ainvoke(
        {"messages": [("human", "follow up")]},
        config={"configurable": {"thread_id": "thread-file"}},
    )

    assert [msg.content for msg in second["messages"]] == [
        "hello",
        "first",
        "follow up",
        "second",
    ]


def test_file_checkpointer_tolerates_corrupt_json(tmp_path) -> None:
    checkpoint_path = tmp_path / "broken_checkpoints.json"
    checkpoint_path.write_text("{not valid json", encoding="utf-8")

    saver = JsonFileCheckpointSaver(checkpoint_path)

    assert saver.get_tuple({"configurable": {"thread_id": "missing"}}) is None


@pytest.mark.asyncio
async def test_file_checkpointer_persists_thread_deletion(tmp_path) -> None:
    checkpoint_path = tmp_path / "agent_checkpoints.json"
    saver = JsonFileCheckpointSaver(checkpoint_path)
    agent = create_agent(
        model=BindToolsFakeModel(responses=[AIMessage(content="first")]),
        tools=None,
        system_prompt="You are helpful",
        checkpointer=saver,
    )

    await agent.ainvoke(
        {"messages": [("human", "hello")]},
        config={"configurable": {"thread_id": "delete-file-thread"}},
    )
    assert JsonFileCheckpointSaver(checkpoint_path).get_tuple(
        {"configurable": {"thread_id": "delete-file-thread"}}
    ) is not None

    saver.delete_thread("delete-file-thread")

    assert JsonFileCheckpointSaver(checkpoint_path).get_tuple(
        {"configurable": {"thread_id": "delete-file-thread"}}
    ) is None


@pytest.mark.asyncio
async def test_clear_thread_deletes_langgraph_checkpoint(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.agent.checkpointer.get_config",
        lambda key, default=None: "memory" if key == "agent.langgraph_checkpointer.backend" else default,
    )

    model = BindToolsFakeModel(responses=[AIMessage(content="first"), AIMessage(content="second")])
    graph_runtime = LangGraphAgentRuntime(
        build_openai_tools_system_prompt=lambda tools: "system",
        create_mcp_manager=lambda: None,
    )
    graph_runtime.agent_executor = create_agent(
        model=model,
        tools=None,
        system_prompt="You are helpful",
        checkpointer=graph_runtime._checkpointer_bundle.checkpointer,
    )

    service = AgentService(session_service=PersistingSessionService())
    service._set_runtime_for_testing(graph_runtime)
    async def initialized_noop() -> None:
        return None

    monkeypatch.setattr(service, "_async_ensure_initialized", initialized_noop)

    first_events = [event async for event in service.ainvoke("hello", "thread-a")]
    assert first_events[-1]["type"] == "final"
    checkpoint = await graph_runtime._checkpointer_bundle.checkpointer.aget_tuple(
        {"configurable": {"thread_id": "thread-a"}}
    )
    assert checkpoint is not None

    await service.clear_thread("thread-a")

    result = await graph_runtime._checkpointer_bundle.checkpointer.aget_tuple(
        {"configurable": {"thread_id": "thread-a"}}
    )
    assert result is None
    assert service._session_service.deleted_sessions == ["thread-a"]

    second_events = [event async for event in service.ainvoke("follow up", "thread-a")]
    assert second_events[-1]["type"] == "final"
    second_checkpoint = await graph_runtime._checkpointer_bundle.checkpointer.aget_tuple(
        {"configurable": {"thread_id": "thread-a"}}
    )
    assert second_checkpoint is not None


@pytest.mark.asyncio
async def test_langgraph_failure_discards_partial_checkpoint(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.agent.checkpointer.get_config",
        lambda key, default=None: "memory" if key == "agent.langgraph_checkpointer.backend" else default,
    )

    @tool
    def boom(text: str) -> str:
        """Always fail while executing a tool."""
        raise RuntimeError(f"boom:{text}")

    failing_model = BindToolsFakeModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[{"name": "boom", "args": {"text": "x"}, "id": "call_1", "type": "tool_call"}],
            )
        ]
    )
    graph_runtime = LangGraphAgentRuntime(
        build_openai_tools_system_prompt=lambda tools: "system",
        create_mcp_manager=lambda: None,
    )
    graph_runtime.agent_executor = create_agent(
        model=failing_model,
        tools=[boom],
        system_prompt="You are helpful",
        checkpointer=graph_runtime._checkpointer_bundle.checkpointer,
    )

    service = AgentService(session_service=PersistingSessionService())
    service._set_runtime_for_testing(graph_runtime)

    async def initialized_noop() -> None:
        return None

    monkeypatch.setattr(service, "_async_ensure_initialized", initialized_noop)

    failed_events = [event async for event in service.ainvoke("hello", "thread-failure")]

    assert any(event["type"] == "error" and "boom:x" in event["content"] for event in failed_events)
    checkpoint = await graph_runtime._checkpointer_bundle.checkpointer.aget_tuple(
        {"configurable": {"thread_id": "thread-failure"}}
    )
    assert checkpoint is None

    recovered_model = BindToolsFakeModel(responses=[AIMessage(content="recovered")])
    graph_runtime.agent_executor = create_agent(
        model=recovered_model,
        tools=[boom],
        system_prompt="You are helpful",
        checkpointer=graph_runtime._checkpointer_bundle.checkpointer,
    )

    recovered_events = [event async for event in service.ainvoke("follow up", "thread-failure")]

    final_events = [event for event in recovered_events if event["type"] == "final"]
    assert final_events[-1]["content"] == "recovered"
    recovered_checkpoint = await graph_runtime._checkpointer_bundle.checkpointer.aget_tuple(
        {"configurable": {"thread_id": "thread-failure"}}
    )
    assert recovered_checkpoint is not None
    recovered_messages = [
        getattr(message, "content", "") for message in recovered_checkpoint.checkpoint["channel_values"]["messages"]
    ]
    assert "" not in recovered_messages
