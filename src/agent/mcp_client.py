import asyncio
from contextlib import AsyncExitStack
from typing import Any, List, Optional, Type

from loguru import logger
from pydantic import BaseModel, create_model
from langchain_core.tools import BaseTool
from langchain_core.callbacks import CallbackManagerForToolRun

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import CallToolResult

from src.core.config import get as get_config

# Connection-level errors that indicate the MCP server process is dead.
# When these occur, we mark the manager as not running and stop all retries.
_FATAL_ERROR_TYPES = (
    "ClosedResourceError",
    "BrokenPipeError",
    "ConnectionResetError",
    "ConnectionRefusedError",
    "ConnectionAbortedError",
    "NotImplementedError",
)

# Maximum consecutive failures allowed per tool within a single agent run.
_MAX_CONSECUTIVE_FAILURES = 2


class PlaywrightMcpManager:
    """Manages the Playwright MCP server process and generates LangChain tools from it."""

    def __init__(self):
        self._enabled = get_config("agent.playwright_mcp.enabled", False)
        if not self._enabled:
            return

        self._command = get_config("agent.playwright_mcp.command", "npx")
        self._args = get_config("agent.playwright_mcp.args", ["-y", "@playwright/mcp"])

        self._exit_stack: Optional[AsyncExitStack] = None
        self._session: Optional[ClientSession] = None
        self._tools_cache: List[BaseTool] = []
        self._is_running = False
        self._call_lock = asyncio.Lock()

        self._manager_task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None

    async def start(self) -> None:
        """Starts the MCP server subprocess and initializes the session."""
        if not self._enabled or self._is_running:
            return

        if self._manager_task and not self._manager_task.done():
            return

        logger.info(
            f"Starting Playwright MCP Server in background task: {self._command} {' '.join(self._args)}"
        )

        loop = asyncio.get_running_loop()
        start_future = loop.create_future()
        self._stop_event = asyncio.Event()
        self._manager_task = asyncio.create_task(self._run_manager_task(start_future))

        try:
            await start_future
        except Exception as e:
            logger.error(f"Failed to start Playwright MCP server: {e}")
            await self.stop()

    async def _run_manager_task(self, start_future: asyncio.Future) -> None:
        """Dedicated background task to hold the anyio/MCP context managers with auto-restart."""
        base_backoff = 2.0
        max_backoff = 60.0
        attempt = 0

        while True:
            if self._stop_event and self._stop_event.is_set():
                break

            await self._cleanup_old_resources()
            self._exit_stack = AsyncExitStack()

            server_params = StdioServerParameters(command=self._command, args=self._args, env=None)

            try:
                read_stream, write_stream = await self._exit_stack.enter_async_context(
                    stdio_client(server_params)
                )
                self._session = await self._exit_stack.enter_async_context(
                    ClientSession(read_stream, write_stream)
                )

                await self._session.initialize()
                await self._fetch_tools()

                for tool in self._tools_cache:
                    if hasattr(tool, "_consecutive_failures"):
                        tool._consecutive_failures = 0

                self._is_running = True
                attempt = 0
                logger.info("Playwright MCP Server initialized successfully.")

                if not start_future.done():
                    start_future.set_result(True)

                if self._stop_event:
                    await self._stop_event.wait()

            except Exception as e:
                self._is_running = False
                if not start_future.done():
                    start_future.set_exception(e)
                    break

                if self._stop_event and self._stop_event.is_set():
                    break

                attempt += 1
                backoff = min(max_backoff, base_backoff * (2 ** (attempt - 1)))
                logger.error(
                    f"MCP background task crashed: {e}. Restarting in {backoff}s (attempt {attempt})..."
                )
                await asyncio.sleep(backoff)

            finally:
                self._is_running = False
                await self._cleanup_old_resources()
                logger.info("Playwright MCP Server stopped and resources cleaned.")

            if self._stop_event and self._stop_event.is_set():
                break

    async def _cleanup_old_resources(self) -> None:
        """Close stale MCP connection resources, ignoring cleanup errors."""
        self._session = None
        if self._exit_stack:
            try:
                await self._exit_stack.aclose()
            except Exception:
                pass
            self._exit_stack = None

    async def stop(self) -> None:
        """Stops the MCP server and cleans up resources."""
        self._is_running = False
        if self._stop_event:
            self._stop_event.set()

        if self._manager_task and not self._manager_task.done():
            try:
                # Wait for the task to finish cleanup
                await asyncio.wait_for(self._manager_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._manager_task.cancel()

        self._manager_task = None
        self._tools_cache = []

    async def _fetch_tools(self) -> None:
        """Fetches tools from the MCP server and wraps them for LangChain."""
        if not self._session:
            return

        try:
            response = await self._session.list_tools()
            self._tools_cache = []
            for tool in response.tools:
                langchain_tool = self._wrap_mcp_tool(tool.name, tool.description, tool.inputSchema)
                self._tools_cache.append(langchain_tool)
            logger.info(f"Loaded {len(self._tools_cache)} tools from Playwright MCP.")
        except Exception as e:
            logger.error(f"Failed to fetch tools from MCP server: {e}")

    def get_langchain_tools(self) -> List[BaseTool]:
        """Returns the list of LangChain tools generated from the MCP server."""
        return self._tools_cache

    def _wrap_mcp_tool(self, name: str, description: str, input_schema: dict) -> BaseTool:
        """Dynamically creates a LangChain BaseTool from an MCP tool definition.

        The tool holds a reference to the manager (self) so it always uses the
        current session, not a stale snapshot captured at creation time.
        """

        # 1. Create a Pydantic model for the input args based on the JSON schema
        fields = {}
        properties = input_schema.get("properties", {})
        required = input_schema.get("required", [])

        for prop_name, prop_info in properties.items():
            prop_type_str = prop_info.get("type", "string")

            # Map json schema types to python types
            python_type = Any
            if prop_type_str == "string":
                python_type = str
            elif prop_type_str == "number":
                python_type = float
            elif prop_type_str == "integer":
                python_type = int
            elif prop_type_str == "boolean":
                python_type = bool

            is_required = prop_name in required

            if is_required:
                fields[prop_name] = (python_type, ...)
            else:
                fields[prop_name] = (Optional[python_type], prop_info.get("default", None))

        # Create dynamic pydantic model
        schema_model = create_model(f"McpTool_{name}Schema", **fields)

        tool_name = name
        tool_desc = description
        manager_ref = self  # hold reference to the manager, not the session

        # 2. Define the BaseTool subclass - holds manager_ref for live session lookup
        class McpWrappedTool(BaseTool):
            name: str = tool_name
            description: str = tool_desc
            args_schema: Type[BaseModel] = schema_model
            _consecutive_failures: int = 0

            async def _arun(
                self, *args, run_manager: Optional[CallbackManagerForToolRun] = None, **kwargs
            ) -> str:
                # --- Guard: MCP server already known to be dead ---
                if not manager_ref._is_running:
                    return (
                        f"[错误] 浏览器服务 MCP 已断开连接，工具 {self.name} 不可用。"
                        "请不要再尝试调用任何 browser_ 开头的工具，改用其他方式完成任务。"
                    )

                # --- Guard: too many consecutive failures for this tool ---
                if self._consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                    return (
                        f"[错误] 工具 {self.name} 已连续失败 {self._consecutive_failures} 次，"
                        "已自动禁用以避免浪费资源。请不要再调用此工具，改用其他方式。"
                    )

                session = manager_ref._session
                if not session:
                    return (
                        f"[错误] 浏览器服务 MCP 未连接，工具 {self.name} 不可用。"
                        "请不要再尝试调用任何 browser_ 开头的工具。"
                    )

                # Filter out None values to avoid violating MCP strict TypeScript schemas
                clean_arguments = {k: v for k, v in kwargs.items() if v is not None}

                try:
                    # Serialize call_tool; MCP stdio is a single JSON-RPC connection.
                    async with manager_ref._call_lock:
                        result: CallToolResult = await session.call_tool(
                            self.name, arguments=clean_arguments
                        )

                    # Serialize the result
                    output_parts = []
                    for content in result.content:
                        if content.type == "text":
                            output_parts.append(content.text)
                        elif content.type == "image":
                            output_parts.append(
                                f"[Image content returned (base64 omitted), mime_type: {content.mimeType}]"
                            )
                        else:
                            output_parts.append(f"[Unknown content type: {content.type}]")

                    if result.isError:
                        self._consecutive_failures += 1
                        return f"MCP 工具错误: {' | '.join(output_parts)}"

                    # Success: reset failure counter.
                    self._consecutive_failures = 0
                    return "\n".join(output_parts)

                except Exception as e:
                    exc_type = type(e).__name__
                    exc_msg = str(e) or "(无详细信息)"
                    logger.warning(f"MCP tool {self.name} failed: [{exc_type}] {exc_msg}")

                    self._consecutive_failures += 1

                    # Detect fatal connection errors and disable browser tools for this session.
                    if exc_type in _FATAL_ERROR_TYPES:
                        logger.error(
                            f"MCP fatal error detected ({exc_type}), marking MCP server as dead. "
                            "All browser tools are now disabled for this session."
                        )
                        manager_ref._is_running = False
                        manager_ref._session = None
                        return (
                            f"[严重错误] 浏览器服务 MCP 进程已崩溃: {exc_type}。"
                            "所有 browser_ 工具已不可用，请不要再尝试调用它们，改用其他方式完成任务。"
                        )

                    return (
                        f"MCP 工具 {self.name} 执行失败: [{exc_type}] {exc_msg}。"
                        f"(已连续失败 {self._consecutive_failures}/{_MAX_CONSECUTIVE_FAILURES} 次)"
                    )

            def _run(self, *args, **kwargs) -> Any:
                raise NotImplementedError("This tool only supports async execution.")

        return McpWrappedTool()
