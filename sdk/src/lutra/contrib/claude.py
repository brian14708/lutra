"""Claude Code sessions using native in-process MCP tools."""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from dataclasses import dataclass, field, replace
from functools import partial
from typing import TYPE_CHECKING, Any

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    PermissionResultAllow,
    ResultMessage,
    SdkMcpTool,
    StreamEvent,
    SystemMessage,
    TextBlock,
    UserMessage,
    create_sdk_mcp_server,
)

from lutra.agent import AgentHost, AgentSettings, TurnResult
from lutra.contrib._conversation import Conversation, output_format, render_tool, structured_output
from lutra.contrib._defaults import agent_env

if TYPE_CHECKING:
    from collections.abc import Mapping

    from claude_agent_sdk import ToolPermissionContext

_LOGGER = logging.getLogger(__name__)


async def _allow_tool(  # ruff: ignore[unused-async]
    _name: str, arguments: dict[str, Any], _context: ToolPermissionContext
) -> PermissionResultAllow:
    return PermissionResultAllow(updated_input=arguments)


@dataclass(frozen=True)
class Claude:
    """A Claude adapter with sandbox-friendly native SDK defaults."""

    options: ClaudeAgentOptions = field(default_factory=ClaudeAgentOptions)
    settings: AgentSettings = field(default_factory=AgentSettings)

    async def run(
        self,
        host: AgentHost,
        prompt: str,
        *,
        output_schema: Mapping[str, object] | None = None,
        state: Conversation | None = None,
    ) -> TurnResult[Conversation]:
        """Run a turn using the supplied native configuration.

        Returns:
            Output and live conversation context.

        """
        conversation = Conversation([] if state is None else list(state.records))
        return await _Turn(self.options, self.settings, host, conversation).run(
            prompt, output_schema=output_schema
        )


class _Turn:
    def __init__(
        self,
        options: ClaudeAgentOptions,
        settings: AgentSettings,
        host: AgentHost,
        conversation: Conversation,
    ) -> None:
        self.options, self.settings = options, settings
        self.host, self.conversation = host, conversation

    async def _tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        result = await self.host.call_tool(name, arguments)
        text = render_tool(result)
        self.conversation.records.append({
            "tool": name,
            "arguments": arguments,
            "result": text,
            "error": result.error is not None,
        })
        return {"content": [{"type": "text", "text": text}], "isError": result.error is not None}

    def _options(self, schema: Mapping[str, object] | None) -> ClaudeAgentOptions:
        if not isinstance(self.options.mcp_servers, dict):
            message = "Claude agent sessions require a mapping for mcp_servers"
            raise TypeError(message)
        if "lutra" in self.options.mcp_servers:
            message = "the lutra MCP server name is reserved for agent tools"
            raise ValueError(message)
        server = create_sdk_mcp_server(
            name="lutra",
            tools=[
                SdkMcpTool(
                    tool["name"],
                    tool["description"],
                    tool["input_schema"],
                    partial(self._tool, tool["name"]),
                )
                for tool in self.host.tools
            ],
        )
        return replace(
            self.options,
            model=self.settings.model or self.options.model,
            cwd=str(self.host.workspace),
            env=agent_env({
                **self.settings.env,
                **self.options.env,
                **(
                    {"ANTHROPIC_BASE_URL": self.settings.base_url} if self.settings.base_url else {}
                ),
                **({"ANTHROPIC_API_KEY": self.settings.api_key} if self.settings.api_key else {}),
            }),
            permission_mode=self.options.permission_mode or "default",
            can_use_tool=(
                self.options.can_use_tool
                or (
                    _allow_tool
                    if self.options.permission_mode is None
                    and self.options.permission_prompt_tool_name is None
                    else None
                )
            ),
            mcp_servers={**self.options.mcp_servers, "lutra": server},
            allowed_tools=[
                *self.options.allowed_tools,
                *(f"mcp__lutra__{tool['name']}" for tool in self.host.tools),
            ],
            output_format=None if schema is None else {"type": "json_schema", "schema": schema},
            resume=None,
            continue_conversation=False,
            session_id=None,
        )

    async def _receive(self, client: ClaudeSDKClient) -> tuple[ResultMessage | None, list[str]]:
        terminal: ResultMessage | None = None
        texts: list[str] = []
        async for message in client.receive_response():
            if isinstance(message, StreamEvent) or (
                isinstance(message, SystemMessage) and message.subtype == "thinking_tokens"
            ):
                continue
            details = dataclasses.asdict(message)
            await self.host.emit("native.activity", {"claude": details})
            if isinstance(message, (AssistantMessage, UserMessage)):
                self.conversation.records.append({"claude": details["content"]})
            if isinstance(message, AssistantMessage):
                texts.extend(
                    block.text for block in message.content if isinstance(block, TextBlock)
                )
            if isinstance(message, ResultMessage):
                terminal = message
        return terminal, texts

    async def run(
        self, prompt: str, *, output_schema: Mapping[str, object] | None = None
    ) -> TurnResult[Conversation]:
        schema = output_format(output_schema)
        request = self.conversation.prompt(prompt)
        self.conversation.records.append({"user": prompt})
        client = ClaudeSDKClient(options=self._options(schema))
        connected = False
        cancelled = False
        try:
            await client.connect()
            connected = True
            await client.query(request)
            terminal, texts = await self._receive(client)
        except asyncio.CancelledError:
            cancelled = True
            try:
                if connected:
                    async with asyncio.timeout(3):
                        await client.interrupt()
            except Exception:
                _LOGGER.debug("Claude interruption failed during cancellation", exc_info=True)
            raise
        finally:
            try:
                await client.disconnect()
            except Exception:
                if not cancelled:
                    raise
        if terminal is None or terminal.is_error or terminal.subtype != "success":
            message = f"Claude turn failed: {terminal}"
            raise RuntimeError(message)
        if schema is None and terminal.result is None and not texts:
            message = "Claude completed without an output message"
            raise ValueError(message)
        output = (
            structured_output(terminal.structured_output)
            if schema is not None
            else terminal.result
            if terminal.result is not None
            else "\n".join(texts)
        )
        self.conversation.records.append({"assistant": output})
        return TurnResult(output, self.conversation, usage=terminal.usage or terminal.model_usage)
