"""Codex adapter for the pinned synchronous app-server SDK."""

from __future__ import annotations

import asyncio
import contextvars
import json
from concurrent.futures import Future
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, TypeVar
from urllib.parse import urlsplit, urlunsplit

from openai_codex.client import CodexClient, CodexConfig
from openai_codex.models import (
    ItemCompletedNotification,
    ThreadTokenUsageUpdatedNotification,
    TurnCompletedNotification,
)

from lutra.agent import AgentHost, AgentSettings, TurnResult
from lutra.contrib._conversation import Conversation, output_format, render_tool, structured_output
from lutra.contrib._defaults import agent_env

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Mapping

    from openai_codex.models import JsonObject


T = TypeVar("T")


def _api_v1(base_url: str) -> str:
    url = urlsplit(base_url)
    path = url.path.rstrip("/") + "/v1"
    return urlunsplit(url._replace(path=path))


class _Bridge:
    """Own reader-thread callbacks in the parent task's event loop and context."""

    def __init__(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.context = contextvars.copy_context()
        self.pending: set[asyncio.Task[Any]] = set()
        self.closed = False

    def call(self, operation: Callable[[], Coroutine[Any, Any, T]]) -> T:
        result: Future[T] = Future()

        def completed(task: asyncio.Task[T]) -> None:
            self.pending.discard(task)
            if task.cancelled():
                result.set_exception(asyncio.CancelledError())
            elif (error := task.exception()) is not None:
                result.set_exception(error)
            else:
                result.set_result(task.result())

        def start() -> None:
            if self.closed:
                result.set_exception(asyncio.CancelledError())
                return
            task = self.loop.create_task(operation(), context=self.context.copy())
            self.pending.add(task)
            task.add_done_callback(completed)

        self.loop.call_soon_threadsafe(start)
        return result.result()

    async def close(self) -> None:
        self.closed = True
        for task in self.pending:
            task.cancel()
        await asyncio.gather(*self.pending, return_exceptions=True)


@dataclass(frozen=True)
class Codex:
    """Codex with sandbox-friendly native client defaults."""

    config: CodexConfig = field(default_factory=CodexConfig)
    thread_options: Mapping[str, Any] = field(default_factory=dict)
    settings: AgentSettings = field(default_factory=AgentSettings)

    async def run(
        self,
        host: AgentHost,
        prompt: str,
        *,
        output_schema: Mapping[str, object] | None = None,
        state: Conversation | None = None,
    ) -> TurnResult[Conversation]:
        """Run a turn and close callbacks before the app-server process.

        Returns:
            Output and live conversation context.

        """
        conversation = Conversation([] if state is None else list(state.records))
        turn = _Turn(self, host, conversation)
        worker = asyncio.create_task(asyncio.to_thread(turn.run, prompt, output_schema))
        try:
            return await asyncio.shield(worker)
        finally:
            await turn.bridge.close()
            await asyncio.to_thread(turn.client.close)
            await asyncio.gather(worker, return_exceptions=True)


class _Turn:
    def __init__(self, adapter: Codex, host: AgentHost, conversation: Conversation) -> None:
        self.adapter, self.host, self.conversation = adapter, host, conversation
        self.bridge = _Bridge()
        self.client = CodexClient(
            replace(
                adapter.config,
                cwd=str(host.workspace),
                env=agent_env({
                    **adapter.settings.env,
                    **(adapter.config.env or {}),
                    **(
                        {"OPENAI_API_KEY": adapter.settings.api_key}
                        if adapter.settings.api_key
                        else {}
                    ),
                }),
                config_overrides=(
                    'approval_policy="never"',
                    'sandbox_mode="danger-full-access"',
                    "analytics.enabled=false",
                    'otel.exporter="none"',
                    'otel.trace_exporter="none"',
                    'otel.metrics_exporter="none"',
                    *adapter.config.config_overrides,
                    *(
                        (
                            'model_provider="lutra"',
                            'model_providers.lutra.name="Lutra"',
                            "model_providers.lutra.base_url="
                            + json.dumps(_api_v1(adapter.settings.base_url)),
                            'model_providers.lutra.wire_api="responses"',
                            "model_providers.lutra.requires_openai_auth=false",
                            'model_providers.lutra.env_key="OPENAI_API_KEY"',
                        )
                        if adapter.settings.base_url
                        else ()
                    ),
                ),
            ),
            approval_handler=self._callback,
        )

    def _callback(self, method: str, params: dict[str, Any] | None) -> dict[str, Any]:
        if method != "item/tool/call" or params is None:
            message = f"unsupported Codex callback: {method}"
            raise ValueError(message)
        return self.bridge.call(lambda: self._tool(params))

    async def _tool(self, params: dict[str, Any]) -> dict[str, Any]:
        name, arguments = params["tool"], params["arguments"]
        result = await self.host.call_tool(name, arguments, call_id=params.get("callId"))
        text = render_tool(result)
        self.conversation.records.append({
            "tool": name,
            "arguments": arguments,
            "result": text,
            "error": result.error is not None,
        })
        return {
            "contentItems": [{"type": "inputText", "text": text}],
            "success": result.error is None,
        }

    def run(self, prompt: str, schema: Mapping[str, object] | None) -> TurnResult[Conversation]:
        self.client.start()
        self.client.initialize()
        options: JsonObject = {
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
            **self.adapter.thread_options,
            **({"model": self.adapter.settings.model} if self.adapter.settings.model else {}),
            **({"modelProvider": "lutra"} if self.adapter.settings.base_url else {}),
            "cwd": str(self.host.workspace),
            "ephemeral": True,
            "dynamicTools": [
                {
                    "type": "function",
                    "name": tool["name"],
                    "description": tool["description"],
                    "inputSchema": tool["input_schema"],
                }
                for tool in self.host.tools
            ],
        }
        thread_id = self.client.thread_start(options).thread.id
        request = self.conversation.prompt(prompt)
        self.conversation.records.append({"user": prompt})
        turn = self.client.turn_start(
            thread_id, request, params={"outputSchema": output_format(schema)}
        ).turn
        self.client.register_turn_notifications(turn.id)
        text: str | None = None
        usage: Mapping[str, object] | None = None
        try:
            while True:
                payload = self.client.next_turn_notification(turn.id).payload
                if isinstance(payload, ThreadTokenUsageUpdatedNotification):
                    usage = payload.token_usage.model_dump(mode="json", by_alias=True)
                elif isinstance(payload, ItemCompletedNotification):
                    details = payload.model_dump(mode="json", by_alias=True)
                    item = payload.item.root
                    if item.type != "dynamicToolCall":
                        self.bridge.call(
                            lambda details=details: self.host.emit(
                                "native.activity", {"codex": details}
                            )
                        )
                    if item.type == "agentMessage":
                        text = item.text
                        self.conversation.records.append({"assistant": text})
                    elif item.type not in {"userMessage", "dynamicToolCall"}:
                        self.conversation.records.append({"codex": details["item"]})
                elif isinstance(payload, TurnCompletedNotification):
                    if payload.turn.status.value != "completed":
                        message = f"Codex turn failed: {payload.turn.error}"
                        raise RuntimeError(message)
                    if text is None:
                        message = "Codex completed without an output message"
                        raise ValueError(message)
                    output = text if schema is None else structured_output(json.loads(text))
                    break
            return TurnResult(output, self.conversation, usage=usage)
        finally:
            self.client.unregister_turn_notifications(turn.id)
