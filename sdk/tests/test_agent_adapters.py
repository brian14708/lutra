"""Pinned provider contracts and real subprocess cancellation."""

from __future__ import annotations

import asyncio
import contextvars
import os
import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys
import threading
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest
from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, SystemMessage
from jsonschema import Draft202012Validator
from lutra.agent import AgentSettings, ToolResult
from lutra.contrib._conversation import output_format
from lutra.contrib.claude import Claude
from lutra.contrib.codex import Codex, _Bridge
from openai_codex.client import CodexConfig
from openai_codex.generated.v2_all import (
    ItemCompletedNotification,
    ItemStartedNotification,
    TurnCompletedNotification,
)
from pydantic import BaseModel, ConfigDict, TypeAdapter

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Mapping
    from pathlib import Path

    from lutra.tools import ToolSchema


@dataclass
class Host:
    workspace: Path
    tools: tuple[ToolSchema, ...] = ()
    events: list[tuple[str, object]] = field(default_factory=list)
    calls: list[tuple[str | None, str, Mapping[str, object]]] = field(default_factory=list)
    started: asyncio.Event = field(default_factory=asyncio.Event)

    async def emit(self, kind: str, data: object) -> None:
        self.events.append((kind, data))

    async def call_tool(
        self, name: str, arguments: Mapping[str, object], *, call_id: str | None = None
    ) -> ToolResult:
        self.calls.append((call_id, name, arguments))
        self.started.set()
        await asyncio.Event().wait()
        return ToolResult()


def test_provider_schema_preserves_nested_root_references() -> None:
    class Child(BaseModel):
        model_config = ConfigDict(strict=True)
        value: int

    class Parent(BaseModel):
        child: Child
        optional: Child | None

    schema = TypeAdapter(Parent).json_schema()
    wrapped = output_format(schema)
    assert wrapped is not None
    assert wrapped["$defs"] == schema["$defs"]
    assert "$defs" not in wrapped["properties"]["output"]
    Draft202012Validator(wrapped).validate({"output": {"child": {"value": 7}, "optional": None}})
    assert "$defs" in schema


def test_provider_schema_preserves_recursive_root_reference() -> None:
    class Node(BaseModel):
        value: int
        child: Node | None = None

    schema = TypeAdapter(Node).json_schema()
    wrapped = output_format(schema)
    assert wrapped is not None
    assert wrapped["properties"]["output"]["$ref"] == schema["$ref"]
    Draft202012Validator(wrapped).validate({
        "output": {"value": 1, "child": {"value": 2, "child": None}}
    })


@pytest.mark.asyncio
async def test_bridge_context_and_closed_callback_race() -> None:
    identity: contextvars.ContextVar[str] = contextvars.ContextVar("parent")
    identity.set("parent-action")
    bridge = _Bridge()

    async def read() -> str:
        await asyncio.sleep(0)
        return identity.get()

    assert await asyncio.to_thread(bridge.call, read) == "parent-action"
    await bridge.close()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.to_thread(bridge.call, read), timeout=2)


@pytest.mark.asyncio
async def test_codex_cancellation_unblocks_reader_and_reaps_process(  # ruff: ignore[complex-structure]
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clients: list[Any] = []
    reader_stopped = threading.Event()

    class Client:
        def __init__(
            self, config: CodexConfig, approval_handler: Callable[[str, dict[str, object]], object]
        ) -> None:
            self.config = config
            self.callback = approval_handler
            self.process: subprocess.Popen[bytes] | None = None
            clients.append(self)

        def start(self) -> None:
            self.process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(300)"])

        def initialize(self) -> None:
            pass

        def thread_start(self, params: dict[str, object]) -> SimpleNamespace:
            assert params["cwd"] == str(tmp_path)
            assert params["dynamicTools"] == []
            assert params["approvalPolicy"] == "never"
            assert params["sandbox"] == "danger-full-access"
            assert self.config.config_overrides == (
                'approval_policy="never"',
                'sandbox_mode="danger-full-access"',
                "analytics.enabled=false",
                'otel.exporter="none"',
                'otel.trace_exporter="none"',
                'otel.metrics_exporter="none"',
                'model="test"',
            )
            assert self.config.env is not None
            assert self.config.env["OTEL_SDK_DISABLED"] == "true"
            return SimpleNamespace(thread=SimpleNamespace(id="thread"))

        @staticmethod
        def turn_start(*_args: object, **_kwargs: object) -> SimpleNamespace:
            return SimpleNamespace(turn=SimpleNamespace(id="turn"))

        def register_turn_notifications(self, _turn: str) -> None:
            pass

        def unregister_turn_notifications(self, _turn: str) -> None:
            pass

        def next_turn_notification(self, _turn: str) -> object:
            try:
                self.callback(
                    "item/tool/call", {"callId": "call", "tool": "blocked", "arguments": {}}
                )
            finally:
                reader_stopped.set()
            message = "unreachable"
            raise AssertionError(message)

        def close(self) -> None:
            if self.process is not None and self.process.poll() is None:
                self.process.terminate()
                self.process.wait(timeout=2)

    monkeypatch.setattr("lutra.contrib.codex.CodexClient", Client)
    host = Host(tmp_path)
    adapter = Codex(config=CodexConfig(config_overrides=('model="test"',)))
    turn = asyncio.create_task(adapter.run(host, "blocked"))
    await asyncio.wait_for(host.started.wait(), timeout=3)
    turn.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(turn, timeout=3)
    assert reader_stopped.is_set()
    assert clients[0].process.poll() is not None
    assert host.calls == [("call", "blocked", {})]


@pytest.mark.asyncio
async def test_codex_schema_terminal_and_explicit_continuation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[tuple[str, object]] = []

    class Client:
        def __init__(self, config: CodexConfig, **_kwargs: object) -> None:
            assert config.env is not None
            assert config.env["OPENAI_API_KEY"] == "test-key"
            assert 'model_providers.lutra.base_url="http://localhost/v1"' in config.config_overrides
            self.notifications: list[object] = []

        def start(self) -> None:
            pass

        def initialize(self) -> None:
            pass

        @staticmethod
        def thread_start(params: dict[str, object]) -> object:
            assert params["model"] == "shared-model"
            assert params["modelProvider"] == "lutra"
            return SimpleNamespace(thread=SimpleNamespace(id="thread"))

        def turn_start(self, _thread: str, prompt: str, *, params: dict[str, object]) -> object:
            requests.append((prompt, params))
            text = "answer" if params["outputSchema"] is None else '{"output": false}'
            user = {
                "type": "userMessage",
                "id": "user",
                "content": [{"type": "text", "text": prompt}],
            }
            tool = {
                "type": "dynamicToolCall",
                "id": "call",
                "tool": "lookup",
                "arguments": {"supplier": "atlas"},
                "status": "inProgress",
            }
            self.notifications = [
                ItemStartedNotification.model_validate({
                    "startedAtMs": 0,
                    "threadId": "thread",
                    "turnId": "turn",
                    "item": user,
                }),
                ItemCompletedNotification.model_validate({
                    "completedAtMs": 1,
                    "threadId": "thread",
                    "turnId": "turn",
                    "item": user,
                }),
                ItemStartedNotification.model_validate({
                    "startedAtMs": 2,
                    "threadId": "thread",
                    "turnId": "turn",
                    "item": tool,
                }),
                ItemCompletedNotification.model_validate({
                    "completedAtMs": 3,
                    "threadId": "thread",
                    "turnId": "turn",
                    "item": {**tool, "status": "completed", "success": True},
                }),
                ItemCompletedNotification.model_validate({
                    "completedAtMs": 0,
                    "threadId": "thread",
                    "turnId": "turn",
                    "item": {"type": "agentMessage", "id": "message", "text": text},
                }),
                TurnCompletedNotification.model_validate({
                    "threadId": "thread",
                    "turn": {"id": "turn", "items": [], "status": "completed", "error": None},
                }),
            ]
            return SimpleNamespace(turn=SimpleNamespace(id="turn"))

        def register_turn_notifications(self, _turn: str) -> None:
            pass

        def unregister_turn_notifications(self, _turn: str) -> None:
            pass

        def next_turn_notification(self, _turn: str) -> object:
            return SimpleNamespace(payload=self.notifications.pop(0))

        def close(self) -> None:
            pass

    monkeypatch.setattr("lutra.contrib.codex.CodexClient", Client)
    host = Host(tmp_path)
    adapter = Codex(
        settings=AgentSettings(
            model="shared-model", base_url="http://localhost", api_key="test-key"
        )
    )
    saved = await adapter.run(host, "work")
    assert saved.output == "answer"
    assert (
        await adapter.run(host, "follow-up", output_schema={"type": "boolean"}, state=saved.state)
    ).output is False
    assert [
        data["codex"]["item"]["type"] if isinstance(data, dict) else None
        for kind, data in host.events
        if kind == "native.activity"
    ] == ["userMessage", "agentMessage", "userMessage", "agentMessage"]
    assert "previous_conversation" in requests[1][0]
    assert "answer" in requests[1][0]
    assert "threadId" not in requests[1][0]
    assert "completedAtMs" not in requests[1][0]
    assert saved.state is not None
    assert saved.state.records == [{"user": "work"}, {"assistant": "answer"}]
    assert host.events


@pytest.mark.asyncio
async def test_claude_native_config_schema_and_nullable_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    options_seen: list[ClaudeAgentOptions] = []
    prompts: list[str] = []
    disconnected: list[bool] = []

    class Client:
        def __init__(self, *, options: ClaudeAgentOptions) -> None:
            options_seen.append(options)
            self.options = options

        @staticmethod
        async def connect() -> None:
            await asyncio.sleep(0)

        @staticmethod
        async def query(prompt: str) -> None:
            prompts.append(prompt)

        async def receive_response(self) -> AsyncIterator[ResultMessage | SystemMessage]:
            for count in range(100):
                yield SystemMessage(subtype="thinking_tokens", data={"estimated_tokens": count})
            yield ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id="session",
                usage={"input_tokens": 10, "output_tokens": 2},
                result="answer",
                structured_output={"output": None} if self.options.output_format else None,
            )

        @staticmethod
        async def disconnect() -> None:
            disconnected.append(True)

    monkeypatch.setattr("lutra.contrib.claude.ClaudeSDKClient", Client)
    options = ClaudeAgentOptions(
        model="test-model", env={"NATIVE": "value"}, allowed_tools=["Read"]
    )
    adapter = Claude(
        options=options,
        settings=AgentSettings(
            model="shared-model", base_url="http://localhost", api_key="test-key"
        ),
    )
    host = Host(
        tmp_path,
        tools=({"name": "lookup", "description": "Lookup", "input_schema": {"type": "object"}},),
    )
    first = await adapter.run(host, "work")
    assert first.output == "answer"
    assert first.usage == {"input_tokens": 10, "output_tokens": 2}
    assert (
        await adapter.run(host, "nullable", output_schema={"type": "null"}, state=first.state)
    ).output is None
    assert len(disconnected) == 2
    assert options.output_format is None
    assert options.allowed_tools == ["Read"]
    assert options_seen[0].model == "shared-model"
    assert options_seen[0].env["ANTHROPIC_BASE_URL"] == "http://localhost"
    assert options_seen[0].env["ANTHROPIC_API_KEY"] == "test-key"
    assert options_seen[0].env["NATIVE"] == "value"
    assert options_seen[0].env["CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC"] == "1"
    assert options_seen[0].permission_mode == "default"
    assert options_seen[0].cwd == str(tmp_path)
    assert "mcp__lutra__lookup" in options_seen[0].allowed_tools
    assert options_seen[1].output_format == {
        "type": "json_schema",
        "schema": {
            "type": "object",
            "properties": {"output": {"type": "null"}},
            "required": ["output"],
            "additionalProperties": False,
        },
    }
    assert "answer" in prompts[1]
    assert "duration_ms" not in prompts[1]
    assert first.state is not None
    assert first.state.records == [{"user": "work"}, {"assistant": "answer"}]
    assert [kind for kind, _ in host.events] == ["native.activity", "native.activity"]


@pytest.mark.asyncio
async def test_claude_cancellation_disconnects_actual_sdk_subprocess(tmp_path: Path) -> None:
    script = tmp_path / "fake-claude"
    pid_file = tmp_path / "pid"
    script.write_text(f"""#!{sys.executable}
import json, os, sys, time
from pathlib import Path
if "-v" in sys.argv:
    print("2.1.163")
    sys.exit(0)
Path({str(pid_file)!r}).write_text(str(os.getpid()))
for line in sys.stdin:
    request = json.loads(line)
    if request.get("type") == "control_request":
        response = {{"subtype": "success", "request_id": request["request_id"], "response": {{}}}}
        print(json.dumps({{"type": "control_response", "response": response}}), flush=True)
""")
    script.chmod(0o755)
    host = Host(tmp_path)
    adapter = Claude(options=ClaudeAgentOptions(cli_path=script))
    turn = asyncio.create_task(adapter.run(host, "wait forever"))
    async with asyncio.timeout(5):
        while not pid_file.exists():  # ruff: ignore[async-busy-wait]
            await asyncio.sleep(0.01)
    await asyncio.sleep(0.1)
    pid = int(pid_file.read_text())
    turn.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(turn, timeout=5)
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
