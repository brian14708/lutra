"""Agent tools execute inside a leaf task's session."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast

import pytest
from lutra import RetryMode, TaskEnvironment
from lutra._context import TaskContext, current_context, task_context
from lutra.agent import Agent, AgentHost, ToolResult, TurnResult

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from lutra.agent import Tool
    from lutra.blob import BlobStore
    from lutra.checkpoint import CheckpointManager


class CallingAdapter:
    @staticmethod
    async def run(
        host: AgentHost,
        prompt: str,
        *,
        output_schema: Mapping[str, object] | None = None,
        state: None = None,
    ) -> TurnResult[None]:
        assert prompt == "lookup"
        assert output_schema is None
        assert state is None
        results = await asyncio.gather(
            host.call_tool("lookup", {"name": "Lutra"}, call_id="one"),
            host.call_tool("lookup", {"name": "Lutra"}, call_id="one"),
        )
        assert results == [ToolResult(value="parent:Lutra"), ToolResult(value="parent:Lutra")]
        invalid = await host.call_tool("lookup", {"name": 42}, call_id="invalid")
        assert invalid.error is not None
        assert "ValidationError" in invalid.error
        return TurnResult("done")


@pytest.mark.asyncio
async def test_session_tools_keep_task_context_and_deduplicate_callbacks(tmp_path: Path) -> None:
    calls: list[str] = []

    async def lookup(name: str) -> str:
        await asyncio.sleep(0)
        calls.append(name)
        return current_context().action_id + ":" + name

    context = TaskContext(
        "run",
        "parent",
        1,
        RetryMode.NONE,
        cast("CheckpointManager", object()),
        cast("BlobStore", object()),
        workspace=tmp_path,
    )
    token = task_context.set(context)
    try:
        async with Agent(adapter=CallingAdapter(), tools=[lookup]).session() as session:
            assert await session.run("lookup") == "done"
            assert session.workspace.is_dir()
            workspace = session.workspace
        assert calls == ["Lutra"]
        assert not workspace.exists()
    finally:
        task_context.reset(token)


async def _task_lookup(name: str) -> str:
    await asyncio.sleep(0)
    return name


def test_agent_rejects_decorated_task_tools() -> None:
    lookup = TaskEnvironment("tools").task(_task_lookup)
    with pytest.raises(TypeError, match="async functions running in the session sandbox"):
        Agent(adapter=CallingAdapter(), tools=[cast("Tool", lookup)])
