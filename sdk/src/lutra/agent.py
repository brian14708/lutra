"""Task-scoped live agent sessions, tools, and artifacts."""

from __future__ import annotations

import asyncio
import inspect
import json
import mimetypes
import stat
import tempfile
import uuid
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, Protocol, Self, TypeVar, overload

from lutra._context import current_context
from lutra._gen.lutra.v1.log_pb import AppendRequest, LogEntry
from lutra.archive import ArchiveError
from lutra.value import BlobRef, dumps

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from types import TracebackType

    from pydantic import BaseModel

    from lutra._context import TaskContext
    from lutra.tools import ToolSchema

T = TypeVar("T")
S = TypeVar("S")
Tool = Callable[..., Awaitable[object]]
_CLEANUP_SECONDS = 10


@dataclass(frozen=True)
class AgentSettings:
    """Common endpoint credentials; base_url is the API root without /v1."""

    model: str | None = None
    base_url: str | None = None
    api_key: str | None = None
    env: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class TurnResult(Generic[S]):
    """Completed output and optional adapter-owned live state."""

    output: object
    state: S | None = None
    usage: Mapping[str, object] | None = None


@dataclass(frozen=True)
class ToolResult:
    """A successful value or tool error, including successful None values."""

    value: object = None
    error: str | None = None


class AgentHost(Protocol):
    """Workspace, tools, and observations available to an adapter."""

    @property
    def workspace(self) -> Path:
        """Directory for native file operations."""
        ...

    @property
    def tools(self) -> Sequence[ToolSchema]:
        """Registered function schemas."""
        ...

    async def call_tool(
        self, name: str, arguments: Mapping[str, object], *, call_id: str | None = None
    ) -> ToolResult:
        """Validate and execute a tool; repeated IDs share one result."""
        ...

    async def emit(self, kind: str, data: object) -> None:
        """Record an observation in the agent log."""
        ...


class AgentAdapter(Protocol[S]):
    """Execute a turn and settle callbacks and provider resources."""

    async def run(
        self,
        host: AgentHost,
        prompt: str,
        *,
        output_schema: Mapping[str, object] | None = None,
        state: S | None = None,
    ) -> TurnResult[S]:
        """Run a turn with optional adapter-owned state."""
        ...


@dataclass(frozen=True)
class _Tool:
    target: Tool
    model: type[BaseModel]
    schema: ToolSchema


class Agent(Generic[S]):
    """Reusable configuration for independent task-scoped sessions."""

    def __init__(self, *, adapter: AgentAdapter[S], tools: Sequence[Tool] = ()) -> None:
        """Register annotated async functions that run in the session sandbox.

        Raises:
            TypeError: If a tool is synchronous or has an unsupported signature.
            ValueError: If tool names are duplicated.

        """
        from lutra._tool_schema import tool_model  # ruff: ignore[import-outside-top-level]

        self._adapter = adapter
        self._tools: dict[str, _Tool] = {}
        for target in tools:
            function = target
            if not inspect.iscoroutinefunction(function):
                message = "agent tools must be async functions running in the session sandbox"
                raise TypeError(message)
            name = function.__name__
            if name in self._tools:
                message = f"duplicate agent tool: {name}"
                raise ValueError(message)
            model = tool_model(function)
            schema: ToolSchema = {
                "name": name,
                "description": inspect.getdoc(function) or "",
                "input_schema": model.model_json_schema(),
            }
            self._tools[name] = _Tool(target, model, schema)

    def session(self) -> AgentSession[S]:
        """Create an independent session.

        Returns:
            An unopened session to use as an async context manager.

        """
        return AgentSession(self)


class AgentSession(Generic[S]):
    """One workspace and live continuation within a task attempt."""

    def __init__(self, agent: Agent[S]) -> None:
        """Create an unopened session through Agent.session()."""
        self._agent = agent
        self._id = uuid.uuid4().hex
        self._directory: tempfile.TemporaryDirectory[str] | None = None
        self._context: TaskContext | None = None
        self._turn = 0
        self._state: S | None = None
        self._operation: asyncio.Task[Any] | None = None
        self._accepting = False
        self._calls: dict[str, tuple[bytes, asyncio.Task[ToolResult]]] = {}

    @property
    def workspace(self) -> Path:
        """The workspace, available while this session is open.

        Raises:
            RuntimeError: If the session is not open.

        """
        if self._directory is None:
            message = "agent session is not open"
            raise RuntimeError(message)
        return Path(self._directory.name)

    @property
    def tools(self) -> Sequence[ToolSchema]:
        """Registered tool schemas exposed to the adapter."""
        return tuple(tool.schema for tool in self._agent._tools.values())

    async def __aenter__(self) -> Self:
        """Create an isolated workspace.

        Returns:
            This session with an empty workspace.

        Raises:
            RuntimeError: If this session was opened already.

        """
        if self._context is not None:
            message = "agent session already opened"
            raise RuntimeError(message)
        self._context = current_context()
        self._directory = tempfile.TemporaryDirectory(
            prefix="lutra-agent-", dir=self._context.workspace
        )
        return self

    def _begin(self) -> None:
        if self._directory is None:
            message = "agent session is not open"
            raise RuntimeError(message)
        if self._operation is not None:
            message = "agent session already has an active turn or artifact collection"
            raise RuntimeError(message)
        self._operation = asyncio.current_task()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Cancel active work and remove the workspace."""
        self._accepting = False
        try:
            async with asyncio.timeout(_CLEANUP_SECONDS):
                if self._operation is not None and self._operation is not asyncio.current_task():
                    self._operation.cancel()
                    await asyncio.gather(self._operation, return_exceptions=True)
        finally:
            self._state = None
            if self._directory is not None:
                self._directory.cleanup()
                self._directory = None

    async def emit(self, kind: str, data: object) -> None:
        """Append a structured observation to the agent log.

        Raises:
            RuntimeError: If the session is not open.

        """
        context = self._context
        if context is None or self._directory is None:
            message = "agent session is not open"
            raise RuntimeError(message)
        if context.log is not None:
            record = dumps({
                "kind": kind,
                "data": data,
                "run_id": context.run_id,
                "action_id": context.action_id,
                "attempt": context.attempt,
                "session": self._id,
                "turn": self._turn,
            })
            await context.log.append(
                AppendRequest(
                    run_id=context.run_id,
                    stream="agent",
                    entries=[LogEntry(key=context.action_id.encode(), value_cbor=record)],
                )
            )

    async def call_tool(
        self, name: str, arguments: Mapping[str, object], *, call_id: str | None = None
    ) -> ToolResult:
        """Deduplicate callbacks by identity within the current turn.

        Returns:
            The tool value or error, shared by duplicate deliveries.

        Raises:
            RuntimeError: If no live turn is accepting calls.
            ValueError: If a repeated call ID has different arguments.

        """
        if not self._accepting:
            message = "agent session is not accepting tool calls"
            raise RuntimeError(message)
        arguments = deepcopy(dict(arguments))
        occurrence = call_id if call_id is not None else uuid.uuid4().hex
        signature = dumps([name, dict(arguments)])
        previous = self._calls.get(occurrence)
        if previous is not None:
            if previous[0] != signature:
                message = f"conflicting tool call identity: {occurrence}"
                raise ValueError(message)
            return await asyncio.shield(previous[1])
        task = asyncio.create_task(self._execute_tool(name, arguments, occurrence))
        self._calls[occurrence] = signature, task
        return await asyncio.shield(task)

    async def _execute_tool(
        self, name: str, arguments: Mapping[str, object], occurrence: str
    ) -> ToolResult:
        from lutra._agent_schema import json_value  # ruff: ignore[import-outside-top-level]

        await self.emit(
            "tool.started", {"id": occurrence, "name": name, "arguments": dict(arguments)}
        )
        try:  # ruff: ignore[too-many-statements-in-try-clause]
            tool = self._agent._tools[name]
            validated = tool.model.model_validate(dict(arguments)).model_dump(by_alias=True)
            value = await tool.target(**validated)
            if not isinstance(value, BlobRef):
                json_value(value)
            result = ToolResult(value=value)
        except Exception as error:  # ruff: ignore[blind-except]
            result = ToolResult(error=f"{type(error).__name__}: {error}")
        await self.emit(
            "tool.completed",
            {"id": occurrence, "name": name, "value": result.value, "error": result.error},
        )
        return result

    @overload
    async def run(self, prompt: str) -> str: ...

    @overload
    async def run(self, prompt: str, *, output_type: type[T]) -> T: ...

    async def run(self, prompt: str, *, output_type: type[Any] | None = None) -> Any:
        """Execute a turn and retain its adapter-owned state.

        Returns:
            Text, or the validated value of the requested output type.

        Raises:
            TypeError: If the prompt or output has an unsupported type.
            RuntimeError: If the adapter returns with active callbacks.

        """
        from pydantic import TypeAdapter  # ruff: ignore[import-outside-top-level]

        from lutra._agent_schema import json_value  # ruff: ignore[import-outside-top-level]

        if not isinstance(prompt, str):
            message = "agent prompt must be text"
            raise TypeError(message)
        validator = None if output_type is None else TypeAdapter(output_type)
        schema = None if validator is None else validator.json_schema()
        self._begin()
        self._turn += 1
        self._calls.clear()
        self._accepting = True
        try:
            try:
                result = await self._agent._adapter.run(
                    self, prompt, output_schema=schema, state=self._state
                )
            finally:
                self._accepting = False
                pending = [task for _, task in self._calls.values() if not task.done()]
                for task in pending:
                    task.cancel()
                async with asyncio.timeout(_CLEANUP_SECONDS):
                    await asyncio.gather(
                        *(task for _, task in self._calls.values()), return_exceptions=True
                    )
                self._calls.clear()
            if pending:
                message = "adapter returned while tool callbacks were still active"
                raise RuntimeError(message)
            if validator is not None:
                output = validator.validate_json(
                    json.dumps(json_value(result.output), allow_nan=False)
                )
            elif isinstance(result.output, str):
                output = result.output
            else:
                message = "agent turn did not return text"
                raise TypeError(message)
            self._state = result.state
            await self.emit("turn.completed", {"usage": result.usage})
            return output
        finally:
            self._operation = None

    async def artifacts(self, *patterns: str) -> dict[str, BlobRef]:
        """Publish matching regular files under sorted workspace-relative keys.

        Returns:
            Blob references keyed by relative file paths; empty if no files match.

        Raises:
            ValueError: If a pattern escapes the workspace.
            ArchiveError: If a matched path is a symlink or special file.

        """
        self._begin()
        assert self._context is not None
        try:
            selected: set[Path] = set()
            for pattern in patterns:
                if not pattern or Path(pattern).is_absolute() or ".." in Path(pattern).parts:
                    message = f"artifact pattern must stay inside the workspace: {pattern!r}"
                    raise ValueError(message)
                for path in self.workspace.glob(pattern):
                    relative = path.relative_to(self.workspace)
                    for component in (relative, *relative.parents):
                        mode = (self.workspace / component).lstat().st_mode
                        if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                            message = f"unsafe artifact path: {path}"
                            raise ArchiveError(message)
                    if path.is_file():
                        selected.add(path)
            entries = [
                (path.relative_to(self.workspace).as_posix(), path) for path in sorted(selected)
            ]
            refs = await asyncio.gather(
                *(
                    self._context.blobs.upload_file(
                        path, mimetypes.guess_type(relative)[0] or "application/octet-stream"
                    )
                    for relative, path in entries
                )
            )
            artifacts = dict(zip((relative for relative, _ in entries), refs, strict=True))
            await self.emit("artifacts.published", artifacts)
            return artifacts
        finally:
            self._operation = None
