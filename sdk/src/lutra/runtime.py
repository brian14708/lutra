"""Durable child calls and waits made by a workflow."""

from __future__ import annotations

import asyncio
import copy
import json
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar, cast
from typing_extensions import override

import pyqwest
from connectrpc.code import Code
from connectrpc.codec import proto_json_codec
from connectrpc.errors import ConnectError

from lutra._gen.lutra.v1.lutra_connect import LutraServiceClient
from lutra._gen.lutra.v1.lutra_pb import (
    CreateTaskActionRequest,
    EnvironmentIdentifier,
    WorkflowCancelRequest,
    WorkflowCompletion,
    WorkflowFuture,
    WorkflowTimerRequest,
    WorkflowWaitResponse,
)
from lutra._result import load_result
from lutra._workflow import duration_millis, validate_signal_name
from lutra.client import _require_action
from lutra.serve import StdioTransport, TaskAPIClient
from lutra.task import normalize_retry

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import timedelta

    from lutra.task import Invocation
    from lutra.workflow import ValueFuture

R = TypeVar("R")
_MAX_CHILD_METADATA_FIELDS = 32
_MAX_CHILD_METADATA_VALUE_BYTES = 1024


class _WorkflowTransport(StdioTransport):
    """Deliver journaled responses in server order despite concurrent HTTP callbacks."""

    def __init__(self, api_client: TaskAPIClient) -> None:
        super().__init__(api_client)
        self.next_delivery = 1
        self.waiting: dict[int, asyncio.Future[None]] = {}
        self.requests: set[asyncio.Task[pyqwest.Response]] = set()
        self.results: dict[bytes, object] = {}
        self.errors: dict[bytes, Exception] = {}
        self.api_client = api_client

    @override
    async def execute(self, request: pyqwest.Request) -> pyqwest.Response:
        task = asyncio.create_task(self._ordered(request))
        self.requests.add(task)
        task.add_done_callback(self._finished)
        return await asyncio.shield(task)

    def _finished(self, task: asyncio.Task[pyqwest.Response]) -> None:
        self.requests.discard(task)
        if not task.cancelled():
            task.exception()

    async def _ordered(self, request: pyqwest.Request) -> pyqwest.Response:
        response = await super().execute(request)
        contents = response.content
        if not isinstance(contents, bytes):
            contents = b"".join([chunk async for chunk in contents])
        value = json.loads(contents)
        meta = value.get("workflowMeta")
        if not isinstance(meta, dict):
            message = "workflow response has no command metadata"
            raise TypeError(message)
        ordinal = meta.get("delivery")
        if not isinstance(ordinal, str) or not ordinal.isascii() or not ordinal.isdecimal():
            message = "workflow response has an invalid delivery sequence"
            raise ValueError(message)
        delivery = int(ordinal)
        if delivery < self.next_delivery or delivery in self.waiting:
            message = "workflow response repeats a delivery sequence"
            raise ValueError(message)
        ready = asyncio.get_running_loop().create_future()
        self.waiting[delivery] = ready
        if delivery == self.next_delivery:
            ready.set_result(None)
        await ready
        try:
            if "completions" in value:
                await self._decode_completions(
                    WorkflowWaitResponse.from_json(contents.decode()).completions
                )
            if code := meta.get("errorCode"):
                raise ConnectError(Code(code), meta.get("errorMessage", "workflow command failed"))
            return response
        finally:
            self.waiting.pop(delivery)
            self.next_delivery += 1
            if next_ready := self.waiting.get(self.next_delivery):
                next_ready.set_result(None)

    async def _decode_completions(self, completions: list[WorkflowCompletion]) -> None:
        # Finish asynchronous child decoding before releasing the next durable reply.
        for completion in completions:
            if completion.future is None or completion.future.kind != WorkflowFuture.Kind.CHILD:
                continue
            if completion.failure != WorkflowCompletion.Failure.UNSPECIFIED:
                continue
            data = completion.value_cbor
            if data in self.results or data in self.errors:
                continue
            try:
                self.results[data] = await load_result(data, self.api_client.resolve_blob)
            except Exception as error:  # ruff: ignore[blind-except] Preserve each result's decoding error until its caller consumes it.
                self.errors[data] = error


@dataclass
class RunContext:
    """Context needed to submit durable calls from a workflow."""

    api_client: TaskAPIClient
    environments: dict[str, EnvironmentIdentifier]
    run_id: str
    action_id: str
    workflow: bool = False
    _sequence: int = field(init=False, default=0)
    _client: LutraServiceClient | None = field(init=False, default=None, repr=False)
    _transport: _WorkflowTransport | None = field(init=False, default=None, repr=False)

    def check_call(self) -> None:
        """Enforce the leaf task boundary before sending an operation.

        Raises:
            RuntimeError: If a task makes a workflow call.

        """
        if not self.workflow:
            message = "tasks are leaf-only; use @environment.workflow to coordinate children"
            raise RuntimeError(message)

    def next_sequence(self) -> int:
        """Order operations before a coroutine yields to the transport.

        Returns:
            The next one-based operation sequence number.

        """
        self.check_call()
        self._sequence += 1
        return self._sequence

    def client(self) -> LutraServiceClient:
        """Return the shared workflow-call client, building it on first use.

        Returns:
            The cached client over the stdio transport.

        """
        self.check_call()
        if self._client is None:
            self._transport = _WorkflowTransport(self.api_client)
            self._client = LutraServiceClient(
                "http://stdio",
                codec=proto_json_codec(),
                send_compression=None,
                accept_compression=(),
                http_client=pyqwest.Client(transport=self._transport),
            )
        return self._client

    async def decode_child(self, data: bytes) -> object:
        """Return an independent child result after ordered blob resolution.

        Returns:
            The decoded child value.

        """
        if self._transport is not None:
            if data in self._transport.errors:
                error = copy.deepcopy(self._transport.errors[data])
                raise error
            if data in self._transport.results:
                return copy.deepcopy(self._transport.results[data])
        return await load_result(data, self.api_client.resolve_blob)


run_context: ContextVar[RunContext] = ContextVar("lutra_run")


def _workflow_context() -> RunContext:
    context = run_context.get(None)
    if context is None or not context.workflow:
        message = "durable waits require an active Lutra workflow"
        raise RuntimeError(message)
    context.check_call()
    return context


@dataclass(frozen=True)
class ChildHandle(Generic[R]):
    """A committed child that can be awaited independently of other children."""

    id: str
    client: LutraServiceClient
    api_client: TaskAPIClient
    key: str
    context: RunContext

    def reference(self) -> WorkflowFuture:
        """Return this child's durable wait identity.

        Returns:
            The typed wire reference.

        """
        self.check_owner()
        return WorkflowFuture(kind=WorkflowFuture.Kind.CHILD, key=self.key, id=self.id)

    def check_owner(self) -> None:
        """Reject handles used by another workflow.

        Raises:
            RuntimeError: If the handle belongs to a different execution.

        """
        if _workflow_context() is not self.context:
            message = "future belongs to a different workflow"
            raise RuntimeError(message)

    async def decode(self, data: bytes) -> R:
        """Decode the child's success or terminal failure.

        Returns:
            The child result.

        """
        return cast("R", await self.context.decode_child(data))

    async def result(self) -> R:
        """Wait for and decode this child's result.

        Returns:
            The decoded result.

        """
        from lutra.workflow import select  # ruff: ignore[import-outside-top-level] Avoid the runtime/futures import cycle.

        return await (await select(self)).result()

    async def cancel(self) -> None:
        """Cancel this child and its pending descendants durably."""
        self.check_owner()
        await self.client.workflow_cancel(
            WorkflowCancelRequest(sequence=self.context.next_sequence(), id=self.id)
        )


async def spawn(
    invocation: Invocation[R],
    *,
    key: str,
    max_attempts: int | None = None,
    delay: timedelta | None = None,
    metadata: Mapping[str, str] | None = None,
) -> ChildHandle[R]:
    """Commit a child with a stable key scoped to the current workflow.

    The key names the Restate journal step. Metadata adds replay-stable labels
    visible in the journal; do not include secrets.

    Returns:
        A handle for independent waits.

    Raises:
        RuntimeError: If no workflow is active or the environment is not registered.
        ValueError: If child metadata exceeds its limits.

    """
    try:
        context = run_context.get()
    except LookupError as exc:
        message = "no active Lutra workflow"
        raise RuntimeError(message) from exc
    context.check_call()
    validate_signal_name(key)
    fields = dict(metadata or {})
    if len(fields) > _MAX_CHILD_METADATA_FIELDS:
        message = "child metadata accepts at most 32 fields"
        raise ValueError(message)
    for name, value in fields.items():
        validate_signal_name(name)
        if not isinstance(value, str) or len(value.encode()) > _MAX_CHILD_METADATA_VALUE_BYTES:
            message = "child metadata values must be strings up to 1024 bytes"
            raise ValueError(message)
    child = invocation.task
    _, attempts = normalize_retry(
        child.retry, child.max_attempts if max_attempts is None else max_attempts
    )
    environment = context.environments.get(child.environment.name)
    if environment is None:
        message = f"environment {child.environment.name!r} is not a registered dependency"
        raise RuntimeError(message)
    client = context.client()
    delay_millis = None if delay is None else duration_millis(delay)
    response = await client.create_task_action(
        CreateTaskActionRequest(
            environment=environment,
            entrypoint_id=child.entrypoint_id,
            action_spec=invocation.action_spec(attempts),
            idempotency_key=key,
            sequence=context.next_sequence(),
            delay_millis=delay_millis,
            metadata=fields,
        )
    )
    return ChildHandle(
        _require_action(response.action).id, client, context.api_client, key, context
    )


async def run(
    invocation: Invocation[R],
    *,
    key: str,
    max_attempts: int | None = None,
    metadata: Mapping[str, str] | None = None,
) -> R:
    """Submit and wait for one child task.

    Returns:
        The decoded child result.

    """
    return await (
        await spawn(invocation, key=key, max_attempts=max_attempts, metadata=metadata)
    ).result()


async def sleep(duration: timedelta) -> None:
    """Suspend a workflow without keeping its sandbox alive.

    The workflow replays from the beginning when the timer expires. Fractional
    milliseconds round up, so the workflow never wakes before the duration.
    """
    await (await _helper_timer(duration, "sleep")).result()


async def _helper_timer(duration: timedelta, label: str) -> ValueFuture[None]:
    from lutra.workflow import ValueFuture  # ruff: ignore[import-outside-top-level] Avoid the runtime/futures import cycle.

    context = _workflow_context()
    millis = duration_millis(duration)
    sequence = context.next_sequence()
    key = f"__lutra:{label}:{sequence}"
    await context.client().workflow_timer(
        WorkflowTimerRequest(sequence=sequence, key=key, duration_millis=millis)
    )
    return ValueFuture(
        key, context, WorkflowFuture(kind=WorkflowFuture.Kind.TIMER, key=key, id=key), type(None)
    )


async def receive(
    name: str,
    value_type: type[R],
    *,
    timeout: timedelta | None = None,  # ruff: ignore[async-function-with-timeout] Durable timer.
) -> R:
    """Wait for a named one-shot signal and validate its value strictly.

    Signals sent before this call remain available. Each name identifies one
    value for the workflow's lifetime. Use distinct names for repeated events.

    Returns:
        The signal decoded as the requested type, including Pydantic models.

    Raises:
        TimeoutError: If the durable timeout expires before a signal arrives.

    """
    from lutra.workflow import ValueFuture, select  # ruff: ignore[import-outside-top-level] Avoid the runtime/futures import cycle.

    context = _workflow_context()
    validate_signal_name(name)
    signal = ValueFuture(
        "signal",
        context,
        WorkflowFuture(kind=WorkflowFuture.Kind.PROMISE, key="signal", id=name),
        value_type,
    )
    if timeout is None:
        return await signal.result()
    timer = await _helper_timer(timeout, "receive")
    completed = await select(signal, timer)
    if completed.future is timer:
        await completed.result()
        message = f"workflow signal {name!r} timed out"
        raise TimeoutError(message)
    return cast("R", await completed.result())
