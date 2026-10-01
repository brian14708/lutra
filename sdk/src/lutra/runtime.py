"""Runtime context used by task bodies for nested submissions."""

from __future__ import annotations

import asyncio
from contextvars import ContextVar
from dataclasses import dataclass, field
from itertools import count
from typing import TYPE_CHECKING, TypeVar

import pyqwest
from connectrpc.codec import proto_json_codec

from lutra._gen.lutra.v1.lutra_connect import LutraServiceClient
from lutra._gen.lutra.v1.lutra_pb import (
    ActionSpec,
    CreateTaskActionRequest,
    EnvironmentIdentifier,
    GetTaskActionRequest,
)
from lutra.client import _require_action
from lutra.serve import StdioTransport, TaskAPIClient
from lutra.task import normalize_retry
from lutra.value import dumps, loads

if TYPE_CHECKING:
    from collections.abc import Iterator

    from lutra.task import Invocation

R = TypeVar("R")


@dataclass
class RunContext:
    """Context needed to submit child actions from a running task."""

    api_client: TaskAPIClient
    environments: dict[str, EnvironmentIdentifier]
    run_id: str
    action_id: str
    child_numbers: Iterator[int] = field(default_factory=lambda: count(1))


run_context: ContextVar[RunContext] = ContextVar("lutra_run")


async def run(invocation: Invocation[R], *, max_attempts: int | None = None) -> R:
    """Submit a child task invocation through the current task host.

    Returns:
        The decoded child task result.

    Raises:
        RuntimeError: If no active task exists or the child task fails.

    """
    try:
        context = run_context.get()
    except LookupError as exc:
        message = "no active Lutra task"
        raise RuntimeError(message) from exc
    api_client = context.api_client
    child = invocation.task
    _, attempts = normalize_retry(
        child.retry, child.max_attempts if max_attempts is None else max_attempts
    )
    environment = context.environments.get(child.environment.name)
    if environment is None:
        msg = f"environment {child.environment.name!r} is not a registered dependency"
        raise RuntimeError(msg)
    client = LutraServiceClient(
        "http://stdio",
        codec=proto_json_codec(),
        send_compression=None,
        accept_compression=(),
        http_client=pyqwest.Client(transport=StdioTransport(api_client)),
    )
    response = await client.create_task_action(
        CreateTaskActionRequest(
            environment=environment,
            entrypoint_id=child.entrypoint_id,
            action_spec=ActionSpec(
                input_cbor=dumps([list(invocation.args), invocation.kwargs]), max_attempts=attempts
            ),
            idempotency_key=f"{context.action_id}:{next(context.child_numbers)}",
        )
    )
    submitted = _require_action(response.action)
    while True:
        state = (
            await client.get_task_action(GetTaskActionRequest(id=submitted.id, wait=True))
        ).action
        if state is None:
            message = "server returned no task action"
            raise RuntimeError(message)
        if state.status == "succeeded":
            return await loads(state.output_cbor, api_client.resolve_blob)  # type: ignore[bad-return]
        if state.status in {"failed", "canceled"}:
            raise RuntimeError(state.error or f"child {state.status}")
        await asyncio.sleep(0)
