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
from lutra._gen.lutra.v1.lutra_pb import CreateTaskActionRequest, GetTaskActionRequest, TaskSpec
from lutra.client import _require_action
from lutra.serve import StdioTransport, TaskAPIClient
from lutra.value import dumps, loads

if TYPE_CHECKING:
    from collections.abc import Iterator

    from lutra.task import Invocation

R = TypeVar("R")


@dataclass
class RunContext:
    api_client: TaskAPIClient
    task_spec: TaskSpec
    run_id: str
    action_id: str
    child_numbers: Iterator[int] = field(default_factory=lambda: count(1))


run_context: ContextVar[RunContext] = ContextVar("lutra_run")


async def run(invocation: Invocation[R]) -> R:
    context = run_context.get()
    api_client = context.api_client
    current = context.task_spec
    child = invocation.task
    source = current.source
    if source is None:
        message = "active task has no source bundle"
        raise RuntimeError(message)
    spec = child.spec(current.project, current.domain, source.uri)
    client = LutraServiceClient(
        "http://stdio",
        codec=proto_json_codec(),
        send_compression=None,
        accept_compression=(),
        http_client=pyqwest.Client(transport=StdioTransport(api_client)),
    )
    response = await client.create_task_action(
        CreateTaskActionRequest(
            spec=spec,
            input_cbor=dumps([list(invocation.args), invocation.kwargs]),
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
