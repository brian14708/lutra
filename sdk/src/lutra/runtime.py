"""Child tasks submitted by a running task."""

from __future__ import annotations

import asyncio
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar, cast

import pyqwest
from connectrpc.codec import proto_json_codec

from lutra._gen.lutra.v1.lutra_connect import LutraServiceClient
from lutra._gen.lutra.v1.lutra_pb import (
    CreateTaskActionRequest,
    EnvironmentIdentifier,
    GetTaskActionRequest,
)
from lutra._result import load_result
from lutra.client import _require_action
from lutra.serve import StdioTransport, TaskAPIClient
from lutra.task import normalize_retry

if TYPE_CHECKING:
    from lutra.task import Invocation

R = TypeVar("R")
_MAX_CHILD_KEY_BYTES = 200


@dataclass
class RunContext:
    """Context needed to submit child tasks from a running task."""

    api_client: TaskAPIClient
    environments: dict[str, EnvironmentIdentifier]
    run_id: str
    action_id: str


run_context: ContextVar[RunContext] = ContextVar("lutra_run")


@dataclass(frozen=True)
class ChildHandle(Generic[R]):
    """A committed child that can be awaited independently of other children."""

    id: str
    client: LutraServiceClient
    api_client: TaskAPIClient

    async def result(self) -> R:
        """Wait for and decode this child's result.

        Returns:
            The decoded result.

        Raises:
            RuntimeError: If the server omits the action or the child fails.

        """
        while True:
            state = (
                await self.client.get_task_action(GetTaskActionRequest(id=self.id, wait=True))
            ).action
            if state is None:
                message = "server returned no task action"
                raise RuntimeError(message)
            if state.status == "succeeded":
                return cast("R", await load_result(state.result_cbor, self.api_client.resolve_blob))
            if state.status in {"failed", "canceled"}:
                if state.result_cbor:
                    await load_result(state.result_cbor, self.api_client.resolve_blob)
                message = f"child {state.status}"
                raise RuntimeError(message)
            await asyncio.sleep(0)


async def spawn(
    invocation: Invocation[R], *, key: str, max_attempts: int | None = None
) -> ChildHandle[R]:
    """Commit a child with a stable key scoped to the current parent task.

    Returns:
        A handle for independent waits.

    Raises:
        RuntimeError: If no task is active or the environment is not registered.
        ValueError: If the key is empty or exceeds 200 UTF-8 bytes.

    """
    try:
        context = run_context.get()
    except LookupError as exc:
        message = "no active Lutra task"
        raise RuntimeError(message) from exc
    if not key or len(key.encode()) > _MAX_CHILD_KEY_BYTES:
        message = "key must be 1 to 200 UTF-8 bytes"
        raise ValueError(message)
    child = invocation.task
    _, attempts = normalize_retry(
        child.retry, child.max_attempts if max_attempts is None else max_attempts
    )
    environment = context.environments.get(child.environment.name)
    if environment is None:
        message = f"environment {child.environment.name!r} is not a registered dependency"
        raise RuntimeError(message)
    client = LutraServiceClient(
        "http://stdio",
        codec=proto_json_codec(),
        send_compression=None,
        accept_compression=(),
        http_client=pyqwest.Client(transport=StdioTransport(context.api_client)),
    )
    response = await client.create_task_action(
        CreateTaskActionRequest(
            environment=environment,
            entrypoint_id=child.entrypoint_id,
            action_spec=invocation.action_spec(attempts),
            idempotency_key=key,
        )
    )
    return ChildHandle(_require_action(response.action).id, client, context.api_client)


async def run(invocation: Invocation[R], *, key: str, max_attempts: int | None = None) -> R:
    """Submit and wait for one child task.

    Returns:
        The decoded child result.

    """
    return await (await spawn(invocation, key=key, max_attempts=max_attempts)).result()
