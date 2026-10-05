"""Observable workflow operation behavior at the Python transport boundary."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from lutra import InvocationCanceledError, PromiseRejectedError
from lutra._gen.lutra.v1.lutra_pb import (
    WorkflowAwakeableResponse,
    WorkflowCompletion,
    WorkflowEntropyResponse,
    WorkflowPromiseResponse,
    WorkflowStateResponse,
    WorkflowTimerResponse,
    WorkflowWaitResponse,
)
from lutra.runtime import RunContext, run_context
from lutra.value import dumps
from lutra.workflow import (
    as_completed,
    awakeable,
    cancel_invocation,
    gather,
    promise,
    reject_awakeable,
    resolve_awakeable,
    select,
    timer,
    wait_completed,
    workflow_random,
    workflow_state,
    workflow_time,
    workflow_uuid,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from lutra._gen.lutra.v1.lutra_connect import LutraServiceClient
    from lutra.serve import TaskAPIClient

pytestmark = pytest.mark.asyncio


@pytest.fixture
def rpc() -> Iterator[AsyncMock]:
    client = AsyncMock()
    context = RunContext(cast("TaskAPIClient", None), {}, "run", "action", workflow=True)
    context._client = cast("LutraServiceClient", client)  # ruff: ignore[private-member-access] Substitute the network boundary.
    token = run_context.set(context)
    try:
        yield client
    finally:
        run_context.reset(token)


async def test_gather_restores_input_order_and_preserves_failure(rpc: AsyncMock) -> None:
    first, second = promise("first", int), promise("second", int)
    rpc.workflow_wait.return_value = WorkflowWaitResponse(
        completions=[
            WorkflowCompletion(future=second.reference(), value_cbor=dumps(2)),
            WorkflowCompletion(future=first.reference(), value_cbor=dumps(1)),
        ]
    )
    assert await gather(first, second) == [1, 2]
    rpc.workflow_wait.return_value.completions[0].failure = WorkflowCompletion.Failure.REJECTED
    rpc.workflow_wait.return_value.completions[0].message = "denied"
    values = await gather(first, second, return_exceptions=True)
    assert values[0] == 1
    assert isinstance(values[1], PromiseRejectedError)
    assert str(values[1]) == "denied"
    with pytest.raises(PromiseRejectedError, match="denied"):
        await gather(first, second)


async def test_completion_iteration_includes_already_completed_once(rpc: AsyncMock) -> None:
    first, second = promise("first", int), promise("second", int)
    rpc.workflow_wait.side_effect = [
        WorkflowWaitResponse(
            completions=[WorkflowCompletion(future=second.reference(), value_cbor=dumps(2))]
        ),
        WorkflowWaitResponse(
            completions=[WorkflowCompletion(future=first.reference(), value_cbor=dumps(1))]
        ),
    ]
    values = [(item.key, await item.result()) async for item in as_completed(first, second)]
    assert values == [("second", 2), ("first", 1)]
    waits = [call.args[0] for call in rpc.workflow_wait.await_args_list]
    assert [[ref.key for ref in request.futures] for request in waits] == [
        ["first", "second"],
        ["first"],
    ]


@pytest.mark.parametrize(
    ("failure", "error"),
    [
        (WorkflowCompletion.Failure.REJECTED, PromiseRejectedError),
        (WorkflowCompletion.Failure.CANCELED, InvocationCanceledError),
        (WorkflowCompletion.Failure.TIMEOUT, TimeoutError),
        (WorkflowCompletion.Failure.CHILD, RuntimeError),
    ],
)
async def test_completion_failure_mapping(
    rpc: AsyncMock, failure: WorkflowCompletion.Failure, error: type[Exception]
) -> None:
    value = promise("value", int)
    rpc.workflow_wait.return_value = WorkflowWaitResponse(
        completions=[
            WorkflowCompletion(future=value.reference(), failure=failure, message="failure")
        ]
    )
    completion = await wait_completed(value)
    assert completion.key == "value"
    with pytest.raises(error, match="failure"):
        await completion.result()


async def test_wait_set_and_response_validation(rpc: AsyncMock) -> None:
    value = promise("value", int)
    assert await gather() == []
    with pytest.raises(ValueError, match="at least one"):
        await select()
    with pytest.raises(ValueError, match="distinct"):
        await gather(value, value)
    rpc.workflow_wait.assert_not_awaited()
    rpc.workflow_wait.return_value = WorkflowWaitResponse()
    with pytest.raises(ValueError, match="incomplete"):
        await select(value)
    other = promise("other", int)
    rpc.workflow_wait.return_value = WorkflowWaitResponse(
        completions=[WorkflowCompletion(future=other.reference(), value_cbor=dumps(1))]
    )
    with pytest.raises(ValueError, match="unexpected"):
        await select(value)


async def test_promise_peek_null_rejection_and_server_conflicts(rpc: AsyncMock) -> None:
    value = promise("value", type(None))
    rpc.workflow_promise.return_value = WorkflowPromiseResponse()
    assert await value.peek() is None
    rpc.workflow_promise.return_value = WorkflowPromiseResponse(
        completed=True, value_cbor=dumps(None)
    )
    completion = await value.peek()
    assert completion is not None
    assert await completion.result() is None
    rpc.workflow_promise.return_value = WorkflowPromiseResponse(
        completed=True, rejected=True, reason="no"
    )
    completion = await value.peek()
    assert completion is not None
    with pytest.raises(PromiseRejectedError, match="no"):
        await completion.result()
    rpc.workflow_promise.side_effect = ConnectError(Code.ALREADY_EXISTS, "conflict")
    with pytest.raises(ConnectError, match="conflict"):
        await value.resolve(None)
    with pytest.raises(ConnectError, match="conflict"):
        await value.reject("no")


async def test_state_strict_type_canonical_values_and_missing(rpc: AsyncMock) -> None:
    state = workflow_state()
    rpc.workflow_state.return_value = WorkflowStateResponse()
    assert await state.get("missing", int) is None
    await state.set("value", {"b": 2, "a": 1})
    assert rpc.workflow_state.await_args.args[0].value_cbor == dumps({"a": 1, "b": 2})
    rpc.workflow_state.return_value = WorkflowStateResponse(found=True, value_cbor=dumps(3))
    assert await state.get("value", int) == 3
    rpc.workflow_state.return_value = WorkflowStateResponse(found=True, value_cbor=dumps("3"))
    with pytest.raises(ValueError, match="valid integer"):
        await state.get("value", int)
    rpc.workflow_state.return_value = WorkflowStateResponse(found=True, value_cbor=b"\x18\x03")
    with pytest.raises(ValueError, match="canonical"):
        await state.get("value", int)
    await state.clear("value")
    rpc.workflow_state.return_value = WorkflowStateResponse(keys=["a", "b"])
    assert await state.keys() == ["a", "b"]


async def test_timer_awakeable_and_concurrent_sequences(rpc: AsyncMock) -> None:
    rpc.workflow_timer.return_value = WorkflowTimerResponse()
    clocks = await asyncio.gather(
        timer(timedelta(microseconds=1), key="first"), timer(timedelta(0), key="second")
    )
    requests = [call.args[0] for call in rpc.workflow_timer.await_args_list]
    assert [(req.sequence, req.key, req.duration_millis) for req in requests] == [
        (1, "first", 1),
        (2, "second", 0),
    ]
    rpc.workflow_wait.return_value = WorkflowWaitResponse(
        completions=[WorkflowCompletion(future=clocks[0].reference(), value_cbor=dumps(None))]
    )
    assert await clocks[0].result() is None
    rpc.workflow_awakeable.return_value = WorkflowAwakeableResponse(id="awakeable-id")
    value = await awakeable(int, key="wake")
    assert value.id == "awakeable-id"
    await resolve_awakeable(value.id, 7)
    assert rpc.workflow_awakeable.await_args.args[0].value_cbor == dumps(7)
    await reject_awakeable(value.id, "rejected")
    assert rpc.workflow_awakeable.await_args.args[0].reason == "rejected"
    with pytest.raises(ValueError, match="only child"):
        await cancel_invocation(value)


@pytest.mark.parametrize("name", ["", "a" * 201, "\u00e9" * 101])
async def test_names_reasons_and_keys_rejected_before_transport(rpc: AsyncMock, name: str) -> None:
    with pytest.raises(ValueError, match="UTF-8"):
        promise(name, int)
    for operation in [
        timer(timedelta(0), key=name),
        awakeable(int, key=name),
        workflow_state().get(name, int),
        workflow_state().set(name, 1),
        workflow_state().clear(name),
        resolve_awakeable(name, 1),
    ]:
        with pytest.raises(ValueError, match="UTF-8"):
            await operation
    assert rpc.mock_calls == []


@pytest.mark.parametrize("reason", ["", "a" * 4097, "\u00e9" * 2049])
async def test_rejection_reasons_are_bounded(rpc: AsyncMock, reason: str) -> None:
    with pytest.raises(ValueError, match="UTF-8"):
        await promise("valid", int).reject(reason)
    with pytest.raises(ValueError, match="UTF-8"):
        await reject_awakeable("sign_valid", reason)
    rpc.workflow_promise.assert_not_awaited()
    rpc.workflow_awakeable.assert_not_awaited()
    assert rpc.mock_calls == []


async def test_duration_and_payload_limits(rpc: AsyncMock) -> None:
    for duration in [timedelta(microseconds=-1), timedelta.max]:
        with pytest.raises(ValueError, match="duration"):
            await timer(duration, key="clock")
    oversized = b"x" * (1 << 20)
    for operation in [
        workflow_state().set("value", oversized),
        promise("value", bytes).resolve(oversized),
        resolve_awakeable("id", oversized),
    ]:
        with pytest.raises(ValueError, match="1 MiB"):
            await operation
    assert rpc.mock_calls == []


async def test_cross_workflow_and_leaf_ownership(rpc: AsyncMock) -> None:
    value, state = promise("value", int), workflow_state()
    token = run_context.set(
        RunContext(cast("TaskAPIClient", None), {}, "other", "other", workflow=True)
    )
    try:
        with pytest.raises(RuntimeError, match="different workflow"):
            await value.resolve(1)
        with pytest.raises(RuntimeError, match="different workflow"):
            await state.get("value", int)
    finally:
        run_context.reset(token)
    token = run_context.set(RunContext(cast("TaskAPIClient", None), {}, "run", "leaf"))
    try:
        with pytest.raises(RuntimeError, match="active Lutra workflow"):
            workflow_state()
        with pytest.raises(RuntimeError, match="active Lutra workflow"):
            await gather()
        with pytest.raises(RuntimeError, match="active Lutra workflow"):
            await workflow_time()
    finally:
        run_context.reset(token)
    assert rpc.mock_calls == []


async def test_journaled_entropy_and_seed_validation(rpc: AsyncMock) -> None:
    rpc.workflow_entropy.return_value = WorkflowEntropyResponse(time_millis=1_000)
    assert await workflow_time() == datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)
    rpc.workflow_entropy.return_value = WorkflowEntropyResponse(seed=bytes(range(32)))
    first, second = await workflow_random(), await workflow_random()
    assert [first.random() for _ in range(4)] == [second.random() for _ in range(4)]
    generated = await workflow_uuid()
    assert str(generated) == "00010203-0405-4607-8809-0a0b0c0d0e0f"
    assert generated.version == 4
    for seed in [b"", b"x" * 31, b"x" * 33]:
        rpc.workflow_entropy.return_value = WorkflowEntropyResponse(seed=seed)
        with pytest.raises(ValueError, match="32 bytes"):
            await workflow_random()
