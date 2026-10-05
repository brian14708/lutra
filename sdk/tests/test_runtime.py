"""Workflow child submission and result decoding."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest
from lutra import RetryMode
from lutra._gen.lutra.v1.lutra_pb import (
    ActionSpec,
    EnvironmentIdentifier,
    TaskAction,
    WorkflowCompletion,
    WorkflowWaitResponse,
)
from lutra.runtime import RunContext, run_context, spawn
from lutra.value import dumps

if TYPE_CHECKING:
    from lutra._gen.lutra.v1.lutra_pb import CreateTaskActionRequest, WorkflowWaitRequest
    from lutra.serve import TaskAPIClient
    from lutra.task import Invocation


class FakeAPI:
    @staticmethod
    async def resolve_blob(_uri: str) -> bytes:
        message = "unexpected blob lookup"
        raise AssertionError(message)


class FakeTask:
    retry = RetryMode.NONE
    max_attempts = 1
    environment = SimpleNamespace(name="env")
    entrypoint_id = 1


class FakeInvocation:
    task = FakeTask()

    @staticmethod
    def action_spec(_attempts: int) -> ActionSpec:
        return ActionSpec(input_cbor=dumps([[], {}]))


@pytest.mark.asyncio
async def test_spawn_all_children_before_awaiting_results(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []
    submissions: list[CreateTaskActionRequest] = []

    class FakeRPC:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        @staticmethod
        async def create_task_action(request: CreateTaskActionRequest) -> object:
            submissions.append(request)
            calls.append(("spawn", request.idempotency_key))
            return SimpleNamespace(action=TaskAction(id=request.idempotency_key))

        @staticmethod
        async def workflow_wait(request: WorkflowWaitRequest) -> WorkflowWaitResponse:
            ref = request.futures[0]
            calls.append(("await", ref.id))
            return WorkflowWaitResponse(
                completions=[WorkflowCompletion(future=ref, value_cbor=dumps(ref.id))]
            )

    monkeypatch.setattr("lutra.runtime.LutraServiceClient", FakeRPC)
    api = cast("TaskAPIClient", FakeAPI())
    token = run_context.set(
        RunContext(api, {"env": EnvironmentIdentifier(name="env")}, "run", "parent", workflow=True)
    )
    try:
        invocation = cast("Invocation[str]", FakeInvocation())
        first = await spawn(invocation, key="first", metadata={"stage": "review"})
        second = await spawn(invocation, key="second")
        assert (first.id, second.id) == ("first", "second")
        assert submissions[0].metadata == {"stage": "review"}
        assert submissions[1].metadata == {}
        with pytest.raises(ValueError, match="metadata"):
            await spawn(invocation, key="invalid", metadata={"stage": "x" * 1025})
        assert await second.result() == "second"
        assert await first.result() == "first"
        assert calls == [
            ("spawn", "first"),
            ("spawn", "second"),
            ("await", "second"),
            ("await", "first"),
        ]
    finally:
        run_context.reset(token)


@pytest.mark.asyncio
async def test_leaf_task_cannot_submit_children() -> None:
    api = cast("TaskAPIClient", FakeAPI())
    token = run_context.set(
        RunContext(api, {"env": EnvironmentIdentifier(name="env")}, "run", "parent")
    )
    try:
        with pytest.raises(RuntimeError, match="tasks are leaf-only"):
            await spawn(cast("Invocation[str]", FakeInvocation()), key="child")
    finally:
        run_context.reset(token)
