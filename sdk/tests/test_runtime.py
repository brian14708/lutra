"""Child submission and independent result waits."""

import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest
from lutra._gen.lutra.v1.lutra_pb import ActionSpec, EnvironmentIdentifier, TaskAction
from lutra.runtime import RunContext, run_context, spawn
from lutra.value import dumps

from lutra import RetryMode

if TYPE_CHECKING:
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
async def test_reordered_spawns_keep_stable_keys_and_independent_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_create = asyncio.Event()
    release_second = asyncio.Event()
    created: list[str] = []

    class FakeRPC:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        @staticmethod
        async def create_task_action(request: object) -> object:
            key = request.idempotency_key  # type: ignore[missing-attribute]
            created.append(key)
            if key == "first":
                await release_create.wait()
            else:
                release_create.set()
            return SimpleNamespace(action=TaskAction(id=key))

        @staticmethod
        async def get_task_action(request: object) -> object:
            key = request.id  # type: ignore[missing-attribute]
            if key == "second":
                await release_second.wait()
            return SimpleNamespace(
                action=TaskAction(id=key, status="succeeded", output_cbor=dumps(key))
            )

    monkeypatch.setattr("lutra.runtime.LutraServiceClient", FakeRPC)
    api = cast("TaskAPIClient", FakeAPI())
    token = run_context.set(
        RunContext(api, {"env": EnvironmentIdentifier(name="env")}, "run", "parent")
    )
    try:
        invocation = cast("Invocation[str]", FakeInvocation())
        first, second = await asyncio.gather(
            spawn(invocation, key="first"), spawn(invocation, key="second")
        )
        assert created == ["first", "second"]
        assert (first.id, second.id) == ("first", "second")
        second_result = asyncio.create_task(second.result())
        assert await first.result() == "first"
        assert not second_result.done()
        release_second.set()
        assert await second_result == "second"
    finally:
        run_context.reset(token)
