"""Run with LUTRA_WORKFLOW_E2E=1 against the local integration stack."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import os
import shlex
import shutil
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

import hello as greetings_example
import pytest
import training as training_example
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from lutra import CacheableError, Client, SubmissionError, TerminalError
from lutra._gen.lutra.v1.lutra_pb import (
    ListTaskActionsRequest,
    TaskActionStatus,
    TaskSignalWorkflowRequest,
)
from lutra._gen.lutra.v1.settings_pb import ListNamespacesRequest, UpsertSettingRequest
from lutra.value import dumps

from tests import workflow_tasks as tasks
from tests.workflow_stack import Stack

if TYPE_CHECKING:
    from collections.abc import Iterator

    from lutra import RunHandle

R = TypeVar("R")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.usefixtures("integration_stack"),
    pytest.mark.asyncio,
    pytest.mark.skipif(
        os.getenv("LUTRA_WORKFLOW_E2E") != "1", reason="requires durable integration stack"
    ),
]


@pytest.fixture(scope="module")
def integration_stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Stack | None]:
    if os.getenv("LUTRA_WORKFLOW_MANAGED") != "1":
        yield None
        return
    root = Path(__file__).resolve().parents[2]
    stack = Stack(
        tmp_path_factory.mktemp("workflow-stack"),
        root / ".data/workflow-proof/server",
        root / ".data/workflow-proof/stack",
    )
    stack.env["LUTRA_WORKER_CONCURRENCY"] = os.getenv("LUTRA_WORKFLOW_PROOF_CONCURRENCY", "1")
    try:
        stack.launch()
        with pytest.MonkeyPatch.context() as patch:
            patch.setenv("LUTRA_URL", stack.env["LUTRA_URL"])
            yield stack
    finally:
        stack.close()


def client() -> Client:
    return Client(os.getenv("LUTRA_URL", "http://127.0.0.1:8080/api"))


async def result(handle: RunHandle[R]) -> R:
    return await asyncio.wait_for(handle.result(), timeout=300)


async def sandbox_running(run_id: str) -> bool:
    process = await asyncio.create_subprocess_exec(
        os.getenv("LUTRA_CONTAINER_RUNTIME", "podman"),
        "ps",
        "--filter",
        f"label=lutra.run={run_id}",
        "--format",
        "{{.ID}}",
        stdout=asyncio.subprocess.PIPE,
    )
    output, _ = await process.communicate()
    assert process.returncode == 0
    return bool(output.strip())


async def test_approval_replay_and_fresh_children(
    integration_stack: Stack | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    sdk = client()
    handle = await sdk.submit(tasks.approval(b"\x00\xff proof"))
    async with asyncio.timeout(300):
        while True:
            if await handle.status() in {"failed", "canceled"}:
                await handle.result()
            actions = (
                await sdk.rpc.list_task_actions(ListTaskActionsRequest(run_id=handle.id))
            ).actions
            completed = [
                action
                for action in actions
                if action.caller_action_id and action.status == "succeeded"
            ]
            if len(completed) == 2:
                break
            await asyncio.sleep(0.2)
        while await sandbox_running(handle.id):  # ruff: ignore[async-busy-wait] Poll the external runtime.
            await asyncio.sleep(0.2)
    if integration_stack is not None:
        await asyncio.to_thread(integration_stack.restart)
    monkeypatch.setenv("PROOF_VERSION", "v2")
    importlib.reload(tasks)
    await sdk._prepare(tasks.environment)  # ruff: ignore[private-member-access] Publish changed environment while the original waits.
    await handle.signal("approval", value=True, idempotency_key="approval-event")
    await handle.signal("approval", value=True, idempotency_key="approval-event")
    value = await result(handle)
    assert isinstance(value, dict)
    before, after = value["before"], value["after"]
    assert value["approved"] is True
    assert all(entry["version"] == "v1" for entry in [*before, after])
    assert len({entry["sandbox"] for entry in [*before, after]}) == 3
    assert all(
        entry["digest"] == hashlib.sha256(b"\x00\xff proof").hexdigest()
        for entry in [*before, after]
    )
    assert await result(handle) == value


async def test_early_signal_and_timeout() -> None:
    sdk = client()
    handle = await sdk.submit(tasks.approval(b"early"))
    await handle.signal("approval", value=False)
    await handle.signal("approval", value=False)
    value = await result(handle)
    assert isinstance(value, dict)
    assert value["approved"] is False
    assert await result(await sdk.submit(tasks.timeout())) == "timed out"


async def test_short_waits_stay_in_one_workflow_container() -> None:
    handle = await client().submit(tasks.short_waits())
    assert await result(handle) == "finished"
    logs = [event async for event in handle.logs()]
    starts = [
        event
        for event in logs
        if isinstance(event.event, dict)
        and "start command launched" in str(event.event.get("message", ""))
    ]
    assert len(starts) == 1


async def test_high_fanout_with_capacity_one() -> None:
    handles = await asyncio.gather(*(client().submit(tasks.fanout(12)) for _ in range(2)))
    assert await asyncio.gather(*(result(handle) for handle in handles)) == [
        list(range(12)),
        list(range(12)),
    ]


async def test_concurrent_results_and_runs() -> None:
    sdk = client()
    for invocation in [tasks.concurrent_results(), tasks.concurrent_runs()]:
        assert await result(await sdk.submit(invocation)) == [0, 1, 2, 3]


async def test_task_invocations_use_fresh_sandboxes() -> None:
    sdk = client()
    first = await result(await sdk.submit(tasks.sandbox_identity()))
    second = await result(await sdk.submit(tasks.sandbox_identity()))
    assert first["sandbox"] != second["sandbox"]
    assert first["count"] == second["count"] == 1
    assert first["previous"] is second["previous"] is False


async def test_commands_can_resolve_a_pending_wait() -> None:
    assert await result(await client().submit(tasks.resolve_while_waiting())) == 7
    assert (
        await result(await client().submit(tasks.internal_timer_keys())) == "timers are independent"
    )


async def test_concurrent_coroutines_replay_in_the_same_order(
    integration_stack: Stack | None,
) -> None:
    sdk = client()
    handle = await sdk.submit(tasks.concurrent_replay())
    async with asyncio.timeout(300):
        while True:
            actions = (
                await sdk.rpc.list_task_actions(ListTaskActionsRequest(run_id=handle.id))
            ).actions
            if len(actions) == 3 and all(
                action.status == "succeeded" for action in actions if action.caller_action_id
            ):
                break
            if await handle.status() in {"failed", "canceled"}:
                await handle.result()
            await asyncio.sleep(0.2)
        while await sandbox_running(handle.id):  # ruff: ignore[async-busy-wait] Observe durable suspension.
            await asyncio.sleep(0.2)
    if integration_stack is not None:
        await asyncio.to_thread(integration_stack.restart)
    await asyncio.gather(
        handle.resolve_promise("continue-0", value=True),
        handle.resolve_promise("continue-1", value=True),
    )
    assert await result(handle) == [10, 11]


async def test_children_execute_concurrently(integration_stack: Stack | None) -> None:
    if integration_stack is None:
        pytest.skip("requires configurable managed capacity")
    original = integration_stack.env["LUTRA_WORKER_CONCURRENCY"]
    try:
        integration_stack.env["LUTRA_WORKER_CONCURRENCY"] = "2"
        await asyncio.to_thread(integration_stack.restart, graceful=True)
        spans = await result(await client().submit(tasks.overlapping_children()))
        assert len(spans) == 2
        assert max(start for start, _ in spans) < min(end for _, end in spans)
    finally:
        integration_stack.env["LUTRA_WORKER_CONCURRENCY"] = original
        await asyncio.to_thread(integration_stack.restart, graceful=True)


async def test_failed_child_does_not_block_gather() -> None:
    value = await result(await client().submit(tasks.gather_failure()))
    assert "child rejected" in value["failure"]
    assert value["value"] == value["again"] == 9


async def test_select_and_completion_iteration() -> None:
    value = await result(await client().submit(tasks.completions()))
    assert value == {
        "first": "ready",
        "first_value": 7,
        "values": {"ready": 7, "clock": None, "child": 8},
    }


async def test_timer_and_promise_races() -> None:
    sdk = client()
    assert await result(await sdk.submit(tasks.promise_race(timer_wins=True))) == {
        "key": "clock",
        "value": None,
    }
    early = await sdk.submit(tasks.promise_race(timer_wins=False))
    await early.signal("event", value=19)
    assert await result(early) == {"key": "event", "value": 19}
    waiting = await sdk.submit(tasks.promise_race(timer_wins=False))
    async with asyncio.timeout(300):
        while await waiting.status() != "waiting":
            if await waiting.status() in {"failed", "canceled"}:
                await waiting.result()
            await asyncio.sleep(0.2)
    await waiting.signal("event", value=23)
    assert await result(waiting) == {"key": "event", "value": 23}


async def test_state_and_promise_outcomes() -> None:
    value = await result(await client().submit(tasks.state_and_promises()))
    assert value == {
        "missing": None,
        "counter": 3,
        "nullable": None,
        "keys": ["counter", "nullable"],
        "cleared": None,
        "pending": True,
        "resolved": 11,
        "conflict": True,
        "rejection": "promise rejected",
    }


async def test_awakeable_outcomes() -> None:
    assert await result(await client().submit(tasks.awakeable_outcomes())) == {
        "accepted": 13,
        "rejection": "awakeable rejected",
    }


async def test_external_awakeable_resolution_and_rejection() -> None:
    sdk = client()
    for rejected in [False, True]:
        handle = await sdk.submit(tasks.external_awakeable())
        async with asyncio.timeout(300):
            while True:
                awakeable_id = await handle.state_get("awakeable-id", str)
                if awakeable_id is not None:
                    break
                if await handle.status() in {"failed", "canceled"}:
                    await handle.result()
                await asyncio.sleep(0.2)
        assert await handle.state_keys() == ["awakeable-id"]
        if rejected:
            await sdk.reject_awakeable(awakeable_id, "external rejection")
            await sdk.reject_awakeable(awakeable_id, "external rejection")
            with pytest.raises(RuntimeError, match="external rejection"):
                await result(handle)
            with pytest.raises(ConnectError) as conflict:
                await sdk.resolve_awakeable(awakeable_id, 31)
            assert conflict.value.code in {Code.ALREADY_EXISTS, Code.FAILED_PRECONDITION}
        else:
            await sdk.resolve_awakeable(awakeable_id, 31)
            await sdk.resolve_awakeable(awakeable_id, 31)
            assert await result(handle) == 31
            with pytest.raises(ConnectError) as conflict:
                await sdk.resolve_awakeable(awakeable_id, 32)
            assert conflict.value.code in {Code.ALREADY_EXISTS, Code.FAILED_PRECONDITION}


async def test_external_promise_rejection() -> None:
    handle = await client().submit(tasks.external_promise())
    async with asyncio.timeout(300):
        while await handle.state_get("ready", bool) is None:
            if await handle.status() in {"failed", "canceled"}:
                await handle.result()
            await asyncio.sleep(0.2)
    await handle.reject_promise("external", "external promise rejected")
    await handle.reject_promise("external", "external promise rejected")
    with pytest.raises(RuntimeError, match="external promise rejected"):
        await result(handle)
    with pytest.raises(ConnectError) as conflict:
        await handle.resolve_promise("external", 1)
    assert conflict.value.code is Code.ALREADY_EXISTS


async def test_concurrent_promise_resolution_is_idempotent() -> None:
    handle = await client().submit(tasks.external_promise())
    await asyncio.gather(*(handle.resolve_promise("external", 17) for _ in range(12)))
    assert await result(handle) == 17
    with pytest.raises(ConnectError) as conflict:
        await handle.resolve_promise("external", 18)
    assert conflict.value.code is Code.ALREADY_EXISTS


async def test_terminal_error_stops_task_retries() -> None:
    sdk = client()
    handle = await sdk.submit(tasks.terminal_failure())
    with pytest.raises(TerminalError, match="stop retries") as failure:
        await result(handle)
    assert failure.value.code == 409
    actions = (await sdk.rpc.list_task_actions(ListTaskActionsRequest(run_id=handle.id))).actions
    assert len(actions) == 1
    assert actions[0].attempts == 1


async def test_deterministic_values_and_state_survive_restart(
    integration_stack: Stack | None,
) -> None:
    sdk = client()
    handle = await sdk.submit(tasks.deterministic_replay())
    async with asyncio.timeout(300):
        while True:
            if await handle.status() in {"failed", "canceled"}:
                await handle.result()
            actions = (
                await sdk.rpc.list_task_actions(ListTaskActionsRequest(run_id=handle.id))
            ).actions
            if any(action.caller_action_id and action.status == "succeeded" for action in actions):
                break
            await asyncio.sleep(0.2)
        while await sandbox_running(handle.id):  # ruff: ignore[async-busy-wait] Wait for durable suspension.
            await asyncio.sleep(0.2)
    if integration_stack is not None:
        await asyncio.to_thread(integration_stack.restart)
    await handle.signal("resume", value=True)
    value = await result(handle)
    assert value["before"] == value["after"]
    assert isinstance(value["before"]["time"], datetime)
    assert value["before"]["time"].utcoffset() == timedelta(0)
    assert uuid.UUID(value["before"]["uuid"]).version == 4
    assert 0 <= value["before"]["random"][0] < 1
    assert 0 <= value["before"]["random"][1] < 1_000_000


async def test_child_cancellation() -> None:
    assert await result(await client().submit(tasks.cancel_child())) == "child canceled"


async def test_parent_cancellation_with_pending_children() -> None:
    sdk = client()
    handle = await sdk.submit(tasks.pending_children())
    async with asyncio.timeout(300):
        while True:
            actions = (
                await sdk.rpc.list_task_actions(ListTaskActionsRequest(run_id=handle.id))
            ).actions
            if len([action for action in actions if action.caller_action_id]) == 3:
                break
            if await handle.status() in {"failed", "canceled"}:
                await handle.result()
            await asyncio.sleep(0.2)
    await handle.cancel()
    async with asyncio.timeout(30):
        while await sandbox_running(handle.id):  # ruff: ignore[async-busy-wait] Poll canceled sandboxes.
            await asyncio.sleep(0.2)
    actions = (await sdk.rpc.list_task_actions(ListTaskActionsRequest(run_id=handle.id))).actions
    assert len(actions) == 4
    assert all(action.status == "canceled" for action in actions)
    assert await result(await sdk.submit(tasks.echo(21))) == 21


async def test_committed_submission_can_be_retried(integration_stack: Stack | None) -> None:
    if integration_stack is None:
        pytest.skip("requires managed Restate endpoint")
    stack = integration_stack
    original = stack.env["LUTRA_RESTATE_INGRESS"]
    key = uuid.uuid4().hex
    sdk = client()
    try:
        stack.env["LUTRA_RESTATE_INGRESS"] = original + "/unavailable"
        await asyncio.to_thread(stack.restart, graceful=True)
        with pytest.raises(SubmissionError) as failure:
            await sdk.submit(tasks.echo(7), idempotency_key=key)
        handle = failure.value.run
        assert failure.value.idempotency_key == key
        assert await handle.status() == "queued"
        assert not await sandbox_running(handle.id)
        stack.env["LUTRA_RESTATE_INGRESS"] = original
        await asyncio.to_thread(stack.restart, graceful=True)
        # The durable outbox recovers delivery without a client retry.
        assert await result(handle) == 7
        assert await handle.retry_submission() is handle
        assert await result(handle) == 7
        replay = await sdk.submit(tasks.echo(7), idempotency_key=key)
        assert replay.id == handle.id
        actions = (
            await sdk.rpc.list_task_actions(ListTaskActionsRequest(run_id=handle.id))
        ).actions
        assert len(actions) == 1
        assert actions[0].attempts == 1
    finally:
        if stack.env["LUTRA_RESTATE_INGRESS"] != original:
            stack.env["LUTRA_RESTATE_INGRESS"] = original
            await asyncio.to_thread(stack.restart, graceful=True)


async def test_failed_cancellation_can_be_retried(integration_stack: Stack | None) -> None:
    if integration_stack is None:
        pytest.skip("requires managed Restate endpoint")
    stack = integration_stack
    handle = await client().submit(tasks.long_workflow())
    original = stack.env["LUTRA_RESTATE_INGRESS"]
    try:
        stack.env["LUTRA_RESTATE_INGRESS"] = original + "/unavailable"
        await asyncio.to_thread(stack.restart, graceful=True)
        with pytest.raises(ConnectError) as failure:
            await handle.cancel()
        assert failure.value.code == Code.UNAVAILABLE
        assert await handle.status() == "canceled"
        stack.env["LUTRA_RESTATE_INGRESS"] = original
        await asyncio.to_thread(stack.restart, graceful=True)
        await handle.cancel()
        await handle.cancel()
        async with asyncio.timeout(30):
            while await sandbox_running(handle.id):  # ruff: ignore[async-busy-wait] Poll the external runtime.
                await asyncio.sleep(0.2)
        assert await handle.status() == "canceled"
    finally:
        if stack.env["LUTRA_RESTATE_INGRESS"] != original:
            stack.env["LUTRA_RESTATE_INGRESS"] = original
            await asyncio.to_thread(stack.restart, graceful=True)


async def test_cache_and_nested_retries() -> None:
    sdk = client()
    key = uuid.uuid4().hex
    handles = await asyncio.gather(*(sdk.submit(tasks.cached(key)) for _ in range(2)))
    values = await asyncio.gather(*(result(handle) for handle in handles))
    assert values[0] == values[1]
    assert isinstance(values[0], dict)
    assert values[0]["value"] == key
    failures = []
    for _ in range(2):
        with pytest.raises(CacheableError) as caught:
            await result(await sdk.submit(tasks.cached_failure(key)))
        failures.append(caught.value.details)
    assert failures[0] == failures[1]
    no_retry = await sdk.submit(tasks.retry(key), max_attempts=1)
    with pytest.raises(RuntimeError, match="intentional first attempt failure"):
        await result(no_retry)
    actions = (await sdk.rpc.list_task_actions(ListTaskActionsRequest(run_id=no_retry.id))).actions
    assert len(actions) == 1
    assert actions[0].attempts == 1
    assert await result(await sdk.submit(tasks.nested(key))) == [
        {"attempt": 2, "saved": key},
        {"attempt": 2, "saved": key},
    ]


async def test_config_redaction_and_blob_callbacks() -> None:
    sdk = client()
    namespaces = (await sdk.settings.list_namespaces(ListNamespacesRequest())).namespaces
    namespace = next(item.id for item in namespaces if item.slug == "default")
    secret = "secret-" + uuid.uuid4().hex
    await sdk.settings.upsert_setting(
        UpsertSettingRequest(
            namespace_id=namespace, path="proof/secret", value_cbor=dumps(secret), sensitive=True
        )
    )
    handle = await sdk.submit(tasks.configured())
    value = await result(handle)
    assert isinstance(value, dict)
    assert secret not in repr(value)
    assert value["digest"] == hashlib.sha256(b"binary\x00\xff" * 200_000).hexdigest()
    records = [event async for event in handle.logs()]
    assert secret not in repr(records)


async def test_cancel_releases_sandboxes() -> None:
    sdk = client()
    for invocation in [tasks.long_task(), tasks.busy_workflow(), tasks.long_workflow()]:
        handle = await sdk.submit(invocation)
        async with asyncio.timeout(300):
            while not await sandbox_running(handle.id):  # ruff: ignore[async-busy-wait] Poll the external service.
                await asyncio.sleep(0.2)
        await handle.cancel()
        await handle.cancel()
        assert await handle.status() == "canceled"
        async with asyncio.timeout(30):
            while await sandbox_running(handle.id):  # ruff: ignore[async-busy-wait] Poll the external runtime.
                await asyncio.sleep(0.2)


@pytest.mark.parametrize("graceful", [False, True])
async def test_active_task_restart(integration_stack: Stack | None, *, graceful: bool) -> None:
    if integration_stack is None:
        pytest.skip("requires managed process restart")
    handle = await client().submit(tasks.interrupted())
    async with asyncio.timeout(300):
        while not await sandbox_running(handle.id):  # ruff: ignore[async-busy-wait] Poll the external runtime.
            await asyncio.sleep(0.2)
    await asyncio.to_thread(integration_stack.restart, graceful=graceful)
    assert await result(handle) == 2
    assert not await sandbox_running(handle.id)


async def test_workflow_failure_is_terminal() -> None:
    handle = await client().submit(tasks.failing_workflow())
    with pytest.raises(RuntimeError, match="intentional workflow failure"):
        await result(handle)
    assert await handle.status() == "failed"


async def test_task_signal_callback_and_conflicting_payloads() -> None:
    sdk = client()
    for conflict in [False, True]:
        handle = await sdk.submit(tasks.task_signal_workflow(conflict=conflict))
        assert await result(handle) == {"event": {"value": 17}, "accepted": True}
    with pytest.raises(ConnectError) as failure:
        await sdk.rpc.task_signal_workflow(
            TaskSignalWorkflowRequest(name="from-task", value_cbor=dumps(17))
        )
    assert failure.value.code is Code.PERMISSION_DENIED
    handle = await sdk.submit(tasks.task_signal())
    with pytest.raises(RuntimeError, match="signals require a workflow run"):
        await result(handle)
    handle = await sdk.submit(tasks.task_cross_run_signal())
    assert await result(handle) == "permission_denied"


async def test_training_cli_entrypoint() -> None:
    uv = shutil.which("uv")
    assert uv is not None
    root = (await asyncio.to_thread(Path(__file__).resolve)).parents[2]
    process = await asyncio.create_subprocess_exec(
        uv,
        "run",
        "sdk/examples/training.py",
        "--epochs",
        "2",
        "--max-concurrent-evaluations",
        "2",
        "--epoch-seconds",
        "0.2",
        "--evaluation-seconds",
        "0.2",
        "--initialization-seconds",
        "0.1",
        "--mock-crash-epoch",
        "0",
        cwd=root,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=300)
    except BaseException:
        process.kill()
        await process.wait()
        raise
    assert process.returncode == 0, stderr.decode()
    assert "Training workflow:" in stdout.decode()
    assert "validation_loss" in stdout.decode()
    assert "Simulating accidental crash" in stderr.decode()
    assert "Restored checkpoint after epoch 0" in stderr.decode()
    assert "entrypoint #1" not in stderr.decode(), stderr.decode()


@pytest.mark.parametrize("restart", [False, True])
async def test_resident_training_example(integration_stack: Stack | None, *, restart: bool) -> None:
    if restart and integration_stack is None:
        pytest.skip("requires managed process restart")
    sdk = client()
    handle = await sdk.submit(
        training_example.training_workflow({
            "epochs": 4,
            "max_concurrent_evaluations": 2,
            "initialization_seconds": 0.1,
            "epoch_seconds": 1.0,
            "evaluation_seconds": 2.0,
        })
    )
    parallel = (
        integration_stack is not None
        and int(integration_stack.env["LUTRA_WORKER_CONCURRENCY"]) >= 4
    )
    restarted = False
    peak = 0
    overlapped = False
    async with asyncio.timeout(300):
        while True:
            actions = (
                await sdk.rpc.list_task_actions(ListTaskActionsRequest(run_id=handle.id))
            ).actions
            evaluations = [
                action
                for action in actions
                if action.environment is not None
                and action.environment.name == "checkpoint-evaluation"
            ]
            running = sum(action.status == "running" for action in evaluations)
            peak = max(peak, running)
            assert running <= 2
            overlapped |= running > 0 and any(
                action.environment is not None
                and action.environment.name == "resident-training"
                and action.status == "running"
                for action in actions
            )
            if restart and not restarted and (running if parallel else len(evaluations)):
                assert integration_stack is not None
                await asyncio.to_thread(integration_stack.restart, graceful=True)
                restarted = True
            if await handle.status() in {"succeeded", "failed", "canceled"}:
                break
            await asyncio.sleep(0.1)
    metrics = await result(handle)
    assert len(metrics) == 4
    assert all("validation_loss" in metric for metric in metrics)
    assert metrics[-1]["validation_loss"] < metrics[0]["validation_loss"]
    actions = (await sdk.rpc.list_task_actions(ListTaskActionsRequest(run_id=handle.id))).actions
    names = [action.environment.name for action in actions if action.environment is not None]
    assert names.count("resident-training") == 1
    assert names.count("checkpoint-evaluation") == 4
    if parallel:
        assert peak == 2
        assert overlapped
    logs = [event async for event in handle.logs()]
    initializations = [
        event
        for event in logs
        if isinstance(event.event, dict)
        and "Initializing model once" in str(event.event.get("message", ""))
    ]
    training_action = next(
        action
        for action in actions
        if action.environment is not None and action.environment.name == "resident-training"
    )
    assert len(initializations) == training_action.attempts


async def test_cancel_cache_owner_preserves_waiter() -> None:
    sdk = client()
    key = uuid.uuid4().hex
    owner = await sdk.submit(tasks.cached_pause(key))
    async with asyncio.timeout(300):
        while not await sandbox_running(owner.id):  # ruff: ignore[async-busy-wait] Poll the external runtime.
            await asyncio.sleep(0.2)
    waiter = await sdk.submit(tasks.cached_pause(key))
    await owner.cancel()
    assert await result(waiter) == key
    assert await owner.status() == "canceled"
    assert not await sandbox_running(owner.id)


async def test_hello_workflow_recovery_and_capabilities(integration_stack: Stack | None) -> None:
    sdk = client()
    handle = await sdk.submit(greetings_example.hello(["Lutra", "Python", "Lutra"]))
    async with asyncio.timeout(300):
        while True:
            actions = (
                await sdk.rpc.list_task_actions(ListTaskActionsRequest(run_id=handle.id))
            ).actions
            if await handle.status() in {"failed", "canceled"}:
                await handle.result()
            if len(actions) == 10 and all(
                action.status == "succeeded" for action in actions if action.caller_action_id
            ):
                break
            await asyncio.sleep(0.2)
        while await sandbox_running(handle.id):  # ruff: ignore[async-busy-wait] Observe suspension releasing every container.
            await asyncio.sleep(0.2)
    retries = [
        action
        for action in actions
        if action.entrypoint_id == greetings_example.retry_once.entrypoint_id
    ]
    assert len(retries) == 3
    assert [action.attempts for action in retries] == [2, 2, 2]
    if integration_stack is not None:
        await asyncio.to_thread(integration_stack.restart)
    await handle.signal("approval", value=True)
    assert await result(handle) == {
        "message": "Hello, Lutra! | Hello, Python! | Hello, Lutra!",
        "approved": True,
        "reminder_timed_out": True,
    }
    events = [event async for event in handle.events()]
    assert any(isinstance(event, TaskActionStatus) and event.cache_hit for event in events)
    rejected = await sdk.submit(greetings_example.hello([]))
    await rejected.signal("approval", value=False)
    await rejected.signal("reminder", value="already sent")
    assert await result(rejected) == {
        "message": "Greeting declined",
        "approved": False,
        "reminder_timed_out": False,
    }


async def test_preparation_retry_does_not_spend_task_attempts(
    integration_stack: Stack | None, tmp_path: Path
) -> None:
    if integration_stack is None:
        pytest.skip("requires managed runtime failure injection")
    stack = integration_stack
    runtime = os.environ.get("LUTRA_CONTAINER_RUNTIME", "podman")
    executable = shutil.which(runtime)
    assert executable is not None
    marker = tmp_path / "failed-build"
    wrapper = tmp_path / runtime
    wrapper.write_text(
        "#!/bin/sh\n"
        f"marker={shlex.quote(str(marker))}\n"
        'case "$*" in\n'
        '  "image inspect --format "*"lutra:runtime-"*)\n'
        '    test -e "$marker" || exit 1;;\n'
        '  "build "*"lutra:runtime-"*)\n'
        '    if ! test -e "$marker"; then touch "$marker"; exit 1; fi;;\n'
        "esac\n"
        f'exec {shlex.quote(executable)} "$@"\n'
    )
    wrapper.chmod(0o700)
    original_path = stack.env["PATH"]
    stack.env["PATH"] = f"{tmp_path}:{original_path}"
    try:
        await asyncio.to_thread(stack.restart, graceful=True)
        sdk = client()
        task = await sdk.submit(tasks.identify(b"preparation"))
        assert (await result(task))["digest"] == hashlib.sha256(b"preparation").hexdigest()
        assert marker.exists()
        actions = (await sdk.rpc.list_task_actions(ListTaskActionsRequest(run_id=task.id))).actions
        assert len(actions) == 1
        assert actions[0].attempts == 1
        marker.unlink()
        workflow = await sdk.submit(tasks.timeout())
        assert await result(workflow) == "timed out"
        assert marker.exists()
        assert (await result(await sdk.submit(tasks.identify(b"after"))))[
            "digest"
        ] == hashlib.sha256(b"after").hexdigest()
    finally:
        stack.env["PATH"] = original_path
        await asyncio.to_thread(stack.restart, graceful=True)


async def test_leaf_task_cannot_bypass_sdk_to_create_children() -> None:
    sdk = client()
    handle = await sdk.submit(tasks.forged_child())
    assert await result(handle) == "leaf child rejected"
    actions = (await sdk.rpc.list_task_actions(ListTaskActionsRequest(run_id=handle.id))).actions
    assert len(actions) == 1


async def test_datetime_signal() -> None:
    handle = await client().submit(tasks.timestamp())
    value = datetime(2026, 10, 6, 0, 0, 0, 120000, timezone(timedelta(hours=8)))
    await handle.signal("timestamp", value=value)
    assert await result(handle) == value


async def test_workflow_preparation_retries(
    integration_stack: Stack | None, tmp_path: Path
) -> None:
    if integration_stack is None:
        pytest.skip("requires managed container runtime")
    runtime = os.getenv("LUTRA_CONTAINER_RUNTIME", "podman")
    executable = shutil.which(runtime)
    assert executable is not None
    marker = tmp_path / "failed-build"
    wrapper = tmp_path / runtime
    wrapper.write_text(
        "#!/bin/sh\n"
        'case "$*" in\n'
        f"  'image inspect lutra:sha-'*) [ -d {shlex.quote(str(marker))} ] || exit 1 ;;\n"
        "esac\n"
        f'if [ "$1" = build ] && mkdir {shlex.quote(str(marker))} 2>/dev/null; then\n'
        "  echo 'temporary build failure' >&2\n"
        "  exit 1\n"
        "fi\n"
        f'exec {shlex.quote(executable)} "$@"\n'
    )
    wrapper.chmod(0o755)
    original_path = integration_stack.env["PATH"]
    integration_stack.env["PATH"] = f"{tmp_path}:{original_path}"
    try:
        await asyncio.to_thread(integration_stack.restart, graceful=True)
        handle = await client().submit(tasks.timeout())
        assert await result(handle) == "timed out"
        assert marker.is_dir()
    finally:
        integration_stack.env["PATH"] = original_path
        await asyncio.to_thread(integration_stack.restart, graceful=True)
