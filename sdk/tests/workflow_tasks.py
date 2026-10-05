# /// script
# requires-python = ">=3.11"
# dependencies = ["lutra"]
#
# [tool.uv.sources]
# lutra = { path = "..", editable = true }
# ///
"""Sandbox programs exercised by the durable engine integration tests."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing_extensions import TypedDict

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from lutra import (
    CacheableError,
    ConfigBinding,
    TaskEnvironment,
    TerminalError,
    as_completed,
    awakeable,
    current_context,
    gather,
    promise,
    receive,
    reject_awakeable,
    resolve_awakeable,
    run,
    select,
    sleep,
    spawn,
    timer,
    workflow_random,
    workflow_state,
    workflow_time,
    workflow_uuid,
)
from lutra._gen.lutra.v1.lutra_pb import SignalRunRequest
from lutra.runtime import run_context
from lutra.value import dumps

children = TaskEnvironment(
    "proof-children", env={"PROOF_VERSION": os.getenv("PROOF_VERSION", "v1")}
)
environment = TaskEnvironment("proof-workflows", dependencies=(children,))
configured_environment = TaskEnvironment("proof-config")

_invocation_count = 0


@children.task
async def task_signal(*, conflict: bool = False) -> bool:
    context = current_context()
    await context.signal_workflow("from-task", {"value": 17})
    await context.signal_workflow("from-task", {"value": 17})
    if conflict:
        try:
            await context.signal_workflow("from-task", {"value": 18})
        except ConnectError:
            return True
        return False
    return True


@environment.workflow
async def task_signal_workflow(*, conflict: bool = False) -> dict[str, object]:
    child = await spawn(task_signal(conflict=conflict), key="signaler")
    event = await promise("from-task", dict[str, int]).result()
    return {"event": event, "accepted": await child.result()}


@children.task
async def task_cross_run_signal() -> str:
    client = current_context()._workflow_client  # ruff: ignore[private-member-access] Exercise raw callback authorization.
    assert client is not None
    try:
        await client.signal_run(
            SignalRunRequest(id=str(uuid.uuid4()), name="forged", value_cbor=dumps(17))
        )
    except ConnectError as exc:
        return exc.code.value
    return "accepted"


@children.task
async def sandbox_identity() -> dict[str, object]:
    global _invocation_count  # ruff: ignore[global-statement] Verify process isolation.
    _invocation_count += 1
    marker = Path("invocation-marker")
    previous = await asyncio.to_thread(marker.exists)
    await asyncio.to_thread(marker.write_text, "visited", encoding="utf-8")
    return {
        "sandbox": os.environ["LUTRA_SANDBOX_ID"],
        "count": _invocation_count,
        "previous": previous,
    }


class Identity(TypedDict):
    digest: str
    sandbox: str
    version: str
    decision: str


class ApprovalResult(TypedDict):
    before: list[Identity]
    after: Identity
    approved: bool


class GatherFailure(TypedDict):
    failure: str
    value: int
    again: int


class Entropy(TypedDict):
    time: datetime
    uuid: str
    random: list[float]


class ReplayResult(TypedDict):
    before: Entropy
    after: Entropy | None


@children.task
def identify(value: bytes) -> Identity:
    marker = Path("/tmp/lutra-proof")  # ruff: ignore[hardcoded-temp-file] Deliberately detect sandbox reuse.
    with marker.open("xb") as output:
        output.write(value)
    return {
        "digest": hashlib.sha256(value).hexdigest(),
        "sandbox": os.environ["LUTRA_SANDBOX_ID"],
        "version": os.environ["PROOF_VERSION"],
        "decision": uuid.uuid4().hex,
    }


@environment.workflow
async def approval(value: bytes) -> ApprovalResult:
    calls = [await spawn(identify(value), key=f"child-{i}") for i in range(2)]
    before = [await call.result() for call in calls]
    accepted = await receive("approval", bool)
    await sleep(timedelta(milliseconds=20))
    after = await run(identify(value), key="after")
    return {"before": before, "after": after, "approved": accepted}


@environment.workflow
async def timeout() -> str:
    try:
        await receive("missing", bool, timeout=timedelta(milliseconds=50))
    except TimeoutError:
        return "timed out"
    return "unexpected event"


@environment.workflow
async def short_waits() -> str:
    for _ in range(3):
        await sleep(timedelta(milliseconds=20))
    return "finished"


@children.task
async def echo(value: int) -> int:
    await asyncio.sleep(0.02)
    return value


@environment.workflow
async def fanout(count: int) -> list[int]:
    calls = await asyncio.gather(
        *(spawn(echo(index), key=f"fanout-{index}") for index in range(count))
    )
    return await gather(*calls)


@children.task
def reject_child() -> None:
    message = "child rejected"
    raise ValueError(message)


@environment.workflow
async def concurrent_results() -> list[int]:
    handles = await asyncio.gather(
        *(spawn(echo(index), key=f"concurrent-{index}") for index in range(4))
    )
    return list(await asyncio.gather(*(handle.result() for handle in handles)))


@environment.workflow
async def concurrent_runs() -> list[int]:
    return list(await asyncio.gather(*(run(echo(index), key=f"run-{index}") for index in range(4))))


@environment.workflow
async def concurrent_replay() -> list[int]:
    async def branch(index: int) -> int:
        value = await run(echo(index), key=f"before-{index}")
        await receive(f"continue-{index}", bool)
        return await run(echo(value + 10), key=f"after-{index}")

    return list(await asyncio.gather(branch(0), branch(1)))


@environment.workflow
async def resolve_while_waiting() -> int:
    pending = promise("inside", int)

    async def resolve() -> None:
        await sleep(timedelta(milliseconds=20))
        await pending.resolve(7)

    value, _ = await asyncio.gather(pending.result(), resolve())
    return value


@environment.workflow
async def internal_timer_keys() -> str:
    await timer(timedelta(days=1), key="sleep:2")
    await sleep(timedelta(0))
    await timer(timedelta(days=1), key="receive:4")
    try:
        await receive("missing", bool, timeout=timedelta(0))
    except TimeoutError:
        return "timers are independent"
    return "unexpected resolution"


@children.task
async def overlapping_child() -> tuple[float, float]:
    started = time.time()
    await asyncio.sleep(3)
    return started, time.time()


@environment.workflow
async def overlapping_children() -> list[tuple[float, float]]:
    handles = await asyncio.gather(
        spawn(overlapping_child(), key="first"), spawn(overlapping_child(), key="second")
    )
    return await gather(*handles)


@environment.workflow
async def gather_failure() -> GatherFailure:
    bad = await spawn(reject_child(), key="bad")
    good = await spawn(echo(9), key="good")
    values = await gather(bad, good, return_exceptions=True)
    good_value = values[1]
    if not isinstance(good_value, int):
        message = "successful child did not return an integer"
        raise TypeError(message)
    return {"failure": str(values[0]), "value": good_value, "again": await good.result()}


@environment.workflow
async def completions() -> dict[str, object]:
    ready = promise("ready", int)
    await ready.resolve(7)
    delayed = await timer(timedelta(milliseconds=40), key="clock")
    child = await spawn(echo(8), key="child")
    first = await select(ready, delayed, child)
    values = {}
    async for completion in as_completed(ready, delayed, child):
        values[completion.key] = await completion.result()
    return {"first": first.key, "first_value": await first.result(), "values": values}


@environment.workflow
async def promise_race(*, timer_wins: bool) -> dict[str, object]:
    event = promise("event", int)
    clock = await timer(
        timedelta(milliseconds=20) if timer_wins else timedelta(days=1), key="clock"
    )
    completion = await select(event, clock)
    return {"key": completion.key, "value": await completion.result()}


@environment.workflow
async def state_and_promises() -> dict[str, object]:
    state = workflow_state()
    missing = await state.get("missing", int)
    await state.set("counter", 3)
    await state.set("nullable", None)
    counter = await state.get("counter", int)
    nullable = await state.get("nullable", type(None))
    keys = await state.keys()
    await state.clear("counter")
    cleared = await state.get("counter", int)
    accepted = promise("accepted", int)
    pending = await accepted.peek()
    await accepted.resolve(11)
    await accepted.resolve(11)
    resolved = await accepted.peek()
    if resolved is None:
        message = "resolved promise missing"
        raise AssertionError(message)
    conflict = False
    try:
        await accepted.resolve(12)
    except ConnectError as error:
        conflict = error.code in {Code.ALREADY_EXISTS, Code.FAILED_PRECONDITION}
    rejected = promise("rejected", int)
    await rejected.reject("promise rejected")
    await rejected.reject("promise rejected")
    try:
        await rejected.result()
    except RuntimeError as error:
        rejection = str(error)
    else:
        rejection = "unexpected resolution"
    return {
        "missing": missing,
        "counter": counter,
        "nullable": nullable,
        "keys": sorted(keys),
        "cleared": cleared,
        "pending": pending is None,
        "resolved": await resolved.result(),
        "conflict": conflict,
        "rejection": rejection,
    }


@environment.workflow
async def awakeable_outcomes() -> dict[str, object]:
    accepted = await awakeable(int, key="accepted")
    await resolve_awakeable(accepted.id, 13)
    rejected = await awakeable(int, key="rejected")
    await reject_awakeable(rejected.id, "awakeable rejected")
    try:
        await rejected.result()
    except RuntimeError as error:
        rejection = str(error)
    else:
        rejection = "unexpected resolution"
    return {"accepted": await accepted.result(), "rejection": rejection}


@environment.workflow
async def external_awakeable() -> int:
    value = await awakeable(int, key="external")
    await workflow_state().set("awakeable-id", value.id)
    return await value.result()


@environment.workflow
async def external_promise() -> int:
    await workflow_state().set("ready", value=True)
    return await promise("external", int).result()


@environment.task(retry="idempotent", max_attempts=3)
def terminal_failure() -> None:
    message = "stop retries"
    raise TerminalError(message, code=409)


@children.task
def remember(value: Entropy) -> Entropy:
    return value


@environment.workflow
async def deterministic_replay() -> ReplayResult:
    random = await workflow_random()
    entropy: Entropy = {
        "time": await workflow_time(),
        "uuid": str(await workflow_uuid()),
        "random": [random.random(), float(random.randrange(1_000_000))],
    }
    state = workflow_state()
    await state.set("entropy", entropy)
    before = await run(remember(entropy), key="entropy", metadata={"phase": "entropy"})
    await receive("resume", bool)
    return {"before": before, "after": await state.get("entropy", Entropy)}


@environment.workflow
async def cancel_child() -> str:
    child = await spawn(long_task(), key="cancelled")
    await child.cancel()
    try:
        await child.result()
    except RuntimeError:
        return "child canceled"
    return "unexpected completion"


@environment.workflow
async def pending_children() -> None:
    handles = await asyncio.gather(
        *(spawn(long_task(), key=f"pending-{index}") for index in range(3))
    )
    await gather(*handles)


@environment.task(cache=True, version="1.0.0")
def cached(value: str) -> dict[str, str]:
    return {"value": value, "decision": uuid.uuid4().hex}


@environment.task(cache=True, version="1.0.0")
def cached_failure(value: str) -> None:
    code = "proof.rejected"
    raise CacheableError(code, {"decision": uuid.uuid4().hex, "value": value})


@environment.task(retry="idempotent", max_attempts=3)
async def retry(value: str) -> dict[str, object]:
    context = current_context()
    await context.checkpoint.append("attempts", context.attempt, event_id=str(context.attempt))
    if context.attempt == 1:
        await context.checkpoint.save("input", value)
        message = "intentional first attempt failure"
        raise RuntimeError(message)
    return {"attempt": context.attempt, "saved": await context.checkpoint.load("input")}


@environment.workflow
async def nested(value: str) -> list[dict[str, object]]:
    handles = [await spawn(retry(value), key=f"retry-{i}") for i in range(2)]
    return [await handle.result() for handle in handles]


@configured_environment.task(config=(ConfigBinding("secret", "proof/secret"),))
async def configured() -> dict[str, object]:
    context = current_context()
    secret = context.config["secret"]
    print(f"configured secret {secret}")  # ruff: ignore[print] Verify output redaction.
    contents = b"binary\x00\xff" * 200_000
    ref = await context.blobs.upload_bytes(contents, "application/octet-stream")
    restored = await context.blobs.download_bytes(ref)
    return {"secret": secret, "blob": ref, "digest": hashlib.sha256(restored).hexdigest()}


@environment.task
async def long_task() -> None:
    await asyncio.sleep(120)


@environment.workflow
async def long_workflow() -> None:
    await sleep(timedelta(days=1))


@environment.workflow
async def busy_workflow() -> None:
    await asyncio.sleep(120)


@environment.workflow
async def failing_workflow() -> None:  # ruff: ignore[unused-async] Workflows require async functions.
    message = "intentional workflow failure"
    raise ValueError(message)


@environment.task(retry="idempotent", max_attempts=3)
async def interrupted() -> int:
    if current_context().attempt == 1:
        await asyncio.sleep(120)
    return current_context().attempt


@environment.task(cache=True, version="1.0.0")
async def cached_pause(value: str) -> str:
    await asyncio.sleep(5)
    return value


@environment.task
async def forged_child() -> str:
    context = run_context.get()
    registered = context.environments[environment.name]
    try:
        await context.api_client.unary(
            "/lutra.v1.LutraService/CreateTaskAction",
            {
                "environment": {
                    "namespaceId": registered.namespace_id,
                    "name": registered.name,
                    "version": registered.version,
                },
                "entrypointId": cached.entrypoint_id,
                "actionSpec": {"inputCbor": base64.b64encode(dumps([["forged"], {}])).decode()},
                "idempotencyKey": "forged-child",
            },
        )
    except ConnectError as error:
        if error.code is Code.PERMISSION_DENIED:
            return "leaf child rejected"
        raise
    return "unexpected child"


@environment.workflow
async def timestamp() -> datetime:
    return await receive("timestamp", datetime)
