"""Workflow declarations and durable calls through the generated stdio transport."""

from __future__ import annotations

import asyncio
import base64
import sys
from datetime import timedelta
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock

import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from lutra import Client, RunHandle, TaskEnvironment, TaskKind, TerminalError, receive, sleep
from lutra._gen.lutra.task.v1.task_pb import ExecuteRequest, ExecuteResponse
from lutra._gen.lutra.v1.lutra_pb import (
    CreateRunRequest,
    CreateRunResponse,
    EnvironmentIdentifier,
    RegisterEnvironmentRequest,
    RegisterEnvironmentResponse,
    Run,
    SignalRunRequest,
    SignalRunResponse,
)
from lutra._result import load_result
from lutra._workflow import decode_signal, duration_millis
from lutra.client import _BundleInputs
from lutra.runtime import RunContext, run_context
from lutra.serve import TaskAPIClient
from lutra.serve.__main__ import _bundled_handler, _load_entrypoint
from lutra.serve._host import _Host, _TaskService
from lutra.task import PromiseRejectedError
from lutra.value import dumps
from lutra.workflow import timer
from pydantic import BaseModel, TypeAdapter, ValidationError

if TYPE_CHECKING:
    from pathlib import Path

    from connectrpc.request import RequestContext


class Approval(BaseModel):
    accepted: bool
    receipt: bytes


async def _review(value: int) -> int:
    await sleep(timedelta(0))
    return value


def _effect(value: int) -> int:
    return value


def test_workflow_and_tasks_share_ordered_entrypoints() -> None:
    env = TaskEnvironment("review")
    first = env.task(_effect)
    workflow = env.workflow(_review)
    last = env.task(_effect)
    assert [(entry.entrypoint_id, entry.kind) for entry in env.entries] == [
        (1, TaskKind.TASK),
        (2, TaskKind.WORKFLOW),
        (3, TaskKind.TASK),
    ]
    assert env.entries == (first, workflow, last)
    assert workflow(42).action_spec(1).input_cbor == b"\x82\x81\x18*\xa0"
    with pytest.raises(TypeError, match="missing a required argument"):
        workflow()  # pyrefly: ignore[missing-argument]


def test_workflow_rejects_synchronous_functions() -> None:
    with pytest.raises(ValueError, match="async function"):
        TaskEnvironment("invalid").workflow(_effect)  # pyrefly: ignore[no-matching-overload]


@pytest.mark.asyncio
async def test_client_registers_and_submits_workflow(monkeypatch: pytest.MonkeyPatch) -> None:
    environment = TaskEnvironment("review")
    environment.task(_effect)
    workflow = environment.workflow(_review)
    bundle = _BundleInputs(
        b"source",
        b"build",
        ".",
        (("python", "effect"), ("python", "review")),
        ("true",),
        (),
        (),
        {},
    )

    def prepare(_environment: TaskEnvironment) -> _BundleInputs:
        return bundle

    monkeypatch.setattr("lutra.client._prepare_bundle", prepare)
    monkeypatch.setattr("lutra.client.upload_blob", AsyncMock(return_value="blob:source"))
    client = Client()
    monkeypatch.setattr(client, "_resolve_namespace_id", AsyncMock(return_value="namespace"))
    registrations: list[RegisterEnvironmentRequest] = []
    submissions: list[CreateRunRequest] = []

    async def register(request: RegisterEnvironmentRequest) -> RegisterEnvironmentResponse:
        await asyncio.sleep(0)
        registrations.append(request)
        return RegisterEnvironmentResponse(
            environment=EnvironmentIdentifier(namespace_id="namespace", name="review", version="v1")
        )

    async def submit(request: CreateRunRequest) -> CreateRunResponse:
        await asyncio.sleep(0)
        submissions.append(request)
        return CreateRunResponse(run=Run(id="workflow-run"))

    monkeypatch.setattr(client.rpc, "register_environment", register)
    monkeypatch.setattr(client.rpc, "create_run", submit)
    handle = await client.submit(workflow(7), idempotency_key="submission")
    assert handle.id == "workflow-run"
    assert len(registrations) == 1
    spec = registrations[0].spec
    assert spec is not None
    assert [(entry.name, entry.workflow) for entry in spec.entrypoints] == [
        ("_effect", False),
        ("_review", True),
    ]
    assert len(submissions) == 1
    assert submissions[0].entrypoint_id == 2
    assert submissions[0].idempotency_key == "submission"
    assert submissions[0].action_spec is not None
    assert submissions[0].action_spec.input_cbor == b"\x82\x81\x07\xa0"


@pytest.mark.parametrize(
    ("duration", "expected"),
    [(timedelta(0), 0), (timedelta(microseconds=1), 1), (timedelta(days=1), 86_400_000)],
)
def test_duration_conversion(duration: timedelta, expected: int) -> None:
    assert duration_millis(duration) == expected


@pytest.mark.parametrize("duration", [timedelta(microseconds=-1), timedelta.max])
def test_duration_rejects_negative_and_overflow(duration: timedelta) -> None:
    with pytest.raises(ValueError, match="duration"):
        duration_millis(duration)


@pytest.mark.asyncio
async def test_helpers_repeat_without_key_collisions_and_preserve_rejection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = TaskAPIClient(cast("_Host", object()))
    calls: list[dict[str, object]] = []

    async def unary(path: str, value: dict[str, object]) -> dict[str, object]:
        await asyncio.sleep(0)
        calls.append(value)
        response: dict[str, object] = {"workflowMeta": {"delivery": str(len(calls))}}
        if path.endswith("WorkflowWait"):
            refs = cast("list[dict[str, object]]", value["futures"])
            completion: dict[str, object] = {"future": refs[0], "valueCbor": "9g=="}
            if refs[0]["kind"] == "KIND_PROMISE":
                if refs[0]["id"] == "rejected":
                    completion.update(failure="FAILURE_REJECTED", message="declined")
                else:
                    completion["valueCbor"] = "9Q=="
            response["completions"] = [completion]
        return response

    monkeypatch.setattr(api, "unary", unary)
    context = RunContext(api, {}, "run", "action", workflow=True)
    token = run_context.set(context)
    try:
        with pytest.raises(ValueError, match="reserved"):
            await timer(timedelta(0), key="__lutra:sleep:1")
        assert calls == []
        await sleep(timedelta(0))
        await sleep(timedelta(microseconds=1))
        # A signal may have exactly the timeout's generated identity.
        assert await receive("__lutra:receive:5", bool, timeout=timedelta(0)) is True
        assert await receive("signal", bool) is True
        with pytest.raises(PromiseRejectedError, match="declined"):
            await receive("rejected", bool, timeout=timedelta(0))
    finally:
        run_context.reset(token)
    timers = [call for call in calls if "key" in call]
    assert [call["key"] for call in timers] == [
        "__lutra:sleep:1",
        "__lutra:sleep:3",
        "__lutra:receive:5",
        "__lutra:receive:8",
    ]
    assert timers[0].get("durationMillis", "0") == "0"
    assert timers[1]["durationMillis"] == "1"


@pytest.mark.asyncio
async def test_durable_waits_require_workflow_context() -> None:
    with pytest.raises(RuntimeError, match="active Lutra workflow"):
        await sleep(timedelta(0))
    api = TaskAPIClient(cast("_Host", object()))
    token = run_context.set(RunContext(api, {}, "run", "action"))
    try:
        with pytest.raises(RuntimeError, match="active Lutra workflow"):
            await receive("approval", bool)
    finally:
        run_context.reset(token)


@pytest.mark.asyncio
async def test_bundled_workflow_uses_durable_rpc(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "workflow.py").write_text(
        "from datetime import timedelta\n"
        "from lutra import TaskEnvironment, sleep, receive\n"
        "env = TaskEnvironment('workflow')\n"
        "@env.workflow\n"
        "async def review(value):\n"
        "    await sleep(timedelta(microseconds=1500))\n"
        "    approved = await receive('approval', bool, timeout=timedelta(seconds=30))\n"
        "    return [value, approved]\n"
    )
    monkeypatch.setenv("LUTRA_BUNDLE_ROOT", str(tmp_path))
    monkeypatch.setenv("LUTRA_ENVIRONMENTS_JSON", "[]")
    api = TaskAPIClient(cast("_Host", object()))
    calls: list[tuple[str, dict[str, object]]] = []

    async def unary(path: str, value: dict[str, object]) -> dict[str, object]:
        await asyncio.sleep(0)
        calls.append((path, value))
        if path.endswith("/WorkflowWait"):
            futures = cast("list[dict[str, object]]", value["futures"])
            return {
                "completions": [
                    {
                        "future": futures[0],
                        "valueCbor": "9Q==" if futures[0]["kind"] == "KIND_PROMISE" else "9g==",
                    }
                ],
                "workflowMeta": {"delivery": str(len(calls))},
            }
        return {"workflowMeta": {"delivery": str(len(calls))}}

    monkeypatch.setattr(api, "unary", unary)
    handler, retry = _bundled_handler("file:workflow.py:review")
    service = _TaskService(handler, retry)
    service.api_client = api
    ctx = cast("RequestContext[ExecuteRequest, ExecuteResponse]", None)
    response = await service.execute(
        ExecuteRequest(
            invocation_id="workflow",
            run_id="run",
            action_id="action",
            content_type="application/cbor",
            input=dumps([[b"document"], {}]),
        ),
        ctx,
    )
    assert response.result_cbor == b"\x82\x48document\xf5"
    assert calls == [
        (
            "/lutra.v1.LutraService/WorkflowTimer",
            {"durationMillis": "2", "sequence": "1", "key": "__lutra:sleep:1"},
        ),
        (
            "/lutra.v1.LutraService/WorkflowWait",
            {
                "sequence": "2",
                "futures": [
                    {"kind": "KIND_TIMER", "key": "__lutra:sleep:1", "id": "__lutra:sleep:1"}
                ],
            },
        ),
        (
            "/lutra.v1.LutraService/WorkflowTimer",
            {"durationMillis": "30000", "sequence": "3", "key": "__lutra:receive:3"},
        ),
        (
            "/lutra.v1.LutraService/WorkflowWait",
            {
                "sequence": "4",
                "futures": [
                    {"kind": "KIND_PROMISE", "key": "signal", "id": "approval"},
                    {"kind": "KIND_TIMER", "key": "__lutra:receive:3", "id": "__lutra:receive:3"},
                ],
            },
        ),
    ]
    assert run_context.get(None) is None


@pytest.mark.asyncio
async def test_bundled_terminal_failure_preserves_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "terminal.py").write_text(
        "from lutra import TaskEnvironment, TerminalError\n"
        "env = TaskEnvironment('terminal')\n"
        "@env.task(retry='idempotent')\n"
        "def fail():\n"
        "    raise TerminalError('order canceled by customer', code=409)\n"
    )
    monkeypatch.setenv("LUTRA_BUNDLE_ROOT", str(tmp_path))
    monkeypatch.setenv("LUTRA_ENVIRONMENTS_JSON", "[]")
    handler, retry = _bundled_handler("file:terminal.py:fail")
    service = _TaskService(handler, retry)
    service.api_client = TaskAPIClient(cast("_Host", object()))
    response = await service.execute(
        ExecuteRequest(
            invocation_id="terminal",
            run_id="run",
            action_id="terminal",
            content_type="application/cbor",
            input=dumps([[], {}]),
        ),
        cast("RequestContext[ExecuteRequest, ExecuteResponse]", None),
    )
    with pytest.raises(TerminalError, match="order canceled") as failure:
        await load_result(response.result_cbor)
    assert failure.value.code == 409


def test_bundled_task_resolves_forward_referenced_types(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "typed.py").write_text(
        "from __future__ import annotations\n"
        "from typing_extensions import TypedDict\n"
        "from datetime import datetime\n"
        "from lutra import TaskEnvironment\n"
        "class Entropy(TypedDict):\n"
        "    time: datetime\n"
        "env = TaskEnvironment('typed')\n"
        "@env.task\n"
        "def remember(value: Entropy) -> Entropy:\n"
        "    return value\n"
    )
    monkeypatch.setenv("LUTRA_BUNDLE_ROOT", str(tmp_path))
    _load_entrypoint("file:typed.py:remember")
    value_type = sys.modules["__lutra_task__"].__dict__["Entropy"]
    value = TypeAdapter(value_type).validate_python({"time": "2026-10-06T00:00:00Z"})
    assert value["time"].year == 2026


@pytest.mark.asyncio
async def test_receive_validates_types_and_reports_timeouts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = TaskAPIClient(cast("_Host", object()))
    calls: list[dict[str, object]] = []

    async def unary(path: str, value: dict[str, object]) -> dict[str, object]:
        await asyncio.sleep(0)
        calls.append(value)
        if path.endswith("WorkflowTimer"):
            return {"workflowMeta": {"delivery": str(len(calls))}}
        futures = cast("list[dict[str, object]]", value["futures"])
        ref = futures[-1] if futures[0]["id"] == "expired" else futures[0]
        data = dumps(
            None if ref["kind"] == "KIND_TIMER" else {"accepted": True, "receipt": b"\x00\xff"}
        )
        return {
            "completions": [{"future": ref, "valueCbor": base64.b64encode(data).decode()}],
            "workflowMeta": {"delivery": str(len(calls))},
        }

    monkeypatch.setattr(api, "unary", unary)
    token = run_context.set(RunContext(api, {}, "run", "action", workflow=True))
    try:
        assert await receive("approval", Approval) == Approval(accepted=True, receipt=b"\x00\xff")
        with pytest.raises(TimeoutError, match="expired"):
            await receive("expired", bool, timeout=timedelta(0))
    finally:
        run_context.reset(token)
    assert calls == [
        {"futures": [{"kind": "KIND_PROMISE", "key": "signal", "id": "approval"}], "sequence": "1"},
        {"key": "__lutra:receive:2", "sequence": "2"},
        {
            "futures": [
                {"kind": "KIND_PROMISE", "key": "signal", "id": "expired"},
                {"kind": "KIND_TIMER", "key": "__lutra:receive:2", "id": "__lutra:receive:2"},
            ],
            "sequence": "3",
        },
    ]
    with pytest.raises(ValidationError):
        await decode_signal(b"\x01", bool)
    with pytest.raises(ValueError, match="canonical"):
        await decode_signal(b"\x18\x01", int)


@pytest.mark.asyncio
async def test_signal_sends_canonical_model_value(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Client()
    sent: list[SignalRunRequest] = []

    async def signal(request: SignalRunRequest) -> SignalRunResponse:
        await asyncio.sleep(0)
        sent.append(request)
        return SignalRunResponse()

    monkeypatch.setattr(client.rpc, "signal_run", signal)
    handle: RunHandle[object] = RunHandle(client, "run-id")
    await handle.signal(
        "approval", Approval(accepted=True, receipt=b"\x00\xff"), idempotency_key="approve-1"
    )
    assert len(sent) == 1
    assert sent[0].id == "run-id"
    assert sent[0].name == "approval"
    assert sent[0].idempotency_key == "approve-1"
    assert sent[0].value_cbor == b"\xa2\x67receipt\x42\x00\xff\x68accepted\xf5"
    with pytest.raises(ValueError, match="name must be"):
        await handle.signal("", value=True)
    with pytest.raises(ValueError, match="1 MiB"):
        await handle.signal("large", b"x" * (1 << 20))
    assert len(sent) == 1


@pytest.mark.asyncio
async def test_concurrent_workflow_calls_have_an_ordered_command_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = TaskAPIClient(cast("_Host", object()))
    calls: list[dict[str, object]] = []

    async def unary(path: str, value: dict[str, object]) -> dict[str, object]:
        calls.append(value)
        await asyncio.sleep(0)
        response: dict[str, object] = {"workflowMeta": {"delivery": value["sequence"]}}
        if path.endswith("WorkflowWait"):
            futures = cast("list[dict[str, object]]", value["futures"])
            response["completions"] = [
                {
                    "future": futures[0],
                    "valueCbor": "9Q==" if futures[0]["kind"] == "KIND_PROMISE" else "9g==",
                }
            ]
        return response

    monkeypatch.setattr(api, "unary", unary)
    token = run_context.set(RunContext(api, {}, "run", "action", workflow=True))
    try:
        assert await asyncio.gather(receive("approval", bool), sleep(timedelta(seconds=1))) == [
            True,
            None,
        ]
        assert calls == [
            {
                "futures": [{"kind": "KIND_PROMISE", "key": "signal", "id": "approval"}],
                "sequence": "1",
            },
            {"durationMillis": "1000", "key": "__lutra:sleep:2", "sequence": "2"},
            {
                "futures": [
                    {"kind": "KIND_TIMER", "key": "__lutra:sleep:2", "id": "__lutra:sleep:2"}
                ],
                "sequence": "3",
            },
        ]
    finally:
        run_context.reset(token)


@pytest.mark.parametrize("cancel_first", [False, True])
@pytest.mark.asyncio
async def test_workflow_delivers_responses_in_journal_order_and_drains_canceled_waits(
    monkeypatch: pytest.MonkeyPatch, *, cancel_first: bool
) -> None:
    api = TaskAPIClient(cast("_Host", object()))
    first_ready, second_ready, submitted = asyncio.Event(), asyncio.Event(), asyncio.Event()
    arrivals: list[str] = []
    completed: list[str] = []

    async def unary(_path: str, value: dict[str, object]) -> dict[str, object]:
        ordinal = str(value["sequence"])
        arrivals.append(ordinal)
        if len(arrivals) == 2:
            submitted.set()
        await (first_ready if ordinal == "1" else second_ready).wait()
        response: dict[str, object] = {"workflowMeta": {"delivery": ordinal}}
        if "futures" in value:
            response["completions"] = [
                {"future": cast("list[object]", value["futures"])[0], "valueCbor": "9g=="}
            ]
        return response

    async def wait(name: str) -> None:
        await sleep(timedelta(milliseconds=1))
        completed.append(name)

    monkeypatch.setattr(api, "unary", unary)
    token = run_context.set(RunContext(api, {}, "run", "action", workflow=True))
    try:
        first, second = asyncio.create_task(wait("first")), asyncio.create_task(wait("second"))
        await asyncio.wait_for(submitted.wait(), timeout=1)
        if cancel_first:
            first.cancel()
        second_ready.set()
        await asyncio.sleep(0)
        assert completed == []
        first_ready.set()
        results = await asyncio.wait_for(
            asyncio.gather(first, second, return_exceptions=True), timeout=1
        )
        assert completed == (["second"] if cancel_first else ["first", "second"])
        if cancel_first:
            assert isinstance(results[0], ConnectError)
            assert results[0].code is Code.CANCELED
    finally:
        run_context.reset(token)
