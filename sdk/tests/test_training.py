from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock

import pytest
import training
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from lutra import TaskContext
from lutra._workflow import encode_signal
from lutra.value import BlobRef, dumps, loads
from pydantic import ValidationError

if TYPE_CHECKING:
    from collections.abc import Awaitable


class TrainingHost:
    def __init__(self, failure: str = "") -> None:
        self.failure = failure
        self.saved: object = None
        self.objects: dict[str, bytes] = {}
        self.events: dict[str, bytes] = {}
        self.deliveries: list[tuple[str, bytes]] = []
        self.attempt = 1
        self.checkpoint = self
        self.blobs = self

    async def load(self, _name: str, default: object) -> object:
        return self.saved if self.saved is not None else default

    async def save(self, _name: str, value: object) -> None:
        self.fail("before_commit")
        self.saved = await loads(dumps(value))
        self.fail("after_commit")

    async def upload_bytes(self, contents: bytes, _mime: str) -> BlobRef:
        uri = (
            "blob:application/cbor," + base64.b64encode(hashlib.sha256(contents).digest()).decode()
        )
        self.objects[uri] = contents
        return BlobRef(uri)

    async def download_bytes(self, ref: BlobRef) -> bytes:
        return self.objects[ref.uri]

    async def signal_workflow(self, name: str, value: object) -> None:
        payload = encode_signal(value)
        self.deliveries.append((name, payload))
        self.fail("before_delivery")
        assert name not in self.events or self.events[name] == payload
        self.events[name] = payload
        self.fail("after_acceptance")

    def fail(self, point: str) -> None:
        if self.failure == point:
            self.failure = ""
            raise ConnectionError(point)


async def invoke_train(
    host: TrainingHost, monkeypatch: pytest.MonkeyPatch, *, mock_crash_epoch: int | None = None
) -> None:
    monkeypatch.setattr(training, "current_context", lambda: cast("TaskContext", host))
    result = training.train.function({
        "epochs": 4,
        "initialization_seconds": 0,
        "epoch_seconds": 0,
        "evaluation_seconds": 0,
        "mock_crash_epoch": mock_crash_epoch,
    })
    assert inspect.isawaitable(result)
    await result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["before_commit", "after_commit", "before_delivery", "after_acceptance"]
)
async def test_training_restores_complete_state_and_resends_exact_event(
    failure: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline = TrainingHost()
    await invoke_train(baseline, monkeypatch)
    assert capsys.readouterr().out.count("Initializing model once") == 1
    recovered = TrainingHost(failure)
    with pytest.raises(ConnectionError, match=failure):
        await invoke_train(recovered, monkeypatch)
    recovered.attempt += 1
    await invoke_train(recovered, monkeypatch)
    assert capsys.readouterr().out.count("Initializing model once") == 2
    assert recovered.events == baseline.events
    assert recovered.saved == baseline.saved
    for name, payload in recovered.deliveries:
        assert payload == baseline.events[name]
    assert len(recovered.events) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", [1, 2])
async def test_example_mock_crash_recovers_without_repeating_completed_epochs(
    attempt: int, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline = TrainingHost()
    await invoke_train(baseline, monkeypatch)
    capsys.readouterr()
    recovered = TrainingHost()
    recovered.attempt = attempt
    with pytest.raises(RuntimeError, match="mock training crash"):
        await invoke_train(recovered, monkeypatch, mock_crash_epoch=1)
    recovered.attempt += 1
    await invoke_train(recovered, monkeypatch, mock_crash_epoch=1)
    output = capsys.readouterr().out
    assert "Restored checkpoint after epoch 1" in output
    assert all(output.count(f"Training epoch {epoch} (") == 1 for epoch in range(4))
    assert recovered.events == baseline.events
    assert recovered.saved == baseline.saved


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_training_limit_must_be_positive_integer(limit: object) -> None:
    with pytest.raises(ValidationError):
        training.TrainingConfig.model_validate({"max_concurrent_evaluations": limit})


@pytest.mark.asyncio
async def test_training_inputs_can_be_submitted_as_cbor() -> None:
    options: training.TrainingOptions = {"epochs": 4, "max_concurrent_evaluations": 2}
    for invocation in [training.train(options), training.training_workflow(options)]:
        spec = invocation.action_spec(invocation.task.max_attempts)
        assert await loads(spec.input_cbor) == [[options], {}]


class Future:
    def __init__(self, key: str, task: asyncio.Task[object]) -> None:
        self.key, self.task = key, task

    async def result(self) -> object:
        return await self.task


class Scheduler:
    """Exercise scheduling with live tasks, then replay recorded selections."""

    def __init__(self, failure: str = "") -> None:
        self.failure = failure
        self.children: dict[str, Future] = {}
        self.promises: dict[str, Future] = {}
        self.waits: list[tuple[list[str], str]] = []
        self.replay = False
        self.cursor = 0
        self.active = 0
        self.peak = 0
        self.overlapped = False
        self.canceled: list[str] = []

    def promise(self, name: str, _type: object) -> Future:
        return self.promises[name]

    async def spawn(self, _invocation: object, *, key: str) -> Future:
        if key in self.children:
            return self.children[key]
        if key == "training":
            for epoch in range(4):
                event = asyncio.get_running_loop().create_future()
                self.promises[f"epoch/{epoch}"] = Future(
                    f"epoch/{epoch}", cast("asyncio.Task[object]", event)
                )
            task = asyncio.create_task(self.train())
        else:
            task = asyncio.create_task(self.evaluate(int(key.split("/")[1])))
        future = Future(key, task)
        self.children[key] = future
        return future

    async def train(self) -> object:
        for epoch in range(4):
            if self.failure == "epoch" and epoch == 0:
                message = "training failed"
                raise RuntimeError(message)
            self.promises[f"epoch/{epoch}"].task.set_result({
                "epoch": epoch,
                "checkpoint": BlobRef("blob:" + base64.b64encode(bytes(32)).decode()),
                "training_metrics": {"loss": float(epoch)},
            })
            await asyncio.sleep(0.01)
            if self.failure == "capacity" and epoch == 2:
                message = "training failed"
                raise RuntimeError(message)
        return None

    async def evaluate(self, epoch: int) -> object:
        self.active += 1
        self.peak = max(self.peak, self.active)
        self.overlapped |= not self.children["training"].task.done()
        try:
            await asyncio.sleep(0.06)
            if self.failure == "evaluation":
                message = "evaluation failed"
                raise RuntimeError(message)
            return {"validation_loss": float(epoch)}
        finally:
            self.active -= 1

    async def select(self, *futures: Future) -> Future:
        keys = [future.key for future in futures]
        if self.replay:
            expected, selected = self.waits[self.cursor]
            self.cursor += 1
            assert keys == expected
            return next(future for future in futures if future.key == selected)
        done, _ = await asyncio.wait(
            [future.task for future in futures], return_when=asyncio.FIRST_COMPLETED
        )
        selected = next(future for future in futures if future.task in done)
        self.waits.append((keys, selected.key))
        return selected

    async def cancel(self, future: Future) -> None:
        self.canceled.append(future.key)
        future.task.cancel()

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(training, "spawn", self.spawn)
        monkeypatch.setattr(training, "promise", self.promise)
        monkeypatch.setattr(training, "select", self.select)
        monkeypatch.setattr(training, "cancel_invocation", self.cancel)

    async def cleanup(self) -> None:
        for child in self.children.values():
            child.task.cancel()
        await asyncio.gather(
            *(child.task for child in self.children.values()), return_exceptions=True
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [1, 2, 3])
async def test_bounded_overlapping_evaluation_replays_same_commands(
    limit: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    scheduler = Scheduler()
    scheduler.install(monkeypatch)
    config: training.TrainingOptions = {"epochs": 4, "max_concurrent_evaluations": limit}
    try:
        result = await cast("Awaitable[object]", training.training_workflow.function(config))
        assert result == [{"validation_loss": float(epoch)} for epoch in range(4)]
        assert scheduler.peak == limit
        assert scheduler.overlapped
        assert list(scheduler.children) == ["training", "eval/0", "eval/1", "eval/2", "eval/3"]
        scheduler.replay = True
        assert (
            await cast("Awaitable[object]", training.training_workflow.function(config)) == result
        )
        assert scheduler.cursor == len(scheduler.waits)
    finally:
        await scheduler.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["epoch", "capacity", "evaluation", "cancellation"])
async def test_terminal_failures_and_cancellation_cancel_children(
    failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    scheduler = Scheduler(failure)
    scheduler.install(monkeypatch)
    config: training.TrainingOptions = {"epochs": 4}
    try:
        if failure == "cancellation":
            running = asyncio.ensure_future(
                cast("Awaitable[object]", training.training_workflow.function(config))
            )
            await asyncio.sleep(0.025)
            running.cancel()
            with pytest.raises(asyncio.CancelledError):
                await running
        else:
            with pytest.raises(RuntimeError, match="failed"):
                await cast("Awaitable[object]", training.training_workflow.function(config))
        assert "training" in scheduler.canceled
        assert all(
            child.task.done() or child.key in scheduler.canceled
            for child in scheduler.children.values()
        )
    finally:
        await scheduler.cleanup()


@pytest.mark.asyncio
async def test_task_signal_uses_canonical_payload_and_propagates_transport_errors() -> None:
    client = AsyncMock()
    context = TaskContext(
        "run", "action", 1, training.train.retry, AsyncMock(), AsyncMock(), _workflow_client=client
    )
    await context.signal_workflow("epoch/0", {"loss": 1.0})
    request = client.task_signal_workflow.call_args.args[0]
    assert request.name == "epoch/0"
    assert request.value_cbor == encode_signal({"loss": 1.0})
    assert not hasattr(request, "run_id")
    client.task_signal_workflow.side_effect = ConnectError(Code.UNAVAILABLE, "lost ack")
    with pytest.raises(ConnectError, match="lost ack"):
        await context.signal_workflow("epoch/0", {"loss": 1.0})
    for name, value in [("", 0), ("x" * 201, 0), ("valid", bytes(1 << 20))]:
        with pytest.raises(ValueError, match=r"name must|exceeds 1 MiB"):
            await context.signal_workflow(name, value)
