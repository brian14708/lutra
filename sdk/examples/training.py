# /// script
# requires-python = ">=3.11"
# dependencies = ["lutra[tui]"]
#
# [tool.uv.sources]
# lutra = { path = "..", editable = true }
# ///
"""Resumable resident training with bounded, overlapping evaluation.

Run against a Lutra stack with ``uv run sdk/examples/training.py --epochs 6
--max-concurrent-evaluations 2``. A small scalar regression model keeps this
example CPU-only. Replace its updates and state with framework equivalents
(including device RNG and sampler state) for a larger model.

Use ``--mock-crash-epoch 2`` to fail once after saving epoch 2, before sending
its event. The retry restores that checkpoint, resends the event, and starts
epoch 3. Initialization, epoch, and evaluation delays have separate CLI flags.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
from typing import TYPE_CHECKING
from typing_extensions import TypedDict

from lutra import (
    Client,
    TaskEnvironment,
    cancel_invocation,
    current_context,
    promise,
    select,
    spawn,
)
from lutra.value import BlobRef, dumps, loads
from pydantic import BaseModel, Field, TypeAdapter

if TYPE_CHECKING:
    from lutra.runtime import ChildHandle


class TrainingConfig(BaseModel):
    """Fixed training schedule and per-workflow evaluation capacity."""

    epochs: int = Field(default=6, strict=True, gt=0)
    max_concurrent_evaluations: int = Field(default=1, strict=True, gt=0)
    initialization_seconds: float = Field(default=2.0, ge=0)
    epoch_seconds: float = Field(default=2.0, ge=0)
    evaluation_seconds: float = Field(default=3.0, ge=0)
    mock_crash_epoch: int | None = Field(default=None, strict=True, ge=0)


class TrainingOptions(TypedDict, total=False):
    epochs: int
    max_concurrent_evaluations: int
    initialization_seconds: float
    epoch_seconds: float
    evaluation_seconds: float
    mock_crash_epoch: int | None


class EpochEvent(TypedDict):
    epoch: int
    checkpoint: BlobRef
    training_metrics: dict[str, float]


class ResumeState(BaseModel):
    """All mutable state needed to start the next epoch."""

    completed_epoch: int = -1
    model: float = 0.0
    optimizer_velocity: float = 0.0
    scheduler_lr: float = 0.01
    rng_version: int
    rng_state: list[int]
    rng_gauss: float | None
    data_order: list[int]


training_environment = TaskEnvironment("resident-training")
evaluation_environment = TaskEnvironment("checkpoint-evaluation")
environment = TaskEnvironment(
    "training-workflow", dependencies=(training_environment, evaluation_environment)
)


@training_environment.task(retry="idempotent", max_attempts=4, cache=False)
async def train(options: TrainingOptions) -> None:
    config = TrainingConfig.model_validate(options)
    if config.mock_crash_epoch is not None and config.mock_crash_epoch >= config.epochs:
        message = "mock_crash_epoch must be less than epochs"
        raise ValueError(message)
    context = current_context()
    print(f"Initializing model once for training attempt {context.attempt}", flush=True)
    await asyncio.sleep(config.initialization_seconds)
    rng = random.Random(42)  # ruff: ignore[suspicious-non-cryptographic-random-usage]
    version, internal, gauss = rng.getstate()
    state = ResumeState(
        rng_version=version, rng_state=list(internal), rng_gauss=gauss, data_order=list(range(8))
    )
    saved = await context.checkpoint.load("latest", default=None)
    if saved is not None:
        event = TypeAdapter(EpochEvent).validate_python(saved, strict=True)
        state = ResumeState.model_validate(
            await loads(await context.blobs.download_bytes(event["checkpoint"]))
        )
        rng.setstate((state.rng_version, tuple(state.rng_state), state.rng_gauss))
        print(
            f"Restored checkpoint after epoch {event['epoch']}; resending saved event", flush=True
        )
        # The saved event is the only publication whose acknowledgement may be lost.
        await context.signal_workflow(f"epoch/{event['epoch']}", event)

    for epoch in range(state.completed_epoch + 1, config.epochs):
        print(f"Training epoch {epoch} ({config.epoch_seconds}s)", flush=True)
        rng.shuffle(state.data_order)
        for index in state.data_order:
            x = (index + 1) / 8
            gradient = 2 * x * (state.model * x - 2 * x)
            state.optimizer_velocity = 0.9 * state.optimizer_velocity + gradient
            state.model -= state.scheduler_lr * state.optimizer_velocity
        state.scheduler_lr *= 0.95
        state.completed_epoch = epoch
        version, internal, gauss = rng.getstate()
        state.rng_version, state.rng_state, state.rng_gauss = version, list(internal), gauss
        await asyncio.sleep(config.epoch_seconds)
        ref = await context.blobs.upload_bytes(dumps(state.model_dump()), "application/cbor")
        event: EpochEvent = {
            "epoch": epoch,
            "checkpoint": ref,
            "training_metrics": {"loss": (state.model - 2) ** 2},
        }
        await context.checkpoint.save("latest", event)
        if epoch == config.mock_crash_epoch:
            print(f"Simulating accidental crash after epoch {epoch} checkpoint commit", flush=True)
            message = "mock training crash before signal delivery"
            raise RuntimeError(message)
        await context.signal_workflow(f"epoch/{epoch}", event)
        print(f"Training committed epoch {epoch}", flush=True)


@evaluation_environment.task(retry="idempotent", max_attempts=3, cache=False)
async def evaluate(event: EpochEvent, seconds: float) -> dict[str, float]:
    print(f"Evaluating epoch {event['epoch']} ({seconds}s)", flush=True)
    state = ResumeState.model_validate(
        await loads(await current_context().blobs.download_bytes(event["checkpoint"]))
    )
    await asyncio.sleep(seconds)
    print(f"Evaluation finished for epoch {event['epoch']}", flush=True)
    return {"validation_loss": (state.model - 2) ** 2}


@environment.workflow
async def training_workflow(  # ruff: ignore[complex-structure,too-many-branches] Explicit scheduling state machine.
    options: TrainingOptions,
) -> list[dict[str, float]]:
    config = TrainingConfig.model_validate(options)
    training = await spawn(train(options), key="training")
    active: dict[str, ChildHandle[dict[str, float]]] = {}
    results: dict[int, dict[str, float]] = {}
    training_done = False

    def consume_evaluation(key: str, metrics: dict[str, float]) -> None:
        results[int(key.split("/")[1])] = metrics
        del active[key]

    try:  # ruff: ignore[too-many-statements-in-try-clause] Cancel all children on any scheduling failure.
        for epoch in range(config.epochs):
            event_future = promise(f"epoch/{epoch}", EpochEvent)
            # Watch evaluation failures even while waiting for the next event.
            while True:
                completed = await select(
                    event_future, *active.values(), *([] if training_done else [training])
                )
                value = await completed.result()
                if completed.key == "training":
                    training_done = True
                elif completed.key == event_future.key:
                    event = TypeAdapter(EpochEvent).validate_python(value, strict=True)
                    break
                else:
                    consume_evaluation(
                        completed.key, TypeAdapter(dict[str, float]).validate_python(value)
                    )
            if event["epoch"] != epoch:
                message = "epoch event number does not match its promise"
                raise ValueError(message)  # ruff: ignore[raise-within-try]
            while len(active) >= config.max_concurrent_evaluations:
                completed = await select(*active.values(), *([] if training_done else [training]))
                value = await completed.result()
                if completed.key == "training":
                    training_done = True
                else:
                    consume_evaluation(
                        completed.key, TypeAdapter(dict[str, float]).validate_python(value)
                    )
            key = f"eval/{epoch}"
            active[key] = await spawn(evaluate(event, config.evaluation_seconds), key=key)
        while active or not training_done:
            completed = await select(*active.values(), *([] if training_done else [training]))
            value = await completed.result()
            if completed.key == "training":
                training_done = True
            else:
                consume_evaluation(
                    completed.key, TypeAdapter(dict[str, float]).validate_python(value)
                )
    except BaseException:
        for child in [training, *active.values()]:
            await cancel_invocation(child)
        raise
    return [results[epoch] for epoch in range(config.epochs)]


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--epochs", type=int, default=6)
    parser.add_argument("--max-concurrent-evaluations", type=int, default=1)
    parser.add_argument("--initialization-seconds", type=float, default=2.0)
    parser.add_argument("--epoch-seconds", type=float, default=2.0)
    parser.add_argument("--evaluation-seconds", type=float, default=3.0)
    parser.add_argument("--mock-crash-epoch", type=int, default=None)
    args = parser.parse_args()
    options: TrainingOptions = {
        "epochs": args.epochs,
        "max_concurrent_evaluations": args.max_concurrent_evaluations,
        "initialization_seconds": args.initialization_seconds,
        "epoch_seconds": args.epoch_seconds,
        "evaluation_seconds": args.evaluation_seconds,
        "mock_crash_epoch": args.mock_crash_epoch,
    }
    TrainingConfig.model_validate(options)
    client = Client(os.getenv("LUTRA_URL", "http://127.0.0.1:8080/api"))
    handle = await client.submit(training_workflow(options))
    print(f"Training workflow: {handle.id}", flush=True)
    print(await handle.result(display="live"))


if __name__ == "__main__":
    asyncio.run(main())
