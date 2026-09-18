"""Worker subprocess entry point.

The server invokes this module with a bundle directory and serialized inputs.
It intentionally has no access to the caller API key; child actions use the
short lived worker capability supplied in the environment.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import importlib
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime
from itertools import starmap
from pathlib import Path
from typing import TYPE_CHECKING

import pyqwest
from protobuf import Oneof

from lutra._bindings import extract_result
from lutra._gen.lutra.v1.run_pb import ActionState, InputBinding
from lutra._gen.lutra.v1.worker_connect import WorkerServiceClient
from lutra._gen.lutra.v1.worker_pb import (
    CreateChildActionRequest,
    DownloadActionArtifactRequest,
    GetActionOutputsRequest,
    RefreshAttemptTokenRequest,
)
from lutra._telemetry import attach_worker_context, configure, detach_worker_context
from lutra.serializer import deserialize, serialize
from lutra.task import Task, _reset_runtime, _set_runtime

logger = logging.getLogger("lutra.worker")

if TYPE_CHECKING:
    from collections.abc import Iterable

# Interval between child-action status polls. Each poll costs the server
# database queries, and map() multiplies that by the number of children.
_POLL_INTERVAL_SECONDS = 0.25

# Bound on concurrently open child actions; further invocations queue on the
# runtime's event loop instead of opening more connections.
_MAX_CONCURRENT_INVOCATIONS = 32
_TERMINAL_ACTION_STATES = {
    ActionState.SUCCEEDED,
    ActionState.FAILED,
    ActionState.CANCELED,
    ActionState.TIMED_OUT,
}


def _binding(slot: str, value: object) -> InputBinding:
    return InputBinding(slot_name=slot, value=Oneof("inline_bytes", serialize(value)))


async def _aclose_transport(transport: pyqwest.HTTPTransport) -> None:
    await transport.aclose()


class Runtime:
    def __init__(self) -> None:
        try:
            context = json.loads(os.environ["LUTRA_WORKER_CONTEXT"])
        except (KeyError, TypeError, ValueError) as exc:
            message = "invalid LUTRA_WORKER_CONTEXT"
            raise RuntimeError(message) from exc
        self.telemetry = configure("lutra-worker", logs=True)
        self._context_token = attach_worker_context(context)
        self.project_id = str(context["project_id"])
        self.run_id = str(context["run_id"])
        self.parent_action_id = str(context["parent_action_id"])
        self.attempt = int(context["attempt"])
        self.fencing_token = str(context["fencing_token"])
        endpoint = str(context.get("endpoint", "http://127.0.0.1:8080/worker-api")).rstrip("/")
        # ConnectRPC already emits the RPC client span. Keep its HTTP
        # transport uninstrumented so each worker RPC does not also produce a
        # nested ``POST`` span for the same request.
        self._rpc_transport = pyqwest.HTTPTransport(enable_otel=False)
        self.rpc_http = pyqwest.Client(transport=self._rpc_transport)
        self.client = WorkerServiceClient(
            endpoint,
            timeout_ms=30_000,
            http_client=self.rpc_http,
            interceptors=[self.telemetry.interceptor],
        )
        self.token = str(context["token"])
        self._token_lock = threading.Lock()
        self._stop_renewal = threading.Event()
        self.task_ids: dict[tuple[str, str], str] = {}
        self._operation_counter = 0
        self._operation_lock = threading.Lock()
        try:
            encoded_task_ids = context.get("task_ids", {})
            self.task_ids = {
                tuple(key.split("\x00", 1)): value
                for key, value in encoded_task_ids.items()
                if "\x00" in key and isinstance(value, str)
            }
        except (TypeError, ValueError):
            self.task_ids = {}
        try:
            expires_at = datetime.fromisoformat(str(context["token_expires_at"]))
            self.token_expires = time.monotonic() + max(0.0, expires_at.timestamp() - time.time())
        except (KeyError, TypeError, ValueError):
            self.token_expires = time.monotonic() + 240
        # All RPCs run on one event loop in a dedicated thread; invoke() and
        # map() submit coroutines to it and block the caller on the result.
        self._semaphore = asyncio.Semaphore(_MAX_CONCURRENT_INVOCATIONS)
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._loop_thread.start()
        self._renewal_thread = threading.Thread(target=self._renew_loop, daemon=True)
        self._renewal_thread.start()

    def _headers(self) -> dict[str, str]:
        with self._token_lock:
            token = self.token
        return {"Authorization": f"Bearer {token}"}

    async def _refresh(self, *, force: bool = False) -> None:
        with self._token_lock:
            expires = self.token_expires
        if not force and time.monotonic() < expires - 20:
            return
        response = await self.client.refresh_attempt_token(
            RefreshAttemptTokenRequest(
                project_id=self.project_id,
                run_id=self.run_id,
                parent_action_id=self.parent_action_id,
                attempt=self.attempt,
                fencing_token=self.fencing_token,
            ),
            headers=self._headers(),
        )
        with self._token_lock:
            self.token = response.token
            if response.expires_at is not None:
                self.token_expires = time.monotonic() + max(
                    0.0, response.expires_at.to_seconds() - time.time()
                )

    def _renew_loop(self) -> None:
        while not self._stop_renewal.wait(60):
            try:
                future = asyncio.run_coroutine_threadsafe(self._refresh(force=True), self._loop)
                future.result()
            except RuntimeError:
                # The event loop is shutting down; no further renewal is possible.
                return
            except Exception:
                # A single failed renewal must not disable the loop: the durable
                # attempt lease is only extended by these calls, and giving up
                # here would let the server reclaim and re-run this action.
                logger.warning("worker attempt token renewal failed", exc_info=True)

    def close(self) -> None:
        self._stop_renewal.set()
        self._renewal_thread.join(timeout=1)
        try:
            future = asyncio.run_coroutine_threadsafe(
                _aclose_transport(self._rpc_transport), self._loop
            )
            future.result(timeout=5)
        except Exception:
            logger.exception("worker HTTP transport close failed")
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._loop_thread.join(timeout=5)
        self._loop.close()
        detach_worker_context(self._context_token)
        self.telemetry.shutdown()

    def _next_operation_id(self) -> str:
        with self._operation_lock:
            self._operation_counter += 1
            return f"op-{self._operation_counter}"

    async def _invoke(
        self, task: Task, args: tuple[object, ...], kwargs: dict[str, object], *, operation_id: str
    ) -> object:
        await self._refresh()
        inputs = [_binding(str(index), value) for index, value in enumerate(args)]
        inputs.extend(starmap(_binding, kwargs.items()))
        async with self._semaphore:
            response = await self.client.create_child_action(
                CreateChildActionRequest(
                    project_id=self.project_id,
                    run_id=self.run_id,
                    parent_action_id=self.parent_action_id,
                    task_id=self.task_ids.get((task.name, task.version), ""),
                    task_name=task.name,
                    task_version=task.version,
                    inputs=inputs,
                    operation_id=operation_id,
                ),
                headers=self._headers(),
            )
            if response.action is None:
                message = "worker did not return a child action"
                raise RuntimeError(message)
            action_id = response.action.action_id
            with self.telemetry.tracer.start_as_current_span(
                "lutra.action.poll",
                attributes={
                    "lutra.project_id": self.project_id,
                    "lutra.run_id": self.run_id,
                    "lutra.action_id": action_id,
                    "lutra.poll_interval": _POLL_INTERVAL_SECONDS,
                },
            ) as poll_span:
                while True:
                    await asyncio.sleep(_POLL_INTERVAL_SECONDS)
                    await self._refresh()
                    outputs = await self.client.get_action_outputs(
                        GetActionOutputsRequest(
                            project_id=self.project_id,
                            run_id=self.run_id,
                            parent_action_id=self.parent_action_id,
                            action_id=action_id,
                        ),
                        headers=self._headers(),
                    )
                    if outputs.action is None or outputs.action.status is None:
                        continue
                    if outputs.action.status.state not in _TERMINAL_ACTION_STATES:
                        continue
                    poll_span.set_attribute("lutra.state", outputs.action.status.state.name)
                    if outputs.action.status.state != ActionState.SUCCEEDED:
                        message = outputs.action.status.failure_message or "child action failed"
                        raise RuntimeError(message)
                    break

            async def download(artifact_id: str) -> bytes:
                with self.telemetry.tracer.start_as_current_span(
                    "lutra.artifact.download",
                    attributes={
                        "lutra.project_id": self.project_id,
                        "lutra.artifact_id": artifact_id,
                    },
                ) as span:
                    artifact = await self.client.download_action_artifact(
                        DownloadActionArtifactRequest(
                            project_id=self.project_id,
                            run_id=self.run_id,
                            parent_action_id=self.parent_action_id,
                            action_id=action_id,
                            artifact_id=artifact_id,
                        ),
                        headers=self._headers(),
                    )
                    span.set_attribute("lutra.size_bytes", len(artifact.data))
                    return artifact.data

            return await extract_result(outputs.outputs, download)

    def invoke(self, task: Task, args: tuple[object, ...], kwargs: dict[str, object]) -> object:
        operation_id = self._next_operation_id()
        future = asyncio.run_coroutine_threadsafe(
            self._invoke(task, args, kwargs, operation_id=operation_id), self._loop
        )
        return future.result()

    def map(self, task: Task, items: Iterable[object]) -> list[object]:
        values = list(items)
        operation_id = self._next_operation_id()

        async def gather_all() -> list[object]:
            children = (
                self._invoke(task, (item,), {}, operation_id=f"{operation_id}/{index}")
                for index, item in enumerate(values)
            )
            return await asyncio.gather(*children)

        future = asyncio.run_coroutine_threadsafe(gather_all(), self._loop)
        return future.result()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--module", required=True)
    parser.add_argument("--function", required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    options = parser.parse_args()
    sys.path.insert(0, str(options.bundle))
    module = importlib.import_module(options.module)
    function: object = module
    for part in options.function.split("."):
        function = getattr(function, part)
    if isinstance(function, Task):
        function = function.function
    if not callable(function):
        message = f"entrypoint {options.function!r} is not callable"
        raise TypeError(message)
    encoded_inputs = json.loads(options.inputs.read_text())
    args = []
    kwargs = {}
    for value in encoded_inputs:
        decoded = deserialize(base64.b64decode(value["value"]))
        if value["slot"].isdigit():
            args.append(decoded)
        else:
            kwargs[value["slot"]] = decoded
    runtime = Runtime()
    token = _set_runtime(runtime)
    try:
        logger.info("worker task started", extra={"lutra.task": options.function})
        with runtime.telemetry.tracer.start_as_current_span(
            "lutra.task", attributes={"lutra.task": options.function}
        ) as span:
            try:
                result = function(*args, **kwargs)
            except Exception as exc:
                span.record_exception(exc)
                logger.exception("worker task failed", extra={"lutra.task": options.function})
                raise
        options.output.write_bytes(serialize(result))
        logger.info("worker task completed", extra={"lutra.task": options.function})
    finally:
        runtime.close()
        _reset_runtime(token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
