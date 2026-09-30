"""Task deployment and run client."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Generic, TypeVar

import httpx
import pyqwest

from lutra._blob import upload_blob
from lutra._bundle import SOURCE_BUNDLE_MIME, build_bundle
from lutra._gen.lutra.v1.blob_connect import BlobServiceClient
from lutra._gen.lutra.v1.blob_pb import GetDownloadRequest
from lutra._gen.lutra.v1.log_pb import StreamCursor
from lutra._gen.lutra.v1.lutra_connect import LutraServiceClient
from lutra._gen.lutra.v1.lutra_pb import (
    CancelRunRequest,
    CreateRunRequest,
    GetRunRequest,
    Run,
    TaskAction,
    TaskActionStatus,
    TaskSpec,
    WatchRunRequest,
)
from lutra.value import dumps, loads

if TYPE_CHECKING:
    import logging
    from collections.abc import AsyncIterator

    from lutra.task import Invocation, Task

R = TypeVar("R")


def _log_message(
    logger: logging.Logger, run_id: str, source: str, message: str, timestamp: str = ""
) -> None:
    instant = datetime.fromisoformat(timestamp) if timestamp else datetime.now(UTC)
    for line in message.splitlines() or [""]:
        log = logger.warning if source == "error" else logger.info
        log(
            "%-12s  %s",
            source,
            line,
            extra={
                "run_id": run_id,
                "event_source": source,
                "event_time": instant.isoformat(timespec="milliseconds"),
            },
        )


class _RunLogger:
    def __init__(self, logger: logging.Logger, run_id: str) -> None:
        self.logger, self.run_id, self.last_error = logger, run_id, ""
        self.started = False

    def event(self, event: Run | LogEvent | TaskActionStatus) -> None:
        if isinstance(event, TaskActionStatus):
            _log_message(
                self.logger,
                self.run_id,
                "task",
                f"{event.name} {event.status} action={event.action_id} "
                f"parent={event.caller_action_id} attempt={event.attempt}",
                event.updated_at,
            )
            if event.error and event.status in {"queued", "failed", "canceled"}:
                _log_message(
                    self.logger,
                    self.run_id,
                    "error",
                    f"task={event.name} action={event.action_id} "
                    f"attempt={event.attempt} | {event.error}",
                    event.updated_at,
                )
            return
        if isinstance(event, LogEvent):
            if not isinstance(event.event, dict):
                message = "invalid task log event"
                raise TypeError(message)
            action = event.event.get("action_id", event.key.decode())
            attempt = event.event.get("attempt", "?")
            source = event.event.get("source", "task")
            timestamp = datetime.fromtimestamp(event.created_unix_nanos / 1e9, UTC).isoformat(
                timespec="milliseconds"
            )
            _log_message(
                self.logger,
                self.run_id,
                str(source),
                f"action={action} attempt={attempt} | {event.message}",
                timestamp,
            )
            return
        if not self.started and event.spec is not None:
            _log_message(
                self.logger,
                self.run_id,
                "deployment",
                f"task={event.spec.name} project={event.spec.project} "
                f"domain={event.spec.domain} version={event.spec.version}",
            )
            if event.spec.image is not None:
                _log_message(
                    self.logger, self.run_id, "environment", f"image={event.spec.image.name}"
                )
        if not self.started and event.root_action_id:
            _log_message(self.logger, self.run_id, "root", f"action={event.root_action_id}")
        self.started = True
        status = event.status
        if event.created_at and event.updated_at:
            duration = datetime.fromisoformat(event.updated_at) - datetime.fromisoformat(
                event.created_at
            )
            status += f" elapsed={duration.total_seconds():.3f}s"
        _log_message(self.logger, self.run_id, "status", status, event.updated_at)
        if event.error and event.error != self.last_error:
            _log_message(self.logger, self.run_id, "error", event.error, event.updated_at)
        self.last_error = event.error


@dataclass(frozen=True)
class LogEvent:
    """A decoded record from a run log stream."""

    stream: str
    sequence: int
    key: bytes
    event: object
    created_unix_nanos: int

    @property
    def message(self) -> str:
        """The task-log message.

        Raises:
            TypeError: If the event does not contain a string message.

        """
        if not isinstance(self.event, dict) or not isinstance(self.event.get("message"), str):
            message = "invalid task log event"
            raise TypeError(message)
        return self.event["message"]


def _require_run(run: Run | None) -> Run:
    if run is None:
        message = "server returned no run"
        raise RuntimeError(message)
    return run


def _require_action(action: TaskAction | None) -> TaskAction:
    if action is None:
        message = "server returned no task action"
        raise RuntimeError(message)
    return action


class RunHandle(Generic[R]):
    """Handle the lifecycle and result of a submitted task run."""

    def __init__(self, client: Client, run_id: str) -> None:
        """Create a handle associated with a client and server run ID."""
        self.client = client
        self.id = run_id

    async def status(self) -> str:
        """Return the current server status for this run.

        Returns:
            The server status string.

        """
        return _require_run((await self.client.rpc.get_run(GetRunRequest(id=self.id))).run).status

    async def watch(self) -> AsyncIterator[Run]:
        """Yield run updates until the server closes the stream.

        Yields:
            The next run update from the server.

        """
        async for response in self.client.rpc.watch_run(WatchRunRequest(id=self.id)):
            yield _require_run(response.run)

    async def result(self, *, logger: logging.Logger | None = None) -> R:
        """Wait for completion and return the decoded task result.

        With a logger, display lifecycle events and drain task logs before returning.

        Returns:
            The decoded task result.

        Raises:
            RuntimeError: If the run fails, is canceled, or ends prematurely.

        """
        terminal: Run | None = None
        writer = _RunLogger(logger, self.id) if logger is not None else None
        events = self.watch() if logger is None else self.events()
        async for event in events:
            if writer is not None:
                writer.event(event)
            if not isinstance(event, Run):
                continue
            if event.status in {"succeeded", "failed", "canceled"}:
                terminal = event
        if terminal is None:
            message = "run status stream ended before completion"
            raise RuntimeError(message)
        if terminal.status != "succeeded":
            raise RuntimeError(terminal.error or f"run {terminal.status}")
        result = await loads(terminal.output_cbor, self.client.resolve_blob)
        if logger is not None:
            _log_message(logger, self.id, "result", repr(result))
        return result  # type: ignore[return-value]

    async def cancel(self) -> None:
        """Request cancellation of this run."""
        await self.client.rpc.cancel_run(CancelRunRequest(id=self.id))

    async def logs(
        self, stream: str = "task_log", *, after_seq: int = 0, key_prefix: bytes = b""
    ) -> AsyncIterator[LogEvent]:
        """Yield decoded records from a run log stream.

        Yields:
            The next decoded record, including its sequence cursor.

        """
        async for event in self.events(stream, after_seq=after_seq, key_prefix=key_prefix):
            if isinstance(event, LogEvent):
                yield event

    async def events(
        self, stream: str = "task_log", *, after_seq: int = 0, key_prefix: bytes = b""
    ) -> AsyncIterator[Run | LogEvent | TaskActionStatus]:
        """Yield run status updates and task logs through completion.

        Yields:
            A run snapshot, child task status, or decoded log record.

        """
        request = WatchRunRequest(
            id=self.id,
            task_logs=StreamCursor(stream=stream, after_seq=after_seq, key_prefix=key_prefix),
        )
        async for update in self.client.rpc.watch_run(request):
            if update.action_status is not None:
                yield update.action_status
            if update.run is not None:
                yield update.run
            response = update.log
            if response is None:
                continue
            event = await loads(response.value_cbor, self.client.resolve_blob)
            yield LogEvent(
                stream=response.stream,
                sequence=response.seq,
                key=bytes(response.key),
                event=event,
                created_unix_nanos=response.created_unix_nanos,
            )


class Client:
    """Submit tasks to a Lutra server and retrieve their results."""

    def __init__(
        self,
        url: str = "http://localhost:8080/api",
        *,
        project: str = "default",
        domain: str = "default",
    ) -> None:
        """Create a client for the API URL, project, and domain."""
        self.rpc = LutraServiceClient(url, http_client=pyqwest.Client())
        self.blob = BlobServiceClient(url, http_client=pyqwest.Client())
        self.project = project
        self.domain = domain

    async def _prepare(self, task: Task[..., R]) -> TaskSpec:
        bundle = build_bundle(task)
        if len(bundle) > 64 << 20:
            message = "source bundle exceeds 64 MiB"
            raise ValueError(message)
        uri = await upload_blob(self.blob, bundle, SOURCE_BUNDLE_MIME)
        return task.spec(self.project, self.domain, uri)

    async def submit(self, invocation: Invocation[R], *, idempotency_key: str = "") -> RunHandle[R]:
        """Submit an invocation and return a handle for its run.

        Returns:
            A handle for the submitted run.

        """
        spec = await self._prepare(invocation.task)
        result = await self.rpc.create_run(
            CreateRunRequest(
                spec=spec,
                input_cbor=dumps([list(invocation.args), invocation.kwargs]),
                idempotency_key=idempotency_key,
            )
        )
        return RunHandle(self, _require_run(result.run).id)

    async def run(
        self,
        invocation: Invocation[R],
        *,
        idempotency_key: str = "",
        logger: logging.Logger | None = None,
    ) -> R:
        """Submit an invocation and wait for its decoded result.

        Pass a Python logger to display run events, task logs, and the result.

        Returns:
            The decoded task result.

        """
        if logger is not None:
            _log_message(
                logger, "", "submit", f"{invocation.task.module}.{invocation.task.qualname}"
            )
            _log_message(
                logger, "", "input", f"args={invocation.args!r} kwargs={invocation.kwargs!r}"
            )
        handle = await self.submit(invocation, idempotency_key=idempotency_key)
        if logger is not None:
            _log_message(logger, handle.id, "run", handle.id)
        return await handle.result(logger=logger)

    async def resolve_blob(self, uri: str) -> bytes:
        """Download and return the contents of a Lutra blob URI.

        Returns:
            The blob contents.

        """
        response = await self.blob.get_download(GetDownloadRequest(uri=uri))
        async with httpx.AsyncClient() as http:
            downloaded = await http.get(response.url)
            downloaded.raise_for_status()
            return downloaded.content
