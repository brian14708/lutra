"""Task deployment and run client."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Generic, Literal, TypeVar

import httpx
import pyqwest

from lutra._blob import upload_blob
from lutra._bundle import SOURCE_BUNDLE_MIME, bundle_bytes
from lutra._gen.lutra.v1.blob_connect import BlobServiceClient
from lutra._gen.lutra.v1.blob_pb import GetDownloadRequest
from lutra._gen.lutra.v1.log_pb import StreamCursor
from lutra._gen.lutra.v1.lutra_connect import LutraServiceClient
from lutra._gen.lutra.v1.lutra_pb import (
    CancelRunRequest,
    Command,
    CreateRunRequest,
    Entrypoint,
    EnvironmentIdentifier,
    EnvironmentSpec,
    GetRunRequest,
    ImageSpec,
    RegisterEnvironmentRequest,
    Resources,
    Run,
    TaskAction,
    TaskActionStatus,
    WatchRunRequest,
)
from lutra._gen.lutra.v1.settings_connect import SettingsServiceClient
from lutra._gen.lutra.v1.settings_pb import ListNamespacesRequest
from lutra._source_bundle import build_source_bundle
from lutra.blob import BlobStore
from lutra.task import CacheableError, normalize_retry
from lutra.value import loads

if TYPE_CHECKING:
    import logging
    from collections.abc import AsyncIterator

    from lutra._live import LiveDisplay
    from lutra.task import Invocation, TaskEnvironment

R = TypeVar("R")
_ID_SUFFIX_LENGTH = 8


def _id_suffix(value: str) -> str:  # ruff: ignore[reimplemented-operator]
    return value[-_ID_SUFFIX_LENGTH:]


def _preview(value: object, limit: int = 160) -> str:
    rendered = repr(value)
    return rendered if len(rendered) <= limit else rendered[: limit - 1] + "…"


def _size(value: bytes) -> str:
    return (
        f"{len(value) / 1024:.1f} KiB"
        if len(value) < 1 << 20
        else f"{len(value) / (1 << 20):.1f} MiB"
    )


def _task_display_status(event: TaskActionStatus) -> str:
    return "cached" if event.status == "succeeded" and event.cache_hit else event.status


def _log_message(  # ruff: ignore[too-many-arguments]
    logger: logging.Logger,
    run_id: str,
    source: str,
    message: str,
    timestamp: str = "",
    *,
    warning: bool = False,
    **fields: object,
) -> None:
    instant = datetime.fromisoformat(timestamp) if timestamp else datetime.now(UTC)
    for line in message.splitlines() or [""]:
        log = logger.warning if warning or source == "error" else logger.info
        log(
            "%-8s %s",
            source,
            line,
            extra={
                "run_id": run_id,
                "event_source": source,
                "event_time": instant.isoformat(timespec="milliseconds"),
                **fields,
            },
        )


class _RunLogger:
    def __init__(
        self,
        logger: logging.Logger,
        run_id: str,
        task_name: str = "",
        entrypoint_names: dict[int, str] | None = None,
    ) -> None:
        self.logger, self.run_id, self.task_name = logger, run_id, task_name
        self.entrypoint_names = entrypoint_names or {}
        self.run_status = ""
        self.root_action_id = ""
        self.last_error = ""
        self.actions: dict[str, tuple[str, int, bool, str]] = {}
        self.action_names: dict[str, str] = {}
        self.phases: set[tuple[str, str]] = set()

    def event(self, event: Run | LogEvent | TaskActionStatus) -> None:
        if isinstance(event, TaskActionStatus):
            self._action(event)
        elif isinstance(event, LogEvent):
            self._output(event)
        else:
            self._run(event)

    def _action(self, event: TaskActionStatus) -> None:
        name = (
            self.task_name
            if self.task_name and event.action_id == self.root_action_id
            else self.entrypoint_names.get(
                event.entrypoint_id, f"entrypoint #{event.entrypoint_id}"
            )
        )
        self.action_names[event.action_id] = name
        previous = self.actions.get(event.action_id)
        current = (event.status, event.attempt, event.cache_hit, event.error)
        if previous == current:
            return
        self.actions[event.action_id] = current
        attempt = (
            f" · attempt {event.attempt}/{event.max_attempts}"
            if event.attempt and event.max_attempts > 1
            else ""
        )
        origin = (
            f" · from {_id_suffix(event.caller_action_id)}"
            if previous is None and event.caller_action_id
            else ""
        )
        error = (
            f" · {event.error}"
            if event.error and event.status in {"queued", "failed", "canceled"}
            else ""
        )
        _log_message(
            self.logger,
            self.run_id,
            "task",
            f"{name} [{_id_suffix(event.action_id)}] "
            f"{_task_display_status(event)}{attempt}{origin}{error}",
            event.updated_at,
            warning=event.status in {"failed", "canceled"},
            action_id=event.action_id,
            parent_action_id=event.caller_action_id,
            entrypoint_id=event.entrypoint_id,
        )

    def _output(self, event: LogEvent) -> None:
        if not isinstance(event.event, dict):
            message = "invalid task log event"
            raise TypeError(message)
        action = str(event.event.get("action_id", event.key.decode(errors="replace")))
        attempt = event.event.get("attempt", "?")
        attempt_label = f" · attempt {attempt}" if attempt != 1 else ""
        phase = str(event.event.get("phase", "task"))
        source = phase if phase in {"pull", "build"} else "output"
        label = self.action_names.get(
            action, self.task_name if action == self.root_action_id else "task"
        )
        phase_key = (action, phase)
        image = ""
        if phase in {"pull", "build"} and phase_key not in self.phases:
            runtime = event.event.get("runtime", "")
            image_name = event.event.get("image", "")
            image = f" · {runtime} {image_name}" if runtime or image_name else ""
        self.phases.add(phase_key)
        timestamp = datetime.fromtimestamp(event.created_unix_nanos / 1e9, UTC).isoformat(
            timespec="milliseconds"
        )
        _log_message(
            self.logger,
            self.run_id,
            source,
            f"{label} [{_id_suffix(action)}]{attempt_label}{image} | {event.message}",
            timestamp,
            action_id=action,
            phase=phase,
        )

    def _run(self, event: Run) -> None:
        if not self.run_status:
            environment = event.environment.name if event.environment is not None else ""
            target = (
                f"{environment}.{self.task_name}"
                if environment and self.task_name
                else environment or self.task_name or f"entrypoint #{event.entrypoint_id}"
            )
            _log_message(
                self.logger,
                self.run_id,
                "run",
                f"{self.run_id} · {target}",
                entrypoint_id=event.entrypoint_id,
            )
        self.root_action_id = event.root_action_id or self.root_action_id
        if event.status == self.run_status and event.error == self.last_error:
            return
        self.run_status = event.status
        status = event.status
        if event.created_at and event.updated_at:
            duration = datetime.fromisoformat(event.updated_at) - datetime.fromisoformat(
                event.created_at
            )
            status += f" · {duration.total_seconds():.1f}s"
        if event.error:
            status += f" · {event.error}"
        _log_message(
            self.logger,
            self.run_id,
            "run",
            status,
            event.updated_at,
            warning=event.status in {"failed", "canceled"},
            action_id=self.root_action_id,
        )
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


@dataclass(frozen=True)
class _BundleInputs:
    source: bytes
    build_context: bytes
    python_paths: tuple[str, ...]
    python_requires: str
    workdir: str
    entrypoints: tuple[tuple[str, ...], ...]
    prepare_command: tuple[str, ...]


def _prepare_bundle(environment: TaskEnvironment) -> _BundleInputs:
    source = environment.dependency_source
    root = source.bundle_root
    bundle = build_source_bundle(
        source, tuple(task.source_file.relative_to(root) for task in environment.tasks)
    )
    return _BundleInputs(
        bundle,
        bundle_bytes(source.build_files),
        source.python_paths,
        source.python_requires,
        source.root.relative_to(root).as_posix(),
        tuple(task.entrypoint(source) for task in environment.tasks),
        source.prepare_command,
    )


def _entrypoint_names(environment: TaskEnvironment) -> dict[int, str]:
    candidates: dict[int, set[str]] = {}
    visited: set[TaskEnvironment] = set()

    def visit(current: TaskEnvironment) -> None:
        if current in visited:
            return
        visited.add(current)
        for task in current.tasks:
            candidates.setdefault(task.entrypoint_id, set()).add(task.qualname)
        for dependency in current.dependencies:
            visit(dependency)

    visit(environment)
    return {
        entrypoint_id: next(iter(names))
        for entrypoint_id, names in candidates.items()
        if len(names) == 1
    }


class RunHandle(Generic[R]):
    """Handle the lifecycle and result of a submitted task run."""

    def __init__(
        self,
        client: Client,
        run_id: str,
        task_name: str = "",
        entrypoint_names: dict[int, str] | None = None,
    ) -> None:
        """Create a handle associated with a client and server run ID."""
        self.client = client
        self.id = run_id
        self.task_name = task_name
        self.entrypoint_names = entrypoint_names or {}

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

    async def result(
        self, *, logger: logging.Logger | None = None, display: Literal["live"] | None = None
    ) -> R:
        """Wait for completion and return the decoded task result.

        Pass a logger for event lines, or ``display="live"`` for a terminal display.

        Returns:
            The decoded task result.

        Raises:
            ValueError: If both output options are requested.

        """
        if logger is not None and display is not None:
            msg = "logger and display cannot be used together"
            raise ValueError(msg)
        if display not in {None, "live"}:
            msg = f"unsupported display: {display!r}"
            raise ValueError(msg)
        if display == "live":
            from lutra._live import LiveDisplay  # ruff: ignore[import-outside-top-level]

            with LiveDisplay(self.id, self.task_name, self.entrypoint_names) as view:
                try:
                    return await self._result(view)
                except Exception as exc:
                    view.fail(str(exc))
                    raise
        writer = (
            _RunLogger(logger, self.id, self.task_name, self.entrypoint_names)
            if logger is not None
            else None
        )
        return await self._result(writer)

    async def _result(  # ruff: ignore[complex-structure]
        self, writer: _RunLogger | LiveDisplay | None
    ) -> R:
        terminal: Run | None = None
        events = self.watch() if writer is None else self.events()
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
            if terminal.status == "failed" and terminal.output_cbor:
                failure = await loads(terminal.output_cbor, self.client.resolve_blob)
                if (
                    isinstance(failure, list)
                    and len(failure) == len(("lutra.cacheable-error.v1", "", None))
                    and failure[0] == "lutra.cacheable-error.v1"
                ):
                    raise CacheableError(failure[1], failure[2])
            raise RuntimeError(terminal.error or f"run {terminal.status}")
        result = await loads(terminal.output_cbor, self.client.resolve_blob)
        if isinstance(writer, _RunLogger):
            _log_message(writer.logger, self.id, "result", _preview(result))
        elif writer is not None:
            writer.finish(result)
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
        self, url: str = "http://localhost:8080/api", *, namespace: str = "default"
    ) -> None:
        """Create a client for the API URL and namespace."""
        self.rpc = LutraServiceClient(url, http_client=pyqwest.Client())
        self.blob = BlobServiceClient(url, http_client=pyqwest.Client())
        self.blobs = BlobStore(self.blob)
        self.namespace = namespace
        self.settings = SettingsServiceClient(url, http_client=pyqwest.Client())
        self._namespace_id: str | None = None

    async def _resolve_namespace_id(self) -> str:
        if self._namespace_id is None:
            response = await self.settings.list_namespaces(ListNamespacesRequest())
            self._namespace_id = next(
                (item.id for item in response.namespaces if item.slug == self.namespace), None
            )
            if self._namespace_id is None:
                msg = f"namespace {self.namespace!r} does not exist"
                raise ValueError(msg)
        return self._namespace_id

    async def _prepare(
        self, environment: TaskEnvironment, logger: logging.Logger | None = None
    ) -> EnvironmentIdentifier:
        namespace_id = await self._resolve_namespace_id()
        ordered: list[TaskEnvironment] = []
        visiting: set[TaskEnvironment] = set()
        visited: set[TaskEnvironment] = set()
        names: dict[str, TaskEnvironment] = {}

        def visit(current: TaskEnvironment) -> None:
            if current in visiting:
                msg = "environment dependencies contain a cycle"
                raise ValueError(msg)
            if current.name in names and names[current.name] is not current:
                msg = f"ambiguous environment name {current.name!r}"
                raise ValueError(msg)
            names[current.name] = current
            if current in visited:
                return
            visiting.add(current)
            for dependency in current.dependencies:
                visit(dependency)
            visiting.remove(current)
            visited.add(current)
            ordered.append(current)

        visit(environment)
        identifiers: dict[TaskEnvironment, EnvironmentIdentifier] = {}
        for current in ordered:
            inputs = _prepare_bundle(current)
            if logger is not None:
                _log_message(
                    logger,
                    "",
                    "prepare",
                    f"{current.name} · source {_size(inputs.source)}, "
                    f"build context {_size(inputs.build_context)}",
                )
            uri = await upload_blob(self.blob, inputs.source, SOURCE_BUNDLE_MIME)
            build_uri = await upload_blob(self.blob, inputs.build_context, SOURCE_BUNDLE_MIME)
            response = await self.rpc.register_environment(
                RegisterEnvironmentRequest(
                    spec=EnvironmentSpec(
                        namespace_id=namespace_id,
                        name=current.name,
                        source_uri=uri,
                        python_paths=list(inputs.python_paths),
                        workdir=inputs.workdir,
                        prepare_command=Command(args=list(inputs.prepare_command)),
                        image=ImageSpec(
                            name=current.image.name,
                            from_image=current.image.from_image,
                            resources=Resources(
                                cpu_millis=current.resources.cpu_millis,
                                memory_bytes=current.resources.memory_bytes,
                            ),
                            env_vars=dict(current.env_vars),
                            build_context_uri=build_uri,
                            platform=current.image.platform or "",
                            python_requires=inputs.python_requires,
                        ),
                        dependencies=[
                            identifiers[dependency]
                            for dependency in dict.fromkeys(current.dependencies)
                        ],
                        entrypoints=[
                            Entrypoint(
                                command=Command(args=list(inputs.entrypoints[index])),
                                name=task.qualname,
                                max_attempts=task.max_attempts,
                                cache=task.cache,
                                task_version=task.version or "" if task.cache else "",
                            )
                            for index, task in enumerate(current.tasks)
                        ],
                    )
                )
            )
            if response.environment is None:
                msg = "server returned no registered environment"
                raise RuntimeError(msg)
            identifiers[current] = response.environment
            if logger is not None:
                _log_message(
                    logger,
                    "",
                    "registered",
                    f"{current.name} · version {_id_suffix(response.environment.version)}",
                )
        return identifiers[environment]

    async def submit(
        self,
        invocation: Invocation[R],
        *,
        idempotency_key: str = "",
        max_attempts: int | None = None,
        logger: logging.Logger | None = None,
    ) -> RunHandle[R]:
        """Submit an invocation and return a handle for its run.

        Returns:
            A handle for the submitted run.

        """
        _, attempts = normalize_retry(
            invocation.task.retry,
            invocation.task.max_attempts if max_attempts is None else max_attempts,
        )
        environment = await self._prepare(invocation.task.environment, logger)
        result = await self.rpc.create_run(
            CreateRunRequest(
                environment=environment,
                entrypoint_id=invocation.task.entrypoint_id,
                action_spec=invocation.action_spec(attempts),
                idempotency_key=idempotency_key,
            )
        )
        return RunHandle(
            self,
            _require_run(result.run).id,
            invocation.task.qualname,
            _entrypoint_names(invocation.task.environment),
        )

    async def run(
        self,
        invocation: Invocation[R],
        *,
        idempotency_key: str = "",
        max_attempts: int | None = None,
        logger: logging.Logger | None = None,
        display: Literal["live"] | None = None,
    ) -> R:
        """Submit an invocation and wait for its decoded result.

        Pass a Python logger for event lines, or ``display="live"`` for a terminal display.

        Returns:
            The decoded task result.

        Raises:
            ValueError: If both output options are requested.

        """
        if logger is not None and display is not None:
            msg = "logger and display cannot be used together"
            raise ValueError(msg)
        if display not in {None, "live"}:
            msg = f"unsupported display: {display!r}"
            raise ValueError(msg)
        arguments = []
        if logger is not None or display == "live":
            if invocation.args:
                arguments.append(f"args={_preview(invocation.args)}")
            if invocation.kwargs:
                arguments.append(f"kwargs={_preview(invocation.kwargs)}")
        target = " ".join([
            f"{invocation.task.environment.name}.{invocation.task.qualname}",
            *arguments,
        ])
        if logger is not None:
            _log_message(logger, "", "submit", target)
        if display == "live":
            from lutra._live import LiveDisplay  # ruff: ignore[import-outside-top-level]

            with LiveDisplay(
                "", invocation.task.qualname, _entrypoint_names(invocation.task.environment)
            ) as view:
                view.prepare(target)
                try:
                    handle = await self.submit(
                        invocation, idempotency_key=idempotency_key, max_attempts=max_attempts
                    )
                    view.run_id = handle.id
                    return await handle._result(view)  # ruff: ignore[private-member-access]
                except Exception as exc:
                    view.fail(str(exc))
                    raise
        handle = await self.submit(
            invocation, idempotency_key=idempotency_key, max_attempts=max_attempts, logger=logger
        )
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
