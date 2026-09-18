"""High-level asynchronous and synchronous Lutra clients."""

from __future__ import annotations

import asyncio
import inspect
import io
import json
import os
import threading
import zipfile
from dataclasses import dataclass, field
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Self

import pyqwest
from opentelemetry import context as otel_context
from opentelemetry.trace import Status, StatusCode, set_span_in_context
from protobuf import Oneof

from lutra._bindings import extract_result, sha256_digest
from lutra._gen.lutra.v1.artifact_connect import ArtifactServiceClient
from lutra._gen.lutra.v1.artifact_pb import (
    AllocateChunkRequest,
    CompleteChunkRequest,
    CreateArtifactRequest,
    DownloadArtifactRequest,
    SealArtifactRequest,
)
from lutra._gen.lutra.v1.project_connect import ProjectServiceClient
from lutra._gen.lutra.v1.project_pb import ListProjectsRequest
from lutra._gen.lutra.v1.run_connect import RunServiceClient
from lutra._gen.lutra.v1.run_pb import (
    CancelRunRequest,
    CreateRunRequest,
    GetRunRequest,
    GetRunResponse,
    InputBinding,
    Run,
    RunState,
)
from lutra._gen.lutra.v1.task_connect import TaskServiceClient
from lutra._gen.lutra.v1.task_pb import CreateTaskRequest
from lutra._telemetry import configure
from lutra.serializer import serialize

if TYPE_CHECKING:
    from types import ModuleType

    from opentelemetry.context import Context
    from opentelemetry.trace import Span

    from lutra.task import Task


@dataclass
class RunHandle:
    client: LutraClient
    project_id: str
    run_id: str
    _launch_span: Span | None = field(default=None, repr=False, compare=False)
    _launch_context: Context | None = field(default=None, repr=False, compare=False)
    _result_requested: bool = field(default=False, repr=False, compare=False)

    def _attach_launch_context(self) -> object | None:
        if self._launch_context is None:
            return None
        return otel_context.attach(self._launch_context)

    @staticmethod
    def _detach_launch_context(token: object | None) -> None:
        if token is not None:
            otel_context.detach(token)  # type: ignore[arg-type]

    def _finish_launch(self, error: BaseException | None = None) -> None:
        span = self._launch_span
        if span is None:
            return
        if error is not None:
            span.record_exception(error)
            span.set_status(Status(StatusCode.ERROR, str(error)))
        span.end()
        self._launch_span = None
        self._launch_context = None

    async def get(self) -> GetRunResponse:
        token = self._attach_launch_context()
        try:
            return await self.client.runs.get_run(
                GetRunRequest(project_id=self.project_id, run_id=self.run_id),
                headers=self.client.headers,
            )
        finally:
            self._detach_launch_context(token)

    async def wait_async(self, *, poll_interval: float = 0.25) -> Run:
        launch_error: RuntimeError | None = None
        completed: Run | None = None
        with self.client.telemetry.tracer.start_as_current_span(
            "lutra.run.poll",
            context=self._launch_context,
            attributes={
                "lutra.project_id": self.project_id,
                "lutra.run_id": self.run_id,
                "lutra.poll_interval": poll_interval,
            },
        ) as poll_span:
            while True:
                # Call the RPC directly so it remains a child of this span.
                response = await self.client.runs.get_run(
                    GetRunRequest(project_id=self.project_id, run_id=self.run_id),
                    headers=self.client.headers,
                )
                if response.run is None:
                    message = "run service returned no run"
                    raise RuntimeError(message)
                state = response.run.state
                if state in {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED}:
                    completed = response.run
                    poll_span.set_attribute("lutra.state", state.name)
                    if state != RunState.SUCCEEDED:
                        launch_error = RuntimeError(f"run finished with state {state}")
                        poll_span.set_status(Status(StatusCode.ERROR, str(launch_error)))
                    break
                await asyncio.sleep(poll_interval)
        if not self._result_requested:
            self._finish_launch(launch_error)
        assert completed is not None
        return completed

    def wait(self, *, poll_interval: float = 0.25) -> Run:
        return asyncio.run(self.wait_async(poll_interval=poll_interval))

    async def result_async(self, *, poll_interval: float = 0.25) -> object:
        self._result_requested = True
        token = self._attach_launch_context()
        try:
            return await self._result_after_wait(poll_interval=poll_interval)
        except BaseException as exc:
            self._finish_launch(exc)
            raise
        finally:
            self._detach_launch_context(token)
            self._finish_launch()

    async def _result_after_wait(self, *, poll_interval: float) -> object:
        run = await self.wait_async(poll_interval=poll_interval)
        if run.state != RunState.SUCCEEDED:
            message = f"run finished with state {run.state}"
            raise RuntimeError(message)
        action = run.root_action
        if action is None:
            return None
        return await extract_result(action.outputs, self._download_artifact)

    async def _download_artifact(self, artifact_id: str) -> bytes:
        with self.client.telemetry.tracer.start_as_current_span(
            "lutra.artifact.download",
            attributes={"lutra.project_id": self.project_id, "lutra.artifact_id": artifact_id},
        ) as span:
            response = await self.client.artifacts.download_artifact(
                DownloadArtifactRequest(project_id=self.project_id, artifact_id=artifact_id),
                headers=self.client.headers,
            )
            data = bytearray()
            for chunk in response.chunks:
                downloaded = await self.client.http.get(chunk.download_url)
                _require_success(downloaded.status, "artifact download")
                data.extend(downloaded.content)
            span.set_attribute("lutra.size_bytes", len(data))
            span.set_attribute("lutra.chunk_count", len(response.chunks))
            return bytes(data)

    def result(self, *, poll_interval: float = 0.25) -> object:
        return asyncio.run(self.result_async(poll_interval=poll_interval))

    async def cancel_async(self) -> Run:
        token = self._attach_launch_context()
        try:
            response = await self.client.runs.cancel_run(
                CancelRunRequest(project_id=self.project_id, run_id=self.run_id),
                headers=self.client.headers,
            )
            return _require_run(response)
        except BaseException as exc:
            self._finish_launch(exc)
            raise
        finally:
            self._detach_launch_context(token)
            self._finish_launch()

    def cancel(self) -> Run:
        return asyncio.run(self.cancel_async())


class LutraClient:
    def __init__(self, endpoint: str, api_key: str | None = None) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.telemetry = configure("lutra-sdk")
        self.interceptors = [self.telemetry.interceptor]
        self._http_transport = pyqwest.HTTPTransport(enable_otel=True)
        self.http = pyqwest.Client(transport=self._http_transport)
        # ConnectRPC already creates and propagates a client span. Keep its
        # transport uninstrumented so a second pyqwest ``POST`` span cannot
        # replace the ConnectRPC trace context on the wire.
        self._rpc_transport = pyqwest.HTTPTransport(enable_otel=False)
        self.rpc_http = pyqwest.Client(transport=self._rpc_transport)
        self._close_lock = threading.Lock()
        self._closed = False
        self.artifacts = ArtifactServiceClient(
            self.endpoint,
            timeout_ms=30_000,
            http_client=self.rpc_http,
            interceptors=self.interceptors,
        )
        self.projects = ProjectServiceClient(
            self.endpoint,
            timeout_ms=30_000,
            http_client=self.rpc_http,
            interceptors=self.interceptors,
        )
        self.tasks = TaskServiceClient(
            self.endpoint,
            timeout_ms=30_000,
            http_client=self.rpc_http,
            interceptors=self.interceptors,
        )
        self.runs = RunServiceClient(
            self.endpoint,
            timeout_ms=30_000,
            http_client=self.rpc_http,
            interceptors=self.interceptors,
        )

    async def close(self) -> None:
        """Close HTTP transports and flush telemetry.

        Cleanup is safe to call more than once. The transports are closed even
        when one of them reports an error, and telemetry is always flushed.
        """
        with self._close_lock:
            if self._closed:
                return
            self._closed = True

        close_error: Exception | None = None
        try:
            for transport in (self._http_transport, self._rpc_transport):
                try:
                    await transport.aclose()
                except Exception as exc:  # ruff: ignore[blind-except] - cleanup must continue
                    if close_error is None:
                        close_error = exc
        finally:
            self.telemetry.shutdown()
        if close_error is not None:
            raise close_error

    async def aclose(self) -> None:
        """Alias for :meth:`close` for async-client compatibility."""
        await self.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

    async def submit(
        self,
        task: Task,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        *,
        project_id: str,
        idempotency_key: str | None = None,
    ) -> RunHandle:
        span = self.telemetry.tracer.start_span(
            "lutra.launch", attributes={"lutra.task": task.name}
        )
        launch_context = set_span_in_context(span)
        token = otel_context.attach(launch_context)
        try:
            with self.telemetry.tracer.start_as_current_span(
                "lutra.run", attributes={"lutra.task": task.name}
            ) as run_span:
                handle = await self._submit(
                    task,
                    args,
                    kwargs,
                    project_id=project_id,
                    idempotency_key=idempotency_key,
                    span=span,
                )
                run_span.set_attribute("lutra.project_id", handle.project_id)
                run_span.set_attribute("lutra.run_id", handle.run_id)
                return handle
        except BaseException as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            span.end()
            raise
        finally:
            otel_context.detach(token)

    async def _submit(  # ruff: ignore[too-many-arguments]
        self,
        task: Task,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        *,
        project_id: str,
        idempotency_key: str | None,
        span: Span,
    ) -> RunHandle:
        if not project_id:
            projects = await self.projects.list_projects(
                ListProjectsRequest(), headers=self.headers
            )
            if not projects.projects:
                message = "no project is available for this run"
                raise ValueError(message)
            project_id = projects.projects[0].project_id
        span.set_attribute("lutra.project_id", project_id)
        bundle = _bundle(task)
        bundle_artifact_id = await self._upload_artifact(project_id, bundle, "application/zip")
        registrations = await asyncio.gather(
            *(
                self.tasks.create_task(
                    CreateTaskRequest(
                        project_id=project_id,
                        name=bundled_task.name,
                        version=bundled_task.version,
                        entrypoint_argv=[
                            "python",
                            "-m",
                            "lutra._runtime",
                            "--module",
                            bundle_module_for(bundled_task.function),
                            "--function",
                            bundled_task.function.__qualname__,
                        ],
                        code_bundle_artifact_id=bundle_artifact_id,
                    ),
                    headers=self.headers,
                )
                for bundled_task in task.environment.tasks
            )
        )
        registered_by_identity = {}
        for bundled_task, registered in zip(task.environment.tasks, registrations, strict=True):
            assert registered.task is not None
            registered_by_identity[bundled_task.name, bundled_task.version] = registered.task
        registered = registered_by_identity[task.name, task.version]
        slots = [(str(index), value) for index, value in enumerate(args)]
        slots.extend(kwargs.items())
        inputs = await asyncio.gather(
            *(self._make_binding(project_id, slot, value) for slot, value in slots)
        )
        created = await self.runs.create_run(
            CreateRunRequest(
                project_id=project_id,
                task_id=registered.task_id,
                inputs=inputs,
                idempotency_key=idempotency_key or "",
            ),
            headers=self.headers,
        )
        assert created.run is not None
        run = created.run
        span.set_attribute("lutra.run_id", run.run_id)
        # Hand the launch span to the handle: it stays open until the caller
        # waits for, cancels, or reads the result of the run, so polling and
        # artifact spans are recorded as its children.
        return RunHandle(self, project_id, run.run_id, span, set_span_in_context(span))

    async def _upload_artifact(self, project_id: str, data: bytes, mime_type: str) -> str:
        with self.telemetry.tracer.start_as_current_span(
            "lutra.artifact.upload",
            attributes={
                "lutra.project_id": project_id,
                "lutra.mime_type": mime_type,
                "lutra.size_bytes": len(data),
            },
        ) as span:
            digest = sha256_digest(data)
            artifact = await self.artifacts.create_artifact(
                CreateArtifactRequest(project_id=project_id, mime_type=mime_type),
                headers=self.headers,
            )
            if artifact.artifact is None:
                message = "artifact service returned no artifact"
                raise RuntimeError(message)
            artifact_id = artifact.artifact.artifact_id
            span.set_attribute("lutra.artifact_id", artifact_id)
            allocated = await self.artifacts.allocate_chunk(
                AllocateChunkRequest(
                    project_id=project_id,
                    artifact_id=artifact_id,
                    chunk_index=0,
                    size_bytes=len(data),
                    digest=digest,
                ),
                headers=self.headers,
            )
            if allocated.chunk is None:
                message = "artifact service returned no chunk"
                raise RuntimeError(message)
            upload_response = await self.http.put(
                allocated.upload_url, headers=allocated.upload_headers, content=data
            )
            _require_success(upload_response.status, "artifact upload")
            await self.artifacts.complete_chunk(
                CompleteChunkRequest(
                    project_id=project_id,
                    artifact_id=artifact_id,
                    chunk_index=0,
                    upload_id=allocated.chunk.upload_id,
                ),
                headers=self.headers,
            )
            await self.artifacts.seal_artifact(
                SealArtifactRequest(project_id=project_id, artifact_id=artifact_id, chunk_count=1),
                headers=self.headers,
            )
            return artifact_id

    async def _make_binding(self, project_id: str, slot: str, value: object) -> InputBinding:
        encoded = serialize(value)
        max_inline = int(os.environ.get("LUTRA_MAX_INLINE_BYTES", str(1 << 20)))
        if len(encoded) <= max_inline:
            return InputBinding(slot_name=slot, value=Oneof("inline_bytes", encoded))
        artifact_id = await self._upload_artifact(project_id, encoded, "application/octet-stream")
        return InputBinding(slot_name=slot, value=Oneof("artifact_id", artifact_id))


_client: LutraClient | None = None


def init(*, endpoint: str = "http://localhost:8080/api", api_key: str | None = None) -> LutraClient:
    global _client
    _client = LutraClient(endpoint, api_key)
    return _client


async def close() -> None:
    """Close the client created by :func:`init`, if any."""
    global _client
    client = _client
    _client = None
    if client is not None:
        await client.close()


def run(
    task: Task,
    *args: object,
    project_id: str | None = None,
    idempotency_key: str | None = None,
    **kwargs: object,
) -> RunHandle:
    if _client is None:
        message = "call lutra.init(endpoint=..., api_key=...) before run"
        raise RuntimeError(message)
    project = project_id or ""
    return asyncio.run(
        _client.submit(task, args, kwargs, project_id=project, idempotency_key=idempotency_key)
    )


def bundle_module_for(function: object) -> str:
    module = getattr(function, "__module__", "")
    return "lutra_user" if module == "__main__" else module


def _require_success(status: int, action: str) -> None:
    if status < HTTPStatus.OK or status >= HTTPStatus.MULTIPLE_CHOICES:
        message = f"{action} failed with HTTP {status}"
        raise RuntimeError(message)


def _require_run(response: GetRunResponse | object) -> Run:
    run = getattr(response, "run", None)
    if run is None:
        message = "run service returned no run"
        raise RuntimeError(message)
    return run


def _excluded(path: Path) -> bool:
    return any(part in {".git", ".venv", "__pycache__", "node_modules"} for part in path.parts)


def _module_sources(module: ModuleType, source_path: Path) -> tuple[Path, list[Path]]:
    # Fallback: bundle the module's own directory. Test runners and direct
    # script loaders sometimes import a file as a top-level module despite its
    # package directory, so the package walk below may not apply.
    archive_root = source_path.parent
    source_files = sorted(archive_root.rglob("*.py"))
    package_tree = source_path.parent
    if package_tree.joinpath("__init__.py").is_file():
        while package_tree.parent.joinpath("__init__.py").is_file():
            package_tree = package_tree.parent
        candidate_root = package_tree.parent
        candidate_path = candidate_root.joinpath(*module.__name__.split("."))
        if (
            candidate_path.with_suffix(".py").is_file()
            or candidate_path.joinpath("__init__.py").is_file()
        ):
            archive_root = candidate_root
            source_files = sorted(package_tree.rglob("*.py"))
    return archive_root, source_files


def _add_module(sources: dict[str, Path], function: object) -> None:
    module = inspect.getmodule(function)
    if module is None or not getattr(module, "__file__", None):
        message = "task source module is not available for bundling"
        raise RuntimeError(message)
    source_path = Path(module.__file__).resolve()
    if not source_path.is_file():
        message = f"task source file does not exist: {source_path}"
        raise RuntimeError(message)
    archive_root, source_files = _module_sources(module, source_path)
    for source_file in source_files:
        if _excluded(source_file):
            continue
        archive_name = source_file.relative_to(archive_root).as_posix()
        if module.__name__ == "__main__" and source_file == source_path:
            archive_name = "lutra_user.py"
        existing = sources.get(archive_name)
        if existing is not None and existing != source_file:
            message = f"task modules contain conflicting bundle path {archive_name!r}"
            raise RuntimeError(message)
        sources[archive_name] = source_file


def _bundle(root: Task) -> bytes:
    sources: dict[str, Path] = {}
    for bundled_task in root.environment.tasks:
        _add_module(sources, bundled_task.function)

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for archive_name, source_file in sorted(sources.items()):
            info = zipfile.ZipInfo(archive_name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, source_file.read_bytes())
        manifest = {
            "tasks": [
                {"name": task.name, "version": task.version, "entrypoint": task.entrypoint}
                for task in root.environment.tasks
            ]
        }
        info = zipfile.ZipInfo("lutra-manifest.json", date_time=(1980, 1, 1, 0, 0, 0))
        info.compress_type = zipfile.ZIP_DEFLATED
        archive.writestr(info, json.dumps(manifest, sort_keys=True, separators=(",", ":")))
    return buffer.getvalue()
