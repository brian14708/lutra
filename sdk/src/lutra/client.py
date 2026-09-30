"""Task deployment and run client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

import httpx
import pyqwest

from lutra._blob import upload_blob
from lutra._bundle import SOURCE_BUNDLE_MIME, build_bundle
from lutra._gen.lutra.v1.blob_connect import BlobServiceClient
from lutra._gen.lutra.v1.blob_pb import GetDownloadRequest
from lutra._gen.lutra.v1.lutra_connect import LutraServiceClient
from lutra._gen.lutra.v1.lutra_pb import (
    CancelRunRequest,
    CreateRunRequest,
    GetRunRequest,
    Run,
    TaskAction,
    TaskSpec,
    WatchRunRequest,
)
from lutra.value import dumps, loads

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from lutra.task import Invocation, Task

R = TypeVar("R")


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

    async def result(self) -> R:
        """Wait for completion and return the decoded task result.

        Returns:
            The decoded task result.

        Raises:
            RuntimeError: If the run fails, is canceled, or ends prematurely.

        """
        async for run in self.watch():
            if run.status == "succeeded":
                return await loads(run.output_cbor, self.client.resolve_blob)  # type: ignore[return-value]
            if run.status in {"failed", "canceled"}:
                raise RuntimeError(run.error or f"run {run.status}")
        message = "run status stream ended before completion"
        raise RuntimeError(message)

    async def cancel(self) -> None:
        """Request cancellation of this run."""
        await self.client.rpc.cancel_run(CancelRunRequest(id=self.id))


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

    async def run(self, invocation: Invocation[R], *, idempotency_key: str = "") -> R:
        """Submit an invocation and wait for its decoded result.

        Returns:
            The decoded task result.

        """
        return await (await self.submit(invocation, idempotency_key=idempotency_key)).result()

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
