"""Serve task calls over multiplexed newline-delimited JSON on stdio."""

from __future__ import annotations

import asyncio
import inspect
import json
import struct
import sys
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, Any

import httpx
import pyqwest
from connectrpc.code import Code
from connectrpc.codec import proto_json_codec
from connectrpc.errors import ConnectError

from lutra._blob import upload_blob
from lutra._gen.lutra.task.v1.task_connect import TaskService, TaskServiceASGIApplication
from lutra._gen.lutra.task.v1.task_pb import ExecuteRequest, ExecuteResponse
from lutra._gen.lutra.v1.blob_connect import BlobServiceClient
from lutra._gen.lutra.v1.blob_pb import GetDownloadRequest
from lutra.value import BlobRef, _parse_blob_name, dumps

if TYPE_CHECKING:
    from collections.abc import Callable

    from connectrpc.request import RequestContext
    from protobuf import DescService

_MAX_LINE = 32 * 1024 * 1024
_ENVELOPE_PREFIX = 5
_END_STREAM_FLAG = 2
_HTTP_OK = 200
_PROTOCOL_OUTPUT = sys.stdout.buffer
_RESULT_THRESHOLD = 64 * 1024
_RESULT_MIME = "application/cbor"


class _StderrProxy:
    def __init__(self) -> None:
        self._stream = sys.stderr
        self.buffer = sys.stderr.buffer

    def write(self, value: str) -> int:
        return self._stream.write(value)

    def flush(self) -> None:
        self._stream.flush()


def _redirect_user_stdout() -> None:
    """Keep user output away from the stdio protocol stream."""
    if not isinstance(sys.stdout, _StderrProxy):
        sys.stdout = _StderrProxy()  # type: ignore[assignment]


def _envelope(value: object) -> bytes:
    body = json.dumps(value, separators=(",", ":")).encode()
    return struct.pack(">BI", 0, len(body)) + body


class _TaskService:
    def __init__(self, handler: Callable[..., Any]) -> None:
        self._handler = handler
        self.api_client: TaskAPIClient | None = None

    @classmethod
    def desc(cls) -> DescService:
        return TaskService.desc()

    async def execute(
        self, request: ExecuteRequest, _ctx: RequestContext[ExecuteRequest, ExecuteResponse]
    ) -> ExecuteResponse:
        if not request.invocation_id:
            raise ConnectError(Code.INVALID_ARGUMENT, "invocation_id is required")
        if self.api_client is None:
            raise ConnectError(Code.INTERNAL, "task API client is unavailable")
        run_token, action_token = self.api_client.set_execution_context(
            request.run_id, request.action_id
        )
        args = (request.invocation_id, request.content_type, request.input, self.api_client)
        try:
            if inspect.iscoroutinefunction(self._handler):
                result = self._handler(*args)
            else:
                result = await asyncio.to_thread(self._handler, *args)
            if inspect.isawaitable(result):
                result = await result
            content_type, output = await normalize_result(result, self.api_client)
            return ExecuteResponse(content_type=content_type, output=output)
        finally:
            self.api_client.reset_execution_context(run_token, action_token)


async def _store_result(api_client: TaskAPIClient, contents: bytes, mime_type: str) -> BlobRef:
    uri = await upload_blob(api_client.blob, contents, mime_type)
    return BlobRef(uri, resolve=True, mime_type=mime_type)


async def normalize_result(result: object, api_client: TaskAPIClient) -> tuple[str, bytes]:  # ruff: ignore[too-many-return-statements]
    """Encode a task result and spill large values to the blob service.

    Returns:
        The result MIME type and CBOR bytes.
    """
    if (
        isinstance(result, tuple)
        and len(result) == 2  # ruff: ignore[magic-value-comparison]
        and isinstance(result[0], str)
        and isinstance(result[1], bytes)
    ):
        content_type, raw = result
        if content_type != "application/cbor":
            return content_type, raw
        if len(raw) > _RESULT_THRESHOLD:
            return _RESULT_MIME, dumps(await _store_result(api_client, raw, _RESULT_MIME))
        return _RESULT_MIME, raw
    content_type, value = _RESULT_MIME, result
    if isinstance(value, bytes):
        if len(value) > _RESULT_THRESHOLD:
            stored = await _store_result(api_client, value, "application/octet-stream")
            return _RESULT_MIME, dumps(stored)
        return content_type, dumps(value)
    encoded = dumps(value)
    if len(encoded) > _RESULT_THRESHOLD:
        return _RESULT_MIME, dumps(await _store_result(api_client, encoded, _RESULT_MIME))
    return content_type, encoded


class TaskAPIClient:
    """Make unary Connect JSON calls to the Go process on the task stdio link."""

    def __init__(self, host: _Host) -> None:
        self._host = host
        self._run_id: ContextVar[str] = ContextVar("lutra_run_id", default="")
        self._action_id: ContextVar[str] = ContextVar("lutra_action_id", default="")
        self.blob = BlobServiceClient(
            "http://stdio",
            codec=proto_json_codec(),
            send_compression=None,
            accept_compression=(),
            http_client=pyqwest.Client(transport=StdioTransport(self)),
        )

    @property
    def run_id(self) -> str:
        return self._run_id.get()

    @property
    def action_id(self) -> str:
        return self._action_id.get()

    def set_execution_context(self, run_id: str, action_id: str) -> tuple[Token[str], Token[str]]:
        return self._run_id.set(run_id), self._action_id.set(action_id)

    def reset_execution_context(self, run_token: Token[str], action_token: Token[str]) -> None:
        self._run_id.reset(run_token)
        self._action_id.reset(action_token)

    async def unary(self, path: str, value: dict[str, Any]) -> dict[str, Any]:
        self._host.next_reverse += 1
        call_id = f"p{self._host.next_reverse}"
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._host.reverse_calls[call_id] = queue
        try:
            await self._host.write({
                "id": call_id,
                "type": "request",
                "path": path,
                "headers": {"Content-Type": ["application/json"]},
                "value": value,
            })
            return await self._read_unary(queue)
        except asyncio.CancelledError:
            await self._host.write({"id": call_id, "type": "cancel"})
            raise
        finally:
            self._host.reverse_calls.pop(call_id, None)

    @staticmethod
    async def _read_unary(queue: asyncio.Queue[dict[str, Any]]) -> dict[str, Any]:
        response: dict[str, Any] | None = None
        failure: dict[str, Any] | None = None
        while True:
            frame = await queue.get()
            if frame["type"] == "response":
                if frame.get("error") is not None:
                    failure = frame["error"]
                else:
                    response = frame.get("value")
                if failure is not None:
                    code = Code(failure.get("code", "unknown"))
                    raise ConnectError(code, failure.get("message", "task API call failed"))
                if response is None:
                    raise ConnectError(Code.INTERNAL, "task API call has no response")
                return response

    async def resolve_blob(self, uri: str) -> bytes:
        _parse_blob_name(uri.removeprefix("blob:"))
        response = await self.blob.get_download(GetDownloadRequest(uri=uri))
        async with httpx.AsyncClient() as http:
            downloaded = await http.get(response.url)
            downloaded.raise_for_status()
            return downloaded.content


class StdioTransport:
    """A pyqwest transport that routes task API calls over stdio."""

    def __init__(self, api_client: TaskAPIClient) -> None:
        self._api_client = api_client

    async def execute(self, request: pyqwest.Request) -> pyqwest.Response:
        if request.method != "POST":
            return pyqwest.Response(status=405, headers=pyqwest.Headers(), content=b"")
        if request.headers.get("content-type") != "application/json":
            message = "stdio transport supports unary Connect JSON only"
            raise ValueError(message)
        content = request.content
        if not isinstance(content, bytes):
            chunks = [chunk async for chunk in content]
            content = b"".join(chunks)
        path = "/" + request.url.split("/", 3)[-1]
        response = await self._api_client.unary(path, json.loads(content))
        return pyqwest.Response(
            status=200,
            headers=pyqwest.Headers({"content-type": "application/json"}),
            content=json.dumps(response, separators=(",", ":")).encode(),
        )


class _Call:
    def __init__(self, call_id: str, path: str, headers: dict[str, list[str]], host: _Host) -> None:
        self.id = call_id
        self.path = path
        self.headers = headers
        self.host = host
        self.input: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.task: asyncio.Task[None] | None = None
        self.streaming = False
        self.buffer = bytearray()
        self.response_started = False
        self.ended = False
        self.status = _HTTP_OK

    async def receive(self) -> dict[str, Any]:
        return await self.input.get()

    async def send(self, event: dict[str, Any]) -> None:  # ruff: ignore[complex-structure]
        if event["type"] == "http.response.start":
            self.status = event["status"]
            headers: dict[str, list[str]] = {}
            for key, value in event.get("headers", []):
                headers.setdefault(key.decode().lower(), []).append(value.decode())
            self.streaming = headers.get("content-type", [""])[0].startswith("application/connect+")
            self.response_started = True
            await self.host.write({
                "id": self.id,
                "type": "headers",
                "status": event["status"],
                "headers": headers,
            })
        elif event["type"] == "http.response.body":
            self.buffer.extend(event.get("body", b""))
            if self.streaming:
                while len(self.buffer) >= _ENVELOPE_PREFIX:
                    flags, length = struct.unpack(">BI", self.buffer[:_ENVELOPE_PREFIX])
                    if len(self.buffer) < _ENVELOPE_PREFIX + length:
                        break
                    payload = json.loads(self.buffer[_ENVELOPE_PREFIX : _ENVELOPE_PREFIX + length])
                    del self.buffer[: _ENVELOPE_PREFIX + length]
                    if flags == _END_STREAM_FLAG:
                        await self.host.write({
                            "id": self.id,
                            "type": "end",
                            "trailers": payload.get("metadata", {}),
                            "error": payload.get("error"),
                        })
                        self.ended = True
                    elif flags == 0:
                        await self.host.write({"id": self.id, "type": "message", "value": payload})
                    else:
                        message = "unsupported Connect envelope flags"
                        raise ValueError(message)
            if not event.get("more_body") and not self.ended:
                if self.buffer:
                    if self.streaming:
                        message = "incomplete Connect response envelope"
                        raise ValueError(message)
                    payload = json.loads(self.buffer)
                    await self.host.write({
                        "id": self.id,
                        "type": "error" if self.status != _HTTP_OK else "message",
                        "value": payload,
                    })
                await self.host.write({"id": self.id, "type": "end"})
                self.ended = True

    async def run(self, app: TaskServiceASGIApplication) -> None:
        headers = [
            (key.encode(), value.encode())
            for key, values in self.headers.items()
            for value in values
        ]
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": self.path,
            "raw_path": self.path.encode(),
            "query_string": b"",
            "root_path": "",
            "headers": headers,
        }
        try:
            await app(scope, self.receive, self.send)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # ruff: ignore[blind-except]
            if not self.ended:
                if not self.response_started:
                    await self.host.write({
                        "id": self.id,
                        "type": "headers",
                        "status": 500,
                        "headers": {"content-type": ["application/json"]},
                    })
                    await self.host.write({
                        "id": self.id,
                        "type": "error",
                        "value": {"code": "internal", "message": str(exc)},
                    })
                await self.host.write({
                    "id": self.id,
                    "type": "end",
                    "error": {"code": "internal", "message": str(exc)},
                })
        finally:
            self.host.calls.pop(self.id, None)


class _Host:
    def __init__(self, app: TaskServiceASGIApplication) -> None:
        self.app = app
        self.calls: dict[str, _Call] = {}
        self.reverse_calls: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self.next_reverse = 0
        self.lock = asyncio.Lock()

    async def write(self, frame: dict[str, Any]) -> None:
        data = json.dumps(frame, separators=(",", ":")).encode() + b"\n"
        if len(data) >= _MAX_LINE:
            message = "stdio frame exceeds limit"
            raise ValueError(message)
        async with self.lock:
            await asyncio.to_thread(self._write, data)

    @staticmethod
    def _write(data: bytes) -> None:
        _PROTOCOL_OUTPUT.write(data)
        _PROTOCOL_OUTPUT.flush()

    async def dispatch(self, frame: dict[str, Any]) -> None:  # ruff: ignore[complex-structure]
        call_id = frame.get("id")
        kind = frame.get("type")
        if not isinstance(call_id, str) or not call_id:
            message = "frame requires a nonempty id"
            raise ValueError(message)
        queue = self.reverse_calls.get(call_id)
        if queue is not None:
            await queue.put(frame)
            return
        if kind == "open":
            if call_id in self.calls:
                message = "duplicate call id"
                raise ValueError(message)
            path = frame.get("path")
            headers = frame.get("headers", {})
            if not isinstance(path, str) or not isinstance(headers, dict):
                message = "invalid open frame"
                raise ValueError(message)
            headers = {key.lower(): values for key, values in headers.items()}
            call = _Call(call_id, path, headers, self)
            self.calls[call_id] = call
            call.task = asyncio.create_task(call.run(self.app))
        else:
            call = self.calls.get(call_id)
            if call is None:
                return
            if kind == "message":
                body = json.dumps(frame["value"], separators=(",", ":")).encode()
                streaming = call.headers.get("content-type", [""])[0].startswith(
                    "application/connect+"
                )
                await call.input.put({
                    "type": "http.request",
                    "body": _envelope(frame["value"]) if streaming else body,
                    "more_body": True,
                })
            elif kind == "half_close":
                await call.input.put({"type": "http.request", "body": b"", "more_body": False})
            elif kind == "cancel":
                await call.input.put({"type": "http.disconnect"})
                if call.task is not None:
                    call.task.cancel()
            else:
                message = "unknown event type"
                raise ValueError(message)


async def serve(handler: Callable[..., Any]) -> None:
    """Serve one task handler until stdin closes.

    Raises:
        ValueError: An input frame is invalid.
    """
    original_stdout = sys.stdout
    _redirect_user_stdout()
    print("task host started")  # ruff: ignore[print]
    service = _TaskService(handler)
    host = _Host(TaskServiceASGIApplication(service, read_max_bytes=_MAX_LINE, compressions=()))
    service.api_client = TaskAPIClient(host)
    reader = asyncio.StreamReader(limit=_MAX_LINE)
    loop = asyncio.get_running_loop()
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin.buffer)
    try:
        while line := await reader.readline():
            if len(line) > _MAX_LINE:
                message = "stdio frame exceeds limit"
                raise ValueError(message)
            await host.dispatch(json.loads(line))
    finally:
        pending = list(host.calls.values())
        for call in pending:
            if call.task is not None:
                call.task.cancel()
        await asyncio.gather(
            *(call.task for call in pending if call.task is not None), return_exceptions=True
        )
        sys.stdout = original_stdout
