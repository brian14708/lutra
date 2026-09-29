"""Task host execution behavior."""

import asyncio
import threading
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock

import cbor2
import pytest
from lutra import task_host
from lutra._gen.lutra.task.v1.task_pb import ExecuteRequest, ExecuteResponse  # ruff: ignore[import-private-name]
from lutra.task_host import ReverseClient, _TaskService, normalize_result  # ruff: ignore[import-private-name]
from lutra.value import BlobRef

if TYPE_CHECKING:
    from connectrpc.request import RequestContext


@pytest.mark.asyncio
async def test_synchronous_handler_does_not_block_other_calls() -> None:
    started = threading.Event()
    release = threading.Event()

    def handler(
        invocation_id: str, content_type: str, payload: bytes, _reverse: ReverseClient
    ) -> tuple[str, bytes]:
        if invocation_id == "slow":
            started.set()
            if not release.wait(2):
                message = "second call could not run"
                raise TimeoutError(message)
        elif invocation_id == "release":
            release.set()
        return content_type, payload

    service = _TaskService(handler)
    service.reverse = cast("ReverseClient", object())
    ctx = cast("RequestContext[ExecuteRequest, ExecuteResponse]", None)
    slow = asyncio.create_task(service.execute(ExecuteRequest(invocation_id="slow"), ctx))
    assert await asyncio.to_thread(started.wait, 1)
    response = await asyncio.wait_for(
        service.execute(ExecuteRequest(invocation_id="release", input=b"ok"), ctx), timeout=1
    )
    assert response.output == b"ok"
    await asyncio.wait_for(slow, timeout=1)


@pytest.mark.asyncio
async def test_result_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    store = AsyncMock(
            return_value=BlobRef(
                "blob:application/octet-stream,"
                "LPJNul+wow4m6DsqxbninhsWHlwFP1hQGv5x6cXg8YI=",
                resolve=True,
            )
    )
    monkeypatch.setattr(task_host, "_store_result", store)
    reverse = cast("ReverseClient", object())
    mime, embedded = await normalize_result(b"x" * 65536, reverse)
    assert mime == "application/cbor"
    assert cbor2.loads(embedded) == b"x" * 65536
    store.assert_not_awaited()

    mime, uploaded = await normalize_result(b"x" * 65537, reverse)
    assert mime == "application/cbor"
    assert b";resolve," in uploaded
    store.assert_awaited_once_with(reverse, b"x" * 65537, "application/octet-stream")


@pytest.mark.asyncio
async def test_multipart_abort_on_missing_etag(monkeypatch: pytest.MonkeyPatch) -> None:
    client = AsyncMock()
    client.presign_part.return_value.url = "https://example.test/upload"
    headers: dict[str, str] = {}
    client.presign_part.return_value.headers = headers
    monkeypatch.setattr(task_host, "_put", AsyncMock(return_value=""))
    with pytest.raises(ValueError, match="ETag"):
        await task_host._upload(client, "session", b"abcdef", 3, 2)  # ruff: ignore[private-member-access]
    client.abort_upload.assert_awaited_once()
    client.complete_upload.assert_not_awaited()
