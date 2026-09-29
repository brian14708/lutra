"""Tests for the hello task's upload failure path."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import httpx
import pytest
from lutra.task_host import _upload  # ruff: ignore[import-private-name]

if TYPE_CHECKING:
    from lutra._gen.lutra.v1.blob_connect import BlobServiceClient


class FailedBlobClient:
    def __init__(self) -> None:
        self.aborted = False
        self.presigned = False

    async def presign_part(self, _request: object) -> SimpleNamespace:
        self.presigned = True
        return SimpleNamespace(url="http://example.test/put", headers={})

    async def abort_upload(self, _request: object) -> None:
        self.aborted = True


@pytest.mark.asyncio
async def test_upload_failure_aborts_session(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fail_put(_url: str, _headers: dict[str, str], _contents: bytes) -> None:
        await asyncio.sleep(0)
        message = "upload failed"
        raise httpx.ConnectError(message)

    monkeypatch.setattr("lutra.task_host._put", fail_put)
    client = FailedBlobClient()
    with pytest.raises(httpx.ConnectError, match="upload failed"):
        await _upload(cast("BlobServiceClient", client), "session", b"hello", 5, 1)
    assert client.presigned
    assert client.aborted
