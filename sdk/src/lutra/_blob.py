"""Upload content-addressed bytes through the blob API."""

from __future__ import annotations

import base64
import hashlib
from contextlib import suppress
from typing import TYPE_CHECKING

import httpx

from lutra._gen.lutra.v1.blob_pb import (
    AbortUploadRequest,
    BlobPart,
    CompleteUploadRequest,
    CreateUploadRequest,
    PresignPartRequest,
)

if TYPE_CHECKING:
    from lutra._gen.lutra.v1.blob_connect import BlobServiceClient


async def _put(url: str, headers: dict[str, str], contents: bytes) -> str:
    async with httpx.AsyncClient() as http:
        response = await http.put(url, content=contents, headers=headers)
        response.raise_for_status()
        return response.headers.get("etag", "")


async def _upload(
    client: BlobServiceClient, session_id: str, contents: bytes, part_size: int, part_count: int
) -> str:
    async def send_parts() -> list[BlobPart]:
        parts = []
        for number in range(1, part_count + 1):
            part = await client.presign_part(
                PresignPartRequest(session_id=session_id, part_number=number)
            )
            chunk = contents[(number - 1) * part_size : number * part_size]
            etag = await _put(part.url, part.headers, chunk)
            if part_count > 1:
                if not etag:
                    msg = "multipart upload returned no ETag"
                    raise ValueError(msg)
                parts.append(BlobPart(number=number, etag=etag))
        return parts

    try:
        parts = await send_parts()
        completed = await client.complete_upload(
            CompleteUploadRequest(session_id=session_id, parts=parts)
        )
    except BaseException:
        with suppress(Exception):
            await client.abort_upload(AbortUploadRequest(session_id=session_id))
        raise
    else:
        return completed.uri


async def upload_blob(client: BlobServiceClient, contents: bytes, mime_type: str) -> str:
    digest = hashlib.sha256(contents).digest()
    uri = f"blob:{mime_type},{base64.b64encode(digest).decode('ascii')}"
    created = await client.create_upload(
        CreateUploadRequest(content_sha256=digest, size=len(contents), mime_type=mime_type)
    )
    if created.already_exists:
        return uri
    returned_uri = await _upload(
        client, created.session_id, contents, created.part_size, created.part_count
    )
    if returned_uri != uri:
        msg = "blob service returned a different URI"
        raise ValueError(msg)
    return uri
