"""Upload content-addressed bytes through the blob API."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

from lutra._gen.lutra.v1.blob_pb import (
    AbortUploadRequest,
    BlobPart,
    CompleteUploadRequest,
    CreateUploadRequest,
    PresignPartRequest,
)
from lutra.archive import create_archive, zstd_chunked_manifest_metadata

if TYPE_CHECKING:
    from typing import BinaryIO

    from lutra._gen.lutra.v1.blob_connect import BlobServiceClient


async def _put(url: str, headers: dict[str, str], contents: bytes) -> str:
    async with httpx.AsyncClient() as http:
        response = await http.put(url, content=contents, headers=headers)
        response.raise_for_status()
        return response.headers.get("etag", "")


async def _upload(
    client: BlobServiceClient, session_id: str, contents: BinaryIO, part_size: int, part_count: int
) -> str:
    async def send_parts() -> list[BlobPart]:
        parts = []
        for number in range(1, part_count + 1):
            part = await client.presign_part(
                PresignPartRequest(session_id=session_id, part_number=number)
            )
            if not 0 < part_size <= 8 << 20:
                msg = "invalid upload part size"
                raise ValueError(msg)
            chunk = await asyncio.to_thread(contents.read, part_size)
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


async def upload_stream(  # ruff: ignore[too-many-arguments,too-many-positional-arguments]
    client: BlobServiceClient,
    contents: BinaryIO,
    digest: bytes,
    size: int,
    mime_type: str,
    metadata: dict[str, str] | None,
) -> str:
    """Create an upload session for pre-hashed content and transfer it.

    Returns:
        The content-addressed blob URI.

    Raises:
        ValueError: If the service returns a different content reference.

    """
    uri = f"blob:{mime_type},{base64.b64encode(digest).decode('ascii')}"
    created = await client.create_upload(
        CreateUploadRequest(
            content_sha256=digest, size=size, mime_type=mime_type, metadata=metadata or {}
        )
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


async def upload_blob(
    client: BlobServiceClient,
    contents: bytes,
    mime_type: str,
    *,
    metadata: dict[str, str] | None = None,
) -> str:
    digest = hashlib.sha256(contents).digest()
    return await upload_stream(
        client, io.BytesIO(contents), digest, len(contents), mime_type, metadata
    )


async def upload_directory(client: BlobServiceClient, directory: Path, *, prefix: str = "") -> str:
    """Create and upload a deterministic zstd archive for a directory.

    Returns:
        The content-addressed blob URI.

    Raises:
        ValueError: If directory is not a real directory.

    """
    directory = Path(directory)
    if not directory.is_dir() or directory.is_symlink():
        message = "upload directory must be a real directory"
        raise ValueError(message)
    with tempfile.TemporaryDirectory() as temporary:
        archive = Path(temporary) / "archive.tar.zst"
        create_archive(directory, archive, prefix=prefix)
        from lutra.blob import BlobStore  # ruff: ignore[import-outside-top-level]

        ref = await BlobStore(client).upload_file(
            archive, "application/x-tar+zstd", metadata=zstd_chunked_manifest_metadata(archive)
        )
        return ref.uri
