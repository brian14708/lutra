"""Direct file transfers using Lutra's object-scoped presigned URLs."""

from __future__ import annotations

import asyncio
import hashlib
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

from lutra._blob import upload_blob, upload_stream
from lutra._gen.lutra.v1.blob_pb import GetDownloadRequest
from lutra.value import BlobRef, _parse_blob_name

if TYPE_CHECKING:
    from collections.abc import Mapping

    from lutra._gen.lutra.v1.blob_connect import BlobServiceClient

_CHUNK_SIZE = 1 << 20


def _stored_ref(ref: BlobRef | str) -> BlobRef:
    result = ref if isinstance(ref, BlobRef) else BlobRef(ref)
    if not result.uri.startswith("blob:"):
        message = "expected a stored blob: URI"
        raise ValueError(message)
    return result


class BlobStore:
    """Transfer file bytes directly between this process and the object store."""

    def __init__(self, client: BlobServiceClient) -> None:
        """Use the RPC channel for authorization and presigning."""
        self._client = client

    async def upload_bytes(
        self, contents: bytes, mime_type: str, *, metadata: Mapping[str, str] | None = None
    ) -> BlobRef:
        """Hash and upload in-memory bytes, returning a durable reference.

        Returns:
            An unresolved, content-addressed reference.

        """
        uri = await upload_blob(self._client, contents, mime_type, metadata=dict(metadata or {}))
        return BlobRef(uri)

    async def upload_file(
        self, path: Path, mime_type: str, *, metadata: Mapping[str, str] | None = None
    ) -> BlobRef:
        """Hash and upload a file in bounded chunks, returning a durable reference.

        The file must remain unchanged until the upload completes. Existing
        content skips the transfer.

        Returns:
            An unresolved, content-addressed reference.

        """
        with await asyncio.to_thread(Path(path).open, "rb") as source:
            digest = hashlib.sha256()
            size = 0
            while chunk := await asyncio.to_thread(source.read, _CHUNK_SIZE):
                digest.update(chunk)
                size += len(chunk)
            checksum = digest.digest()
            await asyncio.to_thread(source.seek, 0)
            uri = await upload_stream(
                self._client, source, checksum, size, mime_type, dict(metadata or {})
            )
        return BlobRef(uri)

    async def presigned_get(self, ref: BlobRef | str) -> str:
        """Return an object-scoped GET URL valid for 24 hours.

        Production stores use HTTPS; local stores may use HTTP.

        Returns:
            The temporary URL. Keep the BlobRef for persistence and cache keys.

        """
        response = await self._client.get_download(GetDownloadRequest(uri=_stored_ref(ref).uri))
        return response.url

    async def download_bytes(self, ref: BlobRef | str) -> bytes:
        """Download and verify blob contents.

        Returns:
            The blob contents.

        Raises:
            ValueError: If the downloaded content has a different digest.

        """
        stored = _stored_ref(ref)
        expected = _parse_blob_name(stored.uri[5:])
        url = await self.presigned_get(stored)
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        async with httpx.AsyncClient() as http, http.stream("GET", url) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes(_CHUNK_SIZE):
                chunks.append(chunk)
                digest.update(chunk)
        if digest.digest() != expected:
            message = "blob content digest mismatch"
            raise ValueError(message)
        return b"".join(chunks)

    async def download_file(self, ref: BlobRef | str, path: Path) -> None:
        """Stream and verify a file, then atomically replace the destination.

        Raises:
            ValueError: If the downloaded content has a different digest.

        """
        stored = _stored_ref(ref)
        expected = _parse_blob_name(stored.uri[5:])
        url = await self.presigned_get(stored)
        path = Path(path)
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as destination:
            temporary = Path(destination.name)
            try:
                digest = hashlib.sha256()
                async with httpx.AsyncClient() as http, http.stream("GET", url) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_bytes(_CHUNK_SIZE):
                        destination.write(chunk)
                        digest.update(chunk)
                if digest.digest() != expected:
                    message = "blob content digest mismatch"
                    raise ValueError(message)
                destination.close()
                await asyncio.to_thread(temporary.replace, path)
            finally:
                temporary.unlink(missing_ok=True)  # ruff: ignore[blocking-path-method-in-async-function]
