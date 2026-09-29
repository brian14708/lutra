"""Canonical CBOR values and Lutra blob references."""

from __future__ import annotations

import asyncio
import base64 as _base64
import binascii
import hashlib
import io
import urllib.parse
from dataclasses import dataclass
from typing import TYPE_CHECKING

import cbor2

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

_DIGEST_LENGTH = 32
_BLOB_TAG = 32


class ValueCodecError(ValueError):
    """Raised when a Lutra value or blob digest name is malformed."""


class UnsupportedCborValueError(TypeError):
    """Raised when a value cannot be represented by the Lutra CBOR profile."""


def _parse_blob_name(name: str) -> bytes:
    """Validate the padded standard Base64 SHA-256 digest in a blob URI.

    Returns:
        The 32-byte digest.

    Raises:
        ValueCodecError: If the digest name is invalid.
    """
    digest_name = name.rsplit(",", 1)[-1]
    try:
        digest = _base64.b64decode(digest_name, validate=True)
    except binascii.Error:
        digest = b""
    if _base64.b64encode(digest).decode("ascii") != digest_name or len(digest) != _DIGEST_LENGTH:
        msg = "invalid blob digest name"
        raise ValueCodecError(msg)
    return digest


@dataclass(frozen=True)
class BlobRef:
    uri: str
    resolve: bool = False
    mime_type: str = "application/octet-stream"

    def __post_init__(self) -> None:
        if self.uri.startswith("blob:"):
            _parse_blob_name(self.uri[5:])
            if self.mime_type == "application/octet-stream" and "," in self.uri:
                mime = self.uri[5:].split(";", 1)[0].split(",", 1)[0]
                if mime:
                    object.__setattr__(self, "mime_type", mime)
        elif not self.uri.startswith("data:"):
            msg = "blob URI must start with blob: or data:"
            raise ValueCodecError(msg)

    @property
    def uri_string(self) -> str:
        attrs = []
        if self.resolve:
            attrs.append("resolve")
        if self.uri.startswith("blob:"):
            scheme, _, payload = self.uri.partition(",")
            if not payload:
                payload = scheme[5:].split(";", 1)[0]
            return (
                "blob:" + self.mime_type + (";" + ";".join(attrs) if attrs else "") + "," + payload
            )
        if self.uri.startswith("data:") and attrs:
            head, sep, payload = self.uri.partition(",")
            return head + ";" + ";".join(attrs) + sep + payload
        return self.uri + (";" + ";".join(attrs) if attrs else "")


def _encode_default(encoder: cbor2.CBOREncoder, value: object) -> None:
    if not isinstance(value, BlobRef):
        msg = f"unsupported CBOR value: {type(value).__name__}"
        raise UnsupportedCborValueError(msg)
    encoder.encode(cbor2.CBORTag(_BLOB_TAG, value.uri_string))


def dumps(value: object) -> bytes:
    """Encode a value using deterministic CBOR.

    Returns:
        The canonical CBOR bytes.
    """
    return cbor2.dumps(value, canonical=True, default=_encode_default)


def value_hash(value: object) -> bytes:
    """Hash a canonical CBOR value.

    Returns:
        The 32-byte SHA-256 digest.
    """
    return hashlib.sha256(dumps(value)).digest()


def _blob_ref(tag: cbor2.CBORTag) -> BlobRef:
    payload = tag.value
    if tag.tag != _BLOB_TAG or not isinstance(payload, str):
        msg = "unsupported CBOR tag"
        raise ValueCodecError(msg)
    if payload.startswith(("data:", "blob:")):
        head, sep, body = payload.partition(",")
        if not sep:
            msg = "invalid data URI"
            raise ValueCodecError(msg)
        pieces = head.split(";")
        attrs = [
            piece
            for piece in pieces[1:]
            if piece == "resolve" or piece.startswith(("resolve=", "mime="))
        ]
        metadata = [piece for piece in pieces[1:] if piece not in attrs]
        base = ";".join([pieces[0], *metadata]) + sep + body
    else:
        base, *attrs = payload.split(";")
    options = {
        item.split("=", 1)[0]: urllib.parse.unquote(item.split("=", 1)[1])
        for item in attrs
        if "=" in item
    }
    if base.startswith("data:"):
        default_mime = base[5:].split(";", 1)[0].split(",", 1)[0] or "text/plain"
    elif base.startswith("blob:") and "," in base:
        default_mime = base[5:].split(";", 1)[0].split(",", 1)[0]
    else:
        default_mime = "application/octet-stream"
    return BlobRef(
        base,
        resolve=("resolve" in attrs or options.get("resolve") == "true"),
        mime_type=options.get("mime", default_mime),
    )


def _decode_tag(_decoder: cbor2.CBORDecoder, tag: cbor2.CBORTag) -> BlobRef:
    return _blob_ref(tag)


async def _resolve(value: object, resolver: Callable[[str], Awaitable[bytes]] | None) -> object:
    if isinstance(value, BlobRef):
        if not value.resolve:
            return value
        if value.uri.startswith("data:"):
            metadata, payload = value.uri[5:].split(",", 1)
            data = (
                _base64.b64decode(payload)
                if metadata.endswith(";base64")
                else urllib.parse.unquote_to_bytes(payload)
            )
        else:
            if resolver is None:
                msg = "blob resolver required"
                raise ValueCodecError(msg)
            data = await resolver(value.uri)
        if value.mime_type == "application/cbor":
            return await loads(data, resolver)
        return data
    if isinstance(value, list):
        return list(await asyncio.gather(*(_resolve(item, resolver) for item in value)))
    if isinstance(value, dict):
        items = await asyncio.gather(*(_resolve(item, resolver) for item in value.values()))
        return dict(zip(value, items, strict=True))
    return value


async def loads(data: bytes, resolver: Callable[[str], Awaitable[bytes]] | None = None) -> object:
    """Decode CBOR, resolving marked blob references recursively.

    Returns:
        The decoded value, with requested blobs resolved.

    Raises:
        ValueCodecError: If the input contains trailing data.
    """
    stream = io.BytesIO(data)
    value = cbor2.CBORDecoder(stream, tag_hook=_decode_tag).decode()
    if stream.tell() != len(data):
        msg = "trailing CBOR data"
        raise ValueCodecError(msg)
    return await _resolve(value, resolver)
