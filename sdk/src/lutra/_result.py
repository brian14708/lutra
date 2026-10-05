from __future__ import annotations

import io
from typing import TYPE_CHECKING

import cbor2

from lutra.task import CacheableError, ConfigError, TerminalError
from lutra.value import ValueCodecError, dumps, loads

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

ERROR_TAG = 30001

# Canonical CBOR header of CBORTag(ERROR_TAG, ...); both encoders are canonical,
# so the success path skips a full decode just to check for the failure tag.
_FAILURE_PREFIX = b"\xd9\x75\x31"


def failure_message(data: bytes) -> str:
    if not data:
        return ""
    fields = _failure(data)
    return str(fields["message"]) if fields is not None else ""


def encode_failure(
    *,
    cacheable: bool,
    message: str,
    details: object = None,
    terminal: bool = False,
    code: int = 500,
) -> bytes:
    fields = {"cacheable": cacheable, "message": message, "details": cbor2.loads(dumps(details))}
    if terminal:
        fields.update(terminal=True, code=code)
    return cbor2.dumps(cbor2.CBORTag(ERROR_TAG, fields), canonical=True)


def _failure(data: bytes) -> dict[str, object] | None:
    stream = io.BytesIO(data)
    value = cbor2.CBORDecoder(stream).decode()
    if stream.tell() != len(data):
        message = "trailing result CBOR data"
        raise ValueCodecError(message)
    if not isinstance(value, cbor2.CBORTag) or value.tag != ERROR_TAG:
        return None
    fields = value.value
    if (
        not isinstance(fields, dict)
        or type(fields.get("cacheable")) is not bool
        or not isinstance(fields.get("message"), str)
        or not fields["message"]
        or "details" not in fields
    ):
        message = "invalid result failure"
        raise ValueCodecError(message)
    if "terminal" in fields and (
        type(fields["terminal"]) is not bool
        or (
            fields["terminal"]
            and (
                type(fields.get("code")) is not int or not 400 <= fields["code"] <= 599  # ruff: ignore[magic-value-comparison] HTTP error code range.
            )
        )
    ):
        message = "invalid terminal result failure"
        raise ValueCodecError(message)
    return fields


async def load_result(
    data: bytes, resolver: Callable[[str], Awaitable[bytes]] | None = None
) -> object:
    if not data.startswith(_FAILURE_PREFIX):
        return await loads(data, resolver)
    fields = _failure(data)
    if fields is None:
        return await loads(data, resolver)
    message = str(fields["message"])
    if fields.get("terminal"):
        code = fields["code"]
        if not isinstance(code, int):
            error = "invalid terminal result code"
            raise ValueCodecError(error)
        raise TerminalError(message, code=code)
    if fields["cacheable"]:
        details = await loads(cbor2.dumps(fields["details"]), resolver)
        raise CacheableError(message, details)
    if message in {"config.invalid", "config.missing"}:
        raise ConfigError(message)
    raise RuntimeError(message)
