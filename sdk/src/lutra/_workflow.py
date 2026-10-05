"""Workflow signal and duration validation at SDK boundaries."""

from datetime import timedelta
from typing import TypeVar

from pydantic import BaseModel, TypeAdapter

from lutra.value import dumps, loads

R = TypeVar("R")
_MAX_SIGNAL_BYTES = 1 << 20
_MAX_NAME_BYTES = 200
_MAX_REASON_BYTES = 4096
_MAX_DURATION_MILLIS = ((1 << 63) - 1) // 1_000_000


def validate_signal_name(name: str) -> None:
    if not isinstance(name, str) or not name or len(name.encode()) > _MAX_NAME_BYTES:
        message = "name must be 1 to 200 UTF-8 bytes"
        raise ValueError(message)


def validate_rejection_reason(reason: str) -> None:
    if not isinstance(reason, str) or not reason or len(reason.encode()) > _MAX_REASON_BYTES:
        message = "rejection reason must be 1 to 4096 UTF-8 bytes"
        raise ValueError(message)


def duration_millis(duration: timedelta) -> int:
    if not isinstance(duration, timedelta):
        message = "duration must be a timedelta"
        raise TypeError(message)
    millis, remainder = divmod(duration, timedelta(milliseconds=1))
    millis += bool(remainder)
    if duration < timedelta(0) or millis > _MAX_DURATION_MILLIS:
        message = "duration must be nonnegative and fit a signed 64-bit nanosecond interval"
        raise ValueError(message)
    return millis


def encode_signal(value: object) -> bytes:
    data = dumps(value.model_dump() if isinstance(value, BaseModel) else value)
    if len(data) > _MAX_SIGNAL_BYTES:
        message = "workflow signal exceeds 1 MiB; send a blob reference"
        raise ValueError(message)
    return data


async def decode_signal(data: bytes, value_type: type[R]) -> R:
    if len(data) > _MAX_SIGNAL_BYTES:
        message = "workflow signal exceeds 1 MiB"
        raise ValueError(message)
    value = await loads(data)
    if dumps(value) != data:
        message = "workflow signal must be canonical CBOR"
        raise ValueError(message)
    return TypeAdapter(value_type).validate_python(value, strict=True)
