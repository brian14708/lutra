"""Value serializers used by task inputs and outputs.

Serializers are named and versioned so task slot metadata can select a stable
wire format without coupling the runtime to one Python implementation.
"""

from __future__ import annotations

import json
import pickle  # ruff: ignore[suspicious-pickle-import]
from typing import Protocol, cast, runtime_checkable


@runtime_checkable
class Serializer(Protocol):
    """Protocol implemented by a task value serializer."""

    name: str
    version: str

    def serialize(self, value: object) -> bytes: ...

    def deserialize(self, data: bytes) -> object: ...


class _JSONSerializer:
    name = "json"
    version = "1"

    def serialize(self, value: object) -> bytes:  # ruff: ignore[no-self-use]
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )

    def deserialize(self, data: bytes) -> object:  # ruff: ignore[no-self-use]
        return json.loads(data.decode("utf-8"))


class _PickleSerializer:
    name = "pickle"
    version = "5"

    def serialize(self, value: object) -> bytes:  # ruff: ignore[no-self-use]
        return pickle.dumps(value, protocol=5)

    def deserialize(self, data: bytes) -> object:  # ruff: ignore[no-self-use]
        return cast("object", pickle.loads(data))  # ruff: ignore[suspicious-pickle-usage]


_serializers: dict[str, Serializer] = {}


def register_serializer(serializer: Serializer) -> None:
    """Register or replace a serializer by name.

    Raises:
        ValueError: If the serializer has no name or version.
        TypeError: If the object does not implement the protocol.
    """

    if not isinstance(serializer, Serializer):
        message = "serializer must implement the Serializer protocol"
        raise TypeError(message)
    if not serializer.name or not serializer.version:
        message = "serializer name and version are required"
        raise ValueError(message)
    _serializers[serializer.name] = serializer


def get_serializer(name: str = "pickle") -> Serializer:
    """Return a registered serializer.

    Returns:
        The registered serializer.

    Raises:
        ValueError: If no serializer is registered under ``name``.
    """

    try:
        return _serializers[name]
    except KeyError as exc:
        available = ", ".join(sorted(_serializers))
        message = f"unknown serializer {name!r}; available: {available}"
        raise ValueError(message) from exc


def serialize(value: object, serializer: str = "pickle") -> bytes:
    return get_serializer(serializer).serialize(value)


def deserialize(data: bytes, serializer: str = "pickle") -> object:
    return get_serializer(serializer).deserialize(data)


def available_serializers() -> tuple[str, ...]:
    return tuple(sorted(_serializers))


register_serializer(_JSONSerializer())
register_serializer(_PickleSerializer())
