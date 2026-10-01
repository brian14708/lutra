"""Action-scoped checkpoints stored as versioned raw log records."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from lutra._gen.lutra.v1.log_pb import AppendRequest, LogEntry, ReadRequest
from lutra.value import dumps, loads

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from lutra.serve import TaskAPIClient


_MAX_KEY_BYTES = 1024
_MAX_READ_LIMIT = 1000


class CheckpointNotFound(KeyError):  # ruff: ignore[error-suffix-on-exception-name]
    """The action has no saved state for this name."""


@dataclass(frozen=True)
class CheckpointEvent:
    """A committed event and its stream cursor."""

    sequence: int
    value: object


class CheckpointManager:
    """Append events and replace named state for the current action."""

    def __init__(self, client: TaskAPIClient, run_id: str, action_id: str) -> None:
        """Bind checkpoint operations to a task host and action."""
        self._client, self._run_id, self._action_id = (client, run_id, action_id)

    def _key(self, kind: Literal["event", "state"], name: str) -> bytes:
        if not isinstance(name, str) or not name:
            message = "checkpoint name must be a nonempty string"
            raise ValueError(message)
        key = f"{self._action_id}/{kind}/{name}".encode()
        if len(key) > _MAX_KEY_BYTES:
            message = "checkpoint key exceeds 1024 UTF-8 bytes"
            raise ValueError(message)
        return key

    async def _append(
        self, kind: Literal["event", "state"], name: str, value: object, event_id: str | None
    ) -> int:
        key = self._key(kind, name)
        if event_id is not None and (not isinstance(event_id, str) or not event_id):
            message = "event_id must be a nonempty string"
            raise ValueError(message)
        append_id = (
            ""
            if event_id is None
            else self._action_id + ":" + hashlib.sha256(dumps([key, event_id])).hexdigest()
        )
        envelope = {"type": f"checkpoint.{kind}.v1", "value": value}
        response = await self._client.log.append(
            AppendRequest(
                run_id=self._run_id,
                stream="checkpoint",
                append_id=append_id,
                entries=[LogEntry(key=key, value_cbor=dumps(envelope))],
            )
        )
        return response.last_seq

    async def append(self, name: str, value: object, event_id: str | None = None) -> int:
        """Commit an event and return its sequence, deduplicating an optional ID.

        Returns:
            The event's sequence number.

        """
        return await self._append("event", name, value, event_id)

    async def _records(
        self, kind: Literal["event", "state"], name: str, after: int, limit: int | None
    ) -> AsyncIterator[CheckpointEvent]:
        key = self._key(kind, name)
        if type(after) is not int or after < 0:
            message = "after must be a nonnegative integer"
            raise ValueError(message)
        if limit is not None and (type(limit) is not int or limit < 1 or limit > _MAX_READ_LIMIT):
            message = "limit must be between 1 and 1000"
            raise ValueError(message)
        remaining = limit
        while remaining is None or remaining > 0:
            response = await self._client.log.read(
                ReadRequest(
                    run_id=self._run_id,
                    stream="checkpoint",
                    key=key,
                    after_seq=after,
                    limit=100 if remaining is None else min(remaining, 100),
                )
            )
            for record in response.records:
                envelope = await loads(record.value_cbor, self._client.resolve_blob)
                if (
                    record.key != key
                    or not isinstance(envelope, dict)
                    or envelope.get("type") != f"checkpoint.{kind}.v1"
                    or "value" not in envelope
                ):
                    message = "invalid checkpoint record or version"
                    raise ValueError(message)
                yield CheckpointEvent(record.seq, envelope["value"])
                after = record.seq
                if remaining is not None:
                    remaining -= 1
            if not response.truncated:
                break
            if not response.records:
                message = "checkpoint read made no progress"
                raise RuntimeError(message)

    def read(self, name: str, after: int = 0, limit: int = 100) -> AsyncIterator[CheckpointEvent]:
        """Yield up to limit events in sequence order after the supplied cursor.

        Returns:
            An async iterator over committed events.

        """
        return self._records("event", name, after, limit)

    async def save(self, name: str, value: object) -> None:
        """Commit the latest named state, retaining older records for audit."""
        await self._append("state", name, value, None)

    async def load(self, name: str, default: object = ...) -> object:
        """Return the latest saved state or the supplied missing-state default.

        Returns:
            The latest saved value or the supplied default.

        Raises:
            CheckpointNotFound: If no state exists and no default was supplied.

        """
        found = False
        latest: object = None
        async for event in self._records("state", name, 0, None):
            found, latest = True, event.value
        if found:
            return latest
        if default is not ...:
            return default
        raise CheckpointNotFound(name)
