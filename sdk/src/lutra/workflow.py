"""Typed durable futures, workflow state, and replay-stable entropy."""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Generic, Literal, Protocol, TypeVar, overload

from pydantic import TypeAdapter

from lutra._gen.lutra.v1.lutra_pb import (
    WorkflowAwakeableRequest,
    WorkflowCancelRequest,
    WorkflowCompletion,
    WorkflowEntropyRequest,
    WorkflowFuture,
    WorkflowPromiseRequest,
    WorkflowStateRequest,
    WorkflowTimerRequest,
    WorkflowWaitRequest,
)
from lutra._workflow import (
    decode_signal,
    duration_millis,
    encode_signal,
    validate_rejection_reason,
    validate_signal_name,
)
from lutra.runtime import RunContext, _workflow_context
from lutra.task import InvocationCanceledError, PromiseRejectedError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import timedelta

R = TypeVar("R")
R_co = TypeVar("R_co", covariant=True)
_SEED_BYTES = 32


class DurableFuture(Protocol[R_co]):
    """An operation owned by one workflow and usable in durable wait sets."""

    @property
    def key(self) -> str:
        """Stable operation key."""
        ...

    def reference(self) -> WorkflowFuture:
        """Return the validated durable identity."""
        ...

    async def decode(self, data: bytes) -> R_co:
        """Decode a completed operation."""
        ...


@dataclass(frozen=True)
class Completion(Generic[R]):
    """One durably selected operation, with a result or failure."""

    key: str
    future: DurableFuture[R]
    _completion: WorkflowCompletion

    async def result(self) -> R:
        """Decode the completion or raise its stable failure.

        Returns:
            The completed operation's value.

        Raises:
            PromiseRejectedError: If a promise or awakeable was rejected.
            InvocationCanceledError: If the child was canceled.
            TimeoutError: If a durable timeout expired.
            RuntimeError: If the child failed without a result envelope.

        """
        self.future.reference()
        failure = self._completion.failure
        message = self._completion.message
        if failure == WorkflowCompletion.Failure.REJECTED:
            raise PromiseRejectedError(message)
        if failure == WorkflowCompletion.Failure.CANCELED:
            raise InvocationCanceledError(message)
        if failure == WorkflowCompletion.Failure.TIMEOUT:
            raise TimeoutError(message)
        if failure == WorkflowCompletion.Failure.CHILD:
            raise RuntimeError(message)
        return await self.future.decode(self._completion.value_cbor)


async def _wait(futures: tuple[DurableFuture[R], ...], *, all_results: bool) -> list[Completion[R]]:
    context = _workflow_context()
    references = [future.reference() for future in futures]
    identities = [(ref.kind, ref.key, ref.id) for ref in references]
    if len({ref.key for ref in references}) != len(references):
        message = "wait sets must contain distinct future keys"
        raise ValueError(message)
    response = await context.client().workflow_wait(
        WorkflowWaitRequest(sequence=context.next_sequence(), futures=references, all=all_results)
    )
    by_identity = dict(zip(identities, futures, strict=True))
    completed: dict[tuple[WorkflowFuture.Kind, str, str], Completion[R]] = {}
    seen = set()
    for item in response.completions:
        ref = item.future
        if ref is None:
            message = "workflow completion has no future"
            raise ValueError(message)
        identity = (ref.kind, ref.key, ref.id)
        if identity not in by_identity or identity in seen:
            message = "workflow returned an unexpected completion"
            raise ValueError(message)
        seen.add(identity)
        completed[identity] = Completion(ref.key, by_identity[identity], item)
    if len(completed) != (len(futures) if all_results else 1):
        message = "workflow returned an incomplete wait result"
        raise ValueError(message)
    if all_results:
        return [completed[identity] for identity in identities]
    return list(completed.values())


@overload
async def gather(
    *futures: DurableFuture[R], return_exceptions: Literal[False] = False
) -> list[R]: ...


@overload
async def gather(
    *futures: DurableFuture[R], return_exceptions: Literal[True]
) -> list[R | Exception]: ...


@overload
async def gather(*futures: DurableFuture[R], return_exceptions: bool) -> list[R | Exception]: ...


async def gather(
    *futures: DurableFuture[R], return_exceptions: bool = False
) -> list[R | Exception]:
    """Wait durably for all operations and return values in input order.

    Returns:
        Results, including exceptions when requested.

    """
    _workflow_context()
    if not futures:
        return []
    completed = await _wait(futures, all_results=True)
    values: list[R | Exception] = []
    for item in completed:
        try:
            values.append(await item.result())
        except Exception as exc:
            if not return_exceptions:
                raise
            values.append(exc)
    return values


async def select(*futures: DurableFuture[R]) -> Completion[R]:
    """Choose the first completed operation durably, including failed operations.

    Returns:
        The selected operation and its stable key.

    Raises:
        ValueError: If the wait set is empty or contains duplicate identities.

    """
    if not futures:
        message = "select requires at least one future"
        raise ValueError(message)
    return (await _wait(futures, all_results=False))[0]


async def wait_completed(*futures: DurableFuture[R]) -> Completion[R]:
    """Wait for the next completion.

    Returns:
        The first durably completed operation.

    """
    return await select(*futures)


async def as_completed(*futures: DurableFuture[R]) -> AsyncIterator[Completion[R]]:
    """Yield every operation once in durable completion order.

    Yields:
        Completions, including already-completed and failed operations.

    """
    _workflow_context()
    remaining = list(futures)
    while remaining:
        completed = await select(*remaining)
        yield completed
        remaining.remove(completed.future)


@dataclass(frozen=True)
class ValueFuture(Generic[R]):
    """A typed promise, timer, or awakeable scoped to its workflow."""

    key: str
    context: RunContext
    _reference: WorkflowFuture
    value_type: type[R]

    def reference(self) -> WorkflowFuture:
        """Validate ownership and return the operation identity.

        Returns:
            The future reference.

        Raises:
            RuntimeError: If another workflow uses this handle.

        """
        if _workflow_context() is not self.context:
            message = "future belongs to a different workflow"
            raise RuntimeError(message)
        return self._reference

    async def decode(self, data: bytes) -> R:
        """Decode the canonical value with strict typing.

        Returns:
            The validated value.

        """
        return await decode_signal(data, self.value_type)

    async def result(self) -> R:
        """Wait durably for this operation.

        Returns:
            The operation's typed value.

        """
        return await (await select(self)).result()


@dataclass(frozen=True)
class Promise(ValueFuture[R]):
    """A named one-shot value scoped to a workflow."""

    async def resolve(self, value: R) -> None:
        """Resolve the promise, allowing identical repeated resolutions."""
        self.reference()
        data = encode_signal(TypeAdapter(self.value_type).validate_python(value, strict=True))
        await self.context.client().workflow_promise(
            WorkflowPromiseRequest(
                sequence=self.context.next_sequence(),
                operation=WorkflowPromiseRequest.Operation.RESOLVE,
                name=self.key,
                value_cbor=data,
            )
        )

    async def reject(self, reason: str) -> None:
        """Reject the promise, allowing identical repeated rejections."""
        self.reference()
        validate_rejection_reason(reason)
        await self.context.client().workflow_promise(
            WorkflowPromiseRequest(
                sequence=self.context.next_sequence(),
                operation=WorkflowPromiseRequest.Operation.REJECT,
                name=self.key,
                reason=reason,
            )
        )

    async def peek(self) -> Completion[R] | None:
        """Read an outcome without waiting, distinguishing pending from resolved null.

        Returns:
            A completion, including rejections, or None while pending.

        """
        ref = self.reference()
        response = await self.context.client().workflow_promise(
            WorkflowPromiseRequest(
                sequence=self.context.next_sequence(),
                operation=WorkflowPromiseRequest.Operation.PEEK,
                name=self.key,
            )
        )
        if not response.completed:
            return None
        return Completion(
            self.key,
            self,
            WorkflowCompletion(
                future=ref,
                value_cbor=response.value_cbor,
                message=response.reason,
                failure=WorkflowCompletion.Failure.REJECTED
                if response.rejected
                else WorkflowCompletion.Failure.UNSPECIFIED,
            ),
        )

    async def wait(self) -> R:
        """Wait for this named promise.

        Returns:
            Its typed value.

        """
        return await self.result()


def promise(name: str, value_type: type[R]) -> Promise[R]:
    """Get a durable named promise without sending a command.

    Returns:
        A workflow-owned typed promise.

    """
    context = _workflow_context()
    validate_signal_name(name)
    return Promise(
        name,
        context,
        WorkflowFuture(kind=WorkflowFuture.Kind.PROMISE, key=name, id=name),
        value_type,
    )


async def timer(duration: timedelta, *, key: str) -> ValueFuture[None]:
    """Start a keyed durable timer that can participate in a race.

    Returns:
        A future which completes with None.

    Raises:
        ValueError: If the key uses the reserved helper prefix.

    """
    context = _workflow_context()
    validate_signal_name(key)
    if key.startswith("__lutra:"):
        message = "timer keys starting with '__lutra:' are reserved"
        raise ValueError(message)
    millis = duration_millis(duration)
    await context.client().workflow_timer(
        WorkflowTimerRequest(sequence=context.next_sequence(), key=key, duration_millis=millis)
    )
    return ValueFuture(
        key, context, WorkflowFuture(kind=WorkflowFuture.Kind.TIMER, key=key, id=key), type(None)
    )


@dataclass(frozen=True)
class Awakeable(ValueFuture[R]):
    """An externally addressable, durable one-shot future."""

    @property
    def id(self) -> str:
        """Opaque ID accepted by awakeable resolution APIs."""
        return self._reference.id


async def awakeable(value_type: type[R], *, key: str) -> Awakeable[R]:
    """Create a typed, keyed awakeable.

    Returns:
        A future and its externally addressable ID.

    """
    context = _workflow_context()
    validate_signal_name(key)
    response = await context.client().workflow_awakeable(
        WorkflowAwakeableRequest(
            sequence=context.next_sequence(),
            operation=WorkflowAwakeableRequest.Operation.CREATE,
            key=key,
        )
    )
    return Awakeable(
        key,
        context,
        WorkflowFuture(kind=WorkflowFuture.Kind.AWAKEABLE, key=key, id=response.id),
        value_type,
    )


async def resolve_awakeable(awakeable_id: str, value: object) -> None:
    """Resolve an awakeable from workflow code."""
    context = _workflow_context()
    validate_signal_name(awakeable_id)
    data = encode_signal(value)
    await context.client().workflow_awakeable(
        WorkflowAwakeableRequest(
            sequence=context.next_sequence(),
            operation=WorkflowAwakeableRequest.Operation.RESOLVE,
            id=awakeable_id,
            value_cbor=data,
        )
    )


async def reject_awakeable(awakeable_id: str, reason: str) -> None:
    """Reject an awakeable from workflow code."""
    context = _workflow_context()
    validate_signal_name(awakeable_id)
    validate_rejection_reason(reason)
    await context.client().workflow_awakeable(
        WorkflowAwakeableRequest(
            sequence=context.next_sequence(),
            operation=WorkflowAwakeableRequest.Operation.REJECT,
            id=awakeable_id,
            reason=reason,
        )
    )


@dataclass(frozen=True)
class WorkflowState:
    """Canonical values owned by the current workflow action."""

    context: RunContext

    def _context(self) -> RunContext:
        if _workflow_context() is not self.context:
            message = "state belongs to a different workflow"
            raise RuntimeError(message)
        return self.context

    async def get(self, key: str, value_type: type[R]) -> R | None:
        """Read a state value and validate it strictly.

        Returns:
            The stored value or None if the key is missing.

        """
        context = self._context()
        validate_signal_name(key)
        response = await context.client().workflow_state(
            WorkflowStateRequest(
                sequence=context.next_sequence(),
                operation=WorkflowStateRequest.Operation.GET,
                key=key,
            )
        )
        return await decode_signal(response.value_cbor, value_type) if response.found else None

    async def set(self, key: str, value: object) -> None:
        """Write a canonical state value."""
        context = self._context()
        validate_signal_name(key)
        data = encode_signal(value)
        await context.client().workflow_state(
            WorkflowStateRequest(
                sequence=context.next_sequence(),
                operation=WorkflowStateRequest.Operation.SET,
                key=key,
                value_cbor=data,
            )
        )

    async def clear(self, key: str) -> None:
        """Remove one state key idempotently."""
        context = self._context()
        validate_signal_name(key)
        await context.client().workflow_state(
            WorkflowStateRequest(
                sequence=context.next_sequence(),
                operation=WorkflowStateRequest.Operation.CLEAR,
                key=key,
            )
        )

    async def keys(self) -> list[str]:
        """List user state keys in sorted order.

        Returns:
            The stored keys.

        """
        context = self._context()
        response = await context.client().workflow_state(
            WorkflowStateRequest(
                sequence=context.next_sequence(), operation=WorkflowStateRequest.Operation.KEYS
            )
        )
        return response.keys


def workflow_state() -> WorkflowState:
    """Access the current workflow's state.

    Returns:
        The workflow-owned state API.

    """
    return WorkflowState(_workflow_context())


async def workflow_time() -> datetime:
    """Read a journaled UTC wall-clock time.

    Returns:
        The same timestamp when this operation replays.

    """
    context = _workflow_context()
    response = await context.client().workflow_entropy(
        WorkflowEntropyRequest(
            sequence=context.next_sequence(), kind=WorkflowEntropyRequest.Kind.TIME
        )
    )
    return datetime.fromtimestamp(response.time_millis / 1000, UTC)


async def workflow_random() -> random.Random:
    """Create an independent replay-stable random generator.

    Returns:
        A generator seeded by a journaled 256-bit seed.

    """
    seed = await _seed(WorkflowEntropyRequest.Kind.SEED)
    return random.Random(int.from_bytes(seed))  # ruff: ignore[suspicious-non-cryptographic-random-usage] Reproducible workflow choices.


async def _seed(kind: WorkflowEntropyRequest.Kind) -> bytes:
    context = _workflow_context()
    response = await context.client().workflow_entropy(
        WorkflowEntropyRequest(sequence=context.next_sequence(), kind=kind)
    )
    if len(response.seed) != _SEED_BYTES:
        message = "workflow random seed must contain 32 bytes"
        raise ValueError(message)
    return response.seed


async def workflow_uuid() -> uuid.UUID:
    """Generate a replay-stable version-4 UUID.

    Returns:
        A UUID derived from a fresh journaled seed.

    """
    seed = await _seed(WorkflowEntropyRequest.Kind.UUID_SEED)
    return uuid.UUID(bytes=seed[:16], version=4)


async def cancel_invocation(future: DurableFuture[object]) -> None:
    """Cancel a child handle belonging to this workflow.

    Raises:
        ValueError: If the future is not a child invocation.

    """
    context = _workflow_context()
    ref = future.reference()
    if ref.kind != WorkflowFuture.Kind.CHILD:
        message = "only child invocations can be canceled"
        raise ValueError(message)
    await context.client().workflow_cancel(
        WorkflowCancelRequest(sequence=context.next_sequence(), id=ref.id)
    )
