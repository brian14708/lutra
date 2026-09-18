"""Task declarations and DAG helpers used by the Lutra SDK."""

from __future__ import annotations

import contextvars
import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, TypeVar

from lutra._bindings import sha256_digest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

T = TypeVar("T")


class _RuntimeProtocol(Protocol):
    def invoke(self, task: Task, args: tuple[object, ...], kwargs: dict[str, object]) -> object: ...

    def map(self, task: Task, items: Iterable[object]) -> list[object]: ...


_runtime_context: contextvars.ContextVar[_RuntimeProtocol | None] = contextvars.ContextVar(
    "lutra_runtime", default=None
)


def _source_version(function: Callable[..., object]) -> str:
    """Return a stable immutable version for the module containing *function*.

    Returns:
        The ``sha256:<hex>`` digest of the module source, falling back to the
        function identity when no source is available.
    """
    try:
        source_path = inspect.getsourcefile(function)
        if source_path is not None:
            source = Path(source_path).read_bytes()
        else:
            source = inspect.getsource(function).encode()
    except (OSError, TypeError):
        source = f"{function.__module__}:{function.__qualname__}".encode()
    return sha256_digest(source)


@dataclass(frozen=True)
class Task:
    """An immutable task identity registered by :func:`run`."""

    environment: TaskEnvironment
    function: Callable[..., object]
    name: str
    version: str

    @property
    def entrypoint(self) -> str:
        return f"{self.function.__module__}:{self.function.__qualname__}"

    def __call__(self, *args: object, **kwargs: object) -> object:
        runtime = _runtime_context.get()
        if runtime is not None:
            return runtime.invoke(self, args, kwargs)
        return self.function(*args, **kwargs)


class TaskEnvironment:
    """Namespace for decorated tasks.

    When no version is supplied, each task gets a ``sha256:`` version derived
    from its module source. Pass ``version=`` to pin an explicit identity.
    """

    def __init__(self, name: str, *, version: str | None = None) -> None:
        self.name = name
        self.version = version
        self.tasks: list[Task] = []

    def task(
        self,
        function: Callable[..., T] | None = None,
        *,
        name: str | None = None,
        version: str | None = None,
    ) -> Callable[[Callable[..., T]], Task] | Task:
        def decorate(fn: Callable[..., T]) -> Task:
            task_version = version or self.version or _source_version(fn)
            task = Task(self, fn, name or f"{self.name}.{fn.__name__}", task_version)
            self.tasks.append(task)
            return task

        return decorate(function) if function is not None else decorate


def map(task: Task, items: Iterable[object]) -> list[object]:
    """Fan out a task over *items*; runtime workers replace this with child actions.

    Returns:
        The task results in input order.
    """
    runtime = _runtime_context.get()
    if runtime is not None:
        return runtime.map(task, items)
    return [task(item) for item in items]


def _set_runtime(runtime: _RuntimeProtocol | None) -> contextvars.Token[_RuntimeProtocol | None]:
    return _runtime_context.set(runtime)


def _reset_runtime(token: contextvars.Token[_RuntimeProtocol | None]) -> None:
    _runtime_context.reset(token)
