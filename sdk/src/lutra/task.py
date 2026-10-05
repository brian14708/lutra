"""Environment-owned deferred task calls."""

from __future__ import annotations

import inspect
import re
import sys
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from functools import update_wrapper
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Generic, ParamSpec, TypeVar, cast, overload

from lutra._dependency import _safe_path
from lutra._gen.lutra.v1.lutra_pb import ActionSpec
from lutra.package_managers import ManagerOutput, PackageManager, Uv, prepare_managers
from lutra.value import dumps

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from types import FunctionType

    from lutra._source_bundle import PreparedSource
    from lutra.tools import ToolSchema
P = ParamSpec("P")
R_co = TypeVar("R_co", covariant=True)
_MAX_ATTEMPTS = 100
_MAX_CACHE_ERROR_DETAILS = 64 << 10
_MAX_TASK_VERSION_LENGTH = 200
_VERSION = re.compile(
    r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
)


class CacheableError(Exception):
    """A deterministic task failure that may be cached."""

    def __init__(self, code: str, details: object = None) -> None:
        """Create a cacheable error with a stable code and details.

        Raises:
            ValueError: If the code or encoded details exceed their limits.

        """
        if not re.fullmatch(r"[a-z][a-z0-9_.-]{0,127}", code):
            message = "invalid cacheable error code"
            raise ValueError(message)
        if len(dumps(details)) > _MAX_CACHE_ERROR_DETAILS:
            message = "cacheable error details exceed 64 KiB"
            raise ValueError(message)
        super().__init__(code)
        self.code = code
        self.details = details


class RetryMode(StrEnum):
    """Policy for retrying reported user-code failures.

    Infrastructure recovery can launch another attempt regardless of this policy.
    """

    NONE = "none"
    IDEMPOTENT = "idempotent"


class TaskKind(StrEnum):
    """Execution semantics of an environment entrypoint."""

    TASK = "task"
    WORKFLOW = "workflow"


class ConfigError(Exception):
    """A non-retryable configuration failure."""

    def __init__(self, code: str = "config.invalid") -> None:
        """Create a failure with no value diagnostics."""
        super().__init__(code)
        self.code = code


class TerminalError(RuntimeError):
    """A user failure that stops retries for this invocation."""

    def __init__(self, message: str, *, code: int = 500) -> None:
        """Create a bounded terminal failure with an HTTP-style code.

        Raises:
            ValueError: If the message or error code is invalid.

        """
        if not isinstance(message, str) or not message or len(message.encode()) > 64 << 10:
            error = "terminal error message must contain 1 byte to 64 KiB"
            raise ValueError(error)
        if type(code) is not int or not 400 <= code <= 599:  # ruff: ignore[magic-value-comparison] HTTP error code range.
            error = "terminal error code must be between 400 and 599"
            raise ValueError(error)
        super().__init__(message)
        self.code = code


class PromiseRejectedError(RuntimeError):
    """A durable promise or awakeable was rejected."""


class InvocationCanceledError(RuntimeError):
    """A child invocation was canceled."""


@dataclass(frozen=True)
class SettingRef:
    """Reference a namespace setting."""

    path: str
    sensitive: bool | None = None


@dataclass(frozen=True)
class ConfigBinding:
    """Expose a setting through the task context."""

    name: str
    setting_ref: str
    required: bool = True
    sensitive: bool | None = None


def normalize_retry(retry: RetryMode | str, max_attempts: int | None) -> tuple[RetryMode, int]:
    """Normalize a declaration or invocation policy.

    Returns:
        The retry mode and limit on attempts that report user-code failures.

    Raises:
        ValueError: If the mode or limit is invalid.

    """
    try:
        mode = RetryMode(retry)
    except (ValueError, TypeError) as exc:
        message = f"invalid retry mode: {retry!r}"
        raise ValueError(message) from exc
    attempts = (1 if mode is RetryMode.NONE else 3) if max_attempts is None else max_attempts
    if type(attempts) is not int or attempts < 1 or attempts > _MAX_ATTEMPTS:
        message = "max_attempts must be between 1 and 100"
        raise ValueError(message)
    if mode is RetryMode.NONE and attempts != 1:
        message = "retry=none requires max_attempts=1"
        raise ValueError(message)
    return mode, attempts


@dataclass(frozen=True)
class TaskImage:
    """Container image assembled from package manager adapters.

    Custom bases need Linux and the system libraries required by Python and
    task dependencies. Use a standard distribution image such as Debian or
    Ubuntu; minimal images such as scratch are unsupported. Pin the base by
    digest when builds must use identical base contents.
    """

    name: str = "container"
    from_image: str = "docker.io/library/python:3.12-slim-bookworm"
    platform: str | None = None


@dataclass(frozen=True, init=False)
class Resources:
    """CPU and memory expressed in exact integer units."""

    cpu_millis: int
    memory_bytes: int

    def __init__(self, cpu: float = 1, memory: str | int = "1Gi") -> None:
        """Convert CPU cores and a byte or unit-suffixed memory quantity.

        Raises:
            ValueError: If CPU or memory is not positive or CPU is fractional millicores.

        """
        millis = Decimal(str(cpu)) * 1000
        if not millis.is_finite() or millis <= 0 or millis != millis.to_integral_value():
            msg = "CPU must be positive in whole millicores"
            raise ValueError(msg)
        if millis > (1 << 32) - 1:
            msg = "CPU exceeds the supported limit"
            raise ValueError(msg)
        if isinstance(memory, str):
            match = re.fullmatch(r"([0-9]+)(Ki|Mi|Gi|Ti|K|M|G|T)?", memory)
            if match is None:
                msg = "memory must be bytes or a quantity such as 512Mi or 1Gi"
                raise ValueError(msg)
            units = {
                "": 1,
                "Ki": 1 << 10,
                "Mi": 1 << 20,
                "Gi": 1 << 30,
                "Ti": 1 << 40,
                "K": 10**3,
                "M": 10**6,
                "G": 10**9,
                "T": 10**12,
            }
            size = int(match[1]) * units[match[2] or ""]
        else:
            size = memory
        if isinstance(size, bool) or size <= 0 or size > (1 << 64) - 1:
            msg = "memory must be positive and within the supported limit"
            raise ValueError(msg)
        object.__setattr__(self, "cpu_millis", int(millis))
        object.__setattr__(self, "memory_bytes", size)


@dataclass(frozen=True, eq=False)
class TaskEnvironment:
    """Own a runtime declaration and its task and workflow entrypoints."""

    name: str
    image: TaskImage = field(default_factory=TaskImage)
    resources: Resources = field(default_factory=Resources)
    env: Mapping[str, str | SettingRef] = field(default_factory=dict)
    dependencies: tuple[TaskEnvironment, ...] = ()
    package_managers: tuple[PackageManager, ...] = field(default_factory=lambda: (Uv(),))
    _entries: list[Task[..., object]] = field(default_factory=list, init=False, repr=False)

    def __post_init__(self) -> None:
        """Copy mutable declarations supplied by the caller."""
        object.__setattr__(self, "env", MappingProxyType(dict(self.env)))
        object.__setattr__(self, "dependencies", tuple(self.dependencies))
        object.__setattr__(self, "package_managers", tuple(self.package_managers))

    @property
    def entries(self) -> tuple[Task[..., object], ...]:
        """Tasks and workflows in declaration order, with stable one-based IDs."""
        return tuple(self._entries)

    @property
    def manager_outputs(self) -> tuple[ManagerOutput, ...]:
        """Composed manager outputs for registration."""
        return prepare_managers(
            tuple(self.package_managers),
            tuple(task.source_file for task in self._entries),
            self.image.platform,
        )

    @overload
    def task(
        self,
        function: Callable[P, R_co | Awaitable[R_co]],
        *,
        retry: RetryMode | str = RetryMode.NONE,
        max_attempts: int | None = None,
        cache: bool = False,
        version: str | None = None,
        config: tuple[ConfigBinding, ...] = (),
    ) -> Task[P, R_co]: ...

    @overload
    def task(
        self,
        function: None = None,
        *,
        retry: RetryMode | str = RetryMode.NONE,
        max_attempts: int | None = None,
        cache: bool = False,
        version: str | None = None,
        config: tuple[ConfigBinding, ...] = (),
    ) -> Callable[[Callable[P, R_co | Awaitable[R_co]]], Task[P, R_co]]: ...

    def task(  # ruff: ignore[too-many-arguments]
        self,
        function: Callable[P, R_co | Awaitable[R_co]] | None = None,
        *,
        retry: RetryMode | str = RetryMode.NONE,
        max_attempts: int | None = None,
        cache: bool = False,
        version: str | None = None,
        config: tuple[ConfigBinding, ...] = (),
    ) -> Task[P, R_co] | Callable[[Callable[P, R_co | Awaitable[R_co]]], Task[P, R_co]]:
        """Declare a module-level task.

        Returns:
            The wrapped entrypoint.

        """

        def declare(target: Callable[P, R_co | Awaitable[R_co]]) -> Task[P, R_co]:
            wrapped = Task(
                self,
                target,
                retry=retry,
                max_attempts=max_attempts,
                cache=cache,
                version=version,
                config=config,
            )
            self._entries.append(wrapped)
            return wrapped

        return declare(function) if function is not None else declare

    @overload
    def workflow(
        self, function: Callable[P, Awaitable[R_co]], *, config: tuple[ConfigBinding, ...] = ()
    ) -> Task[P, R_co]: ...

    @overload
    def workflow(
        self, function: None = None, *, config: tuple[ConfigBinding, ...] = ()
    ) -> Callable[[Callable[P, Awaitable[R_co]]], Task[P, R_co]]: ...

    def workflow(
        self,
        function: Callable[P, Awaitable[R_co]] | None = None,
        *,
        config: tuple[ConfigBinding, ...] = (),
    ) -> Task[P, R_co] | Callable[[Callable[P, Awaitable[R_co]]], Task[P, R_co]]:
        """Declare an async workflow that replays in fresh sandboxes.

        Use ``lutra.run`` and ``lutra.spawn`` for effects, and durable futures
        for coordination. Keep command submission order deterministic. Use
        ``workflow_time``, ``workflow_random``, and ``workflow_uuid`` for
        replay-stable entropy. Access files and external services inside tasks.

        Returns:
            The deferred workflow entrypoint.

        """

        def declare(target: Callable[P, Awaitable[R_co]]) -> Task[P, R_co]:
            wrapped = Task(self, target, config=config, kind=TaskKind.WORKFLOW)
            self._entries.append(wrapped)
            return wrapped

        return declare(function) if function is not None else declare


@dataclass(frozen=True)
class Invocation(Generic[R_co]):
    """A task and arguments captured for deferred execution."""

    task: Task[..., R_co]
    args: tuple[object, ...]
    kwargs: dict[str, object]

    def action_spec(self, max_attempts: int) -> ActionSpec:
        """Build the wire action specification for this invocation.

        Returns:
            The encoded action specification.

        """
        input_cbor = dumps([list(self.args), self.kwargs])
        return ActionSpec(
            input_cbor=input_cbor,
            max_attempts=max_attempts,
            cache=self.task.cache,
            task_version=self.task.version or "" if self.task.cache else "",
        )


class Task(Generic[P, R_co]):
    """A callable entrypoint owned by a task environment."""

    def __init__(  # ruff: ignore[too-many-arguments]
        self,
        environment: TaskEnvironment,
        function: Callable[P, R_co | Awaitable[R_co]],
        *,
        retry: RetryMode | str = RetryMode.NONE,
        max_attempts: int | None = None,
        cache: bool = False,
        version: str | None = None,
        config: tuple[ConfigBinding, ...] = (),
        kind: TaskKind = TaskKind.TASK,
    ) -> None:
        """Wrap a module-level function.

        Raises:
            ValueError: If the function is not defined at module scope.

        """
        if "<locals>" in function.__qualname__:
            msg = "tasks must be defined at module scope"
            raise ValueError(msg)
        if kind is TaskKind.WORKFLOW and any((
            not inspect.iscoroutinefunction(function),
            retry != RetryMode.NONE,
            max_attempts not in {None, 1},
            cache,
        )):
            message = "workflows require an async function without task retries, or caching"
            raise ValueError(message)
        self.kind = kind
        self.environment = environment
        self.config = tuple(config)
        self.retry, self.max_attempts = normalize_retry(retry, max_attempts)
        if type(cache) is not bool:
            message = "cache must be a boolean"
            raise ValueError(message)
        if (
            cache
            and version is not None
            and (len(version) > _MAX_TASK_VERSION_LENGTH or not _VERSION.fullmatch(version))
        ):
            message = "cached tasks require a semantic version such as 1.2.0"
            raise ValueError(message)
        self.cache, self.version = cache, version
        self.source_file = _safe_path(
            Path(inspect.getfile(cast("FunctionType", function))).absolute()
        )
        module_name = function.__module__
        loaded = sys.modules.get(module_name)
        spec = getattr(loaded, "__spec__", None)
        if module_name == "__main__":
            module_name = spec.name if spec is not None and spec.name else self.source_file.stem
        self.file_entrypoint = function.__module__ == "__main__" and spec is None
        self.function = function
        self.module = module_name
        self.qualname = function.__qualname__
        self.entrypoint_id = len(environment.entries) + 1
        update_wrapper(self, function)
        self.__module__ = module_name
        self.__signature__ = inspect.signature(function)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Invocation[R_co]:
        """Capture validated arguments for a task invocation.

        Returns:
            The captured invocation.

        """
        inspect.signature(self.function).bind(*args, **kwargs)
        return Invocation(self, args, kwargs)

    def tool_schema(self) -> ToolSchema:
        """Export this task as a tool.

        Returns:
            The name, docstring, and JSON Schema for keyword arguments.

        """
        from lutra._tool_schema import tool_schema  # ruff: ignore[import-outside-top-level]

        return tool_schema(self.function)

    def entrypoint(self, source: PreparedSource) -> str:
        """Return the server registration declaration.

        Returns:
            The entrypoint declaration.

        Raises:
            ValueError: If a file entrypoint contains a colon.

        """
        if self.source_file == source.script or self.file_entrypoint:
            relative = self.source_file.relative_to(source.bundle_root).as_posix()
            if ":" in relative:
                msg = "task source path cannot contain a colon"
                raise ValueError(msg)
            entrypoint = f"file:{relative}:{self.qualname}"
        else:
            entrypoint = f"{self.module}:{self.qualname}"
        return entrypoint
