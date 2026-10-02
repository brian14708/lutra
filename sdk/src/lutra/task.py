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

from lutra._dependency import UvSource, discover_source
from lutra._gen.lutra.v1.lutra_pb import ActionSpec
from lutra.value import dumps

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from types import FunctionType
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
    """Python task retry policy."""

    NONE = "none"
    IDEMPOTENT = "idempotent"


def normalize_retry(retry: RetryMode | str, max_attempts: int | None) -> tuple[RetryMode, int]:
    """Normalize a declaration or invocation policy.

    Returns:
        The retry mode and total attempt limit.

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
        message = "retry=none only permits one attempt"
        raise ValueError(message)
    return mode, attempts


@dataclass(frozen=True)
class TaskImage:
    """Runtime image. Docker builds FROM from_image; the base needs Python and uv."""

    name: str = "local-python"
    from_image: str = ""
    build_context: Path | None = None
    build_command: tuple[str, ...] | None = None
    workdir: str = "."


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
    """Own a runtime declaration and its complete task entrypoint set.

    Relative source_includes use the declaring file's directory. Entries may
    name files, directories, or globs. They override ignore rules.
    """

    name: str
    image: TaskImage = field(default_factory=TaskImage)
    resources: Resources = field(default_factory=Resources)
    env_vars: Mapping[str, str] = field(default_factory=dict)
    dependencies: tuple[TaskEnvironment, ...] = ()
    source_includes: tuple[Path, ...] = ()
    import_roots: tuple[Path, ...] | None = None
    _tasks: list[Task[..., object]] = field(default_factory=list, init=False, repr=False)
    _declaring_file: Path | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Copy mutable declarations supplied by the caller.

        Raises:
            TypeError: If source_includes is a bare path or string.

        """
        object.__setattr__(self, "env_vars", MappingProxyType(dict(self.env_vars)))
        object.__setattr__(self, "dependencies", tuple(self.dependencies))
        if isinstance(self.source_includes, (str, Path)):
            msg = "source_includes must be a sequence of paths"
            raise TypeError(msg)
        object.__setattr__(self, "source_includes", tuple(self.source_includes))
        frame = inspect.currentframe()
        caller = frame.f_back.f_back if frame is not None and frame.f_back is not None else None
        if caller is not None:
            filename = Path(caller.f_code.co_filename)
            if filename.is_file():
                object.__setattr__(self, "_declaring_file", filename.resolve())
        if self.import_roots is not None:
            object.__setattr__(self, "import_roots", tuple(self.import_roots))

    def resolved_source_includes(self) -> tuple[Path, ...]:
        """Resolve include paths against the file that declared this environment.

        Returns:
            Paths anchored at the declaration file.

        Raises:
            ValueError: If no declaration file is available for relative paths.

        """
        if not self.source_includes:
            return ()
        if self._declaring_file is None:
            msg = "source_includes require an environment declared in a file"
            raise ValueError(msg)
        return tuple(
            path if path.is_absolute() else self._declaring_file.parent / path
            for path in self.source_includes
        )

    @property
    def tasks(self) -> tuple[Task[..., object], ...]:
        """The declared entrypoints."""
        return tuple(self._tasks)

    @property
    def dependency_source(self) -> UvSource:
        """The one locked dependency source shared by all entrypoints.

        Raises:
            ValueError: If entrypoints do not share a locked source.

        """
        if not self._tasks:
            msg = "an environment must declare at least one task"
            raise ValueError(msg)
        sources = [discover_source(task.source_file) for task in self._tasks]
        first = sources[0]
        if any(source.identity != first.identity for source in sources[1:]):
            msg = "all environment tasks must share one locked project or script"
            raise ValueError(msg)
        return first

    @overload
    def task(
        self,
        function: Callable[P, R_co | Awaitable[R_co]],
        *,
        retry: RetryMode | str = RetryMode.NONE,
        max_attempts: int | None = None,
        cache: bool = False,
        version: str | None = None,
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
    ) -> Callable[[Callable[P, R_co | Awaitable[R_co]]], Task[P, R_co]]: ...

    def task(
        self,
        function: Callable[P, R_co | Awaitable[R_co]] | None = None,
        *,
        retry: RetryMode | str = RetryMode.NONE,
        max_attempts: int | None = None,
        cache: bool = False,
        version: str | None = None,
    ) -> Task[P, R_co] | Callable[[Callable[P, R_co | Awaitable[R_co]]], Task[P, R_co]]:
        """Declare a module-level task.

        Returns:
            The wrapped entrypoint.

        """

        def declare(target: Callable[P, R_co | Awaitable[R_co]]) -> Task[P, R_co]:
            wrapped = Task(
                self, target, retry=retry, max_attempts=max_attempts, cache=cache, version=version
            )
            self._tasks.append(wrapped)
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
    ) -> None:
        """Wrap a module-level function.

        Raises:
            ValueError: If the function is not defined at module scope.

        """
        if "<locals>" in function.__qualname__:
            msg = "tasks must be defined at module scope"
            raise ValueError(msg)
        self.environment = environment
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
        self.source_file = Path(inspect.getfile(cast("FunctionType", function))).resolve()
        source = discover_source(self.source_file)
        module_name = function.__module__
        loaded = sys.modules.get(module_name)
        spec = getattr(loaded, "__spec__", None)
        if module_name == "__main__":
            module_name = spec.name if spec is not None and spec.name else self.source_file.stem
        self.function = function
        self.module = module_name
        self.qualname = function.__qualname__
        if source.is_script or function.__module__ == "__main__":
            relative = self.source_file.relative_to(source.bundle_root).as_posix()
            if ":" in relative:
                msg = "task source path cannot contain a colon"
                raise ValueError(msg)
            self.entrypoint_value = f"file:{relative}:{self.qualname}"
        else:
            self.entrypoint_value = f"{self.module}:{self.qualname}"
        self.entrypoint_id = len(environment.tasks) + 1
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

    def entrypoint(self) -> tuple[str, ...]:
        """Return the server registration declaration.

        Returns:
            The entrypoint declaration.

        """
        return ("./.venv/bin/python", "-m", "lutra.serve", self.entrypoint_value)
