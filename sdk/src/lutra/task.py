"""Environment-owned deferred task calls."""

from __future__ import annotations

import inspect
import re
import sys
from dataclasses import dataclass, field
from decimal import Decimal
from functools import update_wrapper
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Generic, ParamSpec, TypeVar, cast

from lutra._bundle import project_root

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from types import FunctionType
P = ParamSpec("P")
R_co = TypeVar("R_co", covariant=True)


@dataclass(frozen=True)
class TaskImage:
    """Declared runtime image."""

    name: str = "local-python"
    reference: str = ""
    build_context: Path | None = None
    build_command: tuple[str, ...] = ("uv", "sync", "--locked", "--no-dev")
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
    """Own a runtime declaration and its complete task entrypoint set."""

    name: str
    image: TaskImage = field(default_factory=TaskImage)
    resources: Resources = field(default_factory=Resources)
    env_vars: Mapping[str, str] = field(default_factory=dict)
    dependencies: tuple[TaskEnvironment, ...] = ()
    _tasks: list[Task[..., object]] = field(default_factory=list, init=False, repr=False)

    def __post_init__(self) -> None:
        """Copy mutable declarations supplied by the caller."""
        object.__setattr__(self, "env_vars", MappingProxyType(dict(self.env_vars)))
        object.__setattr__(self, "dependencies", tuple(self.dependencies))

    @property
    def tasks(self) -> tuple[Task[..., object], ...]:
        """The declared entrypoints."""
        return tuple(self._tasks)

    @property
    def source_root(self) -> Path:
        """The locked project shared by every entrypoint.

        Raises:
            ValueError: If no tasks exist or their locked projects differ.

        """
        if not self._tasks:
            msg = "an environment must declare at least one task"
            raise ValueError(msg)
        roots = {project_root(task.source_file) for task in self._tasks}
        if len(roots) != 1:
            msg = "all environment tasks must belong to the same locked project"
            raise ValueError(msg)
        return roots.pop()

    def task(self, function: Callable[P, R_co | Awaitable[R_co]]) -> Task[P, R_co]:
        """Declare a module-level task.

        Returns:
            The wrapped entrypoint.

        """
        wrapped = Task(self, function)
        self._tasks.append(wrapped)
        return wrapped


@dataclass(frozen=True)
class Invocation(Generic[R_co]):
    """A task and arguments captured for deferred execution."""

    task: Task[..., R_co]
    args: tuple[object, ...]
    kwargs: dict[str, object]


class Task(Generic[P, R_co]):
    """A callable entrypoint owned by a task environment."""

    def __init__(
        self, environment: TaskEnvironment, function: Callable[P, R_co | Awaitable[R_co]]
    ) -> None:
        """Wrap a module-level function.

        Raises:
            ValueError: If the function is not defined at module scope.

        """
        if "<locals>" in function.__qualname__:
            msg = "tasks must be defined at module scope"
            raise ValueError(msg)
        self.environment = environment
        self.source_file = Path(inspect.getfile(cast("FunctionType", function))).resolve()
        module_name = function.__module__
        loaded = sys.modules.get(module_name)
        spec = getattr(loaded, "__spec__", None)
        if module_name == "__main__":
            module_name = (
                spec.name
                if spec is not None and spec.name
                else ".".join(
                    self.source_file
                    .relative_to(project_root(self.source_file))
                    .with_suffix("")
                    .parts
                )
            )
        self.function = function
        self.module = module_name
        self.qualname = function.__qualname__
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
