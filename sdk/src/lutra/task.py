"""Deferred async task calls."""

from __future__ import annotations

import hashlib
import inspect
import sys
from dataclasses import dataclass
from functools import update_wrapper
from pathlib import Path
from typing import TYPE_CHECKING, Generic, ParamSpec, TypeVar, cast

from lutra._gen.lutra.v1.lutra_pb import SourceBundle, TaskImage, TaskSpec

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from types import FunctionType

P = ParamSpec("P")
R = TypeVar("R")
LOCAL_TASK_IMAGE = "local-python"


@dataclass(frozen=True)
class Invocation(Generic[R]):
    task: Task[..., R]
    args: tuple[object, ...]
    kwargs: dict[str, object]


class Task(Generic[P, R]):
    def __init__(self, function: Callable[P, Coroutine[object, object, R]]) -> None:
        if not inspect.iscoroutinefunction(function):
            message = "tasks must be async functions"
            raise TypeError(message)
        if "<locals>" in function.__qualname__:
            message = "tasks must be defined at module scope"
            raise ValueError(message)
        module_name = function.__module__
        loaded = sys.modules.get(module_name)
        spec = getattr(loaded, "__spec__", None)
        if module_name == "__main__" and spec is not None and spec.name:
            module_name = spec.name
        self.function = function
        self.name = function.__name__
        self.module = module_name
        self.qualname = function.__qualname__
        self.source_file = Path(inspect.getfile(cast("FunctionType", function))).resolve()
        update_wrapper(self, function)
        self.__module__ = module_name
        self.__signature__ = inspect.signature(function)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Invocation[R]:
        inspect.signature(self.function).bind(*args, **kwargs)
        return Invocation(self, args, kwargs)

    def spec(self, project: str, domain: str, source_uri: str) -> TaskSpec:
        identity = f"{source_uri}\0{self.module}\0{self.qualname}\0{LOCAL_TASK_IMAGE}"
        version = hashlib.sha256(identity.encode()).hexdigest()
        return TaskSpec(
            project=project,
            domain=domain,
            name=self.name,
            module=self.module,
            qualname=self.qualname,
            version=version,
            source=SourceBundle(uri=source_uri),
            image=TaskImage(name=LOCAL_TASK_IMAGE),
        )


def task(function: Callable[P, Coroutine[object, object, R]]) -> Task[P, R]:
    return Task(function)
