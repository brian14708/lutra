"""Optional JSON Schema generation for task arguments."""

from __future__ import annotations

import inspect
from types import UnionType
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NotRequired,
    Required,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)
from typing_extensions import is_typeddict

from pydantic import BaseModel, ConfigDict, Field, create_model

if TYPE_CHECKING:
    from collections.abc import Callable

    from lutra.tools import ToolSchema


def _check_annotation(annotation: object, seen: set[object]) -> None:
    if annotation in seen or annotation in {str, int, float, bool, type(None)}:
        return
    origin, args = get_origin(annotation), get_args(annotation)
    if origin is Literal and all(
        type(value) in {str, int, float, bool, type(None)} for value in args
    ):
        return
    if is_typeddict(annotation):
        seen.add(annotation)
        for value in get_type_hints(annotation, include_extras=True).values():
            _check_annotation(value, seen)
        return
    if origin in {Union, UnionType, Required, NotRequired} or (origin is list and len(args) == 1):
        for value in args:
            _check_annotation(value, seen)
        return
    if origin is dict and len(args) == 2 and args[0] is str:  # ruff: ignore[magic-value-comparison]
        _check_annotation(args[1], seen)
        return
    message = f"unsupported tool annotation: {annotation!r}"
    raise TypeError(message)


def tool_model(function: Callable[..., object]) -> type[BaseModel]:
    """Export keyword parameters from a task's resolved annotations.

    Returns:
        A provider-neutral tool definition.

    Raises:
        TypeError: If a parameter cannot be represented as JSON arguments.

    """
    hints = get_type_hints(function, include_extras=True)
    fields: dict[str, Any] = {}
    for name, parameter in inspect.signature(function).parameters.items():
        if parameter.kind not in {parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY}:
            message = f"unsupported tool parameter: {name} ({parameter.kind.description})"
            raise TypeError(message)
        if name not in hints:
            message = f"tool parameter {name!r} requires an annotation"
            raise TypeError(message)
        annotation = hints[name]
        _check_annotation(annotation, set())
        default = ... if parameter.default is parameter.empty else parameter.default
        fields[f"argument_{len(fields)}"] = (annotation, Field(default=default, alias=name))
    return create_model(
        function.__name__, __config__=ConfigDict(extra="forbid", strict=True), **fields
    )


def tool_schema(function: Callable[..., object]) -> ToolSchema:
    """Describe a function for registration as a tool.

    Returns:
        The function name, docstring, and keyword argument JSON schema.

    """
    model = tool_model(function)
    return {
        "name": function.__name__,
        "description": inspect.getdoc(function) or "",
        "input_schema": model.model_json_schema(),
    }
