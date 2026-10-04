from __future__ import annotations

from datetime import datetime  # ruff: ignore[typing-only-standard-library-import]
from typing import Literal, NotRequired
from typing_extensions import TypedDict

import pytest
from jsonschema import Draft202012Validator, ValidationError
from lutra import Task, TaskEnvironment

environment = TaskEnvironment("tools")


class Filter(TypedDict):
    active: bool
    tags: NotRequired[list[str]]


@environment.task
def search(
    supplier: Literal["atlas", "beacon", "cedar"],
    filters: list[Filter],
    *,
    limit: int = 10,
    note: str | None = None,
    prices: dict[str, float] | None = None,
) -> str:
    """Search suppliers."""  # ruff: ignore[docstring-missing-returns]
    return f"{supplier}: {filters}, {limit}, {note}, {prices}"


@environment.task
def missing(value: object) -> object:
    return value


missing.function.__annotations__.pop("value")


@environment.task
def positional(value: str, /) -> str:
    return value


@environment.task
def variadic(*values: str) -> tuple[str, ...]:
    return values


@environment.task
def keywords(**values: str) -> dict[str, str]:
    return values


@environment.task
def unsupported(value: datetime) -> datetime:
    return value


@environment.task
def invalid_keys(value: dict[int, str]) -> dict[int, str]:
    return value


def test_export_schema() -> None:
    schema = search.tool_schema()
    assert schema["name"] == "search"
    assert schema["description"] == "Search suppliers."
    inputs = schema["input_schema"]
    assert inputs["required"] == ["supplier", "filters"]
    assert inputs["additionalProperties"] is False
    assert inputs["properties"]["supplier"]["enum"] == ["atlas", "beacon", "cedar"]
    assert inputs["properties"]["limit"]["default"] == 10
    validator = Draft202012Validator(inputs)
    validator.validate({"supplier": "atlas", "filters": [{"active": True}], "note": None})
    with pytest.raises(ValidationError):
        validator.validate({"supplier": "atlas", "filters": [{"active": "yes"}]})
    with pytest.raises(ValidationError):
        validator.validate({"supplier": "atlas"})
    with pytest.raises(ValidationError):
        validator.validate({"supplier": "atlas", "filters": [], "extra": 1})


@pytest.mark.parametrize(
    "task", [missing, positional, variadic, keywords, unsupported, invalid_keys]
)
def test_reject_unsupported(task: Task[..., object]) -> None:
    with pytest.raises(TypeError):
        task.tool_schema()


@environment.task
def unusual_names(
    _private: str,
    *,
    model_config: bool,
    __doc__: int = 1,  # ruff: ignore[builtin-argument-shadowing]
) -> str:
    return f"{_private} {model_config} {__doc__}"


def test_preserve_python_parameter_names() -> None:
    schema = unusual_names.tool_schema()["input_schema"]
    assert schema["required"] == ["_private", "model_config"]
    assert set(schema["properties"]) == {"_private", "model_config", "__doc__"}
    assert schema["properties"]["__doc__"]["default"] == 1
