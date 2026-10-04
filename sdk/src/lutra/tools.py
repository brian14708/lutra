"""Provider-neutral task tool descriptions."""

from typing import Any
from typing_extensions import TypedDict


class ToolSchema(TypedDict):
    """A task name, description, and JSON Schema for keyword arguments."""

    name: str
    description: str
    input_schema: dict[str, Any]
