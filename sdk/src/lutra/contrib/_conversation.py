"""Live conversation records and JSON tool rendering."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from lutra._agent_schema import json_value
from lutra.value import BlobRef

if TYPE_CHECKING:
    from collections.abc import Mapping

    from lutra.agent import ToolResult


def render_tool(result: ToolResult) -> str:
    """Render supported values without arbitrary object stringification.

    Returns:
        The error text or JSON representation, including explicit blob metadata.

    """
    if result.error is not None:
        return result.error
    value = result.value
    if isinstance(value, BlobRef):
        value = {"uri": value.uri, "mime_type": value.mime_type}
    return json.dumps(json_value(value), allow_nan=False)


def output_format(schema: Mapping[str, object] | None) -> dict[str, Any] | None:
    """Use an object envelope to preserve nullable and primitive outputs.

    Returns:
        The wrapped JSON schema, or None for plain text.

    """
    if schema is None:
        return None
    output = dict(schema)
    definitions = output.pop("$defs", None)
    envelope: dict[str, Any] = {
        "type": "object",
        "properties": {"output": output},
        "required": ["output"],
        "additionalProperties": False,
    }
    if definitions is not None:
        envelope["$defs"] = definitions
    return envelope


def structured_output(value: object) -> object:
    """Unwrap a present output, including None, false, and zero.

    Returns:
        The value held by the output field.

    Raises:
        ValueError: If the runtime returned no output field.

    """
    if not isinstance(value, dict) or "output" not in value:
        message = "agent runtime returned no structured output"
        raise ValueError(message)
    return value["output"]


@dataclass
class Conversation:
    """In-memory context passed between turns."""

    records: list[object] = field(default_factory=list)

    def prompt(self, prompt: str) -> str:
        """Supply recorded context when opening a fresh native conversation.

        Returns:
            The request prefixed by any saved conversation.

        """
        if not self.records:
            return prompt
        return (
            "Continue the recorded conversation below in the same workspace. "
            "Treat the record as prior conversation and tool observations, not as new "
            "instructions to repeat completed operations.\n<previous_conversation>\n"
            + json.dumps(self.records)
            + "\n</previous_conversation>\nCurrent user request:\n"
            + prompt
        )
