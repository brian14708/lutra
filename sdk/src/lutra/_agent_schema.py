"""JSON value validation for agent outputs and tools."""

from __future__ import annotations

import json


def json_value(value: object) -> object:
    """Validate a JSON value without silently stringifying unsupported objects.

    Returns:
        The validated scalar or recursively validated collection.

    Raises:
        TypeError: If the value cannot be represented in JSON.

    """
    if value is None or type(value) in {str, bool, int, float}:
        json.dumps(value, allow_nan=False)
        return value
    if isinstance(value, list):
        return [json_value(item) for item in value]
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return {key: json_value(item) for key, item in value.items()}
    message = f"unsupported JSON value: {type(value).__name__}"
    raise TypeError(message)
