"""Example task returning a greeting from CBOR parameters."""

from __future__ import annotations

from typing import TYPE_CHECKING

from connectrpc.code import Code
from connectrpc.errors import ConnectError

from lutra.value import loads

if TYPE_CHECKING:
    from lutra.task_host import ReverseClient


_MAX_NAME_LENGTH = 100


async def hello(
    _invocation_id: str, content_type: str, payload: bytes, _reverse: ReverseClient
) -> str:
    """Unpack a name and return its greeting.

    Returns:
        The greeting string.

    Raises:
        ConnectError: If the parameters are invalid.
    """
    if content_type != "application/cbor":
        raise ConnectError(Code.INVALID_ARGUMENT, "parameters must be application/cbor")
    try:
        parameters = await loads(payload)
    except (TypeError, ValueError, EOFError) as error:
        raise ConnectError(Code.INVALID_ARGUMENT, "invalid CBOR parameters") from error
    if not isinstance(parameters, dict):
        raise ConnectError(Code.INVALID_ARGUMENT, "parameters must be an object")
    name = parameters.get("name")
    if not isinstance(name, str) or not name.strip() or len(name) > _MAX_NAME_LENGTH:
        raise ConnectError(
            Code.INVALID_ARGUMENT, "name must be a nonempty string of at most 100 characters"
        )

    return f"Hello, {name}!"
