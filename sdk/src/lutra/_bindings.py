"""Shared helpers for task result bindings and artifact content digests."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

from lutra.serializer import deserialize

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from lutra._gen.lutra.v1.run_pb import InputBinding


def sha256_digest(data: bytes) -> str:
    """Return the algorithm-qualified digest used for artifact payloads.

    The format must byte-match the server's ``internal/digest`` package.

    Returns:
        The ``sha256:<hex>`` digest of *data*.
    """
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


async def extract_result(
    bindings: Iterable[InputBinding], download: Callable[[str], Awaitable[bytes]]
) -> object:
    """Deserialize the ``result`` binding, downloading its artifact when needed.

    Args:
        bindings: Output bindings of a finished action.
        download: Resolves an artifact ID to its payload bytes.

    Returns:
        The deserialized result, or ``None`` when no result binding exists.

    Raises:
        TypeError: If the result binding holds neither bytes nor an artifact ID.
    """
    binding = next((value for value in bindings if value.slot_name == "result"), None)
    if binding is None or binding.value is None:
        return None
    value = binding.value.value
    if isinstance(value, bytes):
        return deserialize(value)
    if not isinstance(value, str):
        message = "unsupported result binding"
        raise TypeError(message)
    return deserialize(await download(value))
