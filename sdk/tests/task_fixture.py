"""Subprocess fixture for the Go stdio transport test."""

import asyncio
import sys

from lutra.task_host import ReverseClient

print("task module loaded")  # ruff: ignore[print]


async def echo(
    invocation_id: str, content_type: str, payload: bytes, reverse: ReverseClient
) -> tuple[str, bytes]:
    sys.stderr.write("task started\n")
    print("task handler called")  # ruff: ignore[print]
    if invocation_id == "wait":
        await asyncio.Event().wait()
    reply = await reverse.unary("/test.Reverse/Echo", {"message": invocation_id})
    if reply.get("message") != invocation_id:
        message = "reverse call returned an unexpected message"
        raise ValueError(message)
    return content_type, payload
