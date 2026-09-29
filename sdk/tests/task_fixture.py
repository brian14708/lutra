"""Subprocess fixture for the Go stdio transport test."""

import asyncio
import sys

from lutra.task_host import TaskAPIClient

print("task module loaded")  # ruff: ignore[print]


async def echo(
    invocation_id: str, content_type: str, payload: bytes, api_client: TaskAPIClient
) -> tuple[str, bytes]:
    sys.stderr.write("task started\n")
    print("task handler called")  # ruff: ignore[print]
    if invocation_id == "wait":
        await asyncio.Event().wait()
    reply = await api_client.unary("/test.Reverse/Echo", {"message": invocation_id})
    if reply.get("message") != invocation_id:
        message = "task API call returned an unexpected message"
        raise ValueError(message)
    return content_type, payload
