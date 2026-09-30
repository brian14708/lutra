"""Subprocess fixture for the Go stdio transport test."""

import asyncio
import logging
import sys

from lutra.serve import TaskAPIClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("task module loaded")


async def echo(
    invocation_id: str, content_type: str, payload: bytes, api_client: TaskAPIClient
) -> tuple[str, bytes]:
    sys.stderr.write("task started\n")
    logger.info("task handler called")
    if invocation_id == "wait":
        await asyncio.Event().wait()
    reply = await api_client.unary("/test.Reverse/Echo", {"message": invocation_id})
    if reply.get("message") != invocation_id:
        message = "task API call returned an unexpected message"
        raise ValueError(message)
    return content_type, payload
