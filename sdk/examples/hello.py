# /// script
# requires-python = ">=3.11"
# dependencies = ["lutra[tui]"]
#
# [tool.uv.sources]
# lutra = { path = "..", editable = true }
# ///
"""Run nested workflows, parallel children, retries, caching, signals, and timers.

Start a Lutra server, then run ``uv run examples/hello.py`` from the SDK directory.
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from typing_extensions import TypedDict

import lutra

environment = lutra.TaskEnvironment(
    name="greetings",
    image=lutra.TaskImage(from_image="docker.io/library/python:3.12-slim-bookworm"),
)


@environment.task(cache=True, version="0.1.0")
async def greeting(name: str) -> str:
    await asyncio.sleep(0)
    print(f"Greeting {name}")
    return f"Hello, {name}!"


@environment.task(retry=lutra.RetryMode.IDEMPOTENT, max_attempts=3)
async def retry_once(value: str) -> str:
    await asyncio.sleep(0)
    context = lutra.current_context()
    prior = await context.checkpoint.load("input", default=None)
    if prior is None:
        await context.checkpoint.save("input", value)
    await context.checkpoint.append("started", value, event_id="started")
    events = [event async for event in context.checkpoint.read("started")]
    print(f"Retry task attempt {context.attempt}, restored={prior!r}, events={len(events)}")
    if context.attempt == 1:
        message = "intentional first attempt failure"
        raise RuntimeError(message)
    return value


@environment.workflow
async def greet_person(name: str) -> str:
    prepared = await lutra.run(retry_once(name), key="prepare", max_attempts=4)
    return await lutra.run(greeting(prepared), key="greeting")


@environment.task
async def summarize(greetings: list[str]) -> str:
    await asyncio.sleep(0)
    print(f"Combining {len(greetings)} greetings")
    return " | ".join(greetings)


class HelloResult(TypedDict):
    message: str
    approved: bool
    reminder_timed_out: bool


@environment.workflow
async def hello(names: list[str]) -> HelloResult:
    # Spawn sequentially; the committed children execute concurrently.
    handles = [
        await lutra.spawn(greet_person(name), key=f"person:{index}")
        for index, name in enumerate(names)
    ]
    greetings = [await handle.result() for handle in handles]
    approved = await lutra.receive("approval", bool)
    await lutra.sleep(timedelta(milliseconds=50))
    reminder_timed_out = False
    try:
        await lutra.receive("reminder", str, timeout=timedelta(milliseconds=50))
    except TimeoutError:
        reminder_timed_out = True
    message = (
        await lutra.run(summarize(greetings), key="summary") if approved else "Greeting declined"
    )
    return {"message": message, "approved": approved, "reminder_timed_out": reminder_timed_out}


async def main() -> None:
    client = lutra.Client(os.environ.get("LUTRA_URL", "http://127.0.0.1:8080/api"))
    handle = await client.submit(hello(["Lutra", "Python", "Lutra"]))
    print(f"Workflow run: {handle.id}")
    await handle.signal("approval", value=True, idempotency_key="hello-approval")
    print(await handle.result(display="live"))


if __name__ == "__main__":
    asyncio.run(main())
