# /// script
# requires-python = ">=3.11"
# dependencies = ["lutra[tui]"]
#
# [tool.uv.sources]
# lutra = { path = "..", editable = true }
# ///
"""Run a greeting graph with nested retries against a local Lutra server.

Start a Lutra server, then run ``uv run examples/hello.py`` from the SDK directory.
"""

from __future__ import annotations

import asyncio
import os

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


@environment.task
async def greet_person(name: str) -> str:
    prepared = await lutra.run(retry_once(name), key="prepare", max_attempts=4)
    return await lutra.run(greeting(prepared), key="greeting")


@environment.task
async def summarize(greetings: list[str]) -> str:
    await asyncio.sleep(0)
    print(f"Combining {len(greetings)} greetings")
    return " | ".join(greetings)


@environment.task
async def hello(names: list[str]) -> str:
    handles = await asyncio.gather(
        *(
            lutra.spawn(greet_person(name), key=f"person:{index}")
            for index, name in enumerate(names)
        )
    )
    greetings = await asyncio.gather(*(handle.result() for handle in handles))
    return await lutra.run(summarize(greetings), key="summary")


async def main() -> None:
    client = lutra.Client(os.environ.get("LUTRA_URL", "http://127.0.0.1:8080/api"))
    await client.run(hello(["Lutra", "Python"]), display="live")


if __name__ == "__main__":
    asyncio.run(main())
