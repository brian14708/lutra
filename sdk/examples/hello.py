"""Run a greeting graph with nested retries against a local Lutra server.

Start a Lutra server, then run ``uv run examples/hello.py`` from the SDK directory.
"""

from __future__ import annotations

import asyncio
import logging
import os

import lutra

environment = lutra.TaskEnvironment(name="greetings")


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
    prepared = await lutra.run(retry_once(name), max_attempts=4)
    return await lutra.run(greeting(prepared))


@environment.task
async def summarize(greetings: list[str]) -> str:
    await asyncio.sleep(0)
    print(f"Combining {len(greetings)} greetings")
    return " | ".join(greetings)


@environment.task
async def hello(names: list[str]) -> str:
    greetings = [await lutra.run(greet_person(name)) for name in names]
    return await lutra.run(summarize(greetings))


async def main() -> None:
    client = lutra.Client(os.environ.get("LUTRA_URL", "http://127.0.0.1:8080/api"))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("lutra")
    await client.run(hello(["Lutra", "Python"]), logger=logger)


if __name__ == "__main__":
    asyncio.run(main())
