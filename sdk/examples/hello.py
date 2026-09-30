"""Run a greeting graph with nested retries against a local Lutra server.

Start a Lutra server, then run ``uv run examples/hello.py`` from the SDK directory.
"""

from __future__ import annotations

import asyncio
import logging
import os

import lutra


@lutra.task
async def greeting(name: str) -> str:
    await asyncio.sleep(0)
    print(f"Greeting {name}")
    return f"Hello, {name}!"


@lutra.task
async def retry_once(value: str) -> str:
    await asyncio.sleep(0)
    print(f"Retry task attempt {os.environ.get('LUTRA_ATTEMPT')}")
    if os.environ.get("LUTRA_ATTEMPT") == "1":
        message = "intentional first attempt failure"
        raise RuntimeError(message)
    return value


@lutra.task
async def greet_person(name: str) -> str:
    prepared = await lutra.run(retry_once(name))
    return await lutra.run(greeting(prepared))


@lutra.task
async def summarize(greetings: list[str]) -> str:
    await asyncio.sleep(0)
    print(f"Combining {len(greetings)} greetings")
    return " | ".join(greetings)


@lutra.task
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
