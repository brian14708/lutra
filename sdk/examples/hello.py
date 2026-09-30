"""Run a greeting task and a retried task against a local Lutra server.

Start a Lutra server, then run ``python -m sdk.examples.hello`` from the repository root.
"""

from __future__ import annotations

import asyncio
import os

import lutra


@lutra.task
async def greeting(name: str) -> str:
    await asyncio.sleep(0)
    return f"Hello, {name}!"


@lutra.task
async def hello(name: str) -> str:
    return await lutra.run(greeting(name))


@lutra.task
async def retry_once(value: str) -> str:
    await asyncio.sleep(0)
    if os.environ.get("LUTRA_ATTEMPT") == "1":
        message = "intentional first attempt failure"
        raise RuntimeError(message)
    return value


async def main() -> None:
    client = lutra.Client(os.environ.get("LUTRA_URL", "http://127.0.0.1:8080/api"))
    print(await client.run(hello("Lutra")))
    print(await client.run(retry_once("retry-ok")))


if __name__ == "__main__":
    asyncio.run(main())
