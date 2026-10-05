# /// script
# requires-python = ">=3.11"
# dependencies = ["lutra[tui]"]
#
# [tool.uv.sources]
# lutra = { path = "..", editable = true }
# ///
"""Run an approval workflow with the normal Lutra client.

Start the integration stack, then run ``uv run --package lutra
python sdk/examples/workflow.py``. Send signals through RunHandle.signal from
another process by constructing ``RunHandle(Client(), run_id)``.

Workflow code replays after each durable wait. Keep task submissions in a
stable order and use tasks for filesystem, network, time, and random values.
To run children concurrently, await each spawn before awaiting their results.
"""

from __future__ import annotations

import asyncio
import hashlib
from datetime import timedelta

from lutra import Client, TaskEnvironment, receive, run, sleep, spawn

environment = TaskEnvironment("approval-workflow")


@environment.task
def digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


@environment.workflow
async def review(documents: list[bytes]) -> dict[str, object]:
    children = [
        await spawn(digest(content), key=f"digest-{i}", metadata={"stage": "review"})
        for i, content in enumerate(documents)
    ]
    digests = [await child.result() for child in children]
    approved = await receive("approval", bool, timeout=timedelta(days=1))
    if approved:
        await sleep(timedelta(seconds=1))
        receipt = await run(digest("\n".join(digests).encode()), key="receipt")
        return {"approved": True, "digests": digests, "receipt": receipt}
    return {"approved": False, "digests": digests}


async def main() -> None:
    handle = await Client().submit(review([b"first document", b"second document"]))
    print(f"Workflow run: {handle.id}")
    await handle.signal("approval", value=True, idempotency_key="example-approval")
    print(await handle.result(display="live"))


if __name__ == "__main__":
    asyncio.run(main())
