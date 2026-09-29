"""Load a bundled task for the stdio host."""

from __future__ import annotations

import asyncio
import importlib
import os

from lutra._gen.lutra.v1.lutra_pb import SourceBundle, TaskImage, TaskSpec
from lutra.runtime import RunContext, run_context
from lutra.task import Task
from lutra.task_host import TaskAPIClient, serve
from lutra.value import dumps, loads

_ARGUMENT_PARTS = 2


def main() -> None:
    module = importlib.import_module(os.environ["LUTRA_TASK_MODULE"])
    target: object = module
    for part in os.environ["LUTRA_TASK_QUALNAME"].split("."):
        target = getattr(target, part)
    if not isinstance(target, Task):
        message = "entry point is not a Lutra task"
        raise TypeError(message)
    task_spec = TaskSpec(
        project=os.environ["LUTRA_TASK_PROJECT"],
        domain=os.environ["LUTRA_TASK_DOMAIN"],
        name=os.environ["LUTRA_TASK_NAME"],
        module=os.environ["LUTRA_TASK_MODULE"],
        qualname=os.environ["LUTRA_TASK_QUALNAME"],
        version=os.environ["LUTRA_TASK_VERSION"],
        source=SourceBundle(uri=os.environ["LUTRA_TASK_SOURCE_URI"]),
        image=TaskImage(name=os.environ["LUTRA_TASK_IMAGE"]),
    )

    async def handler(
        run_id: str, content_type: str, payload: bytes, api_client: TaskAPIClient
    ) -> object:
        if content_type != "application/cbor":
            message = "expected CBOR input"
            raise ValueError(message)
        arguments = await loads(payload, api_client.resolve_blob)
        if (
            not isinstance(arguments, list)
            or len(arguments) != _ARGUMENT_PARTS
            or not isinstance(arguments[0], list)
            or not isinstance(arguments[1], dict)
            or not all(isinstance(key, str) for key in arguments[1])
        ):
            message = "invalid task arguments"
            raise ValueError(message)
        args, kwargs = arguments
        token = run_context.set(RunContext(api_client, task_spec, run_id))
        try:
            return "application/cbor", dumps(await target.function(*args, **kwargs))
        finally:
            run_context.reset(token)

    asyncio.run(serve(handler))


if __name__ == "__main__":
    main()
