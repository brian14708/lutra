"""Load a bundled task for the stdio host."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import os
from typing import TYPE_CHECKING

from lutra._gen.lutra.v1.lutra_pb import SourceBundle, TaskImage, TaskSpec
from lutra.runtime import RunContext, run_context
from lutra.serve import TaskAPIClient, serve
from lutra.serve._host import _redirect_user_stdout
from lutra.task import Task
from lutra.value import dumps, loads

if TYPE_CHECKING:
    from collections.abc import Callable

_ARGUMENT_PARTS = 2


def _bundled_handler() -> Callable[..., object]:
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
        token = run_context.set(
            RunContext(api_client, task_spec, api_client.run_id or run_id, api_client.action_id)
        )
        try:
            return "application/cbor", dumps(await target.function(*args, **kwargs))
        finally:
            run_context.reset(token)

    return handler


def main() -> None:
    """Load a task callable and serve it over the stdio protocol."""
    parser = argparse.ArgumentParser(description="Serve a Lutra task on stdio")
    parser.add_argument("callable", nargs="?", help="module:callable")
    args = parser.parse_args()
    _redirect_user_stdout()
    if args.callable:
        module_name, separator, name = args.callable.partition(":")
        if not separator or not module_name or not name:
            parser.error("callable must be module:callable")
        handler = getattr(importlib.import_module(module_name), name)
    else:
        handler = _bundled_handler()
    asyncio.run(serve(handler))


if __name__ == "__main__":
    main()
