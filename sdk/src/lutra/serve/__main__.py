"""Load a bundled task for the stdio host."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import json
import os
from typing import TYPE_CHECKING

from lutra._gen.lutra.v1.lutra_pb import EnvironmentIdentifier
from lutra.runtime import RunContext, run_context
from lutra.serve import TaskAPIClient, serve
from lutra.serve._host import _redirect_user_stdout
from lutra.task import Task
from lutra.value import dumps, loads

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

_ARGUMENT_PARTS = 2


def _load_environments() -> dict[str, EnvironmentIdentifier]:
    environment_values = json.loads(os.environ["LUTRA_ENVIRONMENTS_JSON"])
    if not isinstance(environment_values, list):
        message = "LUTRA_ENVIRONMENTS_JSON must be an environment identifier list"
        raise TypeError(message)
    environments: dict[str, EnvironmentIdentifier] = {}
    for value in environment_values:
        identifier = EnvironmentIdentifier.from_json(json.dumps(value))
        if not identifier.name or identifier.name in environments:
            message = "environment identifier names must be nonempty and unique"
            raise ValueError(message)
        environments[identifier.name] = identifier
    return environments


def _bundled_handler(entrypoint: str) -> Callable[..., Awaitable[tuple[str, bytes]]]:
    module_name, separator, qualname = entrypoint.partition(":")
    if not separator or not module_name or not qualname:
        message = "entrypoint must be module:qualname"
        raise ValueError(message)
    module = importlib.import_module(module_name)
    target: object = module
    for part in qualname.split("."):
        target = getattr(target, part)
    if not isinstance(target, Task):
        message = "entry point is not a Lutra task"
        raise TypeError(message)
    environments = _load_environments()

    async def handler(
        run_id: str, content_type: str, payload: bytes, api_client: TaskAPIClient
    ) -> tuple[str, bytes]:
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
            RunContext(api_client, environments, api_client.run_id or run_id, api_client.action_id)
        )
        try:
            if inspect.iscoroutinefunction(target.function):
                result = target.function(*args, **kwargs)
            else:
                result = await asyncio.to_thread(target.function, *args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
            return "application/cbor", dumps(result)
        finally:
            run_context.reset(token)

    return handler


def main() -> None:
    """Load a task callable and serve it over the stdio protocol."""
    parser = argparse.ArgumentParser(description="Serve a Lutra task on stdio")
    parser.add_argument("callable", help="module:callable")
    args = parser.parse_args()
    _redirect_user_stdout()
    if "LUTRA_ENVIRONMENTS_JSON" in os.environ:
        handler = _bundled_handler(args.callable)
    else:
        module_name, separator, name = args.callable.partition(":")
        if not separator or not module_name or not name:
            parser.error("callable must be module:callable")
        handler = getattr(importlib.import_module(module_name), name)
    asyncio.run(serve(handler))


if __name__ == "__main__":
    main()
