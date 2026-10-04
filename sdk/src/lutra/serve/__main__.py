"""Load a bundled task for the stdio host."""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import importlib
import inspect
import json
import os
import re
import runpy
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING

from lutra._context import current_context, task_context
from lutra._gen.lutra.v1.lutra_pb import EnvironmentIdentifier
from lutra.runtime import RunContext, run_context
from lutra.serve import TaskAPIClient, serve
from lutra.serve._host import _redirect_user_stdout
from lutra.task import ConfigError, RetryMode, Task
from lutra.value import dumps, loads

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

_ARGUMENT_PARTS = 2
_MAX_CONFIG_BYTES = 64 << 10


async def _load_config() -> dict[str, object]:
    try:
        payload = base64.b64decode(os.environ.get("LUTRA_CONFIG_CBOR", "oA=="), validate=True)
        value = await loads(payload)
    except (ValueError, TypeError, binascii.Error):
        raise ConfigError from None
    if len(payload) > _MAX_CONFIG_BYTES or not isinstance(value, dict) or dumps(value) != payload:
        raise ConfigError
    config: dict[str, object] = {}
    for name, entry in value.items():
        if not isinstance(name, str) or re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name) is None:
            raise ConfigError
        _validate_binding(entry)
        if entry["present"]:
            config[name] = entry["value"]
    return config


def _validate_binding(entry: object) -> None:
    if not isinstance(entry, dict) or set(entry) != {"present", "value", "sensitive", "required"}:
        raise ConfigError
    if any(type(entry[key]) is not bool for key in ("present", "sensitive", "required")):
        raise ConfigError
    if entry["required"] and not entry["present"]:
        code = "config.missing"
        raise ConfigError(code)
    if not entry["present"] and entry["value"] is not None:
        raise ConfigError


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


def _load_entrypoint(entrypoint: str) -> Task[..., object]:
    if entrypoint.startswith("file:"):
        relative, separator, qualname = entrypoint[5:].partition(":")
        path = Path(relative)
        if (
            not separator
            or not relative
            or not qualname
            or path.is_absolute()
            or ".." in path.parts
        ):
            message = "entrypoint file must be a relative bundle path"
            raise ValueError(message)
        bundle_root = Path(os.environ.get("LUTRA_BUNDLE_ROOT", "."))
        target: object = runpy.run_path(str(bundle_root / path), run_name="__lutra_task__")
    else:
        module_name, separator, qualname = entrypoint.partition(":")
        if not separator or not module_name or not qualname:
            message = "entrypoint must be module:qualname"
            raise ValueError(message)
        target = importlib.import_module(module_name)
    for part in qualname.split("."):
        target = target[part] if isinstance(target, dict) else getattr(target, part)
    if not isinstance(target, Task):
        message = "entry point is not a Lutra task"
        raise TypeError(message)
    return target


def _bundled_handler(
    entrypoint: str,
) -> tuple[Callable[..., Awaitable[tuple[str, bytes]]], RetryMode]:
    environments = _load_environments()

    async def handler(
        run_id: str, content_type: str, payload: bytes, api_client: TaskAPIClient
    ) -> tuple[str, bytes]:
        config = await _load_config()
        task_context.set(replace(current_context(), config=MappingProxyType(config)))
        target = _load_entrypoint(entrypoint)
        if {binding.name for binding in target.config} != set(config):
            optional = {binding.name for binding in target.config if not binding.required}
            required = {binding.name for binding in target.config if binding.required}
            if not required <= set(config) or not set(config) <= required | optional:
                raise ConfigError
        task_context.set(replace(current_context(), retry=target.retry))
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

    return handler, RetryMode.NONE


def main() -> None:
    """Load a task callable and serve it over the stdio protocol."""
    parser = argparse.ArgumentParser(description="Serve a Lutra task on stdio")
    parser.add_argument("callable", help="module:callable")
    args = parser.parse_args()
    _redirect_user_stdout()
    if "LUTRA_ENVIRONMENTS_JSON" in os.environ:
        handler, retry = _bundled_handler(args.callable)
    else:
        module_name, separator, name = args.callable.partition(":")
        if not separator or not module_name or not name:
            parser.error("callable must be module:callable")
        handler = getattr(importlib.import_module(module_name), name)
        retry = RetryMode.NONE
    asyncio.run(serve(handler, retry=retry))


if __name__ == "__main__":
    main()
