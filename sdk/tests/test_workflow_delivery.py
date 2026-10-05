"""Generated client delivery order includes asynchronous child result decoding."""

from __future__ import annotations

import asyncio
import base64
import hashlib
from typing import TYPE_CHECKING, Any, cast

import pytest
from lutra import CacheableError
from lutra._result import encode_failure
from lutra.runtime import ChildHandle, RunContext, run_context
from lutra.value import BlobRef, dumps
from lutra.workflow import workflow_state

if TYPE_CHECKING:
    from lutra.serve import TaskAPIClient


@pytest.mark.asyncio
@pytest.mark.parametrize("slow_key", ["first", "second"])
async def test_child_blob_latency_does_not_change_following_command_order(slow_key: str) -> None:
    calls: list[tuple[str, str, str]] = []
    digests: dict[str, str] = {}
    downloads: list[str] = []

    class API:
        delivery = 0

        async def unary(self, path: str, value: dict[str, Any]) -> dict[str, Any]:
            self.delivery += 1
            metadata = {"delivery": str(self.delivery)}
            if path.endswith("WorkflowWait"):
                reference = value["futures"][0]
                key = reference["key"]
                calls.append((value["sequence"], "wait", key))
                digest = base64.b64encode(hashlib.sha256(key.encode()).digest()).decode()
                digests[digest] = key
                payload = dumps(BlobRef("blob:" + digest, resolve=True))
                return {
                    "workflowMeta": metadata,
                    "completions": [
                        {"future": reference, "valueCbor": base64.b64encode(payload).decode()}
                    ],
                }
            calls.append((value["sequence"], "set", value["key"]))
            return {"workflowMeta": metadata}

        @staticmethod
        async def resolve_blob(uri: str) -> bytes:
            key = digests[uri.rsplit(",", 1)[-1]]
            await asyncio.sleep(0.02 if key == slow_key else 0)
            downloads.append(key)
            return key.encode()

    api = cast("TaskAPIClient", API())
    context = RunContext(api, {}, "run", "parent", workflow=True)
    token = run_context.set(context)

    async def branch(key: str) -> bytes:
        child = ChildHandle[bytes](key, context.client(), api, key, context)
        value = await child.result()
        await workflow_state().set(key, value)
        return value

    try:
        assert await asyncio.gather(branch("first"), branch("second")) == [b"first", b"second"]
        assert calls == [
            ("1", "wait", "first"),
            ("2", "wait", "second"),
            ("3", "set", "first"),
            ("4", "set", "second"),
        ]
        assert downloads == ["first", "second"]
    finally:
        run_context.reset(token)


@pytest.mark.asyncio
async def test_equal_child_outputs_do_not_share_mutable_decoded_values() -> None:
    class API:
        delivery = 0

        async def unary(self, _path: str, value: dict[str, Any]) -> dict[str, Any]:
            self.delivery += 1
            return {
                "workflowMeta": {"delivery": str(self.delivery)},
                "completions": [
                    {
                        "future": value["futures"][0],
                        "valueCbor": base64.b64encode(dumps({"items": []})).decode(),
                    }
                ],
            }

        @staticmethod
        async def resolve_blob(_uri: str) -> bytes:
            message = "unexpected blob resolution"
            raise AssertionError(message)

    api = cast("TaskAPIClient", API())
    context = RunContext(api, {}, "run", "parent", workflow=True)
    token = run_context.set(context)
    try:
        first = ChildHandle[dict[str, list[str]]]("first", context.client(), api, "first", context)
        second = ChildHandle[dict[str, list[str]]](
            "second", context.client(), api, "second", context
        )
        value = await first.result()
        value["items"].append("changed locally")
        assert await second.result() == {"items": []}
        assert await first.result() == {"items": []}
    finally:
        run_context.reset(token)


@pytest.mark.asyncio
async def test_equal_child_errors_do_not_share_mutable_details() -> None:
    payload = encode_failure(cacheable=True, message="denied", details={"items": []})

    class API:
        delivery = 0

        async def unary(self, _path: str, value: dict[str, Any]) -> dict[str, Any]:
            self.delivery += 1
            return {
                "workflowMeta": {"delivery": str(self.delivery)},
                "completions": [
                    {"future": value["futures"][0], "valueCbor": base64.b64encode(payload).decode()}
                ],
            }

        @staticmethod
        async def resolve_blob(_uri: str) -> bytes:
            message = "unexpected blob resolution"
            raise AssertionError(message)

    api = cast("TaskAPIClient", API())
    context = RunContext(api, {}, "run", "parent", workflow=True)
    token = run_context.set(context)
    try:
        first = ChildHandle[object]("first", context.client(), api, "first", context)
        second = ChildHandle[object]("second", context.client(), api, "second", context)
        with pytest.raises(CacheableError, match="denied") as first_error:
            await first.result()
        details = first_error.value.details
        assert isinstance(details, dict)
        details["items"].append("changed locally")
        with pytest.raises(CacheableError, match="denied") as second_error:
            await second.result()
        assert second_error.value.details == {"items": []}
        assert second_error.value is not first_error.value
    finally:
        run_context.reset(token)
