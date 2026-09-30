"""Run log records delivered through WatchRun."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest
from lutra._gen.lutra.v1.log_pb import LogRecord
from lutra._gen.lutra.v1.lutra_pb import Run, WatchRunResponse
from lutra.client import Client, LogEvent, RunHandle
from lutra.value import dumps

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from lutra._gen.lutra.v1.lutra_pb import WatchRunRequest


@pytest.mark.asyncio
async def test_logs_resume_through_watch_run(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Client()
    requests: list[WatchRunRequest] = []

    async def watch(request: WatchRunRequest) -> AsyncIterator[WatchRunResponse]:
        await asyncio.sleep(0)
        requests.append(request)
        yield WatchRunResponse(run=Run(id="run", status="succeeded"))
        yield WatchRunResponse(
            log=LogRecord(
                stream="task_log",
                seq=9,
                key=b"action",
                created_unix_nanos=123,
                value_cbor=dumps({"type": "task.log.v1", "message": "hello"}),
            )
        )

    monkeypatch.setattr(client.rpc, "watch_run", watch)
    handle: RunHandle[object] = RunHandle(client, "run")
    records = [record async for record in handle.logs(after_seq=8, key_prefix=b"action")]
    assert records == [
        LogEvent(
            stream="task_log",
            sequence=9,
            key=b"action",
            event={"type": "task.log.v1", "message": "hello"},
            created_unix_nanos=123,
        )
    ]
    assert records[0].message == "hello"
    assert requests[0].id == "run"
    cursor = requests[0].task_logs
    assert cursor is not None
    assert (cursor.stream, cursor.after_seq, cursor.key_prefix) == ("task_log", 8, b"action")
