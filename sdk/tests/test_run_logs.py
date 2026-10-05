"""Run log records delivered through WatchRun."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import pytest
from lutra._gen.lutra.v1.log_pb import LogRecord
from lutra._gen.lutra.v1.lutra_pb import Run, TaskActionStatus, WatchRunResponse
from lutra._live import LiveDisplay
from lutra.client import Client, LogEvent, RunHandle, _RunLogger
from lutra.value import dumps

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from lutra._gen.lutra.v1.lutra_pb import WatchRunRequest


def test_entrypoint_names_disambiguate_child_environments(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("lutra-test-names")
    writer = _RunLogger(logger, "run", "training_workflow", {1: "ambiguous"})
    view = LiveDisplay("run", "training_workflow", {1: "ambiguous"})
    with caplog.at_level(logging.INFO, logger=logger.name):
        for action, name in [("training", "train"), ("evaluation", "evaluate")]:
            event = TaskActionStatus(
                action_id=action, entrypoint_id=1, entrypoint_name=name, status="running"
            )
            writer.event(event)
            view.event(event)
            assert writer.action_names[action] == name
            assert view.tasks[action].name == name
    assert "train [" in caplog.text
    assert "evaluate [" in caplog.text
    legacy = TaskActionStatus(action_id="legacy", entrypoint_id=2, status="running")
    writer.event(legacy)
    view.event(legacy)
    assert writer.action_names["legacy"] == "entrypoint #2"
    assert view.tasks["legacy"].name == "entrypoint #2"


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
