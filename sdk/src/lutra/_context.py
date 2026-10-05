"""Context of one task host execution."""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING

from lutra._gen.lutra.v1.lutra_pb import TaskSignalWorkflowRequest
from lutra._workflow import encode_signal, validate_signal_name

if TYPE_CHECKING:
    from collections.abc import Mapping

    from lutra._gen.lutra.v1.log_connect import LogServiceClient
    from lutra._gen.lutra.v1.lutra_connect import LutraServiceClient
    from lutra.blob import BlobStore
    from lutra.checkpoint import CheckpointManager
    from lutra.task import RetryMode


@dataclass(frozen=True)
class TaskContext:
    """Action identity, retry policy, and checkpoint access for this attempt."""

    run_id: str
    action_id: str
    attempt: int
    retry: RetryMode
    checkpoint: CheckpointManager
    blobs: BlobStore
    config: Mapping[str, object] = field(default_factory=lambda: MappingProxyType[str, object]({}))

    workspace: Path = field(default_factory=Path.cwd)
    log: LogServiceClient | None = None
    _workflow_client: LutraServiceClient | None = field(default=None, repr=False, compare=False)

    async def signal_workflow(self, name: str, value: object) -> None:
        """Publish a one-shot value to this task's root workflow.

        Return after durable acceptance. Identical repeated values succeed;
        conflicting values fail. Transport failures should be retried with the
        same value before advancing task state.

        Raises:
            RuntimeError: If the task host has no workflow callback client.

        """
        validate_signal_name(name)
        data = encode_signal(value)
        if self._workflow_client is None:
            message = "task workflow callback is unavailable"
            raise RuntimeError(message)
        await self._workflow_client.task_signal_workflow(
            TaskSignalWorkflowRequest(name=name, value_cbor=data)
        )


task_context: ContextVar[TaskContext] = ContextVar("lutra_task_context")


def current_context() -> TaskContext:
    """Return the context of the running task.

    Returns:
        The current task context.

    Raises:
        RuntimeError: If called outside a running task.

    """
    try:
        return task_context.get()
    except LookupError as exc:
        message = "no active Lutra task"
        raise RuntimeError(message) from exc
