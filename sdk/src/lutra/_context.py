"""Context of one task host execution."""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
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
