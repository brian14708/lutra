"""Opt-in terminal rendering for a run's event stream."""

from __future__ import annotations

import logging
import sys
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

from lutra._gen.lutra.v1.lutra_pb import Run, TaskActionStatus
from lutra._result import failure_message
from lutra.client import (
    LogEvent,
    _agent_message,
    _entrypoint_name,
    _id_suffix,
    _preview,
    _RunLogger,
    _task_display_status,
)

if TYPE_CHECKING:
    from types import TracebackType

    from rich.console import Group, RenderableType
    from rich.live import Live
    from rich.tree import Tree

_RECENT_LOGS = 12
_INSTANCE_COLORS = (
    "bright_cyan",
    "bright_magenta",
    "bright_yellow",
    "bright_green",
    "bright_blue",
    "bright_red",
    "cyan",
    "magenta",
    "yellow",
    "green",
    "blue",
    "red",
)
_STATUS_STYLE = {
    "queued": "dim",
    "building": "bright_blue",
    "running": "cyan",
    "waiting": "yellow",
    "succeeded": "green",
    "cached": "bold bright_magenta",
    "failed": "red",
    "canceled": "red",
}


@dataclass
class _Task:
    name: str
    parent_id: str
    status: str
    attempt: int
    max_attempts: int
    error: str


class LiveDisplay:
    """Render streamed run events in a TTY, or as plain lines when piped."""

    def __init__(self, run_id: str, task_name: str, entrypoint_names: dict[int, str]) -> None:
        self.run_id = run_id
        self.task_name = task_name
        self.entrypoint_names = entrypoint_names
        self.status = "preparing"
        self.target = task_name
        self.error = ""
        self.result_text = ""
        self.root_action_id = ""
        self.tasks: dict[str, _Task] = {}
        self.logs: deque[tuple[str, str, str]] = deque(maxlen=_RECENT_LOGS)
        self._instance_colors: dict[str, str] = {}
        self._live: Live | None = None
        self._plain: _RunLogger | None = None
        self._handler: logging.Handler | None = None

    def __enter__(self) -> Self:
        if sys.stderr.isatty():
            try:
                from rich.console import Console  # ruff: ignore[import-outside-top-level]
                from rich.live import Live  # ruff: ignore[import-outside-top-level]
            except ImportError:
                pass
            else:
                self._live = Live(
                    self._render(), console=Console(file=sys.stderr), refresh_per_second=4
                )
                self._live.__enter__()
                return self
        logger = logging.getLogger(f"lutra.live.{id(self)}")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        self._handler = logging.StreamHandler(sys.stderr)
        self._handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        logger.addHandler(self._handler)
        self._plain = _RunLogger(logger, self.run_id, self.task_name, self.entrypoint_names)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._live is not None:
            self._live.__exit__(exc_type, exc_value, traceback)
        if self._handler is not None and self._plain is not None:
            self._plain.logger.removeHandler(self._handler)
            self._handler.close()

    def prepare(self, target: str) -> None:
        self.target = target
        if self._plain is not None:
            self._plain.logger.info("submit   %s", target)
        self._refresh()

    def event(self, event: Run | LogEvent | TaskActionStatus) -> None:
        if self._plain is not None:
            self._plain.run_id = self.run_id
            self._plain.event(event)
            return
        if isinstance(event, Run):
            self.status = event.status
            self.root_action_id = event.root_action_id or self.root_action_id
            self.error = failure_message(event.result_cbor)
            if event.environment is not None:
                self.target = f"{event.environment.name}.{self.task_name}"
        elif isinstance(event, TaskActionStatus):
            name = (
                self.task_name
                if event.action_id == self.root_action_id and self.task_name
                else event.entrypoint_name
                or _entrypoint_name(self.entrypoint_names, event.entrypoint_id)
            )
            self.tasks[event.action_id] = _Task(
                name,
                event.caller_action_id,
                _task_display_status(event),
                event.attempt,
                event.max_attempts,
                failure_message(event.result_cbor),
            )
        else:
            if not isinstance(event.event, dict):
                msg = "invalid task log event"
                raise TypeError(msg)
            action = str(event.event.get("action_id", event.key.decode(errors="replace")))
            name = self.tasks[action].name if action in self.tasks else "task"
            label = f"{name} [{_id_suffix(action)}]"
            phase = str(event.event.get("phase", "task"))
            prefix = f"{phase} · " if phase in {"pull", "build"} else ""
            message = _agent_message(event.event) if event.stream == "agent" else event.message
            for line in message.splitlines() or [""]:
                self.logs.append((action, label, f"{prefix}{line}"))
        self._refresh()

    def finish(self, result: object) -> None:
        self.status = "succeeded"
        self.result_text = _preview(result)
        if self._plain is not None:
            self._plain.logger.info("result   %s", self.result_text)
        self._refresh()

    def fail(self, error: str) -> None:
        self.status = "failed"
        self.error = error
        if self._plain is not None and error != self._plain.last_error:
            self._plain.logger.error("run      %s", error)
        self._refresh()

    def _refresh(self) -> None:
        if self._live is not None:
            self._live.update(self._render(), refresh=False)

    def _render(self) -> Group:  # ruff: ignore[complex-structure]
        from rich.console import Group  # ruff: ignore[import-outside-top-level]
        from rich.panel import Panel  # ruff: ignore[import-outside-top-level]
        from rich.text import Text  # ruff: ignore[import-outside-top-level]
        from rich.tree import Tree  # ruff: ignore[import-outside-top-level]

        heading = Text(f"{self.target}  [{_id_suffix(self.run_id)}]  {self.status}")
        heading.stylize(_STATUS_STYLE.get(self.status, "cyan"))
        if self.error:
            heading.append(f" · {self.error}", style="red")
        tree = Tree(heading)
        children: dict[str, list[str]] = {}
        for action_id, task in self.tasks.items():
            parent = task.parent_id if task.parent_id in self.tasks else ""
            children.setdefault(parent, []).append(action_id)

        def add_tasks(parent: Tree, parent_id: str, ancestors: set[str]) -> None:
            for action_id in children.get(parent_id, []):
                if action_id in ancestors:
                    continue
                task = self.tasks[action_id]
                label = Text(f"{task.name} [{_id_suffix(action_id)}]  {task.status}")
                label.stylize(_STATUS_STYLE.get(task.status, "white"))
                if task.max_attempts > 1 and task.attempt:
                    label.append(f" · attempt {task.attempt}/{task.max_attempts}")
                if task.error and task.status in {"failed", "canceled", "queued"}:
                    label.append(f" · {task.error}", style="red")
                branch = parent.add(label)
                add_tasks(branch, action_id, ancestors | {action_id})

        add_tasks(tree, "", set())
        logs = Text()
        for index, (action, label, message) in enumerate(self.logs):
            if index:
                logs.append("\n")
            color = self._instance_colors.setdefault(
                action, _INSTANCE_COLORS[len(self._instance_colors) % len(_INSTANCE_COLORS)]
            )
            logs.append(label, style=color)
            logs.append(f": {message}")
        if not self.logs:
            logs.append("Waiting for task output…")
        parts: list[RenderableType] = [tree, Panel(logs, title="Recent output", border_style="dim")]
        if self.result_text:
            parts.append(Text(f"Result: {self.result_text}", style="green"))
        return Group(*parts)
