"""Default subprocess environment for agents running inside Lutra tasks."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping


def agent_env(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Disable telemetry unless the caller explicitly overrides a flag.

    Returns:
        Environment flags merged with caller overrides.

    """
    return {
        "DO_NOT_TRACK": "1",
        "OTEL_SDK_DISABLED": "true",
        "DISABLE_TELEMETRY": "1",
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
        **(env or {}),
    }
