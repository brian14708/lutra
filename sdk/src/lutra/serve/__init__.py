"""Serve task calls over multiplexed newline-delimited JSON on stdio."""

from lutra.serve._host import (  # ruff: ignore[unused-import]
    StdioTransport,
    TaskAPIClient,
    _Host,
    _redirect_user_stdout,
    _TaskService,
    normalize_result,
    serve,
)
