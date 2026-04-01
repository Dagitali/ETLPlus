"""
:mod:`etlplus.cli._commands.log` module.

Typer command for raw persisted history events.
"""

from __future__ import annotations

import typer

from .._handlers.history import history_handler
from ._app import app
from ._helpers import call_handler
from ._options import HistoryFollowOption
from ._options import HistoryLimitOption
from ._options import HistorySinceOption
from ._options import HistoryUntilOption
from ._options import RunIdOption
from ._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'log_cmd',
]


# SECTION: FUNCTIONS ======================================================== #


@app.command('log')
def log_cmd(
    ctx: typer.Context,
    run_id: RunIdOption = None,
    since: HistorySinceOption = None,
    until: HistoryUntilOption = None,
    limit: HistoryLimitOption = None,
    follow: HistoryFollowOption = False,
) -> int:
    """
    Inspect raw persisted local run events.

    This command provides a real-time view of raw persisted events, with
    optional filtering. For a normalized view of persisted run history, use the
    'history' command instead.
    Parameters
    ----------
    ctx : typer.Context
        Typer context.
    run_id : RunIdOption, optional
        Run ID for filtering log entries.
    since : HistorySinceOption, optional
        Lower timestamp bound for emitted records.
    until : HistoryUntilOption, optional
        Upper timestamp bound for emitted records.
    limit : HistoryLimitOption, optional
        Maximum number of log entries to emit.
    follow : HistoryFollowOption, optional
        Whether to keep polling for new matching events.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    return call_handler(
        history_handler,
        state=ensure_state(ctx),
        run_id=run_id,
        since=since,
        until=until,
        limit=limit,
        follow=follow,
        raw=True,
    )
