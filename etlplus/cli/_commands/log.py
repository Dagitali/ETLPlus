"""
:mod:`etlplus.cli._commands.log` module.

Typer command for raw persisted history events.
"""

from __future__ import annotations

import typer

from .._handlers import history_handler as handle_history
from ._helpers import call_handler
from ._state import ensure_state
from .app import app
from .options import HistoryFollowOption
from .options import HistoryLimitOption
from .options import HistorySinceOption
from .options import HistoryUntilOption
from .options import RunIdOption

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'handle_history',
    'log_cmd',
]


# SECTION: FUNCTIONS ======================================================== #


@app.command('log')
def log_cmd(
    ctx: typer.Context,
    follow: HistoryFollowOption = False,
    limit: HistoryLimitOption = None,
    run_id: RunIdOption = None,
    since: HistorySinceOption = None,
    until: HistoryUntilOption = None,
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
    follow : HistoryFollowOption, optional
        Whether to follow the log output in real-time.
    limit : HistoryLimitOption, optional
        Maximum number of log entries to return.
    run_id : RunIdOption, optional
        Run ID to filter log entries for.
    since : HistorySinceOption, optional
        Start date for log entries.
    until : HistoryUntilOption, optional
        End date for log entries.

    Returns
    -------
    int
        Exit code (0 if checks passed, non-zero if any checks failed).
    """
    return call_handler(
        handle_history,
        state=ensure_state(ctx),
        follow=follow,
        limit=limit,
        raw=True,
        run_id=run_id,
        since=since,
        until=until,
    )
