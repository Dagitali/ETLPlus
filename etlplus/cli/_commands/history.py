"""
:mod:`etlplus.cli._commands.history` module.

Typer command for normalized persisted run history.
"""

from __future__ import annotations

import typer

from .._handlers.history import history_handler
from ._app import app
from ._helpers import call_handler
from ._options.common import JobOption
from ._options.history import HistoryJsonOption
from ._options.history import HistoryLimitOption
from ._options.history import HistoryRawOption
from ._options.history import HistorySinceOption
from ._options.history import HistoryStatusOption
from ._options.history import HistoryTableOption
from ._options.history import HistoryUntilOption
from ._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'history_cmd',
]


# SECTION: FUNCTIONS ======================================================== #


@app.command('history')
def history_cmd(
    ctx: typer.Context,
    job: JobOption = None,
    since: HistorySinceOption = None,
    until: HistoryUntilOption = None,
    status: HistoryStatusOption = None,
    limit: HistoryLimitOption = None,
    raw: HistoryRawOption = False,
    json_output: HistoryJsonOption = False,
    table: HistoryTableOption = False,
) -> int:
    """
    Inspect persisted local run history.

    This command provides a normalized view of persisted run events, with
    optional filtering and formatting. For real-time inspection of raw events,
    use the 'log' command instead.

    Parameters
    ----------
    ctx : typer.Context
        Typer context.
    job : JobOption, optional
        Job name for filtering history.
    since : HistorySinceOption, optional
        Lower timestamp bound for emitted records.
    until : HistoryUntilOption, optional
        Upper timestamp bound for emitted records.
    status : HistoryStatusOption, optional
        Run status for filtering history.
    limit : HistoryLimitOption, optional
        Maximum number of history entries to emit.
    raw : HistoryRawOption, optional
        Whether to emit raw append events instead of normalized runs.
    json_output : HistoryJsonOption, optional
        Whether to emit JSON explicitly.
    table : HistoryTableOption, optional
        Whether to emit a Markdown table.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    return call_handler(
        history_handler,
        state=ensure_state(ctx),
        job=job,
        since=since,
        until=until,
        status=status,
        limit=limit,
        raw=raw,
        table=table,
        json_output=json_output,
    )
