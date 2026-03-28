"""
:mod:`etlplus.cli._commands.history` module.

Typer command for normalized persisted run history.
"""

from __future__ import annotations

import typer

from .. import _handlers as handlers
from ._helpers import call_handler
from ._state import ensure_state
from .app import app
from .options import HistoryJsonOption
from .options import HistoryLimitOption
from .options import HistoryRawOption
from .options import HistorySinceOption
from .options import HistoryStatusOption
from .options import HistoryTableOption
from .options import HistoryUntilOption
from .options import JobOption

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
    limit: HistoryLimitOption = None,
    raw: HistoryRawOption = False,
    since: HistorySinceOption = None,
    status: HistoryStatusOption = None,
    json_output: HistoryJsonOption = False,
    table: HistoryTableOption = False,
    until: HistoryUntilOption = None,
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
        Job to filter history for.
    limit : HistoryLimitOption, optional
        Maximum number of history entries to return.
    raw : HistoryRawOption, optional
        Whether to return raw history entries.
    since : HistorySinceOption, optional
        Start date for history entries.
    status : HistoryStatusOption, optional
        Status to filter history entries by.
    json_output : HistoryJsonOption, optional
        Whether to output history in JSON format.
    table : HistoryTableOption, optional
        Whether to output history in table format.
    until : HistoryUntilOption, optional
        End date for history entries.

    Returns
    -------
    int
        Exit code (0 if checks passed, non-zero if any checks failed).
    """
    return call_handler(
        handlers.history_handler,
        state=ensure_state(ctx),
        job=job,
        json_output=json_output,
        limit=limit,
        raw=raw,
        since=since,
        status=status,
        table=table,
        until=until,
    )
