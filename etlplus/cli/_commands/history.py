"""
:mod:`etlplus.cli._commands.history` module.

Typer command for normalized persisted run history.
"""

from __future__ import annotations

import typer

from .._handlers.history import history_handler
from ._app import app
from ._helpers import call_history_command
from ._options.common import JobOption
from ._options.history import HistoryJsonOption
from ._options.history import HistoryLevelOption
from ._options.history import HistoryLimitOption
from ._options.history import HistoryPipelineOption
from ._options.history import HistoryRawOption
from ._options.history import HistorySinceOption
from ._options.history import HistoryStatusOption
from ._options.history import HistoryTableOption
from ._options.history import HistoryUntilOption
from ._options.history import RunIdOption
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
    level: HistoryLevelOption = 'run',
    job: JobOption = None,
    pipeline: HistoryPipelineOption = None,
    run_id: RunIdOption = None,
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
    level : HistoryLevelOption, optional
        Whether to inspect run-level or job-level persisted history.
    job : JobOption, optional
        Job name for filtering history.
    pipeline : HistoryPipelineOption, optional
        Pipeline name for filtering history.
    run_id : RunIdOption, optional
        Run identifier for filtering history.
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
    return call_history_command(
        history_handler,
        ctx=ctx,
        state=ensure_state(ctx),
        level=level,
        job=job,
        limit=limit,
        pipeline=pipeline,
        run_id=run_id,
        since=since,
        status=status,
        until=until,
        raw=raw,
        table=table,
        json_output=json_output,
    )
