"""
:mod:`etlplus.cli._commands.report` module.

Typer command for aggregated normalized history reports.
"""

from __future__ import annotations

import typer

from .._handlers.history import report_handler
from ._app import app
from ._helpers import call_history_handler
from ._options.common import JobOption
from ._options.history import HistoryJsonOption
from ._options.history import HistoryLevelOption
from ._options.history import HistoryPipelineOption
from ._options.history import HistorySinceOption
from ._options.history import HistoryStatusOption
from ._options.history import HistoryTableOption
from ._options.history import HistoryUntilOption
from ._options.history import ReportGroupByOption
from ._options.history import RunIdOption
from ._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'report_cmd',
]


# SECTION: FUNCTIONS ======================================================== #


@app.command('report')
def report_cmd(
    ctx: typer.Context,
    level: HistoryLevelOption = 'run',
    group_by: ReportGroupByOption = 'job',
    job: JobOption = None,
    pipeline: HistoryPipelineOption = None,
    run_id: RunIdOption = None,
    since: HistorySinceOption = None,
    status: HistoryStatusOption = None,
    until: HistoryUntilOption = None,
    json_output: HistoryJsonOption = False,
    table: HistoryTableOption = False,
) -> int:
    """
    Aggregate normalized persisted run history.

    Parameters
    ----------
    ctx : typer.Context
        Typer context.
    level : HistoryLevelOption, optional
        Whether to aggregate run-level or job-level history rows.
    group_by : ReportGroupByOption, optional
        Grouping dimension for the report.
    job : JobOption, optional
        Job name filter.
    pipeline : HistoryPipelineOption, optional
        Pipeline name filter.
    run_id : RunIdOption, optional
        Run identifier filter.
    since : HistorySinceOption, optional
        Lower timestamp bound for emitted records.
    status : HistoryStatusOption, optional
        Status filter.
    until : HistoryUntilOption, optional
        Upper timestamp bound for emitted records.
    json_output : HistoryJsonOption, optional
        Whether to emit JSON explicitly.
    table : HistoryTableOption, optional
        Whether to emit a Markdown table.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    return call_history_handler(
        report_handler,
        state=ensure_state(ctx),
        level=level,
        group_by=group_by,
        job=job,
        pipeline=pipeline,
        run_id=run_id,
        since=since,
        status=status,
        json_output=json_output,
        table=table,
        until=until,
    )
