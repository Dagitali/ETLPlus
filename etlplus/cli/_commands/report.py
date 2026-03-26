"""
:mod:`etlplus.cli._commands.report` module.

Typer command for aggregated normalized history reports.
"""

from __future__ import annotations

import typer

from etlplus.cli import _handlers as handlers
from etlplus.cli._commands.app import app
from etlplus.cli._commands.options import HistoryJsonOption
from etlplus.cli._commands.options import HistorySinceOption
from etlplus.cli._commands.options import HistoryTableOption
from etlplus.cli._commands.options import HistoryUntilOption
from etlplus.cli._commands.options import JobOption
from etlplus.cli._commands.options import ReportGroupByOption
from etlplus.cli._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = ['report_cmd']


# SECTION: FUNCTIONS ======================================================== #


@app.command('report')
def report_cmd(
    ctx: typer.Context,
    group_by: ReportGroupByOption = 'job',
    job: JobOption = None,
    json_output: HistoryJsonOption = False,
    since: HistorySinceOption = None,
    table: HistoryTableOption = False,
    until: HistoryUntilOption = None,
) -> int:
    """
    Aggregate normalized persisted run history.

    This command provides aggregated reports based on persisted run events,
    with optional grouping, filtering, and formatting. For real-time
    inspection of raw events, use the 'log' command instead.

    Parameters
    ----------
    ctx : typer.Context
        Typer context.
    group_by : ReportGroupByOption, optional
        Grouping option for the report (defaults to 'job').
    job : JobOption, optional
        Specific job to include in the report (defaults to all jobs).
    json_output : HistoryJsonOption, optional
        Whether to output the report in JSON format.
    since : HistorySinceOption, optional
        Start date for the report (defaults to the earliest available date).
    table : HistoryTableOption, optional
        Whether to display the report in a table format (defaults to False).
    until : HistoryUntilOption, optional
        End date for the report (defaults to the latest available date).

    Returns
    -------
    int
        Exit code (0 if checks passed, non-zero if any checks failed).
    """
    state = ensure_state(ctx)
    return int(
        handlers.report_handler(
            group_by=group_by,
            job=job,
            json_output=json_output,
            pretty=state.pretty,
            since=since,
            table=table,
            until=until,
        ),
    )
