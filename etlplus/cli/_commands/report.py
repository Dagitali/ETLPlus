"""
:mod:`etlplus.cli._commands.report` module.

Typer command for aggregated normalized history reports.
"""

from __future__ import annotations

import typer

from .._handlers.history import report_handler
from ._app import app
from ._helpers import call_handler
from ._options import HistoryJsonOption
from ._options import HistorySinceOption
from ._options import HistoryTableOption
from ._options import HistoryUntilOption
from ._options import JobOption
from ._options import ReportGroupByOption
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
    group_by: ReportGroupByOption = 'job',
    job: JobOption = None,
    since: HistorySinceOption = None,
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
    group_by : ReportGroupByOption, optional
        Grouping dimension for the report.
    job : JobOption, optional
        Job name filter.
    since : HistorySinceOption, optional
        Lower timestamp bound for emitted records.
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
    return call_handler(
        report_handler,
        state=ensure_state(ctx),
        group_by=group_by,
        job=job,
        json_output=json_output,
        since=since,
        table=table,
        until=until,
    )
