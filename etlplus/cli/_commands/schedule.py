"""
:mod:`etlplus.cli._commands.schedule` module.

Typer command for inspecting configured schedules.
"""

from __future__ import annotations

from typing import Any

import typer

from .._handlers.schedule import schedule_handler
from ._app import app
from ._helpers import CommandHelperPolicy
from ._options.common import ConfigOption
from ._options.common import EmitScheduleFormatOption
from ._options.common import RunPendingSchedulesOption
from ._options.common import ScheduleNameOption
from ._options.common import ShowScheduleStateOption
from ._options.common import StructuredEventFormatOption
from ._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'schedule_cmd',
]


# SECTION: FUNCTIONS ======================================================== #


@app.command('schedule')
def schedule_cmd(
    ctx: typer.Context,
    config: ConfigOption,
    schedule: ScheduleNameOption = None,
    emit: EmitScheduleFormatOption = None,
    run_pending: RunPendingSchedulesOption = False,
    show_state: ShowScheduleStateOption = False,
    event_format: StructuredEventFormatOption = None,
) -> int:
    """
    List schedule definitions from a YAML configuration.

    Parameters
    ----------
    ctx : typer.Context
        Typer context for the command invocation.
    config : ConfigOption
        Path to the YAML configuration file to inspect.
    schedule : ScheduleNameOption, optional
        Optional schedule name filter.
    emit : EmitScheduleFormatOption, optional
        Optional helper format to emit for a named schedule.
    run_pending : RunPendingSchedulesOption, optional
        Whether to execute due schedules once in local mode.
    show_state : ShowScheduleStateOption, optional
        Whether to include persisted local scheduler state in summary output.
    event_format : StructuredEventFormatOption, optional
        Structured event output format forwarded to scheduled runs.

    Returns
    -------
    int
        Exit code indicating success or failure of the command.
    """
    if emit is not None and schedule is None:
        CommandHelperPolicy.fail_usage("'--emit' requires '--schedule'.")
    if emit is not None and run_pending:
        CommandHelperPolicy.fail_usage(
            '--run-pending cannot be combined with --emit.',
        )

    handler_kwargs: Any = {
        'config': config,
        'emit': emit,
        'schedule_name': schedule,
    }
    if show_state:
        handler_kwargs['show_state'] = True
    if run_pending:
        handler_kwargs['run_pending'] = True
    if event_format is not None:
        handler_kwargs['event_format'] = event_format

    return CommandHelperPolicy.call_handler(
        schedule_handler,
        state=ensure_state(ctx),
        **handler_kwargs,
    )
