"""
:mod:`etlplus.cli._commands.schedule` module.

Typer command for inspecting configured schedules.
"""

from __future__ import annotations

import typer

from .._handlers.schedule import schedule_handler
from ._app import app
from ._helpers import CommandHelperPolicy
from ._options.common import ConfigOption
from ._options.common import EmitScheduleFormatOption
from ._options.common import RunPendingSchedulesOption
from ._options.common import ScheduleNameOption
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

    state = ensure_state(ctx)
    if run_pending and event_format is not None:
        return CommandHelperPolicy.call_handler(
            schedule_handler,
            state=state,
            config=config,
            event_format=event_format,
            emit=emit,
            run_pending=True,
            schedule_name=schedule,
        )
    if run_pending:
        return CommandHelperPolicy.call_handler(
            schedule_handler,
            state=state,
            config=config,
            emit=emit,
            run_pending=True,
            schedule_name=schedule,
        )
    if event_format is not None:
        return CommandHelperPolicy.call_handler(
            schedule_handler,
            state=state,
            config=config,
            event_format=event_format,
            emit=emit,
            schedule_name=schedule,
        )

    return CommandHelperPolicy.call_handler(
        schedule_handler,
        state=state,
        config=config,
        emit=emit,
        schedule_name=schedule,
    )
