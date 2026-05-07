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
from ._options.common import ScheduleNameOption
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

    Returns
    -------
    int
        Exit code indicating success or failure of the command.
    """
    if emit is not None and schedule is None:
        CommandHelperPolicy.fail_usage("'--emit' requires '--schedule'.")

    return CommandHelperPolicy.call_handler(
        schedule_handler,
        state=ensure_state(ctx),
        config=config,
        emit=emit,
        schedule_name=schedule,
    )
