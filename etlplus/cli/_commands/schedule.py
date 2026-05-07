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
) -> int:
    """
    List schedule definitions from a YAML configuration.

    Parameters
    ----------
    ctx : typer.Context
        Typer context for the command invocation.
    config : ConfigOption
        Path to the YAML configuration file to inspect.

    Returns
    -------
    int
        Exit code indicating success or failure of the command.
    """
    return CommandHelperPolicy.call_handler(
        schedule_handler,
        state=ensure_state(ctx),
        config=config,
    )
