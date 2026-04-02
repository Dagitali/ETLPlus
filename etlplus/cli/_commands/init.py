"""
:mod:`etlplus.cli._commands.init` module.

Typer command for scaffolding one minimal ETLPlus starter project.
"""

from __future__ import annotations

import typer

from .._handlers.init import init_handler
from ._app import app
from ._helpers import call_handler
from ._options.init import InitDirectoryArgument
from ._options.init import InitForceOption
from ._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'init_cmd',
]


# SECTION: FUNCTIONS ======================================================== #


@app.command('init')
def init_cmd(
    ctx: typer.Context,
    directory: InitDirectoryArgument = '.',
    force: InitForceOption = False,
) -> int:
    """
    Scaffold one minimal ETLPlus starter project.

    Parameters
    ----------
    ctx : typer.Context
        Typer context.
    directory : InitDirectoryArgument, optional
        Target directory for the scaffold. Defaults to the current directory.
    force : InitForceOption, optional
        Whether to overwrite existing scaffold files.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    return call_handler(
        init_handler,
        state=ensure_state(ctx),
        directory=directory,
        force=force,
    )
