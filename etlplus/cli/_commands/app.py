"""
:mod:`etlplus.cli._commands.app` module.

Typer application object for the ``etlplus`` CLI.
"""

from __future__ import annotations

import typer

from .._constants import CLI_DESCRIPTION
from .._constants import CLI_EPILOG

# SECTION: EXPORTS ========================================================== #


__all__ = ['app']


# SECTION: VARIABLES ======================================================== #


app = typer.Typer(
    name='etlplus',
    help=CLI_DESCRIPTION,
    epilog=CLI_EPILOG,
    add_completion=True,
    no_args_is_help=False,
    rich_markup_mode='markdown',
)
