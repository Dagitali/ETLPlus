"""
:mod:`etlplus.cli._commands._options.init` module.

Init-command Typer option aliases for CLI command modules.
"""

from __future__ import annotations

from typing import Annotated

import typer

from .helpers import typer_flag_option_alias

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Types
    'InitDirectoryArgument',
    'InitForceOption',
]


# SECTION: TYPES ============================================================ #


InitDirectoryArgument = Annotated[
    str,
    typer.Argument(
        metavar='PATH',
        help='Directory to scaffold with starter ETLPlus files.',
    ),
]

InitForceOption = typer_flag_option_alias(
    '--force',
    '-f',
    help_text='Overwrite scaffold files when they already exist.',
)
