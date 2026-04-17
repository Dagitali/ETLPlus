"""
:mod:`etlplus.cli._commands._options.resources` module.

Source and target Typer option aliases for CLI command modules.
"""

from __future__ import annotations

from typing import Annotated

import typer

from ....file import FileFormat
from .helpers import typer_connector_option_alias
from .helpers import typer_format_option_alias
from .helpers import typer_resource_argument_kwargs

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Types
    'SourceArg',
    'SourceFormatOption',
    'SourceTypeOption',
    'TargetArg',
    'TargetFormatOption',
    'TargetTypeOption',
]


# SECTION: TYPES ============================================================ #


SourceArg = Annotated[
    str,
    typer.Argument(
        ...,
        **typer_resource_argument_kwargs(context='source'),
    ),
]

SourceFormatOption = typer_format_option_alias(
    FileFormat | None,
    '--source-format',
    context='source',
)

SourceTypeOption = typer_connector_option_alias(
    str | None,
    '--source-type',
    context='source',
)

TargetArg = Annotated[
    str,
    typer.Argument(
        ...,
        **typer_resource_argument_kwargs(context='target'),
    ),
]

TargetFormatOption = typer_format_option_alias(
    FileFormat | None,
    '--target-format',
    context='target',
)

TargetTypeOption = typer_connector_option_alias(
    str | None,
    '--target-type',
    context='target',
)
