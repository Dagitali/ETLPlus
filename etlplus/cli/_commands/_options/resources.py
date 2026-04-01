"""
:mod:`etlplus.cli._commands._options.resources` module.

Source and target Typer option aliases for CLI command modules.
"""

from __future__ import annotations

from typing import Annotated

import typer

from ....file import FileFormat
from .helpers import _typer_connector_option_kwargs
from .helpers import _typer_format_option_kwargs
from .helpers import _typer_resource_argument_kwargs

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
        **_typer_resource_argument_kwargs(context='source'),
    ),
]

SourceFormatOption = Annotated[
    FileFormat | None,
    typer.Option(
        '--source-format',
        **_typer_format_option_kwargs(context='source'),
    ),
]

SourceTypeOption = Annotated[
    str | None,
    typer.Option(
        '--source-type',
        **_typer_connector_option_kwargs(context='source'),
    ),
]

TargetArg = Annotated[
    str,
    typer.Argument(
        ...,
        **_typer_resource_argument_kwargs(context='target'),
    ),
]

TargetFormatOption = Annotated[
    FileFormat | None,
    typer.Option(
        '--target-format',
        **_typer_format_option_kwargs(context='target'),
    ),
]

TargetTypeOption = Annotated[
    str | None,
    typer.Option(
        '--target-type',
        **_typer_connector_option_kwargs(context='target'),
    ),
]
