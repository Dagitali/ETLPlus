"""
:mod:`etlplus.cli._commands._option_common` module.

Common Typer option aliases shared across CLI command modules.
"""

from __future__ import annotations

from typing import Annotated
from typing import Literal

import typer

from ._option_helpers import _typer_flag_option_kwargs
from ._option_helpers import _typer_value_option_kwargs

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'CheckConfigOption',
    'ConfigOption',
    'JobOption',
    'OutputOption',
    'PipelineOption',
    'PrettyOption',
    'QuietOption',
    'StructuredEventFormatOption',
    'VerboseOption',
    'VersionOption',
]


# SECTION: TYPES ============================================================ #


CheckConfigOption = Annotated[
    str | None,
    typer.Option(
        '--config',
        **_typer_value_option_kwargs(
            'Path to YAML-formatted configuration file.',
            metavar='PATH',
        ),
    ),
]

ConfigOption = Annotated[
    str,
    typer.Option(
        ...,
        '--config',
        **_typer_value_option_kwargs(
            'Path to YAML-formatted configuration file.',
            metavar='PATH',
            show_default=None,
        ),
    ),
]

JobOption = Annotated[
    str | None,
    typer.Option(
        '-j',
        '--job',
        **_typer_value_option_kwargs(
            'Name of the job to run',
            show_default=None,
        ),
    ),
]

OutputOption = Annotated[
    str | None,
    typer.Option(
        '--output',
        '-o',
        **_typer_value_option_kwargs(
            'Write output to file PATH (default: STDOUT).',
            metavar='PATH',
            show_default=None,
        ),
    ),
]

PipelineOption = Annotated[
    str | None,
    typer.Option(
        '-p',
        '--pipeline',
        **_typer_value_option_kwargs(
            'Name of the pipeline to run',
            show_default=None,
        ),
    ),
]

PrettyOption = Annotated[
    bool,
    typer.Option(
        '--pretty/--no-pretty',
        **_typer_flag_option_kwargs(
            'Pretty-print JSON output (default: pretty).',
        ),
    ),
]

QuietOption = Annotated[
    bool,
    typer.Option(
        '--quiet',
        '-q',
        **_typer_flag_option_kwargs(
            'Suppress warnings and non-essential output.',
        ),
    ),
]

StructuredEventFormatOption = Annotated[
    Literal['jsonl'] | None,
    typer.Option(
        '--event-format',
        **_typer_value_option_kwargs(
            'Emit structured command events to STDERR (currently: jsonl).',
            metavar='FORMAT',
        ),
    ),
]

VerboseOption = Annotated[
    bool,
    typer.Option(
        '--verbose',
        '-v',
        **_typer_flag_option_kwargs('Emit extra diagnostics to STDERR.'),
    ),
]

VersionOption = Annotated[
    bool,
    typer.Option(
        '--version',
        '-V',
        **_typer_flag_option_kwargs(
            'Show the version and exit.',
            is_eager=True,
        ),
    ),
]
