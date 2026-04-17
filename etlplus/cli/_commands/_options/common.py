"""
:mod:`etlplus.cli._commands._options.common` module.

Common Typer option aliases shared across CLI command modules.
"""

from __future__ import annotations

from typing import Annotated
from typing import Literal

import typer

from .helpers import _typer_option_alias
from .helpers import typer_flag_option_alias
from .helpers import typer_flag_option_kwargs
from .helpers import typer_value_option_alias
from .helpers import typer_value_option_kwargs

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Types
    'CaptureTracebacksOption',
    'CheckConfigOption',
    'ConfigOption',
    'ContinueOnFailOption',
    'HistoryBackendOption',
    'HistoryEnabledOption',
    'HistoryStateDirOption',
    'JobOption',
    'OutputOption',
    'PipelineOption',
    'PrettyOption',
    'QuietOption',
    'RunAllOption',
    'StructuredEventFormatOption',
    'VerboseOption',
    'VersionOption',
]


# SECTION: TYPES ============================================================ #


CheckConfigOption = typer_value_option_alias(
    str | None,
    '--config',
    help_text='Path to YAML-formatted configuration file.',
    metavar='PATH',
)

ConfigOption = Annotated[
    str,
    typer.Option(
        ...,
        '--config',
        **typer_value_option_kwargs(
            'Path to YAML-formatted configuration file.',
            metavar='PATH',
            show_default=None,
        ),
    ),
]

ContinueOnFailOption = typer_flag_option_alias(
    '--continue-on-fail',
    help_text=(
        'Continue running independent jobs after a failure and skip only '
        'blocked downstream jobs.'
    ),
)

CaptureTracebacksOption = _typer_option_alias(
    bool | None,
    '--capture-tracebacks/--no-capture-tracebacks',
    **typer_flag_option_kwargs(
        (
            'Persist capped failure tracebacks in local run history when '
            'history is enabled.'
        ),
        show_default=None,
    ),
)

JobOption = typer_value_option_alias(
    str | None,
    '-j',
    '--job',
    help_text='Name of the job to run',
    show_default=None,
)

HistoryBackendOption = typer_value_option_alias(
    Literal['sqlite', 'jsonl'] | None,
    '--history-backend',
    help_text='Override the local history backend (sqlite or jsonl).',
    metavar='BACKEND',
    show_default=None,
)

HistoryEnabledOption = _typer_option_alias(
    bool | None,
    '--history/--no-history',
    **typer_flag_option_kwargs(
        'Enable or disable local run-history persistence for this run.',
        show_default=None,
    ),
)

HistoryStateDirOption = typer_value_option_alias(
    str | None,
    '--history-state-dir',
    help_text='Override the local history state directory.',
    metavar='PATH',
    show_default=None,
)

OutputOption = typer_value_option_alias(
    str | None,
    '--output',
    '-o',
    help_text='Write output to file PATH (default: STDOUT).',
    metavar='PATH',
    show_default=None,
)

PipelineOption = typer_value_option_alias(
    str | None,
    '-p',
    '--pipeline',
    help_text='Name of the pipeline to run',
    show_default=None,
)

PrettyOption = _typer_option_alias(
    bool,
    '--pretty/--no-pretty',
    **typer_flag_option_kwargs('Pretty-print JSON output (default: pretty).'),
)

QuietOption = typer_flag_option_alias(
    '--quiet',
    '-q',
    help_text='Suppress warnings and non-essential output.',
)

RunAllOption = typer_flag_option_alias(
    '--all',
    help_text='Run all configured jobs in DAG order.',
)

StructuredEventFormatOption = typer_value_option_alias(
    Literal['jsonl'] | None,
    '--event-format',
    help_text='Emit structured command events to STDERR (currently: jsonl).',
    metavar='FORMAT',
)

VerboseOption = typer_flag_option_alias(
    '--verbose',
    '-v',
    help_text='Emit extra diagnostics to STDERR.',
)

VersionOption = typer_flag_option_alias(
    '--version',
    '-V',
    help_text='Show the version and exit.',
    is_eager=True,
)
