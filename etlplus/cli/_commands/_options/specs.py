"""
:mod:`etlplus.cli._commands._options.specs` module.

Inspection, render, and JSON-spec Typer option aliases for CLI commands.
"""

from __future__ import annotations

from typing import Annotated
from typing import Literal

import typer

from .helpers import _typer_flag_option_kwargs
from .helpers import _typer_value_option_kwargs

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Types
    'JobsOption',
    'OperationsOption',
    'PipelinesOption',
    'ReadinessOption',
    'RenderConfigOption',
    'RenderOutputOption',
    'RenderSpecOption',
    'RenderTableOption',
    'RenderTemplateOption',
    'RenderTemplatePathOption',
    'RulesOption',
    'StrictOption',
    'SourcesOption',
    'SummaryOption',
    'TargetsOption',
    'TransformsOption',
]


# SECTION: TYPES ============================================================ #


JobsOption = Annotated[
    bool,
    typer.Option(
        '--jobs',
        **_typer_flag_option_kwargs('List available job names and exit'),
    ),
]

OperationsOption = Annotated[
    str,
    typer.Option(
        '--operations',
        **_typer_value_option_kwargs(
            'Transformation operations as JSON string.',
            show_default=None,
        ),
    ),
]

PipelinesOption = Annotated[
    bool,
    typer.Option(
        '--pipelines',
        **_typer_flag_option_kwargs('List ETL pipelines'),
    ),
]

ReadinessOption = Annotated[
    bool,
    typer.Option(
        '--readiness',
        **_typer_flag_option_kwargs(
            'Run runtime and optional config readiness checks.',
        ),
    ),
]

RenderConfigOption = Annotated[
    str | None,
    typer.Option(
        '--config',
        **_typer_value_option_kwargs(
            'Pipeline YAML that includes table_schemas for rendering.',
            metavar='PATH',
        ),
    ),
]

RenderOutputOption = Annotated[
    str | None,
    typer.Option(
        '--output',
        '-o',
        **_typer_value_option_kwargs(
            'Write rendered SQL to PATH (default: STDOUT).',
            metavar='PATH',
            show_default=None,
        ),
    ),
]

RenderSpecOption = Annotated[
    str | None,
    typer.Option(
        '--spec',
        **_typer_value_option_kwargs(
            'Standalone table spec file (.yml/.yaml/.json).',
            metavar='PATH',
        ),
    ),
]

RenderTableOption = Annotated[
    str | None,
    typer.Option(
        '--table',
        **_typer_value_option_kwargs(
            'Filter to a single table name from table_schemas.',
            metavar='NAME',
            show_default=None,
        ),
    ),
]

RenderTemplateOption = Annotated[
    Literal['ddl', 'view'] | None,
    typer.Option(
        '--template',
        '-t',
        **_typer_value_option_kwargs(
            'Template key (ddl/view).',
            metavar='KEY',
            show_default=True,
        ),
    ),
]

RenderTemplatePathOption = Annotated[
    str | None,
    typer.Option(
        '--template-path',
        **_typer_value_option_kwargs(
            'Explicit path to a Jinja template file (overrides template key).',
            metavar='PATH',
            show_default=None,
        ),
    ),
]

RulesOption = Annotated[
    str,
    typer.Option(
        '--rules',
        **_typer_value_option_kwargs(
            'Validation rules as JSON string.',
            show_default=None,
        ),
    ),
]

SourcesOption = Annotated[
    bool,
    typer.Option(
        '--sources',
        **_typer_flag_option_kwargs('List data sources'),
    ),
]

StrictOption = Annotated[
    bool,
    typer.Option(
        '--strict',
        **_typer_flag_option_kwargs(
            (
                'Enable stricter config diagnostics that surface malformed '
                'entries normally ignored by the tolerant loader.'
            ),
        ),
    ),
]

SummaryOption = Annotated[
    bool,
    typer.Option(
        '--summary',
        **_typer_flag_option_kwargs(
            'Show pipeline summary (name, version, sources, targets, jobs)',
        ),
    ),
]

TargetsOption = Annotated[
    bool,
    typer.Option(
        '--targets',
        **_typer_flag_option_kwargs('List data targets'),
    ),
]

TransformsOption = Annotated[
    bool,
    typer.Option(
        '--transforms',
        **_typer_flag_option_kwargs('List data transforms'),
    ),
]
