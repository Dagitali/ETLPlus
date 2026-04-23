"""
:mod:`etlplus.cli._commands._options.specs` module.

Inspection, render, and JSON-spec Typer option aliases for CLI commands.
"""

from __future__ import annotations

from typing import Literal

from .helpers import typer_flag_option_alias
from .helpers import typer_value_option_alias

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Types
    'GraphOption',
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


GraphOption = typer_flag_option_alias(
    '--graph',
    help_text='Validate job dependencies and print DAG execution order.',
)

JobsOption = typer_flag_option_alias(
    '--jobs',
    help_text='List available job names and exit',
)

OperationsOption = typer_value_option_alias(
    str,
    '--operations',
    help_text='Transformation operations as JSON string.',
    show_default=None,
)

PipelinesOption = typer_flag_option_alias(
    '--pipelines',
    help_text='List ETL pipelines',
)

ReadinessOption = typer_flag_option_alias(
    '--readiness',
    help_text='Run runtime and optional config readiness checks.',
)

RenderConfigOption = typer_value_option_alias(
    str | None,
    '--config',
    help_text='Pipeline YAML that includes table_schemas for rendering.',
    metavar='PATH',
)

RenderOutputOption = typer_value_option_alias(
    str | None,
    '--output',
    '-o',
    help_text='Write rendered SQL to PATH (default: STDOUT).',
    metavar='PATH',
    show_default=None,
)

RenderSpecOption = typer_value_option_alias(
    str | None,
    '--spec',
    help_text='Standalone table spec file (.yml/.yaml/.json).',
    metavar='PATH',
)

RenderTableOption = typer_value_option_alias(
    str | None,
    '--table',
    help_text='Filter to a single table name from table_schemas.',
    metavar='NAME',
    show_default=None,
)

RenderTemplateOption = typer_value_option_alias(
    Literal['ddl', 'view'] | None,
    '--template',
    '-t',
    help_text='Template key (ddl/view).',
    metavar='KEY',
    show_default=True,
)

RenderTemplatePathOption = typer_value_option_alias(
    str | None,
    '--template-path',
    help_text='Explicit path to a Jinja template file (overrides template key).',
    metavar='PATH',
    show_default=None,
)

RulesOption = typer_value_option_alias(
    str,
    '--rules',
    help_text='Validation rules as JSON string.',
    show_default=None,
)

SourcesOption = typer_flag_option_alias(
    '--sources',
    help_text='List data sources',
)

StrictOption = typer_flag_option_alias(
    '--strict',
    help_text=(
        'Enable stricter config diagnostics that surface malformed entries '
        'normally ignored by the tolerant loader.'
    ),
)

SummaryOption = typer_flag_option_alias(
    '--summary',
    help_text='Show pipeline summary (name, version, sources, targets, jobs)',
)

TargetsOption = typer_flag_option_alias(
    '--targets',
    help_text='List data targets',
)

TransformsOption = typer_flag_option_alias(
    '--transforms',
    help_text='List data transforms',
)
