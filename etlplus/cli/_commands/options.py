"""
:mod:`etlplus.cli._commands.options` module.

Shared Typer argument and option aliases for CLI command modules.
"""

from __future__ import annotations

from typing import Annotated
from typing import Literal

import typer

from ...file import FileFormat
from .._options import typer_format_option_kwargs

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'CheckConfigOption',
    'ConfigOption',
    'HistoryFollowOption',
    'HistoryJsonOption',
    'HistoryLimitOption',
    'HistoryRawOption',
    'HistorySinceOption',
    'HistoryStatusOption',
    'HistoryTableOption',
    'HistoryUntilOption',
    'JobOption',
    'JobsOption',
    'OperationsOption',
    'OutputOption',
    'PipelineOption',
    'PipelinesOption',
    'ReadinessOption',
    'RenderConfigOption',
    'RenderOutputOption',
    'RenderSpecOption',
    'RenderTableOption',
    'RenderTemplateOption',
    'RenderTemplatePathOption',
    'ReportGroupByOption',
    'RulesOption',
    'RunIdOption',
    'SourceArg',
    'SourceFormatOption',
    'SourceTypeOption',
    'SourcesOption',
    'StructuredEventFormatOption',
    'SummaryOption',
    'TargetArg',
    'TargetFormatOption',
    'TargetTypeOption',
    'TargetsOption',
    'TransformsOption',
]


# SECTION: TYPES ============================================================ #

CheckConfigOption = Annotated[
    str | None,
    typer.Option(
        '--config',
        metavar='PATH',
        help='Path to YAML-formatted configuration file.',
        show_default=False,
    ),
]

ConfigOption = Annotated[
    str,
    typer.Option(
        ...,
        '--config',
        metavar='PATH',
        help='Path to YAML-formatted configuration file.',
    ),
]

HistoryFollowOption = Annotated[
    bool,
    typer.Option(
        '--follow',
        help='Keep polling for newly persisted matching raw history events.',
    ),
]

HistoryJsonOption = Annotated[
    bool,
    typer.Option(
        '--json',
        help='Format output as JSON explicitly.',
    ),
]

HistoryLimitOption = Annotated[
    int | None,
    typer.Option(
        '--limit',
        min=1,
        help='Maximum number of history records to emit.',
        show_default=False,
    ),
]

HistoryRawOption = Annotated[
    bool,
    typer.Option(
        '--raw',
        help='Emit raw append events instead of normalized runs.',
    ),
]

HistorySinceOption = Annotated[
    str | None,
    typer.Option(
        '--since',
        metavar='ISO8601',
        help='Emit only records at or after the given ISO-8601 timestamp.',
        show_default=False,
    ),
]

HistoryStatusOption = Annotated[
    str | None,
    typer.Option(
        '--status',
        help='Filter persisted runs by status.',
        show_default=False,
    ),
]

HistoryTableOption = Annotated[
    bool,
    typer.Option(
        '--table',
        help='Format normalized history output as a Markdown table.',
    ),
]

HistoryUntilOption = Annotated[
    str | None,
    typer.Option(
        '--until',
        metavar='ISO8601',
        help='Emit only records at or before the given ISO-8601 timestamp.',
        show_default=False,
    ),
]

JobOption = Annotated[
    str | None,
    typer.Option(
        '-j',
        '--job',
        help='Name of the job to run',
    ),
]

JobsOption = Annotated[
    bool,
    typer.Option(
        '--jobs',
        help='List available job names and exit',
    ),
]

OperationsOption = Annotated[
    str,
    typer.Option(
        '--operations',
        help='Transformation operations as JSON string.',
    ),
]

OutputOption = Annotated[
    str | None,
    typer.Option(
        '--output',
        '-o',
        metavar='PATH',
        help='Write output to file PATH (default: STDOUT).',
    ),
]

PipelineOption = Annotated[
    str | None,
    typer.Option(
        '-p',
        '--pipeline',
        help='Name of the pipeline to run',
    ),
]

PipelinesOption = Annotated[
    bool,
    typer.Option(
        '--pipelines',
        help='List ETL pipelines',
    ),
]

ReadinessOption = Annotated[
    bool,
    typer.Option(
        '--readiness',
        help='Run runtime and optional config readiness checks.',
    ),
]

RenderConfigOption = Annotated[
    str | None,
    typer.Option(
        '--config',
        metavar='PATH',
        help='Pipeline YAML that includes table_schemas for rendering.',
        show_default=False,
    ),
]

RenderOutputOption = Annotated[
    str | None,
    typer.Option(
        '--output',
        '-o',
        metavar='PATH',
        help='Write rendered SQL to PATH (default: STDOUT).',
    ),
]

RenderSpecOption = Annotated[
    str | None,
    typer.Option(
        '--spec',
        metavar='PATH',
        help='Standalone table spec file (.yml/.yaml/.json).',
        show_default=False,
    ),
]

RenderTableOption = Annotated[
    str | None,
    typer.Option(
        '--table',
        metavar='NAME',
        help='Filter to a single table name from table_schemas.',
    ),
]

RenderTemplateOption = Annotated[
    Literal['ddl', 'view'] | None,
    typer.Option(
        '--template',
        '-t',
        metavar='KEY',
        help='Template key (ddl/view).',
        show_default=True,
    ),
]

RenderTemplatePathOption = Annotated[
    str | None,
    typer.Option(
        '--template-path',
        metavar='PATH',
        help='Explicit path to a Jinja template file (overrides template key).',
    ),
]

ReportGroupByOption = Annotated[
    Literal['day', 'job', 'status'],
    typer.Option(
        '--group-by',
        help='Grouping dimension for aggregated history reports.',
        show_default=True,
    ),
]

RulesOption = Annotated[
    str,
    typer.Option(
        '--rules',
        help='Validation rules as JSON string.',
    ),
]

RunIdOption = Annotated[
    str | None,
    typer.Option(
        '--run-id',
        help='Filter persisted runs by run identifier.',
        show_default=False,
    ),
]

StructuredEventFormatOption = Annotated[
    Literal['jsonl'] | None,
    typer.Option(
        '--event-format',
        metavar='FORMAT',
        help='Emit structured command events to STDERR (currently: jsonl).',
        show_default=False,
    ),
]

SourceArg = Annotated[
    str,
    typer.Argument(
        ...,
        metavar='SOURCE',
        help=(
            'Extract data from SOURCE (JSON payload, file path, '
            'URI/URL, or - for STDIN). Use --source-format to override the '
            'inferred data format and --source-type to override the inferred '
            'data connector.'
        ),
    ),
]

SourceFormatOption = Annotated[
    FileFormat | None,
    typer.Option(
        '--source-format',
        **typer_format_option_kwargs(context='source'),
    ),
]

SourceTypeOption = Annotated[
    str | None,
    typer.Option(
        '--source-type',
        metavar='CONNECTOR',
        show_default=False,
        rich_help_panel='I/O overrides',
        help='Override the inferred source type (api, database, file).',
    ),
]

SourcesOption = Annotated[
    bool,
    typer.Option(
        '--sources',
        help='List data sources',
    ),
]

SummaryOption = Annotated[
    bool,
    typer.Option(
        '--summary',
        help='Show pipeline summary (name, version, sources, targets, jobs)',
    ),
]

TargetArg = Annotated[
    str,
    typer.Argument(
        ...,
        metavar='TARGET',
        help=(
            'Load data into TARGET (file path, URI/URL, or - for '
            'STDOUT). Use --target-format to override the inferred data '
            'format and --target-type to override the inferred data connector.'
        ),
    ),
]

TargetFormatOption = Annotated[
    FileFormat | None,
    typer.Option(
        '--target-format',
        **typer_format_option_kwargs(context='target'),
    ),
]

TargetTypeOption = Annotated[
    str | None,
    typer.Option(
        '--target-type',
        metavar='CONNECTOR',
        show_default=False,
        rich_help_panel='I/O overrides',
        help='Override the inferred target type (api, database, file).',
    ),
]

TargetsOption = Annotated[
    bool,
    typer.Option(
        '--targets',
        help='List data targets',
    ),
]

TransformsOption = Annotated[
    bool,
    typer.Option(
        '--transforms',
        help='List data transforms',
    ),
]
