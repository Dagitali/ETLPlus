"""
:mod:`etlplus.cli._commands.options` module.

Shared Typer argument and option aliases for CLI command modules.
"""

from __future__ import annotations

from typing import Annotated
from typing import Literal

import typer

from ...file import FileFormat
from .._options import _typer_connector_option_kwargs
from .._options import _typer_flag_option_kwargs
from .._options import _typer_resource_argument_kwargs
from .._options import _typer_timestamp_option_kwargs
from .._options import _typer_value_option_kwargs
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
    'PrettyOption',
    'QuietOption',
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

HistoryFollowOption = Annotated[
    bool,
    typer.Option(
        '--follow',
        **_typer_flag_option_kwargs(
            'Keep polling for newly persisted matching raw history events.',
        ),
    ),
]

HistoryJsonOption = Annotated[
    bool,
    typer.Option(
        '--json',
        **_typer_flag_option_kwargs('Format output as JSON explicitly.'),
    ),
]

HistoryLimitOption = Annotated[
    int | None,
    typer.Option(
        '--limit',
        min=1,
        **_typer_value_option_kwargs(
            'Maximum number of history records to emit.',
        ),
    ),
]

HistoryRawOption = Annotated[
    bool,
    typer.Option(
        '--raw',
        **_typer_flag_option_kwargs(
            'Emit raw append events instead of normalized runs.',
        ),
    ),
]

HistorySinceOption = Annotated[
    str | None,
    typer.Option(
        '--since',
        **_typer_timestamp_option_kwargs(bound='since'),
    ),
]

HistoryStatusOption = Annotated[
    str | None,
    typer.Option(
        '--status',
        **_typer_value_option_kwargs('Filter persisted runs by status.'),
    ),
]

HistoryTableOption = Annotated[
    bool,
    typer.Option(
        '--table',
        **_typer_flag_option_kwargs(
            'Format normalized history output as a Markdown table.',
        ),
    ),
]

HistoryUntilOption = Annotated[
    str | None,
    typer.Option(
        '--until',
        **_typer_timestamp_option_kwargs(bound='until'),
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

PipelinesOption = Annotated[
    bool,
    typer.Option(
        '--pipelines',
        **_typer_flag_option_kwargs('List ETL pipelines'),
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

ReportGroupByOption = Annotated[
    Literal['day', 'job', 'status'],
    typer.Option(
        '--group-by',
        **_typer_value_option_kwargs(
            'Grouping dimension for aggregated history reports.',
            show_default=True,
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

RunIdOption = Annotated[
    str | None,
    typer.Option(
        '--run-id',
        **_typer_value_option_kwargs('Filter persisted runs by run identifier.'),
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
        **typer_format_option_kwargs(context='source'),
    ),
]

SourceTypeOption = Annotated[
    str | None,
    typer.Option(
        '--source-type',
        **_typer_connector_option_kwargs(context='source'),
    ),
]

SourcesOption = Annotated[
    bool,
    typer.Option(
        '--sources',
        **_typer_flag_option_kwargs('List data sources'),
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
        **typer_format_option_kwargs(context='target'),
    ),
]

TargetTypeOption = Annotated[
    str | None,
    typer.Option(
        '--target-type',
        **_typer_connector_option_kwargs(context='target'),
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
