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
from .._options import _typer_path_option_kwargs
from .._options import _typer_resource_argument_kwargs
from .._options import _typer_timestamp_option_kwargs
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
        **_typer_path_option_kwargs(
            'Path to YAML-formatted configuration file.',
        ),
    ),
]

ConfigOption = Annotated[
    str,
    typer.Option(
        ...,
        '--config',
        **_typer_path_option_kwargs(
            'Path to YAML-formatted configuration file.',
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
        help='Maximum number of history records to emit.',
        show_default=False,
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
        help='Filter persisted runs by status.',
        show_default=False,
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
        help='Name of the job to run',
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
        help='Transformation operations as JSON string.',
    ),
]

OutputOption = Annotated[
    str | None,
    typer.Option(
        '--output',
        '-o',
        **_typer_path_option_kwargs(
            'Write output to file PATH (default: STDOUT).',
            show_default=None,
        ),
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
        **_typer_flag_option_kwargs('List ETL pipelines'),
    ),
]

PrettyOption = Annotated[
    bool,
    typer.Option(
        '--pretty/--no-pretty',
        help='Pretty-print JSON output (default: pretty).',
    ),
]

QuietOption = Annotated[
    bool,
    typer.Option(
        '--quiet',
        '-q',
        help='Suppress warnings and non-essential output.',
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
        **_typer_path_option_kwargs(
            'Pipeline YAML that includes table_schemas for rendering.',
        ),
    ),
]

RenderOutputOption = Annotated[
    str | None,
    typer.Option(
        '--output',
        '-o',
        **_typer_path_option_kwargs(
            'Write rendered SQL to PATH (default: STDOUT).',
            show_default=None,
        ),
    ),
]

RenderSpecOption = Annotated[
    str | None,
    typer.Option(
        '--spec',
        **_typer_path_option_kwargs(
            'Standalone table spec file (.yml/.yaml/.json).',
        ),
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
        **_typer_path_option_kwargs(
            'Explicit path to a Jinja template file (overrides template key).',
            show_default=None,
        ),
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
