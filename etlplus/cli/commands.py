"""
:mod:`etlplus.cli.commands` module.

Typer application and subcommands for the ``etlplus`` command-line interface
(CLI). Typer (Click) is used for CLI parsing, help text, and subcommand
dispatch. The Typer layer focuses on ergonomics (git-style subcommands,
optional inference of resource types, stdin/stdout piping, and quality-of-life
flags), while delegating business logic to the existing :func:`*_handler`
handlers.

Subcommands
-----------
- ``check``: inspect a pipeline configuration
- ``extract``: extract data from files, databases, or REST APIs
- ``load``: load data to files, databases, or REST APIs
- ``render``: render SQL DDL from table schema specs
- ``transform``: transform records
- ``validate``: validate data against rules

Notes
-----
- Use ``-`` to read from stdin or to write to stdout.
- Commands ``extract`` and ``transform`` support the command-line option
    ``--source-type`` to override inferred resource types.
- Commands ``transform`` and ``load`` support the command-line option
    ``--target-type`` to override inferred resource types.
"""

from __future__ import annotations

from typing import Annotated

import typer

from .. import __version__
from ..utils import json_type
from . import handlers
from .constants import CLI_DESCRIPTION
from .constants import CLI_EPILOG
from .constants import DATA_CONNECTORS
from .constants import DEFAULT_FILE_FORMAT
from .constants import FILE_FORMATS
from .state import CliState
from .state import ensure_state
from .state import format_namespace_kwargs
from .state import infer_resource_type_or_exit
from .state import infer_resource_type_soft
from .state import log_inferred_resource
from .state import optional_choice
from .state import resolve_resource_type
from .state import stateful_namespace
from .state import validate_choice

# SECTION: EXPORTS ========================================================== #


__all__ = ['app']


# SECTION: TYPE ALIASES ==================================================== #


OperationsOption = Annotated[
    str,
    typer.Option(
        '--operations',
        help='Transformation operations as JSON string.',
    ),
]

PipelineConfigOption = Annotated[
    str,
    typer.Option(
        ...,
        '--config',
        metavar='PATH',
        help='Path to pipeline YAML configuration file.',
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
        help='Write rendered SQL to PATH (default: stdout).',
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
    str,
    typer.Option(
        '--template',
        '-t',
        metavar='KEY|PATH',
        help='Template key (ddl/view) or path to a Jinja template file.',
        show_default=True,
    ),
]

RenderTemplatePathOption = Annotated[
    str | None,
    typer.Option(
        '--template-path',
        metavar='PATH',
        help=(
            'Explicit path to a Jinja template file (overrides template key).'
        ),
    ),
]

RulesOption = Annotated[
    str,
    typer.Option(
        '--rules',
        help='Validation rules as JSON string.',
    ),
]

SourceFormatOption = Annotated[
    str | None,
    typer.Option(
        '--source-format',
        metavar='FORMAT',
        show_default=False,
        rich_help_panel='Format overrides',
        help=(
            'Input payload format when SOURCE is - or an inline payload. '
            'File sources infer format from the extension.'
        ),
    ),
]

SourceInputArg = Annotated[
    str,
    typer.Argument(
        ...,
        metavar='SOURCE',
        help=(
            'Extract from SOURCE. Use --from/--source-type to override the '
            'inferred connector when needed.'
        ),
    ),
]

SourceOverrideOption = Annotated[
    str | None,
    typer.Option(
        '--source-type',
        metavar='CONNECTOR',
        show_default=False,
        rich_help_panel='I/O overrides',
        help='Override the inferred source type (file, database, api).',
    ),
]

StdinFormatOption = Annotated[
    str | None,
    typer.Option(
        '--source-format',
        metavar='FORMAT',
        show_default=False,
        rich_help_panel='Format overrides',
        help='Input payload format when reading from stdin (default: json).',
    ),
]

StreamingSourceArg = Annotated[
    str,
    typer.Argument(
        ...,
        metavar='SOURCE',
        help=(
            'Data source to transform or validate (path, JSON payload, or '
            '- for stdin).'
        ),
    ),
]

TargetFormatOption = Annotated[
    str | None,
    typer.Option(
        '--target-format',
        metavar='FORMAT',
        show_default=False,
        rich_help_panel='Format overrides',
        help=(
            'Payload format when TARGET is - or a non-file connector. File '
            'targets infer format from the extension.'
        ),
    ),
]

TargetInputArg = Annotated[
    str,
    typer.Argument(
        ...,
        metavar='TARGET',
        help=(
            'Load JSON data from stdin into TARGET. Use --to/--target-type '
            'to override connector inference when needed. Source data must '
            'be piped into stdin.'
        ),
    ),
]

TargetOverrideOption = Annotated[
    str | None,
    typer.Option(
        '--target-type',
        metavar='CONNECTOR',
        show_default=False,
        rich_help_panel='I/O overrides',
        help='Override the inferred target type (file, database, api).',
    ),
]

TargetPathOption = Annotated[
    str | None,
    typer.Option(
        '--target',
        metavar='PATH',
        help='Target file for transformed or validated output (- for stdout).',
    ),
]


# SECTION: TYPER APP ======================================================== #


app = typer.Typer(
    name='etlplus',
    help=CLI_DESCRIPTION,
    epilog=CLI_EPILOG,
    add_completion=True,
    no_args_is_help=False,
    rich_markup_mode='markdown',
)


@app.callback(invoke_without_command=True)
def _root(
    ctx: typer.Context,
    version: bool = typer.Option(
        False,
        '--version',
        '-V',
        is_eager=True,
        help='Show the version and exit.',
    ),
    pretty: bool = typer.Option(
        True,
        '--pretty/--no-pretty',
        help='Pretty-print JSON output (default: pretty).',
    ),
    quiet: bool = typer.Option(
        False,
        '--quiet',
        '-q',
        help='Suppress warnings and non-essential output.',
    ),
    verbose: bool = typer.Option(
        False,
        '--verbose',
        '-v',
        help='Emit extra diagnostics to stderr.',
    ),
) -> None:
    """
    Seed the Typer context with runtime flags and handle root-only options.
    """
    ctx.obj = CliState(pretty=pretty, quiet=quiet, verbose=verbose)

    if version:
        typer.echo(f'etlplus {__version__}')
        raise typer.Exit(0)

    if ctx.invoked_subcommand is None and not ctx.resilient_parsing:
        typer.echo(ctx.command.get_help(ctx))
        raise typer.Exit(0)


@app.command('check')
def check_cmd(
    ctx: typer.Context,
    config: PipelineConfigOption,
    jobs: bool = typer.Option(
        False,
        '--jobs',
        help='List available job names and exit',
    ),
    pipelines: bool = typer.Option(
        False,
        '--pipelines',
        help='List ETL pipelines',
    ),
    sources: bool = typer.Option(
        False,
        '--sources',
        help='List data sources',
    ),
    summary: bool = typer.Option(
        False,
        '--summary',
        help='Show pipeline summary (name, version, sources, targets, jobs)',
    ),
    targets: bool = typer.Option(
        False,
        '--targets',
        help='List data targets',
    ),
    transforms: bool = typer.Option(
        False,
        '--transforms',
        help='List data transforms',
    ),
) -> int:
    """Inspect a pipeline configuration."""
    state = ensure_state(ctx)
    ns = stateful_namespace(
        state,
        command='check',
        config=config,
        jobs=jobs,
        pipelines=pipelines,
        sources=sources,
        summary=summary,
        targets=targets,
        transforms=transforms,
    )
    return int(handlers.check_handler(ns))


@app.command('extract')
def extract_cmd(
    ctx: typer.Context,
    source: SourceInputArg,
    source_format: SourceFormatOption | None = None,
    source_type: SourceOverrideOption | None = None,
) -> int:
    """Extract data from files, databases, or REST APIs."""
    state = ensure_state(ctx)

    source_type = optional_choice(
        source_type,
        DATA_CONNECTORS,
        label='source_type',
    )
    source_format = optional_choice(
        source_format,
        FILE_FORMATS,
        label='source_format',
    )

    resolved_source = source
    resolved_source_type = source_type or infer_resource_type_or_exit(
        resolved_source,
    )

    log_inferred_resource(
        state,
        role='source',
        value=resolved_source,
        resource_type=resolved_source_type,
    )

    format_kwargs = format_namespace_kwargs(
        format_value=source_format,
        default=DEFAULT_FILE_FORMAT,
    )
    ns = stateful_namespace(
        state,
        command='extract',
        source_type=resolved_source_type,
        source=resolved_source,
        **format_kwargs,
    )
    return int(handlers.extract_handler(ns))


@app.command('load')
def load_cmd(
    ctx: typer.Context,
    target: TargetInputArg,
    source_format: StdinFormatOption | None = None,
    target_format: TargetFormatOption | None = None,
    target_type: TargetOverrideOption | None = None,
) -> int:
    """Load data into a file, database, or REST API."""
    state = ensure_state(ctx)

    source_format = optional_choice(
        source_format,
        FILE_FORMATS,
        label='source_format',
    )
    target_type = optional_choice(
        target_type,
        DATA_CONNECTORS,
        label='target_type',
    )
    target_format = optional_choice(
        target_format,
        FILE_FORMATS,
        label='target_format',
    )

    resolved_target = target
    resolved_target_type = target_type or infer_resource_type_or_exit(
        resolved_target,
    )

    resolved_source_value = '-'
    resolved_source_type = infer_resource_type_soft(resolved_source_value)

    log_inferred_resource(
        state,
        role='source',
        value=resolved_source_value,
        resource_type=resolved_source_type,
    )
    log_inferred_resource(
        state,
        role='target',
        value=resolved_target,
        resource_type=resolved_target_type,
    )

    format_kwargs = format_namespace_kwargs(
        format_value=target_format,
        default=DEFAULT_FILE_FORMAT,
    )
    ns = stateful_namespace(
        state,
        command='load',
        source=resolved_source_value,
        source_format=source_format,
        target_type=resolved_target_type,
        target=resolved_target,
        **format_kwargs,
    )
    return int(handlers.load_handler(ns))


@app.command('render')
def render_cmd(
    ctx: typer.Context,
    config: RenderConfigOption = None,
    spec: RenderSpecOption = None,
    table: RenderTableOption = None,
    template: RenderTemplateOption = 'ddl',
    template_path: RenderTemplatePathOption = None,
    output: RenderOutputOption = None,
) -> int:
    """Render SQL DDL from table schemas defined in YAML/JSON configs."""
    state = ensure_state(ctx)
    ns = stateful_namespace(
        state,
        command='render',
        config=config,
        spec=spec,
        table=table,
        template=template,
        template_path=template_path,
        output=output,
    )
    return int(handlers.render_handler(ns))


@app.command('run')
def run_cmd(
    ctx: typer.Context,
    config: PipelineConfigOption,
    job: str | None = typer.Option(
        None,
        '-j',
        '--job',
        help='Name of the job to run',
    ),
    pipeline: str | None = typer.Option(
        None,
        '-p',
        '--pipeline',
        help='Name of the pipeline to run',
    ),
) -> int:
    """Execute an ETL job or pipeline from a YAML configuration."""
    state = ensure_state(ctx)
    ns = stateful_namespace(
        state,
        command='run',
        config=config,
        job=job,
        pipeline=pipeline,
    )
    return int(handlers.run_handler(ns))


@app.command('transform')
def transform_cmd(
    ctx: typer.Context,
    operations: OperationsOption = '{}',
    source: StreamingSourceArg = '-',
    source_format: SourceFormatOption | None = None,
    source_type: SourceOverrideOption | None = None,
    target: TargetPathOption | None = None,
    target_format: TargetFormatOption | None = None,
    target_type: TargetOverrideOption | None = None,
) -> int:
    """Transform records using JSON-described operations."""
    state = ensure_state(ctx)

    source_format = optional_choice(
        source_format,
        FILE_FORMATS,
        label='source_format',
    )
    source_type = optional_choice(
        source_type,
        DATA_CONNECTORS,
        label='source_type',
    )
    target_format = optional_choice(
        target_format,
        FILE_FORMATS,
        label='target_format',
    )
    target_format_kwargs = format_namespace_kwargs(
        format_value=target_format,
        default=DEFAULT_FILE_FORMAT,
    )
    target_type = optional_choice(
        target_type,
        DATA_CONNECTORS,
        label='target_type',
    )

    resolved_source_type = source_type or infer_resource_type_soft(source)
    resolved_source_value = source if source is not None else '-'
    resolved_target_value = target if target is not None else '-'

    if resolved_source_type is not None:
        resolved_source_type = validate_choice(
            resolved_source_type,
            DATA_CONNECTORS,
            label='source_type',
        )

    resolved_target_type = resolve_resource_type(
        explicit_type=None,
        override_type=target_type,
        value=resolved_target_value,
        label='target_type',
    )

    log_inferred_resource(
        state,
        role='source',
        value=resolved_source_value,
        resource_type=resolved_source_type,
    )
    log_inferred_resource(
        state,
        role='target',
        value=resolved_target_value,
        resource_type=resolved_target_type,
    )

    ns = stateful_namespace(
        state,
        command='transform',
        source=resolved_source_value,
        source_type=resolved_source_type,
        operations=json_type(operations),
        target=resolved_target_value,
        source_format=source_format,
        target_type=resolved_target_type,
        target_format=target_format_kwargs['format'],
        **target_format_kwargs,
    )
    return int(handlers.transform_handler(ns))


@app.command('validate')
def validate_cmd(
    ctx: typer.Context,
    rules: RulesOption = '{}',
    source: StreamingSourceArg = '-',
    source_format: SourceFormatOption | None = None,
    source_type: SourceOverrideOption | None = None,
    target: TargetPathOption | None = None,
) -> int:
    """Validate data against JSON-described rules."""
    source_format = optional_choice(
        source_format,
        FILE_FORMATS,
        label='source_format',
    )
    source_type = optional_choice(
        source_type,
        DATA_CONNECTORS,
        label='source_type',
    )
    source_format_kwargs = format_namespace_kwargs(
        format_value=source_format,
        default=DEFAULT_FILE_FORMAT,
    )

    state = ensure_state(ctx)
    resolved_source_type = source_type or infer_resource_type_soft(source)

    log_inferred_resource(
        state,
        role='source',
        value=source,
        resource_type=resolved_source_type,
    )

    ns = stateful_namespace(
        state,
        command='validate',
        source=source,
        source_type=resolved_source_type,
        rules=json_type(rules),  # convert CLI string to dict
        target=target,
        source_format=source_format,
        **source_format_kwargs,
    )
    return int(handlers.validate_handler(ns))
