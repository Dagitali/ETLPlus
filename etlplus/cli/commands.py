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
from typing import Any
from typing import Literal
from typing import cast

import typer

from .. import __version__
from ..enums import FileFormat
from . import handlers
from .constants import CLI_DESCRIPTION
from .constants import CLI_EPILOG
from .constants import DATA_CONNECTORS
from .constants import FILE_FORMATS
from .io import parse_json_payload
from .options import typer_format_option_kwargs
from .state import CliState
from .state import ensure_state
from .state import infer_resource_type_or_exit
from .state import infer_resource_type_soft
from .state import log_inferred_resource
from .state import optional_choice
from .state import resolve_resource_type
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
    FileFormat | None,
    typer.Option(
        '--source-format',
        **typer_format_option_kwargs(context='source'),
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
    FileFormat | None,
    typer.Option(
        '--source-format',
        **typer_format_option_kwargs(context='source'),
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
    FileFormat | None,
    typer.Option(
        '--target-format',
        **typer_format_option_kwargs(context='target'),
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


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _parse_json_option(
    value: str,
    flag: str,
) -> Any:
    """
    Parse JSON option values and surface a helpful CLI error.

    Parameters
    ----------
    value : str
        The JSON string to parse.
    flag : str
        The CLI flag name for error messages.

    Returns
    -------
    Any
        The parsed JSON value.

    Raises
    ------
    typer.BadParameter
        When the JSON is invalid.
    """
    try:
        return parse_json_payload(value)
    except ValueError as e:
        raise typer.BadParameter(
            f'Invalid JSON for {flag}: {e}',
        ) from e


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

    Parameters
    ----------
    ctx : typer.Context
        The Typer command context.
    version : bool, optional
        Show the version and exit. Default is ``False``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.
    quiet : bool, optional
        Whether to suppress warnings and non-essential output. Default is
        ``False``.
    verbose : bool, optional
        Whether to emit extra diagnostics to stderr. Default is ``False``.

    Raises
    ------
    typer.Exit
        When ``--version`` is provided or no subcommand is invoked.
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
    """
    Inspect a pipeline configuration.

    Parameters
    ----------
    ctx : typer.Context
        The Typer context.
    config : PipelineConfigOption
        Path to pipeline YAML configuration file.
    jobs : bool, optional
        List available job names and exit. Default is ``False``.
    pipelines : bool, optional
        List ETL pipelines. Default is ``False``.
    sources : bool, optional
        List data sources. Default is ``False``.
    summary : bool, optional
        Show pipeline summary (name, version, sources, targets, jobs). Default
        is ``False``.
    targets : bool, optional
        List data targets. Default is ``False``.
    transforms : bool, optional
        List data transforms. Default is ``False``.

    Returns
    -------
    int
        Exit code.
    """
    state = ensure_state(ctx)
    return int(
        handlers.check_handler(
            config=config,
            jobs=jobs,
            pipelines=pipelines,
            sources=sources,
            summary=summary,
            targets=targets,
            transforms=transforms,
            pretty=state.pretty,
        ),
    )


@app.command('extract')
def extract_cmd(
    ctx: typer.Context,
    source: SourceInputArg,
    source_format: SourceFormatOption | None = None,
    source_type: SourceOverrideOption | None = None,
) -> int:
    """
    Extract data from files, databases, or REST APIs.

    Parameters
    ----------
    ctx : typer.Context
        The Typer context.
    source : SourceInputArg
        Extract from SOURCE. Use --from/--source-type to override the inferred
        connector when needed.
    source_format : SourceFormatOption | None, optional
        Format of the source. Overrides filename-based inference when provided.
        Default is ``None``.
    source_type : SourceOverrideOption | None, optional
        Override the inferred source type (file, database, api). Default is
        ``None``.

    Returns
    -------
    int
        Exit code.
    """
    state = ensure_state(ctx)

    source_type = optional_choice(
        source_type,
        DATA_CONNECTORS,
        label='source_type',
    )
    source_format = cast(
        SourceFormatOption,
        optional_choice(
            source_format,
            FILE_FORMATS,
            label='source_format',
        ),
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

    return int(
        handlers.extract_handler(
            source_type=resolved_source_type,
            source=resolved_source,
            format_hint=source_format,
            format_explicit=source_format is not None,
            pretty=state.pretty,
        ),
    )


@app.command('load')
def load_cmd(
    ctx: typer.Context,
    target: TargetInputArg,
    source_format: StdinFormatOption = None,
    target_format: TargetFormatOption = None,
    target_type: TargetOverrideOption = None,
) -> int:
    """
    Load data into a file, database, or REST API.

    Parameters
    ----------
    ctx : typer.Context
        The Typer context.
    target : TargetInputArg
        Load JSON data from stdin into TARGET. Use --to/--target-type to
        override connector inference when needed. Source data must be piped
        into stdin.
    source_format : StdinFormatOption, optional
        Format of the source. Overrides filename-based inference when provided.
        Default is ``None``.
    target_format : TargetFormatOption, optional
        Format of the target. Overrides filename-based inference when provided.
        Default is ``None``.
    target_type : TargetOverrideOption, optional
        Override the inferred target type (file, database, api). Default is
        ``None``.

    Returns
    -------
    int
        Exit code.
    """
    state = ensure_state(ctx)

    source_format = cast(
        StdinFormatOption,
        optional_choice(
            source_format,
            FILE_FORMATS,
            label='source_format',
        ),
    )
    target_type = optional_choice(
        target_type,
        DATA_CONNECTORS,
        label='target_type',
    )
    target_format = cast(
        TargetFormatOption,
        optional_choice(
            target_format,
            FILE_FORMATS,
            label='target_format',
        ),
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

    return int(
        handlers.load_handler(
            source=resolved_source_value,
            target_type=resolved_target_type,
            target=resolved_target,
            source_format=source_format,
            target_format=target_format,
            format_explicit=target_format is not None,
            output=None,
            pretty=state.pretty,
        ),
    )


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
    """
    Render SQL DDL from table schemas defined in YAML/JSON configs.

    Parameters
    ----------
    ctx : typer.Context
        The Typer context.
    config : RenderConfigOption
        Pipeline YAML that includes table_schemas for rendering.
    spec : RenderSpecOption, optional
        Standalone table spec file (.yml/.yaml/.json).
    table : RenderTableOption, optional
        Filter to a single table name from table_schemas.
    template : RenderTemplateOption
        Template key (ddl/view) or path to a Jinja template file.
    template_path : RenderTemplatePathOption, optional
        Explicit path to a Jinja template file (overrides template key).
    output : RenderOutputOption, optional
        Write rendered SQL to PATH (default: stdout).

    Returns
    -------
    int
        Exit code.
    """
    state = ensure_state(ctx)
    return int(
        handlers.render_handler(
            config=config,
            spec=spec,
            table=table,
            template=template,
            template_path=template_path,
            output=output,
            pretty=state.pretty,
            quiet=state.quiet,
        ),
    )


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
    """
    Execute an ETL job or pipeline from a YAML configuration.

    Parameters
    ----------
    ctx : typer.Context
        The Typer context.
    config : PipelineConfigOption
        Path to pipeline YAML configuration file.
    job : str | None, optional
        Name of the job to run. Default is ``None``.
    pipeline : str | None, optional
        Name of the pipeline to run. Default is ``None``.

    Returns
    -------
    int
        Exit code.
    """
    state = ensure_state(ctx)
    return int(
        handlers.run_handler(
            config=config,
            job=job,
            pipeline=pipeline,
            pretty=state.pretty,
        ),
    )


@app.command('transform')
def transform_cmd(
    ctx: typer.Context,
    operations: OperationsOption = '{}',
    source: StreamingSourceArg = '-',
    source_format: SourceFormatOption = None,
    source_type: SourceOverrideOption = None,
    target: TargetPathOption = None,
    target_format: TargetFormatOption = None,
    target_type: TargetOverrideOption = None,
) -> int:
    """
    Transform records using JSON-described operations.

    Parameters
    ----------
    ctx : typer.Context
        The Typer context.
    operations : OperationsOption
        Transformation operations as JSON string.
    source : StreamingSourceArg
        Data source to transform (path, JSON payload, or - for stdin).
    source_format : SourceFormatOption, optional
        Format of the source. Overrides filename-based inference when provided.
        Default is ``None``.
    source_type : SourceOverrideOption, optional
        Override the inferred source type (file, database, api). Default is
        ``None``.
    target : TargetPathOption, optional
        Target file for transformed output (- for stdout). Default is ``None``.
    target_format : TargetFormatOption, optional
        Format of the target. Overrides filename-based inference when provided.
        Default is ``None``.
    target_type : TargetOverrideOption, optional
        Override the inferred target type (file, database, api). Default is
        ``None``.

    Returns
    -------
    int
        Exit code.
    """
    state = ensure_state(ctx)

    source_format = cast(
        SourceFormatOption,
        optional_choice(
            source_format,
            FILE_FORMATS,
            label='source_format',
        ),
    )
    source_type = optional_choice(
        source_type,
        DATA_CONNECTORS,
        label='source_type',
    )
    target_format = cast(
        TargetFormatOption,
        optional_choice(
            target_format,
            FILE_FORMATS,
            label='target_format',
        ),
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

    return int(
        handlers.transform_handler(
            source=resolved_source_value,
            operations=_parse_json_option(operations, '--operations'),
            target=resolved_target_value,
            source_format=source_format,
            target_format=target_format,
            format_explicit=target_format is not None,
            pretty=state.pretty,
        ),
    )


@app.command('validate')
def validate_cmd(
    ctx: typer.Context,
    rules: RulesOption = '{}',
    source: StreamingSourceArg = '-',
    source_format: SourceFormatOption = None,
    source_type: SourceOverrideOption = None,
    target: TargetPathOption = None,
) -> int:
    """
    Validate data against JSON-described rules.

    Parameters
    ----------
    ctx : typer.Context
        The Typer context.
    rules : RulesOption
        Validation rules as JSON string.
    source : StreamingSourceArg
        Data source to validate (path, JSON payload, or - for stdin).
    source_format : SourceFormatOption, optional
        Format of the source. Overrides filename-based inference when provided.
        Default is ``None``.
    source_type : SourceOverrideOption, optional
        Override the inferred source type (file, database, api). Default is
        ``None``.
    target : TargetPathOption, optional
        Target file for validated output (- for stdout). Default is ``None``.

    Returns
    -------
    int
        Exit code.
    """
    source_format = cast(
        SourceFormatOption,
        optional_choice(
            source_format,
            FILE_FORMATS,
            label='source_format',
        ),
    )
    source_type = optional_choice(
        source_type,
        DATA_CONNECTORS,
        label='source_type',
    )
    state = ensure_state(ctx)
    resolved_source_type = source_type or infer_resource_type_soft(source)

    log_inferred_resource(
        state,
        role='source',
        value=source,
        resource_type=resolved_source_type,
    )

    return int(
        handlers.validate_handler(
            source=source,
            rules=_parse_json_option(rules, '--rules'),
            source_format=source_format,
            target=target,
            format_explicit=source_format is not None,
            pretty=state.pretty,
        ),
    )
