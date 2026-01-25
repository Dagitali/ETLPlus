"""
:mod:`etlplus.cli.commands` module.

Typer application and subcommands for the ``etlplus`` command-line interface
(CLI). Typer (Click) is used for CLI parsing, help text, and subcommand
dispatch. The Typer layer focuses on ergonomics (git-style subcommands,
optional inference of resource types, STDIN/STDOUT piping, and quality-of-life
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
- Use ``-`` to read from STDIN or to write to STDOUT.
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
from ..file import FileFormat
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

PipelineConfigOption = Annotated[
    str,
    typer.Option(
        ...,
        '--config',
        metavar='PATH',
        help='Path to pipeline YAML configuration file.',
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

SourceArg = Annotated[
    str,
    typer.Argument(
        ...,
        metavar='SOURCE',
        help=(
            'Extract data from SOURCE (JSON payload, file/folder path, '
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
        help=(
            'Override the inferred source type (api, database, file, folder).'
        ),
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
            'Load data into TARGET (file/folder path, URI/URL, or - for '
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
        help=(
            'Override the inferred target type (api, database, file, folder).'
        ),
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
        help='Emit extra diagnostics to STDERR.',
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
        Whether to emit extra diagnostics to STDERR. Default is ``False``.

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
    jobs: JobsOption = False,
    pipelines: PipelinesOption = False,
    sources: SourcesOption = False,
    summary: SummaryOption = False,
    targets: TargetsOption = False,
    transforms: TransformsOption = False,
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

    Raises
    ------
    typer.Exit
        When argument order is invalid or required arguments are missing.
    """
    # Argument order enforcement.
    if not config:
        typer.echo("Error: Missing required option '--config'.", err=True)
        raise typer.Exit(2)

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
    source: SourceArg = '-',
    source_format: SourceFormatOption = None,
    source_type: SourceTypeOption = None,
) -> int:
    """
    Extract data from files, databases, or REST APIs.

    Parameters
    ----------
    ctx : typer.Context
        The Typer context.
    source : SourceArg, optional
        Source (JSON payload, file/folder path, URL/URI, or - for STDIN)
        from which to extract data. Default is ``-``.
    source_format : SourceFormatOption, optional
        Data source format. Overrides the inferred format (``csv``, ``json``,
        etc.) based on filename extension or STDIN content. Default is
        ``None``.
    source_type : SourceTypeOption, optional
        Data source type. Overrides the inferred type (``api``, ``database``,
        ``file``, ``folder``) based on URI/URL schema. Default is ``None``.

    Returns
    -------
    int
        Exit code.

    Raises
    ------
    typer.Exit
        When argument order is invalid or required arguments are missing.
    """
    state = ensure_state(ctx)

    # Argument order enforcement
    if source.startswith('--'):
        typer.echo(
            f"Error: Option '{source}' must follow the 'SOURCE' argument.",
            err=True,
        )
        raise typer.Exit(2)
    if not source:
        typer.echo("Error: Missing required argument 'SOURCE'.", err=True)
        raise typer.Exit(2)

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

    resolved_source_type = source_type or infer_resource_type_or_exit(source)

    log_inferred_resource(
        state,
        role='source',
        value=source,
        resource_type=resolved_source_type,
    )

    return int(
        handlers.extract_handler(
            source_type=resolved_source_type,
            source=source,
            format_hint=source_format,
            format_explicit=source_format is not None,
            pretty=state.pretty,
        ),
    )


@app.command('load')
def load_cmd(
    ctx: typer.Context,
    source_format: SourceFormatOption = None,
    target: TargetArg = '-',
    target_format: TargetFormatOption = None,
    target_type: TargetTypeOption = None,
) -> int:
    """
    Load data into a file, database, or REST API.

    Parameters
    ----------
    ctx : typer.Context
        The Typer context.
    source_format : SourceFormatOption, optional
        Data source format. Overrides the inferred format (``csv``, ``json``,
        etc.) based on filename extension or STDIN content. Default is
        ``None``.
    target : TargetArg, optional
        Target (file/folder path, URL/URI, or - for STDOUT) into which to load
        data. Default is ``-``.
    target_format : TargetFormatOption, optional
        Target data format. Overrides the inferred format (``csv``, ``json``,
        etc.) based on filename extension. Default is ``None``.
    target_type : TargetTypeOption, optional
        Data target type. Overrides the inferred type (``api``, ``database``,
        ``file``, ``folder``) based on URI/URL schema. Default is ``None``.

    Returns
    -------
    int
        Exit code.

    Raises
    ------
    typer.Exit
        When argument order is invalid or required arguments are missing.
    """
    # Argument order enforcement
    if target.startswith('--'):
        typer.echo(
            f"Error: Option '{target}' must follow the 'TARGET' argument.",
            err=True,
        )
        raise typer.Exit(2)
    if not target:
        typer.echo("Error: Missing required argument 'TARGET'.", err=True)
        raise typer.Exit(2)

    state = ensure_state(ctx)

    source_format = cast(
        SourceFormatOption,
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
    output: OutputOption = None,
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
    output : OutputOption, optional
        Path of file to which to write rendered SQL (default: STDOUT).

    Returns
    -------
    int
        Exit code.

    Raises
    ------
    typer.Exit
        When argument order is invalid or required arguments are missing.
    """
    # Argument order enforcement
    if not (config or spec):
        typer.echo(
            "Error: Missing required option '--config' or '--spec'.",
            err=True,
        )
        raise typer.Exit(2)

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
    job: JobOption = None,
    pipeline: PipelineOption = None,
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

    Raises
    ------
    typer.Exit
        When argument order is invalid or required arguments are missing.
    """
    # Argument order enforcement
    if not config:
        typer.echo("Error: Missing required option '--config'.", err=True)
        raise typer.Exit(2)

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
    source: SourceArg = '-',
    source_format: SourceFormatOption = None,
    source_type: SourceTypeOption = None,
    target: TargetArg = '-',
    target_format: TargetFormatOption = None,
    target_type: TargetTypeOption = None,
) -> int:
    """
    Transform records using JSON-described operations.

    Parameters
    ----------
    ctx : typer.Context
        The Typer context.
    operations : OperationsOption, optional
        Transformation operations as JSON string. Default is ``{}``.
    source : SourceArg, optional
        Source (JSON payload, file/folder path, URL/URI, or - for STDIN) from
        which to extract data. Default is ``-``.
    source_format : SourceFormatOption, optional
        Data source format. Overrides the inferred format (``csv``, ``json``,
        etc.) based on filename extension or STDIN content. Default is
        ``None``.
    source_type : SourceTypeOption, optional
        Data source type. Overrides the inferred type (``api``, ``database``,
        ``file``, ``folder``) based on URI/URL schema. Default is ``None``.
    target : TargetArg, optional
        Target (file/folder path, URL/URI, or - for STDOUT) into which to load
        data. Default is ``-``.
    target_format : TargetFormatOption, optional
        Target data format. Overrides the inferred format (``csv``, ``json``,
        etc.) based on filename extension. Default is ``None``.
    target_type : TargetTypeOption, optional
        Data target type. Overrides the inferred type (``api``, ``database``,
        ``file``, ``folder``) based on URI/URL schema. Default is ``None``.

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
    source: SourceArg = '-',
    source_format: SourceFormatOption = None,
    source_type: SourceTypeOption = None,
    output: OutputOption = '-',
) -> int:
    """
    Validate data against JSON-described rules.

    Parameters
    ----------
    ctx : typer.Context
        The Typer context.
    rules : RulesOption
        Validation rules as JSON string.
    source : SourceArg
        Data source to validate (path, JSON payload, or - for STDIN).
    source_format : SourceFormatOption, optional
        Data source format. Overrides the inferred format (``csv``, ``json``,
        etc.) based on filename extension or STDIN content. Default is
        ``None``.
    source_type : SourceTypeOption, optional
        Data source type. Overrides the inferred type (``api``, ``database``,
        ``file``, ``folder``) based on URI/URL schema. Default is ``None``.
    output : OutputOption, optional
        Output file for validated output (- for STDOUT). Default is ``None``.

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
            target=output,
            format_explicit=source_format is not None,
            pretty=state.pretty,
        ),
    )
