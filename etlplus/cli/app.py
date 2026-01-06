"""
:mod:`etlplus.cli.app` module.

Defines the main `Typer` application for the ``etlplus`` command-line
interface (CLI).

Typer-First Interface
---------------------
The CLI is implemented using `Typer` (Click) for parsing, help text, and
subcommand dispatch. The Typer layer focuses on ergonomics (git-style
subcommands, optional inference of resource types, stdin/stdout piping, and
quality-of-life flags), while delegating business logic to the existing
``cmd_*`` handlers.

Namespace Adapter
-----------------
The command handlers continue to accept an ``argparse.Namespace`` for
backwards compatibility with existing ``cmd_*`` functions and tests. The
Typer commands adapt parsed arguments into an ``argparse.Namespace`` and then
call the corresponding ``cmd_*`` handler.

Subcommands
-----------
- ``extract``: extract data from files, databases, or REST APIs
- ``validate``: validate data against rules
- ``transform``: transform records
- ``load``: load data to files, databases, or REST APIs

Notes
-----
- Use ``-`` to read from stdin or to write to stdout.
- Commands ``extract`` and ``transform`` support the command-line option
    ``--from`` to override inferred resource types.
- Commands ``transform`` and ``load`` support the command-line option ``--to``
    to override inferred resource types.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import typer

from .. import __version__
from ..enums import DataConnectorType
from ..enums import FileFormat
from ..utils import json_type
from .handlers import cmd_extract
from .handlers import cmd_list
from .handlers import cmd_load
from .handlers import cmd_pipeline
from .handlers import cmd_run
from .handlers import cmd_transform
from .handlers import cmd_validate

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Apps
    'app',
]


# SECTION: CONSTANTS ======================================================== #


CLI_DESCRIPTION: Final[str] = '\n'.join(
    [
        'ETLPlus - A Swiss Army knife for simple ETL operations.',
        '',
        '    Provide a subcommand and options. Examples:',
        '',
        '    etlplus extract in.csv > out.json',
        '    etlplus validate in.json --rules \'{"required": ["id"]}\'',
        (
            '    etlplus transform --from file in.json '
            '--operations \'{"select": ["id"]}\' --to file -o out.json'
        ),
        '    etlplus extract in.csv | etlplus load --to file out.json',
        '    cat data.json | etlplus load --to api https://example.com/data',
        '',
        '    Override format inference when extensions are misleading:',
        '',
        '    etlplus extract data.txt --source-format csv',
        '    etlplus load payload.bin --target-format json',
    ],
)

CLI_EPILOG: Final[str] = '\n'.join(
    [
        'Tip:',
        '    --source-format and --target-format override format inference '
        'based on filename extensions when needed.',
    ],
)

PROJECT_URL: Final[str] = 'https://github.com/Dagitali/ETLPlus'

EXTRACT_ARGS = typer.Argument(
    ...,
    metavar='[SOURCE_TYPE] SOURCE',
    help=(
        'Extract from a SOURCE. You may provide SOURCE_TYPE explicitly as '
        'the first positional argument, or omit it and use --from or let '
        'etlplus infer it from the SOURCE.'
    ),
)
LOAD_ARGS = typer.Argument(
    ...,
    metavar='[TARGET_TYPE] TARGET',
    help=(
        'Load JSON data from stdin into a target. Provide TARGET only, or '
        'prefix it with TARGET_TYPE to avoid --to. Source data must be piped '
        'into stdin.'
    ),
)


# SECTION: INTERNAL CONSTANTS =============================================== #


_DB_SCHEMES = (
    'postgres://',
    'postgresql://',
    'mysql://',
)

_SOURCE_CHOICES: Final[frozenset[str]] = frozenset(DataConnectorType.choices())
_FORMAT_CHOICES: Final[frozenset[str]] = frozenset(FileFormat.choices())


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True)
class CliState:
    """Mutable container for runtime CLI toggles."""

    pretty: bool = True
    quiet: bool = False
    verbose: bool = False


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _ensure_state(
    ctx: typer.Context,
) -> CliState:
    """
    Return the :class:`CliState` stored on the :mod:`typer` context.

    Parameters
    ----------
    ctx : typer.Context
        Typer execution context provided to the command.

    Returns
    -------
    CliState
        Mutable CLI flag container stored on ``ctx``.
    """
    if not isinstance(getattr(ctx, 'obj', None), CliState):
        ctx.obj = CliState()
    return ctx.obj


def _format_namespace_kwargs(
    *,
    format_value: str | None,
    default: str,
) -> dict[str, object]:
    """
    Return common namespace kwargs for format handling.

    Parameters
    ----------
    format_value : str | None
        User-provided format value from the CLI option.
    default : str
        Default format to use when none is provided.

    Returns
    -------
    dict[str, object]
        Keyword arguments for format-related namespace attributes.
    """
    return {
        'format': (format_value or default),
        '_format_explicit': (format_value is not None),
    }


def _infer_resource_type(
    value: str,
) -> str:
    """
    Infer the resource type from a path, URL, or DSN string.

    Parameters
    ----------
    value : str
        Raw CLI argument that represents a source or target.

    Returns
    -------
    str
        One of ``file``, ``database``, or ``api`` based on heuristics.

    Raises
    ------
    ValueError
        If the resource type could not be inferred.
    """
    val = (value or '').strip()
    low = val.lower()

    if val == '-':
        return 'file'
    if low.startswith(('http://', 'https://')):
        return 'api'
    if low.startswith(_DB_SCHEMES):
        return 'database'

    path = Path(val)
    if path.exists() or path.suffix:
        return 'file'

    raise ValueError(
        'Could not infer resource type. Use --from/--to to specify it.',
    )


def _infer_resource_type_or_exit(
    value: str,
) -> str:
    """Infer a resource type and map ``ValueError`` to ``BadParameter``.

    Parameters
    ----------
    value : str
        CLI value describing a source/target.

    Returns
    -------
    str
        Inferred resource type.

    Raises
    ------
    typer.BadParameter
        If heuristics fail to infer a resource type.
    """
    try:
        return _infer_resource_type(value)
    except ValueError as exc:  # pragma: no cover - exercised indirectly
        raise typer.BadParameter(str(exc)) from exc


def _infer_resource_type_soft(
    value: str | None,
) -> str | None:
    """
    Make a best-effort inference that tolerates inline payloads.

    Parameters
    ----------
    value : str | None
        CLI value describing a source/target.

    Returns
    -------
    str | None
        Inferred resource type, or ``None`` if inference failed.
    """
    if value is None:
        return None
    try:
        return _infer_resource_type(value)
    except ValueError:
        return None


def _log_inferred_resource(
    state: CliState,
    *,
    role: str,
    value: str,
    resource_type: str | None,
) -> None:
    """
    Emit a uniform verbose message for inferred resource types.

    Parameters
    ----------
    state : CliState
        Current CLI state stored on the Typer context.
    role : str
        Friendly label for the resource (e.g., ``source`` or ``target``).
    value : str
        Resource value provided on the CLI.
    resource_type : str | None
        Inferred resource type or ``None`` if not inferred.
    """
    if not state.verbose or resource_type is None:
        return
    print(
        f'Inferred {role}_type={resource_type} for {role}={value}',
        file=sys.stderr,
    )


def _ns(
    **kwargs: object,
) -> argparse.Namespace:
    """Build an :class:`argparse.Namespace` for the legacy handlers.

    Parameters
    ----------
    **kwargs : object
        Attributes applied to the resulting namespace.

    Returns
    -------
    argparse.Namespace
        Namespace compatible with the ``cmd_*`` handler signatures.
    """
    return argparse.Namespace(**kwargs)


def _optional_choice(
    value: str | None,
    choices: Collection[str],
    *,
    label: str,
) -> str | None:
    """
    Validate optional CLI choice inputs while preserving ``None``.

    Parameters
    ----------
    value : str | None
        Candidate value provided by the CLI option.
    choices : Collection[str]
        Allowed options for the parameter.
    label : str
        Friendly label rendered in error messages.

    Returns
    -------
    str | None
        Sanitized choice or ``None`` when the option is omitted.
    """
    if value is None:
        return None
    return _validate_choice(value, choices, label=label)


def _parse_positional_resource(
    args: list[str],
    *,
    subject: str,
    subject_type_label: str,
    error_hint: str | None = None,
) -> tuple[str | None, str]:
    """
    Return the explicit resource type (if any) and value.

    Parameters
    ----------
    args : list[str]
        Positional arguments provided to the command.
    subject : str
        Friendly label for the resource (e.g., ``SOURCE`` or ``TARGET``).
    subject_type_label : str
        Friendly label for the resource type (e.g., ``SOURCE_TYPE`` or
        ``TARGET_TYPE``).
    error_hint : str | None
        Optional hint to include in the error message.

    Returns
    -------
    tuple[str | None, str]
        Explicit resource type (if any) and resource value.

    Raises
    ------
    typer.BadParameter
        If the arguments are invalid.
    """
    match args:
        case [resource_type, resource_value]:
            return resource_type, resource_value
        case [resource_value]:
            return None, resource_value
    message = f'Provide {subject} or {subject_type_label} {subject}.'
    if error_hint:
        message = f'{message} {error_hint}'
    raise typer.BadParameter(message)


def _resolve_resource_type(
    *,
    explicit_type: str | None,
    override_type: str | None,
    value: str,
    label: str,
    conflict_error: str | None = None,
    legacy_file_error: str | None = None,
) -> str:
    """
    Resolve resource type preference order and validate it.

    Parameters
    ----------
    explicit_type : str | None
        Explicit resource type provided by the user.
    override_type : str | None
        Resource type provided by an overriding option.
    value : str
        Resource value to infer type from if no explicit or override type is
        given.
    label : str
        Friendly label for error messages.
    conflict_error : str | None
        Error message to raise if there is a conflict between explicit and
        override types.
    legacy_file_error : str | None
        Error message to raise if the explicit type is a legacy 'file' type.

    Returns
    -------
    str
        Resolved and validated resource type.

    Raises
    ------
    typer.BadParameter
        If there is a conflict between explicit and override types, or if the
        explicit type is a legacy 'file' type.
    """
    if explicit_type is not None:
        if override_type is not None and conflict_error:
            raise typer.BadParameter(conflict_error)
        if legacy_file_error and explicit_type.strip().lower() == 'file':
            raise typer.BadParameter(legacy_file_error)
        candidate = explicit_type
    else:
        candidate = override_type or _infer_resource_type_or_exit(value)
    return _validate_choice(candidate, _SOURCE_CHOICES, label=label)


def _stateful_namespace(
    state: CliState,
    *,
    command: str,
    **kwargs: object,
) -> argparse.Namespace:
    """
    Attach CLI state toggles to a handler namespace.

    Parameters
    ----------
    state : CliState
        Current CLI state stored on the Typer context.
    command : str
        Logical command name (e.g., ``extract``).
    **kwargs : object
        Additional attributes required by the handler.

    Returns
    -------
    argparse.Namespace
        Namespace compatible with the ``cmd_*`` handler signatures.
    """
    return _ns(
        command=command,
        pretty=state.pretty,
        quiet=state.quiet,
        verbose=state.verbose,
        **kwargs,
    )


def _validate_choice(
    value: str,
    choices: Collection[str],
    *,
    label: str,
) -> str:
    """
    Validate CLI input against a whitelist of choices.

    Parameters
    ----------
    value : str
        Candidate value from the CLI option or argument.
    choices: Collection[str]
        Allowed values for the option.
    label : str
        Friendly label rendered in the validation error message.

    Returns
    -------
    str
        Sanitized and validated value.

    Raises
    ------
    typer.BadParameter
        If ``value`` is not present in ``choices``.
    """
    v = (value or '').strip()
    if v in choices:
        return v
    allowed = ', '.join(sorted(choices))
    raise typer.BadParameter(
        f"Invalid {label} '{value}'. Choose from: {allowed}",
    )


# SECTION: TYPER APP ======================================================== #


# Typer application instance (subcommands are registered below).
app = typer.Typer(
    name='etlplus',
    # help='ETLPlus - A Swiss Army knife for simple ETL operations.',
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
        Typer execution context provided to the command.
    version : bool
        If True, print the etlplus version and exit.
    pretty : bool
        Whether to pretty-print JSON output.
    quiet : bool
        Whether to suppress warnings and non-essential output.
    verbose : bool
        Whether to emit extra diagnostics to stderr.

    Raises
    ------
    typer.Exit
        If ``--version`` is provided or no subcommand is invoked.
    """
    ctx.obj = CliState(pretty=pretty, quiet=quiet, verbose=verbose)

    if version:
        typer.echo(f'etlplus {__version__}')
        raise typer.Exit(0)

    if ctx.invoked_subcommand is None and not ctx.resilient_parsing:
        typer.echo(ctx.command.get_help(ctx))
        raise typer.Exit(0)


@app.command('extract')
def extract_cmd(
    ctx: typer.Context,
    args: list[str] = EXTRACT_ARGS,
    from_: str | None = typer.Option(
        None,
        '--from',
        help='Override the inferred source type (file, database, api).',
    ),
    source_format: str | None = typer.Option(
        None,
        '--source-format',
        help=(
            'Payload format when not a file (or when SOURCE is -). '
            'For normal file paths, format is inferred from extension.'
        ),
    ),
) -> int:
    """
    Extract data from files, databases, or REST APIs.

    Parameters
    ----------
    ctx : typer.Context
        Typer execution context provided to the command.
    args : list[str]
        Positional arguments: either SOURCE, or SOURCE_TYPE SOURCE. The
        legacy ``SOURCE_TYPE=file`` form is rejected; use ``--from file``
        instead.
    from_ : str | None
        Override the inferred source type.
    source_format : str | None
        Payload format when not a file.

    Returns
    -------
    int
        Zero on success.

    Raises
    ------
    typer.BadParameter
        If invalid parameters are provided.

    Examples
    --------
    - Extract from a file (type inferred):
        etlplus extract in.csv
    - Extract from a file (explicit via flag):
        etlplus extract --from file in.csv
    - Extract from an API:
        etlplus extract https://example.com/data.json
        etlplus extract --from api https://example.com/data.json
    - Extract from a database DSN:
        etlplus extract --from database postgresql://user:pass@host/db
    - Pipe into transform/load:
        etlplus extract in.csv \
        | etlplus transform --operations '{"select":["a"]}'

    Notes
    -----
    - The ``extract`` command always writes JSON to stdout.
    - CSV output is unsupported for this command.
    - Use shell redirection (``>``) or pipelines to persist the output.
    """
    if len(args) not in (1, 2):
        raise typer.BadParameter('Provide SOURCE, or SOURCE_TYPE SOURCE.')

    state = _ensure_state(ctx)

    from_ = _optional_choice(from_, _SOURCE_CHOICES, label='from')
    source_format = _optional_choice(
        source_format,
        _FORMAT_CHOICES,
        label='source_format',
    )

    explicit_source_type, resolved_source = _parse_positional_resource(
        args,
        subject='SOURCE',
        subject_type_label='SOURCE_TYPE',
    )

    source_type = _resolve_resource_type(
        explicit_type=explicit_source_type,
        override_type=from_,
        value=resolved_source,
        label='source_type',
        conflict_error='Do not combine --from with an explicit SOURCE_TYPE.',
        legacy_file_error=(
            "Legacy form 'etlplus extract file SOURCE' is no longer "
            'supported. Omit SOURCE_TYPE or pass --from file instead.'
            if explicit_source_type is not None
            else None
        ),
    )

    _log_inferred_resource(
        state,
        role='source',
        value=resolved_source,
        resource_type=source_type,
    )

    format_kwargs = _format_namespace_kwargs(
        format_value=source_format,
        default='json',
    )
    ns = _stateful_namespace(
        state,
        command='extract',
        source_type=source_type,
        source=resolved_source,
        **format_kwargs,
    )
    return int(cmd_extract(ns))


@app.command('list')
def list_cmd(
    ctx: typer.Context,
    config: str = typer.Option(
        ...,
        '--config',
        help='Path to pipeline YAML configuration file',
    ),
    pipelines: bool = typer.Option(
        False,
        '--pipelines',
        help='List ETL pipelines',
    ),
    jobs: bool = typer.Option(
        False,
        '--jobs',
        help='List available job names and exit',
    ),
    sources: bool = typer.Option(False, '--sources', help='List data sources'),
    targets: bool = typer.Option(False, '--targets', help='List data targets'),
    transforms: bool = typer.Option(
        False,
        '--transforms',
        help='List data transforms',
    ),
) -> int:
    """
    Print ETL entities from a pipeline YAML configuration.

    Parameters
    ----------
    ctx : typer.Context
        Typer execution context provided to the command.
    config : str
        Path to pipeline YAML configuration file.
    pipelines : bool
        If True, list ETL pipelines.
    jobs : bool
        If True, list available job names and exit.
    sources : bool
        If True, list data sources.
    targets : bool
        If True, list data targets.
    transforms : bool
        If True, list data transforms.

    Returns
    -------
    int
        Zero on success.
    """
    state = _ensure_state(ctx)
    ns = _stateful_namespace(
        state,
        command='list',
        config=config,
        pipelines=pipelines,
        jobs=jobs,
        sources=sources,
        targets=targets,
        transforms=transforms,
    )
    return int(cmd_list(ns))


@app.command('load')
def load_cmd(
    ctx: typer.Context,
    args: list[str] = LOAD_ARGS,
    source_format: str | None = typer.Option(
        None,
        '--source-format',
        help=(
            'Input payload format when reading from stdin. '
            'JSON is assumed when omitted.'
        ),
    ),
    to: str | None = typer.Option(
        None,
        '--to',
        help='Override the inferred target type (file, database, api).',
    ),
    target_format: str | None = typer.Option(
        None,
        '--target-format',
        help=(
            'Payload format when not a file (or when TARGET is -). '
            'For normal file targets, format is inferred from extension.'
        ),
    ),
) -> int:
    """
    Load data into a file, database, or REST API.

    Parameters
    ----------
    ctx : typer.Context
        Typer execution context provided to the command.
    args : list[str]
        Positional arguments: either TARGET, or TARGET_TYPE TARGET. Source
        data must be piped into stdin.
    source_format : str | None
        Hint for parsing stdin payloads (json or csv).
    to : str | None
        Override the inferred target type.
    target_format : str | None
        Payload format when not a file target (or when TARGET is ``-``).

    Returns
    -------
    int
        Zero on success.

    Raises
    ------
    typer.BadParameter
        If the arguments are invalid

    Examples
    --------
    - Pipe into a file:
        etlplus extract in.csv \
        | etlplus transform --operations '{"select":["a"]}' \
        | etlplus load --to file out.json
    - Read from stdin and write to a file:
        etlplus load out.json
    - Write to stdout:
        etlplus load --to file -

    Notes
    -----
    - The ``load`` command reads JSON from stdin.
    - CSV input is unsupported unless ``--source-format csv`` is provided.
    - Convert upstream before piping into ``load`` when working with other
        formats.
    """
    if len(args) not in (1, 2):
        raise typer.BadParameter('Provide TARGET, or TARGET_TYPE TARGET.')

    state = _ensure_state(ctx)

    source_format = _optional_choice(
        source_format,
        _FORMAT_CHOICES,
        label='source_format',
    )
    to = _optional_choice(to, _SOURCE_CHOICES, label='to')
    target_format = _optional_choice(
        target_format,
        _FORMAT_CHOICES,
        label='format',
    )

    explicit_target_type, resolved_target = _parse_positional_resource(
        args,
        subject='TARGET',
        subject_type_label='TARGET_TYPE',
        error_hint=(
            'Pipe source data into stdin (for example, "cat input.json | '
            'etlplus load out.json").'
        ),
    )

    resolved_target_type = _resolve_resource_type(
        explicit_type=explicit_target_type,
        override_type=to,
        value=resolved_target,
        label='target_type',
        conflict_error='Do not combine --to with an explicit TARGET_TYPE.',
        legacy_file_error=(
            "Legacy form 'etlplus load file TARGET' is no longer "
            'supported. Omit TARGET_TYPE or pass --to file instead.'
            if explicit_target_type is not None
            else None
        ),
    )

    resolved_source_value = '-'
    resolved_source_type = _infer_resource_type_soft(resolved_source_value)

    _log_inferred_resource(
        state,
        role='source',
        value=resolved_source_value,
        resource_type=resolved_source_type,
    )
    _log_inferred_resource(
        state,
        role='target',
        value=resolved_target,
        resource_type=resolved_target_type,
    )

    format_kwargs = _format_namespace_kwargs(
        format_value=target_format,
        default='json',
    )
    ns = _stateful_namespace(
        state,
        command='load',
        source=resolved_source_value,
        source_format=source_format,
        target_type=resolved_target_type,
        target=resolved_target,
        **format_kwargs,
    )
    return int(cmd_load(ns))


@app.command('pipeline')
def pipeline_cmd(
    ctx: typer.Context,
    config: str = typer.Option(
        ...,
        '--config',
        help='Path to pipeline YAML configuration file',
    ),
    list_jobs: bool = typer.Option(
        False,
        '--list',
        help='List available job names and exit',
    ),
    run_job: str | None = typer.Option(
        None,
        '--run',
        metavar='JOB',
        help='Run a specific job by name',
    ),
) -> int:
    """
    Inspect or run a pipeline YAML configuration.

    Parameters
    ----------
    ctx : typer.Context
        Typer execution context provided to the command.
    config : str
        Path to pipeline YAML configuration file.
    list_jobs : bool
        If True, list available job names and exit.
    run_job : str | None
        Name of a specific job to run.

    Returns
    -------
    int
        Zero on success.
    """
    state = _ensure_state(ctx)
    ns = _stateful_namespace(
        state,
        command='pipeline',
        config=config,
        list=list_jobs,
        run=run_job,
    )
    return int(cmd_pipeline(ns))


@app.command('run')
def run_cmd(
    ctx: typer.Context,
    config: str = typer.Option(
        ...,
        '--config',
        help='Path to pipeline YAML configuration file',
    ),
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
        Typer execution context provided to the command.
    config : str
        Path to pipeline YAML configuration file.
    job : str | None
        Name of the job to run.
    pipeline : str | None
        Name of the pipeline to run.

    Returns
    -------
    int
        Zero on success.
    """
    state = _ensure_state(ctx)
    ns = _stateful_namespace(
        state,
        command='run',
        config=config,
        job=job,
        pipeline=pipeline,
    )
    return int(cmd_run(ns))


@app.command('transform')
def transform_cmd(
    ctx: typer.Context,
    source: str = typer.Argument(
        '-',
        metavar='SOURCE',
        help=(
            'Data source to transform '
            '(file path, JSON string, or - for stdin).'
        ),
    ),
    from_: str | None = typer.Option(
        None,
        '--from',
        help='Override the inferred source type (file, database, api).',
    ),
    source_format: str | None = typer.Option(
        None,
        '--source-format',
        help=(
            'Input payload format when SOURCE is - or a literal payload. '
            'For files, the format is inferred from the extension.'
        ),
    ),
    operations: str = typer.Option(
        '{}',
        '--operations',
        help='Transformation operations as JSON string',
    ),
    target: str | None = typer.Option(
        None,
        '--target',
        help='Output file to save transformed data (JSON). Use - for stdout.',
    ),
    to: str | None = typer.Option(
        None,
        '--to',
        help='Override the inferred target type (file, database, api).',
    ),
    target_format: str | None = typer.Option(
        None,
        '--target-format',
        help=(
            'Output payload format '
            'when writing to stdout or non-file targets. '
            'File outputs infer format from the extension.'
        ),
    ),
) -> int:
    """
    Transform records using JSON-described operations.

    Parameters
    ----------
    ctx : typer.Context
        Typer execution context provided to the command.
    source : str
        Data source (file path or ``-`` for stdin).
    from_ : str | None
        Override the inferred source type.
    source_format : str | None
        Input payload format when not a file (or when SOURCE is -).
    operations : str
        Transformation operations as a JSON string.
    target : str | None
        Optional output path. Use ``-`` for stdout.
    to : str | None
        Override the inferred target type.
    target_format : str | None
        Output payload format when not a file target (or when OUTPUT is -).
        Accepts ``--target-format``.

    Returns
    -------
    int
        Zero on success.

    Examples
    --------
    - Transform data from a file and write to another file:
        etlplus transform --from file in.json \
        --operations '{"select": ["id", "name"]}' \
        --to file out.json
    - Transform data from stdin and write to stdout:
        cat in.json \
        | etlplus transform \
        --operations '{"filter": {"field": "age", "gt": 30}}'
    - Transform data from a file and write to stdout:
        etlplus transform --from file in.csv \
        --source-format csv \
        --operations '{"select": ["id", "email"]}'
    - Transform data from stdin and write to a file:
        cat in.json \
        | etlplus transform --operations '{"sort": ["-created_at"]}' \
        --to file out.json

    Notes
    -----
    - The ``transform`` command reads JSON from stdin when SOURCE is ``-``.
    - CSV input is unsupported for this command.
    - Convert upstream before piping into ``transform``.
    """
    state = _ensure_state(ctx)

    source_format = _optional_choice(
        source_format,
        _FORMAT_CHOICES,
        label='source_format',
    )
    target_format = _optional_choice(
        target_format,
        _FORMAT_CHOICES,
        label='format',
    )
    target_format_kwargs = _format_namespace_kwargs(
        format_value=target_format,
        default='json',
    )
    from_ = _optional_choice(from_, _SOURCE_CHOICES, label='from')
    to = _optional_choice(to, _SOURCE_CHOICES, label='to')

    resolved_source_type = from_ or _infer_resource_type_soft(source)
    resolved_source_value = source if source is not None else '-'
    resolved_target_value = target if target is not None else '-'

    if resolved_source_type is not None:
        resolved_source_type = _validate_choice(
            resolved_source_type,
            _SOURCE_CHOICES,
            label='source_type',
        )

    resolved_target_type = _resolve_resource_type(
        explicit_type=None,
        override_type=to,
        value=resolved_target_value,
        label='target_type',
    )

    _log_inferred_resource(
        state,
        role='source',
        value=resolved_source_value,
        resource_type=resolved_source_type,
    )
    _log_inferred_resource(
        state,
        role='target',
        value=resolved_target_value,
        resource_type=resolved_target_type,
    )

    ns = _stateful_namespace(
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
    return int(cmd_transform(ns))


@app.command('validate')
def validate_cmd(
    ctx: typer.Context,
    source: str = typer.Argument(
        '-',
        metavar='SOURCE',
        help=(
            'Data source to validate (file path, JSON string, or - for stdin).'
        ),
    ),
    rules: str = typer.Option(
        '{}',
        '--rules',
        help='Validation rules as JSON string',
    ),
    target: str | None = typer.Option(
        None,
        '--target',
        help='Target file to save validated data (JSON). Use - for stdout.',
    ),
    source_format: str | None = typer.Option(
        None,
        '--source-format',
        help=(
            'Input payload format when SOURCE is - (JSON or CSV). '
            'Files infer format from extensions.'
        ),
    ),
) -> int:
    """
    Validate data against JSON-described rules.

    Parameters
    ----------
    ctx : typer.Context
        Typer execution context provided to the command.
    source : str
        Data source (file path or ``-`` for stdin).
    rules : str
        Validation rules as a JSON string.
    target : str | None
        Optional output path. Use ``-`` for stdout.
    source_format : str | None
        Optional stdin format hint (JSON or CSV) when SOURCE is ``-``.

    Returns
    -------
    int
        Zero on success.
    """
    source_format = _optional_choice(
        source_format,
        _FORMAT_CHOICES,
        label='source_format',
    )
    source_format_kwargs = _format_namespace_kwargs(
        format_value=source_format,
        default='json',
    )

    state = _ensure_state(ctx)
    inferred_source_type = _infer_resource_type_soft(source)

    _log_inferred_resource(
        state,
        role='source',
        value=source,
        resource_type=inferred_source_type,
    )

    ns = _stateful_namespace(
        state,
        command='validate',
        source=source,
        source_type=inferred_source_type,
        rules=json_type(rules),  # convert CLI string to dict
        output=target,
        source_format=source_format,
        **source_format_kwargs,
    )
    return int(cmd_validate(ns))
