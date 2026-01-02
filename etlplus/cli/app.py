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
- Use ``-`` to read from stdin and ``--output -`` (or ``load ... file -``) to
    write to stdout.
- ``extract`` supports ``--from`` and ``load`` supports ``--to`` to override
    inferred resource types.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from typing import Literal

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
        '    etlplus extract file in.csv -o out.json',
        '    etlplus validate in.json --rules \'{"required": ["id"]}\'',
        '    etlplus transform in.json --operations \'{"select": ["id"]}\'',
        '    etlplus load in.json file out.json',
        '',
        '    Enforce error if --format is provided for files. Examples:',
        '',
        '    etlplus extract file in.csv --format csv --strict-format',
        '    etlplus load in.json file out.csv --format csv --strict-format',
    ],
)

CLI_EPILOG: Final[str] = '\n'.join(
    [
        'Environment:',
        (
            '    ETLPLUS_FORMAT_BEHAVIOR controls behavior when '
            '--format is provided for files.'
        ),
        '    Values:',
        '        - error|fail|strict: treat as error',
        '        - warn (default): print a warning',
        '        - ignore|silent: no message',
        '',
        'Note:',
        '    --strict-format overrides the environment behavior.',
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
    metavar='[SOURCE] [TARGET_TYPE] TARGET',
    help=(
        'Load SOURCE into a target. SOURCE defaults to - (stdin). You may '
        'provide legacy positional form: SOURCE TARGET_TYPE TARGET.'
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


# SECTION: TYPE ALIASES ===================================================== #


type FormatContext = Literal['source', 'target']


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
        Namespace compatible with the existing ``cmd_*`` handlers.
    """

    return argparse.Namespace(**kwargs)


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
    label: str
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


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True)
class CliState:
    """Mutable container for runtime CLI toggles."""

    pretty: bool = True
    quiet: bool = False
    verbose: bool = False


# SECTION: TYPER APP ======================================================== #


# Typer application instance (subcommands are registered below).
app = typer.Typer(
    name='etlplus',
    # help='ETLPlus - A Swiss Army knife for simple ETL operations.',
    help=CLI_DESCRIPTION,
    epilog=CLI_EPILOG,
    add_completion=True,
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

    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
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
    output: str | None = typer.Option(
        None,
        '-o',
        '--output',
        help='Output file to save extracted data (JSON). Use - for stdout.',
    ),
    strict_format: bool = typer.Option(
        False,
        '--strict-format',
        help=(
            'Treat providing --format for file sources as an error '
            '(overrides environment behavior)'
        ),
    ),
    source_format: str | None = typer.Option(
        None,
        '--format',
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
        Positional arguments: either SOURCE, or SOURCE_TYPE SOURCE.
    from_ : str | None
        Override the inferred source type.
    output : str | None
        Output file to save extracted data.
    strict_format : bool
        Whether to enforce strict format behavior.
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

    - Extract from a file (explicit):
        etlplus extract file in.csv
        etlplus extract --from file in.csv

    - Extract from an API:
        etlplus extract https://example.com/data.json
        etlplus extract --from api https://example.com/data.json

    - Extract from a database DSN:
        etlplus extract --from database postgresql://user:pass@host/db

    - Pipe into transform/load:
        etlplus extract in.csv \
        | etlplus transform --operations '{"select":["a"]}'
    """
    state = _ensure_state(ctx)

    if len(args) > 2:
        raise typer.BadParameter('Provide SOURCE, or SOURCE_TYPE SOURCE.')

    if from_ is not None:
        from_ = _validate_choice(from_, _SOURCE_CHOICES, label='from')

    if source_format is not None:
        source_format = _validate_choice(
            source_format,
            _FORMAT_CHOICES,
            label='format',
        )

    if len(args) == 2:
        if from_ is not None:
            raise typer.BadParameter(
                'Do not combine --from with an explicit SOURCE_TYPE.',
            )
        source_type = _validate_choice(
            args[0],
            _SOURCE_CHOICES,
            label='source_type',
        )
        source = args[1]
    else:
        source = args[0]
        if from_ is not None:
            source_type = from_
        else:
            try:
                source_type = _infer_resource_type(source)
            except ValueError as e:
                raise typer.BadParameter(str(e)) from e

        source_type = _validate_choice(
            source_type,
            _SOURCE_CHOICES,
            label='source_type',
        )

    if state.verbose:
        print(
            f'Inferred source_type={source_type} for source={source}',
            file=sys.stderr,
        )

    ns = _ns(
        command='extract',
        source_type=source_type,
        source=source,
        output=output,
        strict_format=strict_format,
        format=(source_format or 'json'),
        _format_explicit=(source_format is not None),
        pretty=state.pretty,
        quiet=state.quiet,
        verbose=state.verbose,
    )
    return int(cmd_extract(ns))


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
    output: str | None = typer.Option(
        None,
        '-o',
        '--output',
        help='Output file to save validated data (JSON). Use - for stdout.',
    ),
    input_format: str | None = typer.Option(
        None,
        '--input-format',
        help='Input payload format for stdin (json or csv).',
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
    output : str | None
        Optional output path. Use ``-`` for stdout.
    input_format : str | None
        Optional stdin format hint (json or csv).

    Returns
    -------
    int
        Zero on success.
    """
    if input_format is not None:
        input_format = _validate_choice(
            input_format,
            _FORMAT_CHOICES,
            label='input_format',
        )

    state = _ensure_state(ctx)

    ns = _ns(
        command='validate',
        source=source,
        rules=json_type(rules),
        output=output,
        input_format=input_format,
        pretty=state.pretty,
        quiet=state.quiet,
        verbose=state.verbose,
    )
    return int(cmd_validate(ns))


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
    operations: str = typer.Option(
        '{}',
        '--operations',
        help='Transformation operations as JSON string',
    ),
    output: str | None = typer.Option(
        None,
        '-o',
        '--output',
        help='Output file to save transformed data (JSON). Use - for stdout.',
    ),
    input_format: str | None = typer.Option(
        None,
        '--input-format',
        help='Input payload format for stdin (json or csv).',
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
    operations : str
        Transformation operations as a JSON string.
    output : str | None
        Optional output path. Use ``-`` for stdout.
    input_format : str | None
        Optional stdin format hint (json or csv).

    Returns
    -------
    int
        Zero on success.
    """
    if input_format is not None:
        input_format = _validate_choice(
            input_format,
            _FORMAT_CHOICES,
            label='input_format',
        )

    state = _ensure_state(ctx)

    ns = _ns(
        command='transform',
        source=source,
        operations=json_type(operations),
        output=output,
        input_format=input_format,
        pretty=state.pretty,
        quiet=state.quiet,
        verbose=state.verbose,
    )
    return int(cmd_transform(ns))


@app.command('load')
def load_cmd(
    ctx: typer.Context,
    args: list[str] = LOAD_ARGS,
    to: str | None = typer.Option(
        None,
        '--to',
        help='Override the inferred target type (file, database, api).',
    ),
    strict_format: bool = typer.Option(
        False,
        '--strict-format',
        help=(
            'Treat providing --format for file targets as an error '
            '(overrides environment behavior)'
        ),
    ),
    target_format: str | None = typer.Option(
        None,
        '--format',
        help=(
            'Payload format when not a file (or when TARGET is -). '
            'For normal file targets, format is inferred from extension.'
        ),
    ),
    input_format: str | None = typer.Option(
        None,
        '--input-format',
        help='Input payload format for stdin (json or csv).',
    ),
) -> int:
    """
    Load data into a file, database, or REST API.

    Parameters
    ----------
    ctx : typer.Context
        Typer execution context provided to the command.
    args : list[str]
        Positional arguments: TARGET, SOURCE TARGET, or SOURCE TARGET_TYPE
        TARGET.
    to : str | None
        Override the inferred target type.
    strict_format : bool
        Whether to enforce strict format behavior.
    target_format : str | None
        Payload format when not a file.
    input_format : str | None
        Input payload format for stdin.

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

    - Legacy form:
        etlplus load in.json file out.json

    - Write to stdout:
        etlplus load in.json file -
    """

    state = _ensure_state(ctx)

    if len(args) > 3:
        raise typer.BadParameter(
            'Provide TARGET, SOURCE TARGET, or SOURCE TARGET_TYPE TARGET.',
        )

    if to is not None:
        to = _validate_choice(to, _SOURCE_CHOICES, label='to')

    if target_format is not None:
        target_format = _validate_choice(
            target_format,
            _FORMAT_CHOICES,
            label='format',
        )

    if input_format is not None:
        input_format = _validate_choice(
            input_format,
            _FORMAT_CHOICES,
            label='input_format',
        )

    # Parse positional args.
    if to is None and len(args) == 3:
        # Legacy: SOURCE TARGET_TYPE TARGET
        source = args[0]
        target_type = _validate_choice(
            args[1],
            _SOURCE_CHOICES,
            label='target_type',
        )
        target = args[2]
    else:
        # Modern: [SOURCE] TARGET (SOURCE defaults to -)
        if len(args) == 1:
            source = '-'
            target = args[0]
        elif len(args) == 2:
            source = args[0]
            target = args[1]
        else:
            raise typer.BadParameter(
                'Missing TARGET. '
                'Provide TARGET, SOURCE TARGET, or legacy form.',
            )

        if to is not None:
            target_type = to
        else:
            try:
                target_type = _infer_resource_type(target)
            except ValueError as e:
                raise typer.BadParameter(str(e)) from e

        target_type = _validate_choice(
            target_type,
            _SOURCE_CHOICES,
            label='target_type',
        )

    if state.verbose:
        print(
            f'Inferred target_type={target_type} for target={target}',
            file=sys.stderr,
        )

    ns = _ns(
        command='load',
        source=source,
        target_type=target_type,
        target=target,
        strict_format=strict_format,
        format=(target_format or 'json'),
        _format_explicit=(target_format is not None),
        input_format=input_format,
        pretty=state.pretty,
        quiet=state.quiet,
        verbose=state.verbose,
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
    list_: bool = typer.Option(
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
    list_ : bool
        If True, list available job names and exit.
    run_job : str | None
        Name of a specific job to run.

    Returns
    -------
    int
        Zero on success.
    """
    state = _ensure_state(ctx)
    ns = _ns(
        command='pipeline',
        config=config,
        list=list_,
        run=run_job,
        pretty=state.pretty,
        quiet=state.quiet,
        verbose=state.verbose,
    )
    return int(cmd_pipeline(ns))


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
    ns = _ns(
        command='list',
        config=config,
        pipelines=pipelines,
        sources=sources,
        targets=targets,
        transforms=transforms,
        pretty=state.pretty,
        quiet=state.quiet,
        verbose=state.verbose,
    )
    return int(cmd_list(ns))


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
    ns = _ns(
        command='run',
        config=config,
        job=job,
        pipeline=pipeline,
        pretty=state.pretty,
        quiet=state.quiet,
        verbose=state.verbose,
    )
    return int(cmd_run(ns))
