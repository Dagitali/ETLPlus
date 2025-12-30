"""
:mod:`etlplus.cli` module.

Entry point for the ``etlplus`` command-line Interface (CLI).

This module wires subcommands via ``argparse`` using
``set_defaults(func=...)`` so dispatch is clean and extensible.

Subcommands
-----------
- ``extract``: extract data from files, databases, or REST APIs
- ``validate``: validate data against rules
- ``transform``: transform records
- ``load``: load data to files, databases, or REST APIs
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from typing import Literal
from typing import cast

import typer

from . import __version__
from .config import PipelineConfig
from .config import load_pipeline_config
from .enums import DataConnectorType
from .enums import FileFormat
from .extract import extract
from .file import File
from .load import load
from .run import run
from .transform import transform
from .types import JSONData
from .utils import json_type
from .utils import print_json
from .validate import validate

# SECTION: CONSTANTS ======================================================= #


CLI_DESCRIPTION = '\n'.join(
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

CLI_EPILOG = '\n'.join(
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

FORMAT_ENV_KEY = 'ETLPLUS_FORMAT_BEHAVIOR'

PROJECT_URL = 'https://github.com/Dagitali/ETLPlus'


# SECTION: TYPER APP ======================================================= #


app = typer.Typer(
    name='etlplus',
    help='ETLPlus - A Swiss Army knife for simple ETL operations.',
    add_completion=True,
)


_SOURCE_CHOICES = set(DataConnectorType.choices())
_FORMAT_CHOICES = set(FileFormat.choices())


def _ns(**kwargs: object) -> argparse.Namespace:
    """Create an :class:`argparse.Namespace` for legacy command handlers."""

    return argparse.Namespace(**kwargs)


def _validate_choice(value: str, choices: set[str], *, label: str) -> str:
    """Validate a string against allowed choices for nice CLI errors."""

    v = (value or '').strip()
    if v in choices:
        return v
    allowed = ', '.join(sorted(choices))
    raise typer.BadParameter(
        f"Invalid {label} '{value}'. Choose from: {allowed}",
    )


@app.callback(invoke_without_command=True)
def _root(
    ctx: typer.Context,
    version: bool = typer.Option(
        False,
        '-V',
        '--version',
        is_eager=True,
        help='Show the version and exit.',
    ),
) -> None:
    """Root command callback to show help or version."""

    if version:
        typer.echo(f'etlplus {__version__}')
        raise typer.Exit(0)

    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit(0)


@app.command('extract')
def extract_cmd(
    source_type: str = typer.Argument(
        ..., help='Type of source to extract from',
    ),
    source: str = typer.Argument(
        ...,
        help=(
            'Source location '
            '(file path, database connection string, or API URL)'
        ),
    ),
    output: str | None = typer.Option(
        None,
        '-o',
        '--output',
        help='Output file to save extracted data (JSON format)',
    ),
    strict_format: bool = typer.Option(
        False,
        '--strict-format',
        help=(
            'Treat providing --format for file sources as an error '
            '(overrides environment behavior)'
        ),
    ),
    format: str | None = typer.Option(
        None,
        '--format',
        help=(
            'Format of the source when not a file. For file sources this '
            'option is ignored and the format is inferred from the filename '
            'extension.'
        ),
    ),
) -> int:
    """Typer front-end for :func:`cmd_extract`."""

    source_type = _validate_choice(
        source_type, _SOURCE_CHOICES, label='source_type',
    )
    if format is not None:
        format = _validate_choice(format, _FORMAT_CHOICES, label='format')

    ns = _ns(
        command='extract',
        source_type=source_type,
        source=source,
        output=output,
        strict_format=strict_format,
        format=(format or 'json'),
        _format_explicit=(format is not None),
    )
    return int(cmd_extract(ns))


@app.command('validate')
def validate_cmd(
    source: str = typer.Argument(
        ..., help='Data source to validate (file path or JSON string)',
    ),
    rules: str = typer.Option(
        '{}',
        '--rules',
        help='Validation rules as JSON string',
    ),
) -> int:
    """Typer front-end for :func:`cmd_validate`."""

    ns = _ns(command='validate', source=source, rules=json_type(rules))
    return int(cmd_validate(ns))


@app.command('transform')
def transform_cmd(
    source: str = typer.Argument(
        ..., help='Data source to transform (file path or JSON string)',
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
        help='Output file to save transformed data',
    ),
) -> int:
    """Typer front-end for :func:`cmd_transform`."""

    ns = _ns(
        command='transform',
        source=source,
        operations=json_type(operations),
        output=output,
    )
    return int(cmd_transform(ns))


@app.command('load')
def load_cmd(
    source: str = typer.Argument(
        ..., help='Data source to load (file path or JSON string)',
    ),
    target_type: str = typer.Argument(..., help='Type of target to load to'),
    target: str = typer.Argument(
        ...,
        help=(
            'Target location '
            '(file path, database connection string, or API URL)'
        ),
    ),
    strict_format: bool = typer.Option(
        False,
        '--strict-format',
        help=(
            'Treat providing --format for file targets as an error '
            '(overrides environment behavior)'
        ),
    ),
    format: str | None = typer.Option(
        None,
        '--format',
        help=(
            'Format of the target when not a file. For file targets this '
            'option is ignored and the format is inferred from the filename '
            'extension.'
        ),
    ),
) -> int:
    """Typer front-end for :func:`cmd_load`."""

    target_type = _validate_choice(
        target_type, _SOURCE_CHOICES, label='target_type',
    )
    if format is not None:
        format = _validate_choice(format, _FORMAT_CHOICES, label='format')

    ns = _ns(
        command='load',
        source=source,
        target_type=target_type,
        target=target,
        strict_format=strict_format,
        format=(format or 'json'),
        _format_explicit=(format is not None),
    )
    return int(cmd_load(ns))


@app.command('pipeline')
def pipeline_cmd(
    config: str = typer.Option(
        ..., '--config', help='Path to pipeline YAML configuration file',
    ),
    list_: bool = typer.Option(
        False, '--list', help='List available job names and exit',
    ),
    run: str | None = typer.Option(
        None, '--run', metavar='JOB', help='Run a specific job by name',
    ),
) -> int:
    """Typer front-end for :func:`cmd_pipeline`."""

    ns = _ns(command='pipeline', config=config, list=list_, run=run)
    return int(cmd_pipeline(ns))


@app.command('list')
def list_cmd(
    config: str = typer.Option(
        ..., '--config', help='Path to pipeline YAML configuration file',
    ),
    pipelines: bool = typer.Option(
        False, '--pipelines', help='List ETL pipelines',
    ),
    sources: bool = typer.Option(False, '--sources', help='List data sources'),
    targets: bool = typer.Option(False, '--targets', help='List data targets'),
    transforms: bool = typer.Option(
        False, '--transforms', help='List data transforms',
    ),
) -> int:
    """Typer front-end for :func:`cmd_list`."""

    ns = _ns(
        command='list',
        config=config,
        pipelines=pipelines,
        sources=sources,
        targets=targets,
        transforms=transforms,
    )
    return int(cmd_list(ns))


@app.command('run')
def run_cmd(
    config: str = typer.Option(
        ..., '--config', help='Path to pipeline YAML configuration file',
    ),
    job: str | None = typer.Option(
        None, '-j', '--job', help='Name of the job to run',
    ),
    pipeline: str | None = typer.Option(
        None, '-p', '--pipeline', help='Name of the pipeline to run',
    ),
) -> int:
    """Typer front-end for :func:`cmd_run`."""

    ns = _ns(command='run', config=config, job=job, pipeline=pipeline)
    return int(cmd_run(ns))


# SECTION: INTERNAL CONSTANTS =============================================== #


_FORMAT_ERROR_STATES = {'error', 'fail', 'strict'}
_FORMAT_SILENT_STATES = {'ignore', 'silent'}


# SECTION: TYPE ALIASES ===================================================== #


type FormatContext = Literal['source', 'target']


# SECTION: INTERNAL CLASSES ================================================= #


class _FormatAction(argparse.Action):
    """Argparse action that records when ``--format`` is provided."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | Sequence[Any] | None,
        option_string: str | None = None,
    ) -> None:  # pragma: no cover - argparse wiring
        setattr(namespace, self.dest, values)
        namespace._format_explicit = True


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _add_format_options(
    parser: argparse.ArgumentParser,
    *,
    context: FormatContext,
) -> None:
    """
    Attach shared ``--format`` options to extract/load parsers.

    Parameters
    ----------
    parser : argparse.ArgumentParser
        Parser to add options to.
    context : FormatContext
        Whether this is a source or target resource.
    """
    parser.set_defaults(_format_explicit=False)
    parser.add_argument(
        '--strict-format',
        action='store_true',
        help=(
            'Treat providing --format for file '
            f'{context}s as an error (overrides environment behavior)'
        ),
    )
    parser.add_argument(
        '--format',
        choices=list(FileFormat.choices()),
        default='json',
        action=_FormatAction,
        help=(
            f'Format of the {context} when not a file. For file {context}s '
            'this option is ignored and the format is inferred from the '
            'filename extension.'
        ),
    )


def _emit_behavioral_notice(
    message: str,
    behavior: str,
) -> None:
    """
    Print or raise based on the configured behavior.

    Parameters
    ----------
    message : str
        The message to emit.
    behavior : str
        The effective format-behavior mode.

    Raises
    ------
    ValueError
        If the behavior is in the error states.
    """
    if behavior in _FORMAT_ERROR_STATES:
        raise ValueError(message)
    if behavior in _FORMAT_SILENT_STATES:
        return
    print(f'Warning: {message}', file=sys.stderr)


def _format_behavior(
    strict: bool,
) -> str:
    """
    Return the effective format-behavior mode.

    Parameters
    ----------
    strict : bool
        Whether to enforce strict format behavior.

    Returns
    -------
    str
        The effective format-behavior mode.
    """
    if strict:
        return 'error'
    env_value = os.getenv(FORMAT_ENV_KEY, 'warn')
    return (env_value or 'warn').strip().lower()


def _handle_format_guard(
    *,
    io_context: Literal['source', 'target'],
    resource_type: str,
    format_explicit: bool,
    strict: bool,
) -> None:
    """
    Warn or raise when --format is used alongside file resources.

    Parameters
    ----------
    io_context : Literal['source', 'target']
        Whether this is a source or target resource.
    resource_type : str
        The type of resource being processed.
    format_explicit : bool
        Whether the --format option was explicitly provided.
    strict : bool
        Whether to enforce strict format behavior.
    """
    if resource_type != 'file' or not format_explicit:
        return
    message = (
        f'--format is ignored for file {io_context}s; '
        'inferred from filename extension.'
    )
    behavior = _format_behavior(strict)
    _emit_behavioral_notice(message, behavior)


def _list_sections(
    cfg: PipelineConfig,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """
    Build sectioned metadata output for the list command.

    Parameters
    ----------
    cfg : PipelineConfig
        The loaded pipeline configuration.
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    dict[str, Any]
        Metadata output for the list command.
    """
    sections: dict[str, Any] = {}
    if getattr(args, 'pipelines', False):
        sections['pipelines'] = [cfg.name]
    if getattr(args, 'sources', False):
        sections['sources'] = [src.name for src in cfg.sources]
    if getattr(args, 'targets', False):
        sections['targets'] = [tgt.name for tgt in cfg.targets]
    if getattr(args, 'transforms', False):
        sections['transforms'] = [
            getattr(trf, 'name', None) for trf in cfg.transforms
        ]
    if not sections:
        sections['jobs'] = _pipeline_summary(cfg)['jobs']
    return sections


def _materialize_csv_payload(
    source: object,
) -> JSONData | str:
    """
    Return parsed CSV rows when ``source`` points at a CSV file.

    Parameters
    ----------
    source : object
        The source of data.

    Returns
    -------
    JSONData | str
        Parsed CSV rows or the original source if not a CSV file.
    """
    if not isinstance(source, str):
        return cast(JSONData, source)
    path = Path(source)
    if path.suffix.lower() != '.csv' or not path.is_file():
        return source
    return _read_csv_rows(path)


def _pipeline_summary(
    cfg: PipelineConfig,
) -> dict[str, Any]:
    """
    Return a human-friendly snapshot of a pipeline config.

    Parameters
    ----------
    cfg : PipelineConfig
        The loaded pipeline configuration.

    Returns
    -------
    dict[str, Any]
        A human-friendly snapshot of a pipeline config.
    """
    sources = [src.name for src in cfg.sources]
    targets = [tgt.name for tgt in cfg.targets]
    jobs = [job.name for job in cfg.jobs]
    return {
        'name': cfg.name,
        'version': cfg.version,
        'sources': sources,
        'targets': targets,
        'jobs': jobs,
    }


def _read_csv_rows(
    path: Path,
) -> list[dict[str, str]]:
    """
    Read CSV rows into dictionaries.

    Parameters
    ----------
    path : Path
        Path to a CSV file.

    Returns
    -------
    list[dict[str, str]]
        List of dictionaries, each representing a row in the CSV file.
    """
    with path.open(newline='', encoding='utf-8') as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _write_json_output(
    data: Any,
    output_path: str | None,
    *,
    success_message: str,
) -> bool:
    """
    Optionally persist JSON data to disk.

    Parameters
    ----------
    data : Any
        Data to write.
    output_path : str | None
        Path to write the output to. None to print to stdout.
    success_message : str
        Message to print upon successful write.

    Returns
    -------
    bool
        True if output was written to a file, False if printed to stdout.
    """
    if not output_path:
        return False
    File(Path(output_path), FileFormat.JSON).write_json(data)
    print(f'{success_message} {output_path}')
    return True


# SECTION: FUNCTIONS ======================================================== #


# -- Command Handlers -- #


def cmd_extract(
    args: argparse.Namespace,
) -> int:
    """
    Extract data from a source.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Zero on success.
    """
    _handle_format_guard(
        io_context='source',
        resource_type=args.source_type,
        format_explicit=getattr(args, '_format_explicit', False),
        strict=getattr(args, 'strict_format', False),
    )

    if args.source_type == 'file':
        result = extract(args.source_type, args.source)
    else:
        result = extract(
            args.source_type,
            args.source,
            file_format=getattr(args, 'format', None),
        )

    if not _write_json_output(
        result,
        getattr(args, 'output', None),
        success_message='Data extracted and saved to',
    ):
        print_json(result)

    return 0


def cmd_validate(
    args: argparse.Namespace,
) -> int:
    """
    Validate data from a source.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Zero on success.
    """
    payload = _materialize_csv_payload(args.source)
    result = validate(payload, args.rules)

    output_path = getattr(args, 'output', None)
    if output_path:
        validated_data = result.get('data')
        if validated_data is not None:
            _write_json_output(
                validated_data,
                output_path,
                success_message='Validation result saved to',
            )
        else:
            print(
                f'Validation failed, no data to save for {output_path}',
                file=sys.stderr,
            )
    else:
        print_json(result)

    return 0


def cmd_transform(
    args: argparse.Namespace,
) -> int:
    """
    Transform data from a source.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Zero on success.
    """
    payload = _materialize_csv_payload(args.source)
    data = transform(payload, args.operations)

    if not _write_json_output(
        data,
        getattr(args, 'output', None),
        success_message='Data transformed and saved to',
    ):
        print_json(data)

    return 0


def cmd_load(
    args: argparse.Namespace,
) -> int:
    """
    Load data into a target.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Zero on success.
    """
    _handle_format_guard(
        io_context='target',
        resource_type=args.target_type,
        format_explicit=getattr(args, '_format_explicit', False),
        strict=getattr(args, 'strict_format', False),
    )

    if args.target_type == 'file':
        result = load(args.source, args.target_type, args.target)
    else:
        result = load(
            args.source,
            args.target_type,
            args.target,
            file_format=getattr(args, 'format', None),
        )

    if not _write_json_output(
        result,
        getattr(args, 'output', None),
        success_message='Data loaded and saved to',
    ):
        print_json(result)

    return 0


def cmd_pipeline(args: argparse.Namespace) -> int:
    """
    Inspect or run a pipeline YAML configuration.

    --list prints job names; --run JOB executes a job end-to-end.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Zero on success.
    """
    cfg = load_pipeline_config(args.config, substitute=True)

    if getattr(args, 'list', False) and not getattr(args, 'run', None):
        print_json({'jobs': _pipeline_summary(cfg)['jobs']})
        return 0

    run_job = getattr(args, 'run', None)
    if run_job:
        result = run(job=run_job, config_path=args.config)
        print_json({'status': 'ok', 'result': result})
        return 0

    print_json(_pipeline_summary(cfg))
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    """
    Print ETL job names from a pipeline YAML configuration.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Zero on success.
    """
    cfg = load_pipeline_config(args.config, substitute=True)
    print_json(_list_sections(cfg, args))
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """
    Execute an ETL job end-to-end from a pipeline YAML configuration.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Zero on success.
    """
    cfg = load_pipeline_config(args.config, substitute=True)

    job_name = getattr(args, 'job', None) or getattr(args, 'pipeline', None)
    if job_name:
        result = run(job=job_name, config_path=args.config)
        print_json({'status': 'ok', 'result': result})
        return 0

    print_json(_pipeline_summary(cfg))
    return 0


# -- Parser -- #


def create_parser() -> argparse.ArgumentParser:
    """
    Create the argument parser for the CLI.

    Returns
    -------
    argparse.ArgumentParser
        Configured parser with subcommands for the CLI.
    """
    parser = argparse.ArgumentParser(
        prog='etlplus',
        description=CLI_DESCRIPTION,
        epilog=CLI_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        # formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        '-V',
        '--version',
        action='version',
        version=f'%(prog)s {__version__}',
    )

    subparsers = parser.add_subparsers(
        dest='command',
        help='Available commands',
    )

    # Define "extract" command.
    extract_parser = subparsers.add_parser(
        'extract',
        help=('Extract data from sources (files, databases, REST APIs)'),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    extract_parser.add_argument(
        'source_type',
        choices=list(DataConnectorType.choices()),
        help='Type of source to extract from',
    )
    extract_parser.add_argument(
        'source',
        help=(
            'Source location '
            '(file path, database connection string, or API URL)'
        ),
    )
    extract_parser.add_argument(
        '-o',
        '--output',
        help='Output file to save extracted data (JSON format)',
    )
    _add_format_options(extract_parser, context='source')
    extract_parser.set_defaults(func=cmd_extract)

    # Define "validate" command.
    validate_parser = subparsers.add_parser(
        'validate',
        help='Validate data from sources',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    validate_parser.add_argument(
        'source',
        help='Data source to validate (file path or JSON string)',
    )
    validate_parser.add_argument(
        '--rules',
        type=json_type,
        default={},
        help='Validation rules as JSON string',
    )
    validate_parser.set_defaults(func=cmd_validate)

    # Define "transform" command.
    transform_parser = subparsers.add_parser(
        'transform',
        help='Transform data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    transform_parser.add_argument(
        'source',
        help='Data source to transform (file path or JSON string)',
    )
    transform_parser.add_argument(
        '--operations',
        type=json_type,
        default={},
        help='Transformation operations as JSON string',
    )
    transform_parser.add_argument(
        '-o',
        '--output',
        help='Output file to save transformed data',
    )
    transform_parser.set_defaults(func=cmd_transform)

    # Define "load" command.
    load_parser = subparsers.add_parser(
        'load',
        help='Load data to targets (files, databases, REST APIs)',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    load_parser.add_argument(
        'source',
        help='Data source to load (file path or JSON string)',
    )
    load_parser.add_argument(
        'target_type',
        choices=list(DataConnectorType.choices()),
        help='Type of target to load to',
    )
    load_parser.add_argument(
        'target',
        help=(
            'Target location '
            '(file path, database connection string, or API URL)'
        ),
    )
    _add_format_options(load_parser, context='target')
    load_parser.set_defaults(func=cmd_load)

    # Define "pipeline" command (reads YAML config).
    pipe_parser = subparsers.add_parser(
        'pipeline',
        help=(
            'Inspect or run pipeline YAML (see '
            f'{PROJECT_URL}/blob/main/docs/pipeline-guide.md)'
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    pipe_parser.add_argument(
        '--config',
        required=True,
        help='Path to pipeline YAML configuration file',
    )
    pipe_parser.add_argument(
        '--list',
        action='store_true',
        help='List available job names and exit',
    )
    pipe_parser.add_argument(
        '--run',
        metavar='JOB',
        help='Run a specific job by name',
    )
    pipe_parser.set_defaults(func=cmd_pipeline)

    # Define "list" command.
    list_parser = subparsers.add_parser(
        'list',
        help='List ETL pipeline metadata',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    list_parser.add_argument(
        '--config',
        required=True,
        help='Path to pipeline YAML configuration file',
    )
    list_parser.add_argument(
        '--pipelines',
        action='store_true',
        help='List ETL pipelines',
    )
    list_parser.add_argument(
        '--sources',
        action='store_true',
        help='List data sources',
    )
    list_parser.add_argument(
        '--targets',
        action='store_true',
        help='List data targets',
    )
    list_parser.add_argument(
        '--transforms',
        action='store_true',
        help='List data transforms',
    )
    list_parser.set_defaults(func=cmd_list)

    # Define "run" command.
    run_parser = subparsers.add_parser(
        'run',
        help=(
            'Run an ETL pipeline '
            f'(see {PROJECT_URL}/blob/main/docs/run-module.md)'
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    run_parser.add_argument(
        '--config',
        required=True,
        help='Path to pipeline YAML configuration file',
    )
    run_parser.add_argument(
        '-j',
        '--job',
        help='Name of the job to run',
    )
    run_parser.add_argument(
        '-p',
        '--pipeline',
        help='Name of the pipeline to run',
    )
    run_parser.set_defaults(func=cmd_run)

    return parser


# -- Main -- #


def main(
    argv: list[str] | None = None,
) -> int:
    """
    Handle CLI's main entry point.

    Parameters
    ----------
    argv : list[str] | None, optional
        List of command-line arguments. If ``None``, uses ``sys.argv``.

    Returns
    -------
    int
        Zero on success, non-zero on error.

    Raises
    ------
    SystemExit
        Re-raises SystemExit exceptions to preserve exit codes.

    Notes
    -----
    This function uses Typer (Click) for parsing/dispatch, but preserves the
    existing `cmd_*` handlers by adapting parsed arguments into an
    :class:`argparse.Namespace`.
    """
    argv = sys.argv[1:] if argv is None else argv
    command = typer.main.get_command(app)

    try:
        result = command.main(
            args=list(argv),
            prog_name='etlplus',
            standalone_mode=False,
        )
        return int(result or 0)

    except typer.Exit as exc:
        return int(exc.exit_code)

    except typer.Abort:
        return 1

    except KeyboardInterrupt:
        # Conventional exit code for SIGINT
        return 130

    except SystemExit as e:
        print(f'Error: {e}', file=sys.stderr)
        raise e

    except (OSError, TypeError, ValueError) as e:
        print(f'Error: {e}', file=sys.stderr)
        return 1
