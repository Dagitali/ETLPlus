"""
etlplus.cli module.

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
import os
import sys
from textwrap import dedent

from . import __version__
from .config import load_pipeline_config
from .enums import FileFormat
from .extract import extract
from .file import File
from .load import load
from .run import run
from .transform import transform
from .utils import json_type
from .utils import print_json
from .validate import validate


# SECTION: FUNCTIONS ======================================================== #


# -- Command Handlers -- #


def cmd_extract(args: argparse.Namespace) -> int:
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

    Raises
    ------
    ValueError
        If strict format behavior is enabled and `--format` is provided
        for a file source.
    """
    # For file sources, infer format from extension rather than --format.
    if args.source_type == 'file':
        # If user explicitly provided --format, warn that it's ignored.
        if getattr(args, '_format_explicit', False):
            env_behavior = os.getenv(
                'ETLPLUS_EXTRACT_FORMAT_BEHAVIOR', 'warn',
            ).lower()
            behavior = 'error' if getattr(args, 'strict_format', False) \
                else env_behavior
            message = (
                '--format is ignored for file sources; inferred from '
                'filename extension.'
            )
            if behavior in {'error', 'fail', 'strict'}:
                raise ValueError(message)
            if behavior not in {'ignore', 'silent'}:
                print(f'Warning: {message}', file=sys.stderr)
        data = extract(args.source_type, args.source)
    else:
        data = extract(args.source_type, args.source, format=args.format)
    if args.output:
        File(args.output, FileFormat.JSON).write_json(data)
        print(f'Data extracted and saved to {args.output}')
    else:
        print_json(data)

    return 0


def cmd_validate(args: argparse.Namespace) -> int:
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
    # ``args.rules`` already parsed by ``_json_type`` (defaults to {}).
    result = validate(args.source, args.rules)
    print_json(result)

    return 0


def cmd_transform(args: argparse.Namespace) -> int:
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
    # ``args.operations`` already parsed by ``_json_type`` (defaults to {}).
    data = transform(args.source, args.operations)
    if args.output:
        File(args.output, FileFormat.JSON).write_json(data)
        print(f'Data transformed and saved to {args.output}')
    else:
        print_json(data)

    return 0


def cmd_load(args: argparse.Namespace) -> int:
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
    result = load(
        args.source,
        args.target_type,
        args.target,
    )
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

    # List mode
    if getattr(args, 'list', False) and not getattr(args, 'run', None):
        jobs = [j.name for j in cfg.jobs if j.name]
        print_json({'jobs': jobs})
        return 0

    # Run mode
    run_job = getattr(args, 'run', None)
    if run_job:
        result = run(job=run_job, config_path=args.config)

        print_json({'status': 'ok', 'result': result})
        return 0

    # Default: print summary.
    summary = {
        'name': cfg.name,
        'version': cfg.version,
        'sources': [getattr(s, 'name', None) for s in cfg.sources],
        'targets': [getattr(t, 'name', None) for t in cfg.targets],
        'jobs': [j.name for j in cfg.jobs if j.name],
    }
    print_json(summary)

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
    jobs = [j.name for j in cfg.jobs if j.name]
    print_json({'jobs': jobs})

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

    run_job = getattr(args, 'run', None)
    if run_job:
        result = run(job=run_job, config_path=args.config)

        print_json({'status': 'ok', 'result': result})
        return 0

    # Default: print summary.
    summary = {
        'name': cfg.name,
        'version': cfg.version,
        'sources': [getattr(s, 'name', None) for s in cfg.sources],
        'targets': [getattr(t, 'name', None) for t in cfg.targets],
        'jobs': [j.name for j in cfg.jobs if j.name],
    }
    print_json(summary)

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
        description=dedent(
            """
            ETLPlus — A Swiss Army knife for simple ETL operations.

                Provide a subcommand and options. Examples:

                etlplus extract file data.csv -o out.json
                etlplus validate data.json --rules '{"required": ['id]'}'
                etlplus transform data.json --operations '{"select": ['id]'}'
                etlplus load data.json file output.json

                # Enforce error if --format is provided for file sources
                etlplus extract file data.csv --format csv --strict-format
            """,
        ).strip(),
        epilog=dedent(
            """
            Environment:
                ETLPLUS_EXTRACT_FORMAT_BEHAVIOR controls
                    behavior when --format is provided for files.
                Values:
                    - error|fail|strict: treat as error
                    - warn (default): print a warning
                    - ignore|silent: no message

            Note:
                --strict-format overrides the environment behavior.
            """,
        ).strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        # formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        '-V', '--version',
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
        help=(
            'Extract data from sources (files, databases, REST APIs)'
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # Track if --format was explicitly provided by the user.

    class _StoreWithFlag(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, self.dest, values)
            setattr(namespace, '_format_explicit', True)

    extract_parser.add_argument(
        'source_type',
        choices=['file', 'database', 'api'],
        help='Type of source to extract from',
    )
    extract_parser.add_argument(
        'source',
        help=(
            'Source location (file path, database connection string, or '
            'API URL)'
        ),
    )
    extract_parser.add_argument(
        '-o', '--output',
        help='Output file to save extracted data (JSON format)',
    )
    extract_parser.set_defaults(_format_explicit=False)
    extract_parser.add_argument(
        '--strict-format',
        action='store_true',
        help=(
            'Treat providing --format for file sources as an error '
            '(overrides environment behavior)'
        ),
    )
    extract_parser.add_argument(
        '--format',
        choices=['json', 'csv', 'xml'],
        default='json',
        action=_StoreWithFlag,
        help=(
            'Format of the source when not a file. For file sources, '
            'this option is ignored and the format is inferred from the '
            'filename extension.'
        ),
    )
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
        '-o', '--output',
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
        choices=['file', 'database', 'api'],
        help='Type of target to load to',
    )
    load_parser.add_argument(
        'target',
        help=(
            'Target location (file path, database connection string, or '
            'API URL)'
        ),
    )
    # For file targets, format is inferred from filename extension.
    load_parser.set_defaults(func=cmd_load)

    # Define "pipeline" command (reads YAML config).
    pipe_parser = subparsers.add_parser(
        'pipeline',
        help='Inspect pipeline YAML (list jobs)',
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
        help='Run an ETL pipeline',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    run_parser.add_argument(
        '--config',
        required=True,
        help='Path to pipeline YAML configuration file',
    )
    run_parser.add_argument(
        '-j', '--job',
        help='Name of the job to run',
    )
    run_parser.add_argument(
        '-p', '--pipeline',
        help='Name of the pipeline to run',
    )
    run_parser.set_defaults(func=cmd_run)

    return parser

# -- Main -- #


def main(argv: list[str] | None = None) -> int:
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

    Notes
    -----
    This function prints results to stdout and errors to stderr.
    """
    parser = create_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    try:
        # Prefer argparse's dispatch to avoid duplicating logic.
        func = getattr(args, 'func', None)
        if callable(func):
            return int(func(args))

        # Fallback: no subcommand function bound.
        parser.print_help()
        return 0

    except KeyboardInterrupt:
        # Conventional exit code for SIGINT
        return 130

    except (OSError, TypeError, ValueError) as e:
        print(f'Error: {e}', file=sys.stderr)
        return 1
