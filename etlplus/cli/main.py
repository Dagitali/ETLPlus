"""
:mod:`etlplus.cli.main` module.

Entry point helpers for the Typer-powered ``etlplus`` CLI.

This module exposes :func:`main` for the console script as well as
:func:`create_parser` for callers that still need an ``argparse`` parser.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import Literal

import typer

from .. import __version__
from ..enums import DataConnectorType
from ..enums import FileFormat
from ..utils import json_type
from .app import PROJECT_URL
from .app import app
from .handlers import FORMAT_ENV_KEY
from .handlers import cmd_extract
from .handlers import cmd_list
from .handlers import cmd_load
from .handlers import cmd_pipeline
from .handlers import cmd_run
from .handlers import cmd_transform
from .handlers import cmd_validate

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'create_parser',
    'main',
]


# SECTION: TYPE ALIASES ===================================================== #


type FormatContext = Literal['source', 'target']


# SECTION: INTERNAL CLASSES ================================================= #


class _FormatAction(argparse.Action):
    """Argparse action that records when ``--format`` is provided."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | Sequence[object] | None,
        option_string: str | None = None,
    ) -> None:  # pragma: no cover
        setattr(namespace, self.dest, values)
        namespace._format_explicit = True


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _add_format_options(
    parser: argparse.ArgumentParser,
    *,
    context: FormatContext,
) -> None:
    """Attach shared ``--format`` options to extract/load parsers."""

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


def _cli_description() -> str:
    return '\n'.join(
        [
            'ETLPlus - A Swiss Army knife for simple ETL operations.',
            '',
            '    Provide a subcommand and options. Examples:',
            '',
            '    etlplus extract file in.csv > out.json',
            '    etlplus validate in.json --rules \'{"required": ["id"]}\'',
            (
                '    etlplus transform in.json --operations '
                '\'{"select": ["id"]}\''
            ),
            '    etlplus extract in.csv | etlplus load --to file out.json',
            '',
            '    Enforce error if --format is provided for files. Examples:',
            '',
            '    etlplus extract file in.csv --format csv --strict-format',
            (
                '    etlplus load --to file out.csv --format csv '
                '--strict-format < data.json'
            ),
        ],
    )


def _cli_epilog(format_env_key: str) -> str:
    return '\n'.join(
        [
            'Environment:',
            (
                f'    {format_env_key} controls behavior when '
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


# SECTION: FUNCTIONS ======================================================== #


def create_parser() -> argparse.ArgumentParser:
    """
    Return the legacy :mod:`argparse` parser wired to current handlers.

    Returns
    -------
    argparse.ArgumentParser
        Parser compatible with historical ``etlplus`` entry points.
    """

    parser = argparse.ArgumentParser(
        prog='etlplus',
        description=_cli_description(),
        epilog=_cli_epilog(FORMAT_ENV_KEY),
        formatter_class=argparse.RawDescriptionHelpFormatter,
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

    extract_parser = subparsers.add_parser(
        'extract',
        help='Extract data from sources (files, databases, REST APIs)',
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
            'Source location (file path, database connection string, '
            'or API URL)'
        ),
    )
    extract_parser.add_argument(
        '-o',
        '--output',
        help='Output file to save extracted data (JSON format)',
    )
    _add_format_options(extract_parser, context='source')
    extract_parser.set_defaults(func=cmd_extract)

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
            'Target location (file path, database connection string, '
            'or API URL)'
        ),
    )
    _add_format_options(load_parser, context='target')
    load_parser.set_defaults(func=cmd_load)

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


def main(
    argv: list[str] | None = None,
) -> int:
    """
    Run the Typer-powered CLI and normalize exit codes.

    Parameters
    ----------
    argv : list[str] | None, optional
        Sequence of command-line arguments excluding the program name. When
        ``None``, defaults to ``sys.argv[1:]``.

    Returns
    -------
    int
        A conventional POSIX exit code: zero on success, non-zero on error.

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
    resolved_argv = sys.argv[1:] if argv is None else list(argv)
    command = typer.main.get_command(app)

    try:
        result = command.main(
            args=resolved_argv,
            prog_name='etlplus',
            standalone_mode=False,
        )
        return int(result or 0)

    except typer.Exit as exc:
        return int(exc.exit_code)

    except typer.Abort:
        return 1

    except KeyboardInterrupt:  # pragma: no cover - interactive path
        # Conventional exit code for SIGINT
        return 130

    except SystemExit as e:
        print(f'Error: {e}', file=sys.stderr)
        raise e

    except (OSError, TypeError, ValueError) as e:
        print(f'Error: {e}', file=sys.stderr)
        return 1
