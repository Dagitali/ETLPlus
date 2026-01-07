"""
:mod:`etlplus.cli.main` module.

Entry point helpers for the Typer-powered ``etlplus`` CLI.

This module exposes :func:`main` for the console script as well as
:func:`create_parser` for callers that still need an ``argparse`` parser.
"""

from __future__ import annotations

import argparse
import contextlib
import sys
from collections.abc import Sequence
from typing import Literal

import click
import typer

from .. import __version__
from ..enums import DataConnectorType
from ..enums import FileFormat
from ..utils import json_type
from .app import PROJECT_URL
from .app import app
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
    """
    Argparse action that records when ``--source-format`` or
    ``--target-format`` is provided."""

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


def _add_boolean_flag(
    parser: argparse.ArgumentParser,
    *,
    name: str,
    help_text: str,
) -> None:
    """Add a toggle that also supports the ``--no-`` prefix via 3.13.

    Parameters
    ----------
    parser : argparse.ArgumentParser
        Parser receiving the flag.
    name : str
        Primary flag name without leading dashes.
    help_text : str
        Help text rendered in ``--help`` output.
    """

    parser.add_argument(
        f'--{name}',
        action=argparse.BooleanOptionalAction,
        default=False,
        help=help_text,
    )


def _add_config_option(
    parser: argparse.ArgumentParser,
    *,
    required: bool = True,
) -> None:
    """Attach the shared ``--config`` option used by legacy commands.

    Parameters
    ----------
    parser : argparse.ArgumentParser
        Parser receiving the option.
    required : bool, optional
        Whether the flag must be provided. Defaults to ``True``.
    """

    parser.add_argument(
        '--config',
        required=required,
        help='Path to pipeline YAML configuration file',
    )


def _add_format_options(
    parser: argparse.ArgumentParser,
    *,
    context: FormatContext,
) -> None:
    """
    Attach shared ``--source-format`` or ``--target-format`` options to
    extract/load parsers.

    Parameters
    ----------
    parser : argparse.ArgumentParser
        Parser to augment.
    context : FormatContext
        Context for the format option: either ``'source'`` or ``'target'``
    """
    parser.set_defaults(_format_explicit=False)
    parser.add_argument(
        '--source-format',
        choices=list(FileFormat.choices()),
        default='json',
        action=_FormatAction,
        help=(
            f'Format of the {context}. Overrides filename-based inference '
            'when provided.'
        ),
    )
    parser.add_argument(
        '--target-format',
        choices=list(FileFormat.choices()),
        default='json',
        action=_FormatAction,
        help=(
            f'Format of the {context}. Overrides filename-based inference '
            'when provided.'
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
                '    etlplus transform --from file in.csv --operations '
                '\'{"select": ["id"]}\' --to file -o out.json'
            ),
            '    etlplus extract in.csv | etlplus load --to file out.json',
            '',
            '    Override format inference when extensions are misleading:',
            '',
            '    etlplus extract data.txt --source-format csv',
            '    etlplus load payload.bin --target-format json',
        ],
    )


def _cli_epilog() -> str:
    return '\n'.join(
        [
            'Tip:',
            '    --source-format and --target-format override format '
            'inference based on filename extensions when needed.',
        ],
    )


def _emit_root_help(
    command: click.Command,
) -> None:
    """
    Print the root ``etlplus`` help text to stderr.

    Parameters
    ----------
    command : click.Command
        The root Typer/Click command.
    """

    ctx = command.make_context('etlplus', [], resilient_parsing=True)
    try:
        with contextlib.redirect_stdout(sys.stderr):
            ctx.get_help()
    finally:
        ctx.close()


def _is_unknown_command_error(
    exc: click.exceptions.UsageError,
) -> bool:
    """Return ``True`` when a :class:`UsageError` indicates bad subcommand."""

    message = getattr(exc, 'message', None) or str(exc)
    return message.startswith('No such command ')


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
        epilog=_cli_epilog(),
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
    subparsers.required = True

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
        '--from',
        dest='from_',
        choices=list(DataConnectorType.choices()),
        help='Override the inferred source type (file, database, api).',
    )
    transform_parser.add_argument(
        '--to',
        dest='to',
        choices=list(DataConnectorType.choices()),
        help='Override the inferred target type (file, database, api).',
    )
    transform_parser.add_argument(
        '--source-format',
        choices=list(FileFormat.choices()),
        dest='source_format',
        help=(
            'Input payload format when SOURCE is - or a literal payload. '
            'File sources infer format from the extension.'
        ),
    )
    transform_parser.add_argument(
        '--target-format',
        dest='target_format',
        choices=list(FileFormat.choices()),
        help=(
            'Output payload format '
            'when writing to stdout or non-file targets. '
            'File targets infer format from the extension.'
        ),
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
    _add_config_option(pipe_parser)
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
    _add_config_option(list_parser)
    _add_boolean_flag(
        list_parser,
        name='pipelines',
        help_text='List ETL pipelines',
    )
    _add_boolean_flag(
        list_parser,
        name='sources',
        help_text='List data sources',
    )
    _add_boolean_flag(
        list_parser,
        name='targets',
        help_text='List data targets',
    )
    _add_boolean_flag(
        list_parser,
        name='transforms',
        help_text='List data transforms',
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
    _add_config_option(run_parser)
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
    click.exceptions.UsageError
        Re-raises Typer/Click usage errors after printing help for unknown
        commands.
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

    except click.exceptions.UsageError as exc:
        if _is_unknown_command_error(exc):
            typer.echo(f'Error: {exc}', err=True)
            _emit_root_help(command)
            return int(getattr(exc, 'exit_code', 2))
        raise

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
