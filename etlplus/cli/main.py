"""
:mod:`etlplus.cli.main` module.

Entry point for the ``etlplus`` command-line interface (CLI).

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
from collections.abc import Sequence
from typing import Any
from typing import Literal

import typer

from .. import __version__
from ..enums import DataConnectorType
from ..enums import FileFormat
from ..utils import json_type
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
    'main',
]


# SECTION: CONSTANTS ======================================================== #


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

PROJECT_URL = 'https://github.com/Dagitali/ETLPlus'

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


# SECTION: FUNCTIONS ======================================================== #


# -- Parser -- #


# TODO: Sunset this function.
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
