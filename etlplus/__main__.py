"""
ETLPlus Command-Line Interface
==============================

Entry point for the ``etlplus`` CLI.

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
import json
import sys
from textwrap import dedent
from typing import Any

from etlplus import __version__
from etlplus.extract import extract
from etlplus.load import load
from etlplus.transform import transform
from etlplus.validate import validate


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _json_type(option: str) -> Any:
    """
    Argparse ``type=`` hook that parses a JSON string.

    Parameters
    ----------
    option
        Raw CLI string to parse as JSON.

    Returns
    -------
    Any
        Parsed JSON value.

    Raises
    ------
    argparse.ArgumentTypeError
        If the input cannot be parsed as JSON.
    """

    try:
        return json.loads(option)
    except json.JSONDecodeError as e:  # pragma: no cover - argparse path
        raise argparse.ArgumentTypeError(
            f'invalid JSON: {e.msg} (pos {e.pos})',
        ) from e


# SECTION: FUNCTIONS ======================================================== #


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

              etlplus extract file data.csv --format csv -o out.json
              etlplus validate data.json --rules '{"required": ["id"]}'
              etlplus transform data.json --operations '{"select": ["id"]}'
              etlplus load data.json file output.json --format json
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

    # Extract command
    extract_parser = subparsers.add_parser(
        'extract',
        help=(
            'Extract data from sources (files, databases, REST APIs)'
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
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
    extract_parser.add_argument(
        '--format',
        choices=['json', 'csv', 'xml'],
        default='json',
        help='Format of the source file to extract (default: json)',
    )

    # Validate command
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
        type=_json_type,
        default={},
        help='Validation rules as JSON string',
    )

    # Transform command
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
        type=_json_type,
        default={},
        help='Transformation operations as JSON string',
    )
    transform_parser.add_argument(
        '-o', '--output',
        help='Output file to save transformed data',
    )

    # Load command
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
    load_parser.add_argument(
        '--format',
        choices=['json', 'csv'],
        default='json',
        help='Format for the target file to load (default: json)',
    )

    return parser


# -- Main -- #


def main(argv: list[str] | None = None) -> int:
    """
    Main entry point for the CLI.

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
        if args.command == 'extract':
            data = extract(
                args.source_type,
                args.source,
                format=args.format,
            )
            if args.output:
                with open(args.output, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
                print(f'Data extracted and saved to {args.output}')
            else:
                print(json.dumps(data, indent=2))

        elif args.command == 'validate':
            rules = args.rules or {}
            validate_result = validate(args.source, rules)
            print(json.dumps(validate_result, indent=2))

        elif args.command == 'transform':
            operations = args.operations or {}
            data = transform(args.source, operations)
            if args.output:
                with open(args.output, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
                print(f'Data transformed and saved to {args.output}')
            else:
                print(json.dumps(data, indent=2))

        elif args.command == 'load':
            load_result = load(
                args.source,
                args.target_type,
                args.target,
                format=args.format,
            )
            print(json.dumps(load_result, indent=2))

        return 0

    except KeyboardInterrupt:
        # Conventional exit code for SIGINT
        return 130
    except Exception as e:  # noqa: BLE001
        print(f'Error: {e}', file=sys.stderr)
        return 1


# SECTION: MAIN EXECUTION =================================================== #


if __name__ == '__main__':
    sys.exit(main())
