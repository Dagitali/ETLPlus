"""
``tests.unit.test_u_cli`` module.

Unit tests for ``etlplus.cli``.

Notes
-----
- Hermetic: no file or network I/O.
- Uses fixtures from `tests/unit/conftest.py` when needed.
"""
from __future__ import annotations

import pytest

from etlplus.cli import create_parser


# SECTION: TESTS ============================================================ #


def test_create_parser() -> None:
    """
    Test that the CLI parser is created and configured correctly.
    """
    parser = create_parser()
    assert parser is not None
    assert parser.prog == 'etlplus'


def test_parser_extract_command() -> None:
    """
    Test parsing of the 'extract' command and its arguments.
    """
    parser = create_parser()
    args = parser.parse_args(
        ['extract', 'file', '/path/to/file.json'],
    )
    assert args.command == 'extract'
    assert args.source_type == 'file'
    assert args.source == '/path/to/file.json'
    assert args.format == 'json'


def test_parser_extract_recognizes_format_option() -> None:
    """
    Test that the 'extract' command recognizes the --format option and sets the
    internal flag.
    """
    parser = create_parser()
    args = parser.parse_args(
        ['extract', 'file', '/path/to/file.csv', '--format', 'csv'],
    )
    assert args.command == 'extract'
    assert args.source_type == 'file'
    assert args.source == '/path/to/file.csv'
    assert args.format == 'csv'

    # Internal flag set when user provided --format explicitly.
    assert getattr(args, '_format_explicit', False) is True


def test_parser_load_command() -> None:
    """
    Test parsing of the 'load' command and its arguments.
    """
    parser = create_parser()
    args = parser.parse_args(
        [
            'load',
            '/path/to/file.json',
            'file',
            '/path/to/output.json',
        ],
    )
    assert args.command == 'load'
    assert args.source == '/path/to/file.json'
    assert args.target_type == 'file'
    assert args.target == '/path/to/output.json'


def test_parser_load_recognizes_format_option() -> None:
    """
    Test that the 'load' command recognizes the --format option and sets the
    internal flag.
    """
    parser = create_parser()
    args = parser.parse_args(
        [
            'load',
            '/path/to/file.json',
            'file',
            '/path/to/output.csv',
            '--format', 'csv',
        ],
    )
    assert args.command == 'load'
    assert args.source == '/path/to/file.json'
    assert args.target_type == 'file'
    assert args.target == '/path/to/output.csv'
    assert args.format == 'csv'

    # Internal flag set when user provided --format explicitly
    assert getattr(args, '_format_explicit', False) is True


def test_parser_no_command() -> None:
    """
    Test parser behavior when no command is provided.
    """
    parser = create_parser()
    args = parser.parse_args([])
    assert args.command is None


def test_parser_transform_command() -> None:
    """
    Test parsing of the 'transform' command and its arguments.
    """
    parser = create_parser()
    args = parser.parse_args(['transform', '/path/to/file.json'])
    assert args.command == 'transform'
    assert args.source == '/path/to/file.json'


def test_parser_validate_command() -> None:
    """
    Test parsing of the 'validate' command and its arguments.
    """
    parser = create_parser()
    args = parser.parse_args(['validate', '/path/to/file.json'])
    assert args.command == 'validate'
    assert args.source == '/path/to/file.json'


def test_parser_version() -> None:
    """
    Test that the CLI parser provides version information.
    """
    parser = create_parser()
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(['--version'])
    assert exc_info.value.code == 0
