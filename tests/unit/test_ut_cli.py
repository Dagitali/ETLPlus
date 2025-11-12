"""
``tests.unit.test_ut_cli`` module.

CLI parser unit tests.

Parser construction and argument parsing only (no end-to-end main()).
"""
from __future__ import annotations

import pytest

from etlplus.cli import create_parser


# SECTION: TESTS ============================================================ #


def test_create_parser() -> None:
    parser = create_parser()
    assert parser is not None
    assert parser.prog == 'etlplus'


def test_parser_extract_command() -> None:
    parser = create_parser()
    args = parser.parse_args(
        ['extract', 'file', '/path/to/file.json'],
    )
    assert args.command == 'extract'
    assert args.source_type == 'file'
    assert args.source == '/path/to/file.json'
    assert args.format == 'json'


def test_parser_load_command() -> None:
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


def test_parser_no_command() -> None:
    parser = create_parser()
    args = parser.parse_args([])
    assert args.command is None


def test_parser_transform_command() -> None:
    parser = create_parser()
    args = parser.parse_args(['transform', '/path/to/file.json'])
    assert args.command == 'transform'
    assert args.source == '/path/to/file.json'


def test_parser_validate_command() -> None:
    parser = create_parser()
    args = parser.parse_args(['validate', '/path/to/file.json'])
    assert args.command == 'validate'
    assert args.source == '/path/to/file.json'


def test_parser_version(capsys: pytest.CaptureFixture[str]) -> None:
    parser = create_parser()
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(['--version'])
    assert exc_info.value.code == 0
