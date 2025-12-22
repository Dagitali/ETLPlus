"""
:mod:`tests.unit.test_u_cli` module.

Unit tests for ``etlplus.cli``.

Notes
-----
- Hermetic: no file or network I/O.
- Uses fixtures from `tests/unit/conftest.py` when needed.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass

import pytest

from etlplus.cli import create_parser

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


@dataclass(frozen=True, slots=True)
class ParserCase:
    """Declarative CLI parser test case."""

    identifier: str
    args: tuple[str, ...]
    expected: dict[str, object]


# Shared parser cases to keep param definitions DRY and self-documenting.
CLI_CASES: tuple[ParserCase, ...] = (
    ParserCase(
        identifier='extract-default-format',
        args=('extract', 'file', '/path/to/file.json'),
        expected={
            'command': 'extract',
            'source_type': 'file',
            'source': '/path/to/file.json',
            'format': 'json',
        },
    ),
    ParserCase(
        identifier='extract-explicit-format',
        args=('extract', 'file', '/path/to/file.csv', '--format', 'csv'),
        expected={
            'command': 'extract',
            'source_type': 'file',
            'source': '/path/to/file.csv',
            'format': 'csv',
            '_format_explicit': True,
        },
    ),
    ParserCase(
        identifier='load-default-format',
        args=('load', '/path/to/file.json', 'file', '/path/to/output.json'),
        expected={
            'command': 'load',
            'source': '/path/to/file.json',
            'target_type': 'file',
            'target': '/path/to/output.json',
        },
    ),
    ParserCase(
        identifier='load-explicit-format',
        args=(
            'load',
            '/path/to/file.json',
            'file',
            '/path/to/output.csv',
            '--format',
            'csv',
        ),
        expected={
            'command': 'load',
            'source': '/path/to/file.json',
            'target_type': 'file',
            'target': '/path/to/output.csv',
            'format': 'csv',
            '_format_explicit': True,
        },
    ),
    ParserCase(
        identifier='no-subcommand',
        args=(),
        expected={'command': None},
    ),
    ParserCase(
        identifier='transform',
        args=('transform', '/path/to/file.json'),
        expected={'command': 'transform', 'source': '/path/to/file.json'},
    ),
    ParserCase(
        identifier='validate',
        args=('validate', '/path/to/file.json'),
        expected={'command': 'validate', 'source': '/path/to/file.json'},
    ),
)

# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='cli_parser')
def cli_parser_fixture() -> argparse.ArgumentParser:
    """
    Provide a fresh CLI parser per test case.

    Returns
    -------
    argparse.ArgumentParser
        Newly constructed parser instance.
    """

    return create_parser()


@pytest.fixture(name='parse_cli')
def parse_cli_fixture(
    cli_parser: argparse.ArgumentParser,
) -> Callable[[list[str]], argparse.Namespace]:
    """Provide a callable that parses CLI args into a namespace."""

    def _parse(args: list[str]) -> argparse.Namespace:
        return cli_parser.parse_args(args)

    return _parse


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestCreateParser:
    """
    Unit test suite for :func:`etlplus.cli.create_parser`.

    Notes
    -----
    - Tests CLI parser creation and argument parsing for all commands.
    """

    def test_create_parser(
        self,
        cli_parser: argparse.ArgumentParser,
    ) -> None:
        """
        Test that the CLI parser is created and configured correctly.
        """
        assert cli_parser is not None
        assert cli_parser.prog == 'etlplus'

    @pytest.mark.parametrize('case', CLI_CASES, ids=lambda c: c.identifier)
    def test_parser_commands(
        self,
        parse_cli: Callable[[list[str]], argparse.Namespace],
        case: ParserCase,
    ) -> None:
        """
        Test CLI command parsing and argument mapping.

        Parameters
        ----------
        parse_cli : Callable[[list[str]], argparse.Namespace]
            Fixture that parses CLI arguments.
        case : ParserCase
            Declarative parser scenario definition.
        """
        args = parse_cli(list(case.args))
        for key, val in case.expected.items():
            assert getattr(args, key, None) == val

    def test_parser_version(
        self,
        cli_parser: argparse.ArgumentParser,
    ) -> None:
        """Test that the CLI parser provides version information."""
        with pytest.raises(SystemExit) as exc_info:
            cli_parser.parse_args(['--version'])
        assert exc_info.value.code == 0
