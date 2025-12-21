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

import pytest

from etlplus.cli import create_parser

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

    @pytest.mark.parametrize(
        'cmd_args,expected_args',
        [
            (
                ['extract', 'file', '/path/to/file.json'],
                {
                    'command': 'extract',
                    'source_type': 'file',
                    'source': '/path/to/file.json',
                    'format': 'json',
                },
            ),
            (
                ['extract', 'file', '/path/to/file.csv', '--format', 'csv'],
                {
                    'command': 'extract',
                    'source_type': 'file',
                    'source': '/path/to/file.csv',
                    'format': 'csv',
                    '_format_explicit': True,
                },
            ),
            (
                ['load', '/path/to/file.json', 'file', '/path/to/output.json'],
                {
                    'command': 'load',
                    'source': '/path/to/file.json',
                    'target_type': 'file',
                    'target': '/path/to/output.json',
                },
            ),
            (
                [
                    'load',
                    '/path/to/file.json',
                    'file',
                    '/path/to/output.csv',
                    '--format',
                    'csv',
                ],
                {
                    'command': 'load',
                    'source': '/path/to/file.json',
                    'target_type': 'file',
                    'target': '/path/to/output.csv',
                    'format': 'csv',
                    '_format_explicit': True,
                },
            ),
            ([], {'command': None}),
            (
                ['transform', '/path/to/file.json'],
                {'command': 'transform', 'source': '/path/to/file.json'},
            ),
            (
                ['validate', '/path/to/file.json'],
                {'command': 'validate', 'source': '/path/to/file.json'},
            ),
        ],
    )
    def test_parser_commands(
        self,
        parse_cli: Callable[[list[str]], argparse.Namespace],
        cmd_args: list[str],
        expected_args: dict[str, object],
    ) -> None:
        """
        Test CLI command parsing and argument mapping.

        Parameters
        ----------
        cmd_args : list[str]
            CLI arguments to parse.
        expected_args : dict[str, object]
            Expected parsed argument values.
        """
        args = parse_cli(cmd_args)
        for key, val in expected_args.items():
            assert getattr(args, key, None) == val

    def test_parser_version(
        self,
        cli_parser: argparse.ArgumentParser,
    ) -> None:
        """Test that the CLI parser provides version information."""
        with pytest.raises(SystemExit) as exc_info:
            cli_parser.parse_args(['--version'])
        assert exc_info.value.code == 0
