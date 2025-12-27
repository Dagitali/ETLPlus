"""
:mod:`tests.unit.test_u_cli` module.

Unit tests for :mod:`etlplus.cli`.

Notes
-----
- These tests are hermetic; they perform no real file or network I/O.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

import pytest

import etlplus.cli as cli

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


type ParseCli = Callable[[Sequence[str]], argparse.Namespace]


@dataclass(frozen=True, slots=True)
class ParserCase:
    """
    Declarative CLI parser test case.

    Attributes
    ----------
    identifier : str
        Stable ID for pytest parametrization.
    args : tuple[str, ...]
        Argument vector passed to :meth:`argparse.ArgumentParser.parse_args`.
    expected : Mapping[str, object]
        Mapping of expected attribute values on the returned namespace.
    """

    identifier: str
    args: tuple[str, ...]
    expected: Mapping[str, object]


# Shared parser cases to keep parametrization DRY and self-documenting.
CLI_CASES: Final[tuple[ParserCase, ...]] = (
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


@dataclass(slots=True)
class ParserStub:
    """Minimal stand-in for :class:`argparse.ArgumentParser`.

    The production :func:`etlplus.cli.main` only needs a ``parse_args`` method
    returning a namespace.

    Attributes
    ----------
    namespace : argparse.Namespace
        Namespace returned by :meth:`parse_args`.
    """

    namespace: argparse.Namespace

    def parse_args(
        self,
        _args: Sequence[str] | None = None,
    ) -> argparse.Namespace:
        """Return the pre-configured namespace."""
        return self.namespace


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='cli_parser')
def cli_parser_fixture() -> argparse.ArgumentParser:
    """
    Provide a fresh CLI parser per test.

    Returns
    -------
    argparse.ArgumentParser
        Newly constructed parser instance.
    """
    return cli.create_parser()


@pytest.fixture(name='parse_cli')
def parse_cli_fixture(
    cli_parser: argparse.ArgumentParser,
) -> ParseCli:
    """
    Provide a callable that parses argv into a namespace.

    Parameters
    ----------
    cli_parser : argparse.ArgumentParser
        Parser instance created per test.

    Returns
    -------
    ParseCli
        Callable that parses CLI args into an :class:`argparse.Namespace`.
    """

    def _parse(args: Sequence[str]) -> argparse.Namespace:
        return cli_parser.parse_args(list(args))

    return _parse


# SECTION: TESTS ============================================================ #


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
        Test that the CLI parser is constructed and reports the CLI tool's
        expected program name.
        """
        assert isinstance(cli_parser, argparse.ArgumentParser)
        assert cli_parser.prog == 'etlplus'

    @pytest.mark.parametrize('case', CLI_CASES, ids=lambda c: c.identifier)
    def test_parser_commands(
        self,
        parse_cli: ParseCli,
        case: ParserCase,
    ) -> None:
        """
        Test CLI command parsing and argument mapping.

        Parameters
        ----------
        parse_cli : ParseCli
            Fixture that parses CLI arguments.
        case : ParserCase
            Declarative parser scenario definition.
        """
        ns = parse_cli(case.args)
        for key, expected in case.expected.items():
            assert getattr(ns, key, None) == expected

    def test_parser_version(
        self,
        cli_parser: argparse.ArgumentParser,
    ) -> None:
        """Test that the CLI parser provides version information."""
        with pytest.raises(SystemExit) as exc_info:
            cli_parser.parse_args(['--version'])
        assert exc_info.value.code == 0


class TestMain:
    """Unit test suite for :func:`etlplus.cli.main`."""

    def test_handles_keyboard_interrupt(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`main` maps keyboard interrupts to exit code 130."""

        def _cmd(*_args: object, **_kwargs: object) -> int:
            raise KeyboardInterrupt

        ns = argparse.Namespace(command='dummy', func=_cmd)
        monkeypatch.setattr(cli, 'create_parser', lambda: ParserStub(ns))

        assert cli.main([]) == 130

    def test_handles_system_exit_from_command(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`main` does not swallow :class:`SystemExit` from the
        dispatched command.
        """

        def _cmd(*_args: object, **_kwargs: object) -> int:
            raise SystemExit(5)

        ns = argparse.Namespace(command='dummy', func=_cmd)
        monkeypatch.setattr(cli, 'create_parser', lambda: ParserStub(ns))

        with pytest.raises(SystemExit) as exc_info:
            cli.main([])
        assert exc_info.value.code == 5

    def test_invokes_parser(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`main` calls :func:`create_parser` and dispatches to
        the command.
        """
        calls: dict[str, int] = {'parser': 0, 'cmd': 0}

        def _cmd(*_args: object, **_kwargs: object) -> int:
            calls['cmd'] += 1
            return 0

        ns = argparse.Namespace(command='dummy', func=_cmd)

        def _fake_create_parser() -> ParserStub:
            calls['parser'] += 1
            return ParserStub(ns)

        monkeypatch.setattr(cli, 'create_parser', _fake_create_parser)

        assert cli.main([]) == 0
        assert calls['parser'] == 1
        assert calls['cmd'] == 1
