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

import etlplus.cli as cli

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

type ParseCli = Callable[[list[str]], argparse.Namespace]


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
    return cli.create_parser()


@pytest.fixture(name='parse_cli')
def parse_cli_fixture(
    cli_parser: argparse.ArgumentParser,
) -> ParseCli:
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


@pytest.mark.unit
class TestMain:
    """Unit test suite for :func:`etlplus.cli.main`."""

    def test_handles_keyboard_interrupt(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`main` returns 130 for :class:`KeyboardInterrupt`.
        """
        # pylint: disable=unused-argument

        def fake_parser():
            class Dummy:
                """
                Dummy parser that raises :class:`KeyboardInterrupt` from cmd.
                """

                def parse_args(self, args=None):  # noqa: ANN001
                    """
                    Parse args method that raises :class:`KeyboardInterrupt`.
                    """

                    class NS:
                        """
                        Dummy namespace that raises :class:`KeyboardInterrupt`.
                        """

                        command = 'dummy'

                        @staticmethod
                        def func(*_args: object) -> None:
                            """Function that simulates a keyboard interrupt."""
                            raise KeyboardInterrupt

                    return NS()

            return Dummy()

        monkeypatch.setattr(cli, 'create_parser', fake_parser)
        assert cli.main([]) == 130

    def test_handles_system_exit(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`main` does not swallow :class:`SystemExit`."""
        # pylint: disable=unused-argument

        def fake_parser():
            class Dummy:
                """
                Dummy parser that raises :class:`SystemExit` from command.
                """

                def parse_args(self, args=None):  # noqa: ANN001
                    """
                    Parse args method that raises :class:`SystemExit`.
                    """

                    class NS:
                        """
                        Dummy namespace that raises :class:`SystemExit`.
                        """

                        command = 'dummy'

                        @staticmethod
                        def func(*_args: object) -> None:
                            """Function that simulates a system exit."""
                            raise SystemExit(5)

                    return NS()

            return Dummy()

        monkeypatch.setattr(cli, 'create_parser', fake_parser)
        with pytest.raises(SystemExit) as exc:
            cli.main([])
        assert exc.value.code == 5

    def test_invokes_parser(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`main` invokes :func:`create_parser` and dispatches.
        """
        # pylint: disable=unused-argument

        called: dict[str, bool] = {}

        def fake_parser():
            """Fake parser that records invocation."""
            called['parser'] = True

            class Dummy:
                """
                Dummy parser that returns a namespace with a no-op command.
                """

                def parse_args(self, args=None):  # noqa: ANN001
                    """
                    Parse args method that returns a dummy namespace.
                    """

                    class NS:
                        """
                        Dummy namespace for no-op command.
                        """

                        command = 'dummy'

                        @staticmethod
                        def func(*_args: object) -> int:
                            """No-op function that returns 0."""
                            return 0

                    return NS()

            return Dummy()

        monkeypatch.setattr(cli, 'create_parser', fake_parser)
        assert cli.main([]) == 0
        assert called['parser'] is True
