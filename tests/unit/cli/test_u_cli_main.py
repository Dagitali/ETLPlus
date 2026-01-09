"""
:mod:`tests.unit.cli.test_u_cli_main` module.

Unit tests for :mod:`etlplus.cli.main`.
"""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from typing import Final
from unittest.mock import Mock

import pytest
import typer

import etlplus.cli.commands as cli_commands_module
from etlplus.cli.main import main as cli_main

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

cli_main_module = import_module('etlplus.cli.main')
PROG_NAME: Final[str] = 'etlplus'


@pytest.fixture(name='stub_command')
def stub_command_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[Callable[..., object]], None]:
    """
    Install a Typer command stub.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Built-in pytest fixture used to alter Typer bindings.

    Returns
    -------
    Callable[[Callable[..., object]], None]
        Callable that patches :func:`typer.main.get_command` so that calls are
        delegated to ``action`` while preserving the Typer interface.
    """

    def _install(action: Callable[..., object]) -> None:
        class _StubCommand:
            def main(
                self,
                *,
                args: list[str],
                prog_name: str,
                standalone_mode: bool,
            ) -> object:
                return action(
                    args=args,
                    prog_name=prog_name,
                    standalone_mode=standalone_mode,
                )

        monkeypatch.setattr(
            typer.main,
            'get_command',
            lambda _app: _StubCommand(),
        )

    return _install


# SECTION: TESTS ============================================================ #


class TestCreateParser:
    """Unit tests for :func:`etlplus.cli.main.create_parser`."""

    def test_extract_parser_sets_handler_and_format_flag(self) -> None:
        """
        Test that the extract parser binds handlers and flag explicit formats.
        """
        # pylint: disable=protected-access

        parser = cli_main_module.create_parser()
        namespace = parser.parse_args(
            ['extract', 'file', 'data.csv', '--source-format', 'json'],
        )

        assert namespace.func is cli_main_module.extract_handler
        assert namespace.source_type == 'file'
        assert namespace.source == 'data.csv'
        assert namespace.source_format == 'json'
        assert namespace._format_explicit is True

    def test_check_parser_supports_boolean_flags(self) -> None:
        """Test that the check parser surfaces boolean flag wiring."""
        parser = cli_main_module.create_parser()
        namespace = parser.parse_args(
            [
                'check',
                '--config',
                'pipelines.yml',
                '--targets',
                '--transforms',
            ],
        )

        assert namespace.func is cli_main_module.check_handler
        assert namespace.command == 'check'
        assert namespace.config == 'pipelines.yml'
        assert namespace.targets is True
        assert namespace.transforms is True

    def test_render_parser_sets_handler(self) -> None:
        """Test that the render parser binds the render handler and options."""

        parser = cli_main_module.create_parser()
        namespace = parser.parse_args(
            [
                'render',
                '--config',
                'pipeline.yml',
                '--table',
                'Customers',
                '--template',
                'ddl',
                '-o',
                'out.sql',
            ],
        )

        assert namespace.func is cli_main_module.render_handler
        assert namespace.config == 'pipeline.yml'
        assert namespace.table == 'Customers'
        assert namespace.template == 'ddl'
        assert namespace.output == 'out.sql'


class TestMain:
    """Unit test suite for :func:`etlplus.cli.main`."""

    def test_command_return_value_is_passthrough(
        self,
        stub_command: Callable[[Callable[..., object]], None],
    ) -> None:
        """
        Test that the command return values flow through unchanged.

        Parameters
        ----------
        stub_command : Callable[[Callable[..., object]], None]
            Fixture that wires Typer's command execution to ``action``.
        """
        captured: dict[str, object] = {}

        def _action(**kwargs: object) -> object:
            captured.update(kwargs)
            return 5

        stub_command(_action)

        assert cli_main(['extract']) == 5
        assert captured['args'] == ['extract']
        assert captured['prog_name'] == PROG_NAME
        assert captured['standalone_mode'] is False

    def test_handles_keyboard_interrupt(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :class:`KeyboardInterrupt` maps to the conventional exit code
        130.
        """
        monkeypatch.setattr(
            cli_commands_module,
            'extract_handler',
            Mock(side_effect=KeyboardInterrupt),
        )
        assert cli_main(['extract', 'foo.csv']) == 130

    def test_handles_os_error(
        self,
        stub_command: Callable[[Callable[..., object]], None],
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that any :class:`OSError` surfaces to stderr and return 1."""

        def _action(**kwargs: object) -> object:  # noqa: ARG001
            raise OSError('disk full')

        stub_command(_action)

        assert cli_main(['anything']) == 1
        assert 'Error: disk full' in capsys.readouterr().err

    def test_handles_system_exit_from_command(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`main` does not swallow `SystemExit` from the
        dispatched command.
        """
        monkeypatch.setattr(
            cli_commands_module,
            'extract_handler',
            Mock(side_effect=SystemExit(5)),
        )
        with pytest.raises(SystemExit) as exc_info:
            cli_main(['extract', 'foo.csv'])
        assert exc_info.value.code == 5

    def test_handles_typer_abort(
        self,
        stub_command: Callable[[Callable[..., object]], None],
    ) -> None:
        """
        Test that ``typer.Abort`` propagates as a generic failure (exit code
        1).
        """

        def _action(**kwargs: object) -> object:  # noqa: ARG001
            raise typer.Abort()

        stub_command(_action)

        assert cli_main(['anything']) == 1

    def test_handles_typer_exit(
        self,
        stub_command: Callable[[Callable[..., object]], None],
    ) -> None:
        """Test that ``typer.Exit`` propagates its exit code."""

        def _action(**kwargs: object) -> object:  # noqa: ARG001
            raise typer.Exit(17)

        stub_command(_action)

        assert cli_main(['anything']) == 17

    def test_no_args_exits_zero(self) -> None:
        """Test that no args prints help and exits with exit code 0."""
        assert cli_main([]) == 0

    @pytest.mark.parametrize(
        ('cli_args', 'expected_message'),
        (
            (['definitely-not-real'], 'No such command'),
            (['--definitely-not-real-option'], 'No such option'),
            (['extract', '--definitely-not-real-option'], 'No such option'),
        ),
    )
    def test_unknown_arguments_emit_usage(
        self,
        cli_args: list[str],
        expected_message: str,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that unknown CLI arguments echo usage help.

        Parameters
        ----------
        cli_args : list[str]
            Command-line invocation passed to :func:`cli_main`.
        expected_message : str
            Substring expected in stderr describing the error.
        capsys : pytest.CaptureFixture[str]
            Pytest capture fixture used to inspect stderr output.
        """
        exit_code = cli_main(cli_args)
        captured = capsys.readouterr()

        assert exit_code == 2
        assert expected_message in captured.err
        assert 'Usage:' in captured.err

    def test_value_error_returns_exit_code_1(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """
        Test that :class:`ValueError` from a command maps to exit code 1.
        """
        monkeypatch.setattr(
            cli_commands_module,
            'extract_handler',
            Mock(side_effect=ValueError('fail')),
        )
        assert cli_main(['extract', 'foo.csv']) == 1
        assert 'Error:' in capsys.readouterr().err
