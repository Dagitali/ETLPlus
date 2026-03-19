"""
:mod:`tests.unit.cli.test_u_cli_main` module.

Unit tests for :mod:`etlplus.cli.main`.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Final
from unittest.mock import Mock

import click
import pytest
import typer

import etlplus.cli._handlers as cli_handlers_module
from etlplus.cli.main import main as cli_main

from .conftest import StubCommand
from .conftest import StubCommandMain

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


PROG_NAME: Final[str] = 'etlplus'
main_module = importlib.import_module('etlplus.cli.main')


# SECTION: TESTS ============================================================ #


class TestMain:
    """Unit tests for :func:`etlplus.cli.main`."""

    def test_command_return_value_is_passthrough(
        self,
        stub_command_main: StubCommandMain,
    ) -> None:
        """
        Test that the command return values flow through unchanged.
        """

        def _action(**kwargs: object) -> object:
            return 5

        captured = stub_command_main(_action)

        assert cli_main(['extract']) == 5
        assert captured['args'] == ['extract']
        assert captured['prog_name'] == PROG_NAME
        assert captured['standalone_mode'] is False

    def test_emit_context_help_none_returns_false(self) -> None:
        """
        Test that the helper returns ``False`` when no context is provided.
        """
        assert main_module._emit_context_help(None) is False

    def test_handles_os_error(
        self,
        stub_command: StubCommand,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """
        Test that any :class:`OSError` surfaces to STDERR and return 1.
        """

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
            cli_handlers_module,
            'extract_handler',
            Mock(side_effect=SystemExit(5)),
        )
        with pytest.raises(SystemExit) as exc_info:
            cli_main(['extract', 'foo.csv'])
        assert exc_info.value.code == 5

    def test_illegal_option_without_context_emits_root_help(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that illegal options without context fall back to root help."""

        class _StubCommand:
            """Stub command that raises a usage error for any option."""

            def main(self, **kwargs: object) -> object:
                """
                Simulate Typer's command-level option parsing and error
                handling.
                """
                raise click.exceptions.NoSuchOption('--bad')

        root_help_calls = {'count': 0}
        monkeypatch.setattr(
            typer.main,
            'get_command',
            lambda _app: _StubCommand(),
        )
        monkeypatch.setattr(
            main_module,
            '_emit_root_help',
            lambda _command: root_help_calls.__setitem__('count', 1),
        )

        exit_code = cli_main(['--bad'])
        assert exit_code == 2
        assert root_help_calls['count'] == 1
        assert 'No such option' in capsys.readouterr().err

    @pytest.mark.parametrize(
        ('exception', 'expected_code', 'expected_err'),
        [
            pytest.param(
                KeyboardInterrupt,
                130,
                None,
                id='keyboard-interrupt',
            ),
            pytest.param(ValueError('fail'), 1, 'Error:', id='value-error'),
        ],
    )
    def test_maps_common_exceptions(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        exception: BaseException | type[BaseException],
        expected_code: int,
        expected_err: str | None,
    ) -> None:
        """
        Test that common exceptions map to expected exit codes.
        """
        side_effect: BaseException = (
            exception() if isinstance(exception, type) else exception
        )
        monkeypatch.setattr(
            cli_handlers_module,
            'extract_handler',
            Mock(side_effect=side_effect),
        )
        assert cli_main(['extract', 'foo.csv']) == expected_code
        stderr = capsys.readouterr().err
        if expected_err is not None:
            assert expected_err in stderr

    def test_maps_direct_typer_exit_from_command_main(
        self,
        stub_command: StubCommand,
    ) -> None:
        """
        Test that direct :class:`typer.Exit` from command.main are mapped.
        """

        def _action(**kwargs: object) -> object:  # noqa: ARG001
            raise typer.Exit(9)

        stub_command(_action)
        assert cli_main(['extract']) == 9

    @pytest.mark.parametrize(
        ('setup', 'expected'),
        [
            (
                lambda mp: mp.setattr(
                    cli_handlers_module,
                    'extract_handler',
                    Mock(side_effect=typer.Abort()),
                ),
                1,
            ),
            (
                lambda mp: mp.setattr(
                    cli_handlers_module,
                    'extract_handler',
                    Mock(side_effect=typer.Exit(17)),
                ),
                17,
            ),
        ],
    )
    def test_maps_typer_exits(
        self,
        setup: Callable[[pytest.MonkeyPatch], None],
        monkeypatch: pytest.MonkeyPatch,
        expected: int,
    ) -> None:
        """Test that Typer exits map to CLI return codes."""
        setup(monkeypatch)
        assert cli_main(['extract', 'foo.csv']) == expected

    def test_no_args_exits_zero(self) -> None:
        """Test that no args prints help and exits with exit code 0."""
        assert cli_main([]) == 0

    @pytest.mark.parametrize(
        ('cli_args', 'expected_message'),
        [
            (['definitely-not-real'], 'No such command'),
            (['--definitely-not-real-option'], 'No such option'),
            (['extract', '--definitely-not-real-option'], 'No such option'),
        ],
    )
    def test_unknown_arguments_emit_usage(
        self,
        cli_args: list[str],
        expected_message: str,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """
        Test that unknown CLI arguments echo usage help.
        """
        exit_code = cli_main(cli_args)
        captured = capsys.readouterr()

        assert exit_code == 2
        assert expected_message in captured.err
        assert 'Usage:' in captured.err

    def test_usage_error_non_option_is_reraised(
        self,
        stub_command: StubCommand,
    ) -> None:
        """Test that unhandled :class:`UsageError` cases be re-raised."""

        def _action(**kwargs: object) -> object:  # noqa: ARG001
            raise click.exceptions.UsageError('boom')

        stub_command(_action)
        with pytest.raises(click.exceptions.UsageError, match='boom'):
            cli_main(['extract'])
