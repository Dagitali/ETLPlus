"""
:mod:`tests.unit.cli.test_u_cli_main` module.

Unit tests for :mod:`etlplus.cli._main`.
"""

from __future__ import annotations

import importlib
from typing import Final
from unittest.mock import Mock

import click
import pytest
import typer

import etlplus.cli._commands.extract as extract_mod
from etlplus.cli import main as cli_main

from .conftest import StubCommandMain

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


PROG_NAME: Final[str] = 'etlplus'
main_mod = importlib.import_module('etlplus.cli._main')


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
        captured = stub_command_main(lambda **_kwargs: 5)

        assert cli_main(['extract']) == 5
        assert captured == {
            'args': ['extract'],
            'prog_name': PROG_NAME,
            'standalone_mode': False,
        }

    def test_emit_context_help_none_returns_false(self) -> None:
        """
        Test that the helper returns ``False`` when no context is provided.
        """
        assert main_mod._emit_context_help(None) is False

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
            main_mod,
            '_emit_root_help',
            lambda _command: root_help_calls.__setitem__('count', 1),
        )

        exit_code = cli_main(['--bad'])
        assert exit_code == 2
        assert root_help_calls['count'] == 1
        assert 'No such option' in capsys.readouterr().err

    def test_load_typer_click_exceptions_falls_back_to_click(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that Click exceptions are used when Typer's vendored module is
        unavailable.
        """
        monkeypatch.setattr(
            main_mod.importlib,
            'import_module',
            Mock(side_effect=ModuleNotFoundError),
        )

        assert main_mod._load_typer_click_exceptions() is click.exceptions

    @pytest.mark.parametrize(
        (
            'side_effect',
            'expected_code',
            'expected_raise_code',
            'expected_err',
        ),
        [
            pytest.param(
                OSError('disk full'),
                1,
                None,
                'Error: disk full',
                id='os-error',
            ),
            pytest.param(
                TypeError('bad call'),
                1,
                None,
                'Error: bad call',
                id='type-error',
            ),
            pytest.param(
                ValueError('fail'),
                1,
                None,
                'Error: fail',
                id='value-error',
            ),
            pytest.param(
                KeyboardInterrupt(),
                130,
                None,
                None,
                id='keyboard-interrupt',
            ),
            pytest.param(
                typer.Abort(),
                1,
                None,
                None,
                id='typer-abort',
            ),
            pytest.param(
                typer.Exit(17),
                17,
                None,
                None,
                id='typer-exit',
            ),
            pytest.param(
                SystemExit(5),
                None,
                5,
                None,
                id='system-exit',
            ),
        ],
    )
    def test_maps_handler_exceptions(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        side_effect: BaseException,
        expected_code: int | None,
        expected_raise_code: int | None,
        expected_err: str | None,
    ) -> None:
        """Handler exceptions should map to the documented CLI outcomes."""
        monkeypatch.setattr(
            extract_mod,
            'extract_handler',
            Mock(side_effect=side_effect),
        )

        if expected_raise_code is not None:
            with pytest.raises(SystemExit) as exc_info:
                cli_main(['extract', 'foo.csv'])
            assert exc_info.value.code == expected_raise_code
            return

        assert cli_main(['extract', 'foo.csv']) == expected_code
        stderr = capsys.readouterr().err
        if expected_err is not None:
            assert expected_err in stderr

    def test_maps_direct_typer_exit_from_command_main(
        self,
        stub_command_main: StubCommandMain,
    ) -> None:
        """
        Test that direct :class:`typer.Exit` from command.main are mapped.
        """
        stub_command_main(typer.Exit(9))
        assert cli_main(['extract']) == 9

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
        stub_command_main: StubCommandMain,
    ) -> None:
        """Test that unhandled :class:`UsageError` cases be re-raised."""
        stub_command_main(click.exceptions.UsageError('boom'))
        with pytest.raises(click.exceptions.UsageError, match='boom'):
            cli_main(['extract'])

    def test_vendored_typer_click_option_error_emits_usage(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """
        Test that Typer's vendored Click errors are handled like Click errors.
        """

        class _TyperUsageError(Exception):
            """Stub Typer-vendored Click usage error."""

            exit_code = 2
            ctx = None

        class _TyperNoSuchOption(_TyperUsageError):
            """Stub Typer-vendored Click no-such-option error."""

            def __str__(self) -> str:
                """Return a Click-like option error message."""
                return 'No such option: --bad'

        class _StubCommand:
            """Stub command that raises a Typer-vendored option error."""

            def main(self, **kwargs: object) -> object:
                """
                Simulate Python 3.14 Typer option parsing behavior.
                """
                raise _TyperNoSuchOption()

        root_help_calls = {'count': 0}
        monkeypatch.setattr(
            typer.main,
            'get_command',
            lambda _app: _StubCommand(),
        )
        monkeypatch.setattr(
            main_mod,
            '_USAGE_ERROR_TYPES',
            (_TyperUsageError,),
        )
        monkeypatch.setattr(
            main_mod,
            '_ILLEGAL_OPTION_ERROR_TYPES',
            (_TyperNoSuchOption,),
        )
        monkeypatch.setattr(
            main_mod,
            '_emit_root_help',
            lambda _command: root_help_calls.__setitem__('count', 1),
        )

        assert cli_main(['--bad']) == 2
        assert root_help_calls['count'] == 1
        assert 'No such option' in capsys.readouterr().err
