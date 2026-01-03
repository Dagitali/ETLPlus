"""
:mod:`tests.unit.test_u_cli_main` module.

Unit tests for :mod:`etlplus.cli.main`.
"""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import Mock

import pytest
import typer

import etlplus.cli.app as cli_app_module
from etlplus.cli.main import main as cli_main

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


def _install_stub_command(
    monkeypatch: pytest.MonkeyPatch,
    *,
    action: Callable[..., object],
) -> None:
    """Replace the Typer command with a stub that triggers ``action``."""

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


# SECTION: TESTS ============================================================ #


class TestMain:
    """Unit test suite for :func:`etlplus.cli.main`."""

    def test_command_return_value_is_passthrough(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure the command return value is normalized into an ``int``."""

        captured: dict[str, object] = {}

        def _action(**kwargs: object) -> object:
            captured.update(kwargs)
            return 5

        _install_stub_command(monkeypatch, action=_action)

        assert cli_main(['extract']) == 5
        assert captured['args'] == ['extract']
        assert captured['prog_name'] == 'etlplus'
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
            cli_app_module,
            'cmd_extract',
            Mock(side_effect=KeyboardInterrupt),
        )
        assert cli_main(['extract', 'file', 'foo']) == 130

    def test_handles_os_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Any :class:`OSError` should surface to stderr and return 1."""

        def _action(**kwargs: object) -> object:  # noqa: ARG001
            raise OSError('disk full')

        _install_stub_command(monkeypatch, action=_action)

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
            cli_app_module,
            'cmd_extract',
            Mock(side_effect=SystemExit(5)),
        )
        with pytest.raises(SystemExit) as exc_info:
            cli_main(['extract', 'file', 'foo'])
        assert exc_info.value.code == 5

    def test_handles_typer_abort(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        ``typer.Abort`` should surface as a generic failure (exit code 1).
        """

        def _action(**kwargs: object) -> object:  # noqa: ARG001
            raise typer.Abort()

        _install_stub_command(monkeypatch, action=_action)

        assert cli_main(['anything']) == 1

    def test_handles_typer_exit(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``typer.Exit`` should propagate its exit code."""

        def _action(**kwargs: object) -> object:  # noqa: ARG001
            raise typer.Exit(17)

        _install_stub_command(monkeypatch, action=_action)

        assert cli_main(['anything']) == 17

    def test_no_args_exits_zero(self) -> None:
        """Test that no args prints help and exits with exit code 0."""
        assert cli_main([]) == 0

    def test_value_error_returns_exit_code_1(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """
        Test that :class:`ValueError` from a command maps to exit code 1."""

        monkeypatch.setattr(
            cli_app_module,
            'cmd_extract',
            Mock(side_effect=ValueError('fail')),
        )
        assert cli_main(['extract', 'file', 'foo']) == 1
        assert 'Error:' in capsys.readouterr().err
