"""
:mod:`tests.unit.cli.test_u_cli_main` module.

Unit tests for :mod:`etlplus.cli.main`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Final
from unittest.mock import Mock

import pytest
import typer

import etlplus.cli.handlers as cli_handlers_module
from etlplus.cli.main import main as cli_main
from tests.unit.cli.conftest import StubCommand

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

PROG_NAME: Final[str] = 'etlplus'


# SECTION: TESTS ============================================================ #


class TestMain:
    """Unit test suite for :func:`etlplus.cli.main`."""

    def test_command_return_value_is_passthrough(
        self,
        stub_command: StubCommand,
    ) -> None:
        """
        Test that the command return values flow through unchanged.

        Parameters
        ----------
        stub_command : StubCommand
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

    @pytest.mark.parametrize(
        ('exception', 'expected_code', 'expected_err'),
        (
            pytest.param(
                KeyboardInterrupt,
                130,
                None,
                id='keyboard-interrupt',
            ),
            pytest.param(ValueError('fail'), 1, 'Error:', id='value-error'),
        ),
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

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        capsys : pytest.CaptureFixture[str]
            Capture fixture for STDOUT/STDERR.
        exception : BaseException | type[BaseException]
            Exception instance or type to trigger.
        expected_code : int
            Expected exit code emitted by :func:`cli_main`.
        expected_err : str | None
            Expected STDERR substring when provided.
        """
        side_effect: BaseException
        if isinstance(exception, type):
            side_effect = exception()
        else:
            side_effect = exception
        monkeypatch.setattr(
            cli_handlers_module,
            'extract_handler',
            Mock(side_effect=side_effect),
        )
        assert cli_main(['extract', 'foo.csv']) == expected_code
        stderr = capsys.readouterr().err
        if expected_err is not None:
            assert expected_err in stderr

    def test_handles_os_error(
        self,
        stub_command: StubCommand,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """
        Test that any :class:`OSError` surfaces to STDERR and return 1.

        Parameters
        ----------
        stub_command : StubCommand
            Fixture that wires Typer's command execution to ``action``.
        capsys : pytest.CaptureFixture[str]
            Capture fixture for STDOUT/STDERR.
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

    @pytest.mark.parametrize(
        ('setup', 'expected'),
        (
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
        ),
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
        """
        Test that unknown CLI arguments echo usage help.

        Parameters
        ----------
        cli_args : list[str]
            Command-line invocation passed to :func:`cli_main`.
        expected_message : str
            Substring expected in STDERR describing the error.
        capsys : pytest.CaptureFixture[str]
            Pytest capture fixture used to inspect STDERR output.
        """
        exit_code = cli_main(cli_args)
        captured = capsys.readouterr()

        assert exit_code == 2
        assert expected_message in captured.err
        assert 'Usage:' in captured.err
