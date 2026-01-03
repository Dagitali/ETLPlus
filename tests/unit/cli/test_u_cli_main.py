"""
:mod:`tests.unit.test_u_cli_main` module.

Unit tests for :mod:`etlplus.cli.main`.
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest

import etlplus.cli.app as cli_app_module
from etlplus.cli.main import main as cli_main

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


class TestMain:
    """Unit test suite for :func:`etlplus.cli.main`."""

    def test_no_args_exits_zero(self) -> None:
        """Test that no args prints help and exits with exit code 0."""
        assert cli_main([]) == 0

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
