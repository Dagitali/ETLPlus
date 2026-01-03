"""
:mod:`tests.unit.test_u_cli_app` module.

Unit tests for :mod:`etlplus.cli.app`.
"""

from __future__ import annotations

import argparse
from unittest.mock import Mock

import pytest
from typer.testing import CliRunner

import etlplus
import etlplus.cli.app as cli_app_module
from etlplus.cli.app import app as cli_app

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


def _capture_cmd(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> tuple[dict[str, object], Mock]:
    """Patch a `cmd_*` handler and capture the received namespace."""
    captured: dict[str, object] = {}

    def _fake(ns: argparse.Namespace) -> int:
        captured['ns'] = ns
        return 0

    mock = Mock(side_effect=_fake)
    monkeypatch.setattr(cli_app_module, name, mock)
    return captured, mock


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='runner')
def runner_fixture() -> CliRunner:
    """Provide a Typer test runner."""
    return CliRunner()


# SECTION: TESTS ============================================================ #


class TestTyperCliAppWiring:
    """Unit test suite for Typer parsing + Namespace adaptation."""

    def test_no_args_prints_help(self, runner: CliRunner) -> None:
        """Test invoking with no args prints help and exits 0."""
        result = runner.invoke(cli_app, [])
        assert result.exit_code == 0
        assert 'ETLPlus' in result.stdout

    def test_version_flag_exits_zero(self, runner: CliRunner) -> None:
        """Test that command option ``--version`` exits successfully."""
        result = runner.invoke(cli_app, ['--version'])
        assert result.exit_code == 0
        assert etlplus.__version__ in result.stdout

    def test_extract_default_format_maps_namespace(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that the ``extract`` command defaults to JSON and marks the data
        format as implicit.
        """
        # pylint: disable=protected-access

        captured, cmd = _capture_cmd(monkeypatch, 'cmd_extract')
        result = runner.invoke(
            cli_app,
            ['extract', 'file', '/path/to/file.json'],
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'extract'
        assert ns.source_type == 'file'
        assert ns.source == '/path/to/file.json'
        assert ns.format == 'json'
        assert ns._format_explicit is False

    def test_extract_explicit_format_maps_namespace(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that the ``extract`` command marks the data format as explicit
        when provided.
        """
        # pylint: disable=protected-access

        captured, cmd = _capture_cmd(monkeypatch, 'cmd_extract')
        result = runner.invoke(
            cli_app,
            ['extract', 'file', '/path/to/file.csv', '--format', 'csv'],
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.format == 'csv'
        assert ns._format_explicit is True

    def test_load_default_format_maps_namespace(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that the `load` command defaults to JSON and marks the data format
        as implicit.
        """
        # pylint: disable=protected-access

        captured, cmd = _capture_cmd(monkeypatch, 'cmd_load')
        result = runner.invoke(
            cli_app,
            ['load', '/path/to/file.json', 'file', '/path/to/out.json'],
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'load'
        assert ns.source == '/path/to/file.json'
        assert ns.target_type == 'file'
        assert ns.target == '/path/to/out.json'
        assert ns.format == 'json'
        assert ns._format_explicit is False

    def test_load_explicit_format_maps_namespace(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that the ``load`` command marks the data format as explicit when
        provided.
        """
        # pylint: disable=protected-access

        captured, cmd = _capture_cmd(monkeypatch, 'cmd_load')
        result = runner.invoke(
            cli_app,
            [
                'load',
                '/path/to/file.json',
                'file',
                '/path/to/out.csv',
                '--format',
                'csv',
            ],
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.format == 'csv'
        assert ns._format_explicit is True

    def test_validate_parses_rules_json(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that the command ``validate`` parses the ``--rules`` command-line
        argument via ``json_type``.
        """
        captured, cmd = _capture_cmd(monkeypatch, 'cmd_validate')
        result = runner.invoke(
            cli_app,
            [
                'validate',
                '/path/to/file.json',
                '--rules',
                '{"required": ["id"]}',
            ],
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'validate'
        assert ns.source == '/path/to/file.json'
        assert isinstance(ns.rules, dict)
        assert ns.rules.get('required') == ['id']

    def test_transform_parses_operations_json(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that the ``transform`` command parses ``--operations``
        command-line argument via ``json_type``.
        """
        captured, cmd = _capture_cmd(monkeypatch, 'cmd_transform')
        result = runner.invoke(
            cli_app,
            [
                'transform',
                '/path/to/file.json',
                '--operations',
                '{"select": ["id"]}',
            ],
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'transform'
        assert ns.source == '/path/to/file.json'
        assert isinstance(ns.operations, dict)
        assert ns.operations.get('select') == ['id']

    def test_pipeline_maps_flags(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that the `pipeline` command maps the ``--list`` command-line
        argument and the ``--run`` command-line argument into the expected
        namespace.
        """
        captured, cmd = _capture_cmd(monkeypatch, 'cmd_pipeline')
        result = runner.invoke(
            cli_app,
            ['pipeline', '--config', 'p.yml', '--list'],
        )
        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'pipeline'
        assert ns.config == 'p.yml'
        assert ns.list is True
        assert ns.run is None

    def test_list_maps_flags(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that the ``list`` command maps section flags into the expected
        namespace.
        """
        captured, cmd = _capture_cmd(monkeypatch, 'cmd_list')
        result = runner.invoke(
            cli_app,
            ['list', '--config', 'p.yml', '--pipelines', '--sources'],
        )
        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'list'
        assert ns.config == 'p.yml'
        assert ns.pipelines is True
        assert ns.sources is True

    def test_run_maps_flags(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that the ``run`` command maps the ``--job``/``--pipeline``
        command-line argument into the expected namespace.
        """
        captured, cmd = _capture_cmd(monkeypatch, 'cmd_run')
        result = runner.invoke(
            cli_app,
            ['run', '--config', 'p.yml', '--job', 'j1'],
        )
        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'run'
        assert ns.config == 'p.yml'
        assert ns.job == 'j1'
