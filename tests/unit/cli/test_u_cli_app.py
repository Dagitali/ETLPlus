"""
:mod:`tests.unit.test_u_cli_app` module.

Unit tests for :mod:`etlplus.cli.app`.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from pathlib import Path
from unittest.mock import Mock

import pytest
import typer
from typer.testing import CliRunner

import etlplus
import etlplus.cli.app as cli_app_module
from etlplus.cli.app import app as cli_app

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

CaptureHelper = Callable[[str], tuple[dict[str, object], Mock]]


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='capture_cmd')
def capture_cmd_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> CaptureHelper:
    """Return a helper to patch a handler and capture its namespace.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Built-in pytest fixture used to alter handler bindings.

    Returns
    -------
    CaptureHelper
        Helper that patches ``cli_app_module.<name>`` and records the namespace
        passed to the handler.
    """

    def _capture(name: str) -> tuple[dict[str, object], Mock]:
        captured: dict[str, object] = {}

        def _fake(ns: argparse.Namespace) -> int:
            captured['ns'] = ns
            return 0

        mock = Mock(side_effect=_fake)
        monkeypatch.setattr(cli_app_module, name, mock)
        return captured, mock

    return _capture


# SECTION: TESTS ============================================================ #


class TestCliAppInternalHelpers:
    """Unit tests for private helper utilities."""

    def test_infer_resource_type_variants(self, tmp_path: Path) -> None:
        """`_infer_resource_type` recognizes URLs, DBs, files, and stdin."""
        # pylint: disable=protected-access

        assert cli_app_module._infer_resource_type('-') == 'file'
        assert (
            cli_app_module._infer_resource_type(
                'https://example.com/data.json',
            )
            == 'api'
        )
        assert (
            cli_app_module._infer_resource_type('postgres://user@host/db')
            == 'database'
        )

        path = tmp_path / 'payload.csv'
        path.write_text('a,b\n1,2\n', encoding='utf-8')
        assert cli_app_module._infer_resource_type(str(path)) == 'file'

    def test_infer_resource_type_invalid_raises(self) -> None:
        """
        Unknown resources raise ``ValueError`` to surface helpful guidance.
        """
        # pylint: disable=protected-access

        with pytest.raises(ValueError):
            cli_app_module._infer_resource_type('unknown-resource')

    def test_optional_choice_passthrough_and_validation(self) -> None:
        """`_optional_choice` preserves None and validates provided values."""
        # pylint: disable=protected-access

        assert (
            cli_app_module._optional_choice(
                None,
                {'json', 'csv'},
                label='format',
            )
            is None
        )

        assert (
            cli_app_module._optional_choice(
                'json',
                {'json', 'csv'},
                label='format',
            )
            == 'json'
        )

        with pytest.raises(typer.BadParameter):
            cli_app_module._optional_choice('yaml', {'json'}, label='format')

    def test_stateful_namespace_includes_cli_flags(self) -> None:
        """State flags propagate into handler namespaces."""
        # pylint: disable=protected-access

        state = cli_app_module.CliState(pretty=False, quiet=True, verbose=True)
        ns = cli_app_module._stateful_namespace(
            state,
            command='extract',
            foo='bar',
        )
        assert ns.command == 'extract'
        assert ns.pretty is False
        assert ns.quiet is True
        assert ns.verbose is True
        assert ns.foo == 'bar'  # pylint: disable=no-member


class TestTyperCliAppWiring:
    """Unit test suite for Typer parsing + Namespace adaptation."""

    def test_extract_default_format_maps_namespace(
        self,
        runner: CliRunner,
        capture_cmd: CaptureHelper,
    ) -> None:
        """
        Test that the ``extract`` command defaults to JSON and marks the data
        format as implicit.
        """
        # pylint: disable=protected-access

        captured, cmd = capture_cmd('cmd_extract')
        result = runner.invoke(
            cli_app,
            ['extract', '/path/to/file.json'],
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
        capture_cmd: CaptureHelper,
    ) -> None:
        """
        Test that the ``extract`` command marks the data format as explicit
        when provided.
        """
        # pylint: disable=protected-access

        captured, cmd = capture_cmd('cmd_extract')
        result = runner.invoke(
            cli_app,
            ['extract', '/path/to/file.csv', '--source-format', 'csv'],
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.format == 'csv'
        assert ns._format_explicit is True

    def test_extract_from_option_sets_source_type_and_state_flags(
        self,
        runner: CliRunner,
        capture_cmd: CaptureHelper,
    ) -> None:
        """
        Test that providing the ``--from`` command-line option and root flags
        influence the handler namespace.
        """
        # pylint: disable=protected-access

        captured, cmd = capture_cmd('cmd_extract')
        result = runner.invoke(
            cli_app,
            [
                '--no-pretty',
                '--quiet',
                'extract',
                '--from',
                'api',
                'https://example.com/data.json',
            ],
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.source_type == 'api'
        assert ns.source == 'https://example.com/data.json'
        assert ns.pretty is False
        assert ns.quiet is True
        assert ns._format_explicit is False

    def test_list_maps_flags(
        self,
        runner: CliRunner,
        capture_cmd: CaptureHelper,
    ) -> None:
        """
        Test that the ``list`` command maps section flags into the expected
        namespace.
        """
        captured, cmd = capture_cmd('cmd_list')
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

    def test_load_default_format_maps_namespace(
        self,
        runner: CliRunner,
        capture_cmd: CaptureHelper,
    ) -> None:
        """
        Test that the `load` command defaults to JSON and marks the data format
        as implicit.
        """
        # pylint: disable=protected-access

        captured, cmd = capture_cmd('cmd_load')
        result = runner.invoke(
            cli_app,
            ['load', '--to', 'file', '/path/to/out.json'],
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'load'
        assert ns.source == '-'
        assert ns.target_type == 'file'
        assert ns.target == '/path/to/out.json'
        assert ns.format == 'json'
        assert ns._format_explicit is False

    def test_load_explicit_format_maps_namespace(
        self,
        runner: CliRunner,
        capture_cmd: CaptureHelper,
    ) -> None:
        """
        Test that the ``load`` command marks the data format as explicit when
        provided.
        """
        # pylint: disable=protected-access

        captured, cmd = capture_cmd('cmd_load')
        result = runner.invoke(
            cli_app,
            [
                'load',
                '--target-format',
                'csv',
                '/path/to/out.csv',
            ],
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.source == '-'
        assert ns.target_type == 'file'
        assert ns.format == 'csv'
        assert ns._format_explicit is True

    def test_load_file_to_file_is_rejected(
        self,
        runner: CliRunner,
    ) -> None:
        """
        Test that supplying SOURCE TARGET after an explicit type still errors.
        """
        result = runner.invoke(
            cli_app,
            ['load', 'file', 'in.json', 'out.json'],
        )

        assert result.exit_code != 0
        assert 'usage: etlplus load' in result.stderr.lower()

    def test_load_to_option_defaults_source_to_stdin(
        self,
        runner: CliRunner,
        capture_cmd: CaptureHelper,
    ) -> None:
        """
        Test that ``source`` defaults to '-' and ``--to`` wins when only TARGET
        is provided wins.
        """

        captured, cmd = capture_cmd('cmd_load')
        result = runner.invoke(
            cli_app,
            ['load', '--to', 'database', 'postgres://db.example.org/app'],
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.source == '-'
        assert ns.target == 'postgres://db.example.org/app'
        assert ns.target_type == 'database'

    def test_no_args_prints_help(self, runner: CliRunner) -> None:
        """Test invoking with no args prints help and exits 0."""
        result = runner.invoke(cli_app, [])
        assert result.exit_code == 0
        assert 'ETLPlus' in result.stdout

    def test_pipeline_maps_flags(
        self,
        runner: CliRunner,
        capture_cmd: CaptureHelper,
    ) -> None:
        """
        Test that the `pipeline` command maps the ``--list`` command-line
        argument and the ``--run`` command-line argument into the expected
        namespace.
        """
        captured, cmd = capture_cmd('cmd_pipeline')
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

    def test_pipeline_run_sets_run_option(
        self,
        runner: CliRunner,
        capture_cmd: CaptureHelper,
    ) -> None:
        """`pipeline --run` wires run metadata into the namespace."""

        captured, cmd = capture_cmd('cmd_pipeline')
        result = runner.invoke(
            cli_app,
            ['pipeline', '--config', 'p.yml', '--run', 'job-2'],
        )

        assert result.exit_code == 0
        cmd.assert_called_once()
        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.run == 'job-2'
        assert ns.list is False

    def test_run_maps_flags(
        self,
        runner: CliRunner,
        capture_cmd: CaptureHelper,
    ) -> None:
        """
        Test that the ``run`` command maps the ``--job``/``--pipeline``
        command-line argument into the expected namespace.
        """
        captured, cmd = capture_cmd('cmd_run')
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

    def test_transform_parses_operations_json(
        self,
        runner: CliRunner,
        capture_cmd: CaptureHelper,
    ) -> None:
        """
        Test that the ``transform`` command parses ``--operations``
        command-line argument via ``json_type``.
        """
        captured, cmd = capture_cmd('cmd_transform')
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

    def test_transform_respects_source_format(
        self,
        runner: CliRunner,
        capture_cmd: CaptureHelper,
    ) -> None:
        """
        Test that the command ``etlplus transform --source-format csv``
        propagates to the namespace.
        """
        captured, cmd = capture_cmd('cmd_transform')
        result = runner.invoke(
            cli_app,
            ['transform', '--source-format', 'csv'],
        )

        assert result.exit_code == 0
        cmd.assert_called_once()
        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.source_format == 'csv'

    def test_validate_parses_rules_json(
        self,
        runner: CliRunner,
        capture_cmd: CaptureHelper,
    ) -> None:
        """
        Test that the command ``validate`` parses the ``--rules`` command-line
        argument via ``json_type``.
        """
        captured, cmd = capture_cmd('cmd_validate')
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

    def test_validate_respects_source_format(
        self,
        runner: CliRunner,
        capture_cmd: CaptureHelper,
    ) -> None:
        """
        Test that command ``validate --source-format csv`` sanitizes into a
        handler namespace.
        """

        captured, cmd = capture_cmd('cmd_validate')
        result = runner.invoke(
            cli_app,
            ['validate', '--source-format', 'csv'],
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        ns = captured['ns']
        assert isinstance(ns, argparse.Namespace)
        assert ns.source_format == 'csv'

    def test_version_flag_exits_zero(self, runner: CliRunner) -> None:
        """Test that command option ``--version`` exits successfully."""
        result = runner.invoke(cli_app, ['--version'])
        assert result.exit_code == 0
        assert etlplus.__version__ in result.stdout
