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
from typer.testing import Result

import etlplus
import etlplus.cli.app as cli_app_module
from etlplus.cli.app import app as cli_app

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

CaptureHelper = Callable[[str], tuple[dict[str, argparse.Namespace], Mock]]
InvokeCli = Callable[..., tuple[Result, argparse.Namespace, Mock]]


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

    def _capture(name: str) -> tuple[dict[str, argparse.Namespace], Mock]:
        captured: dict[str, argparse.Namespace] = {}

        def _fake(ns: argparse.Namespace) -> int:
            captured['ns'] = ns
            return 0

        mock = Mock(side_effect=_fake)
        monkeypatch.setattr(cli_app_module, name, mock)
        return captured, mock

    return _capture


@pytest.fixture(name='invoke_cli')
def invoke_cli_fixture(
    runner: CliRunner,
    capture_cmd: CaptureHelper,
) -> InvokeCli:
    """Invoke the Typer CLI and capture the patched handler call.

    Parameters
    ----------
    runner : CliRunner
        Typer CLI runner fixture.
    capture_cmd : CaptureHelper
        Helper that patches handler bindings and records the namespace.

    Returns
    -------
    InvokeCli
        Callable that invokes the CLI, returning the ``Result``, handler
        namespace, and mock used for assertion.
    """

    def _invoke(
        handler: str,
        *args: str,
    ) -> tuple[Result, argparse.Namespace, Mock]:
        captured, cmd = capture_cmd(handler)
        result = runner.invoke(cli_app, list(args))
        return result, captured['ns'], cmd

    return _invoke


# SECTION: TESTS ============================================================ #


class TestCliAppInternalHelpers:
    """Unit tests for private helper utilities."""

    @pytest.mark.parametrize(
        ('raw', 'expected'),
        (
            ('-', 'file'),
            ('https://example.com/data.json', 'api'),
            ('postgres://user@host/db', 'database'),
        ),
    )
    def test_infer_resource_type_variants(
        self,
        raw: str,
        expected: str,
    ) -> None:
        """
        Test that :func:`_infer_resource_type` classifies common resource
        inputs.
        """
        # pylint: disable=protected-access

        assert cli_app_module._infer_resource_type(raw) == expected

    def test_infer_resource_type_file_path(self, tmp_path: Path) -> None:
        """
        Test that :func:`_infer_resource_type` detects local files via
        extension parsing.
        """
        # pylint: disable=protected-access

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

    @pytest.mark.parametrize(
        ('choice', 'expected'),
        ((None, None), ('json', 'json')),
    )
    def test_optional_choice_passthrough_and_validation(
        self,
        choice: str | None,
        expected: str | None,
    ) -> None:
        """
        Test that :func:`_optional_choice` preserves ``None`` and normalizes
        valid values.
        """
        # pylint: disable=protected-access

        assert (
            cli_app_module._optional_choice(
                choice,
                {'json', 'csv'},
                label='format',
            )
            == expected
        )

    @pytest.mark.parametrize('invalid', ('yaml', 'parquet'))
    def test_optional_choice_rejects_invalid(self, invalid: str) -> None:
        """Test that invalid choices raise :class:`typer.BadParameter`."""
        # pylint: disable=protected-access

        with pytest.raises(typer.BadParameter):
            cli_app_module._optional_choice(invalid, {'json'}, label='format')

    def test_stateful_namespace_includes_cli_flags(self) -> None:
        """Test that state flags propagate into handler namespaces."""
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
        invoke_cli: InvokeCli,
    ) -> None:
        """
        Test that ``extract`` defaults to JSON and marks the data format as
        implicit.
        """
        # pylint: disable=protected-access

        result, ns, cmd = invoke_cli(
            'cmd_extract',
            'extract',
            '/path/to/file.json',
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'extract'
        assert ns.source_type == 'file'
        assert ns.source == '/path/to/file.json'
        assert ns.format == 'json'
        assert ns._format_explicit is False

    def test_extract_explicit_format_maps_namespace(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """
        Test that ``extract`` marks the data format as explicit when provided.
        """
        # pylint: disable=protected-access

        result, ns, cmd = invoke_cli(
            'cmd_extract',
            'extract',
            '/path/to/file.csv',
            '--source-format',
            'csv',
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        assert isinstance(ns, argparse.Namespace)
        assert ns.format == 'csv'
        assert ns._format_explicit is True

    def test_extract_from_option_sets_source_type_and_state_flags(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """
        Test that root flags propagate into the handler namespace for
        ``extract``.
        """
        # pylint: disable=protected-access

        result, ns, cmd = invoke_cli(
            'cmd_extract',
            '--no-pretty',
            '--quiet',
            'extract',
            '--source-type',
            'api',
            'https://example.com/data.json',
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        assert isinstance(ns, argparse.Namespace)
        assert ns.source_type == 'api'
        assert ns.source == 'https://example.com/data.json'
        assert ns.pretty is False
        assert ns.quiet is True
        assert ns._format_explicit is False

    def test_check_maps_flags(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """
        Test that ``check`` maps section flags into the handler namespace.
        """
        result, ns, cmd = invoke_cli(
            'cmd_check',
            'check',
            '--config',
            'p.yml',
            '--pipelines',
            '--sources',
        )
        assert result.exit_code == 0
        cmd.assert_called_once()

        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'check'
        assert ns.config == 'p.yml'
        assert ns.pipelines is True
        assert ns.sources is True

    def test_load_default_format_maps_namespace(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """
        Test that ``load`` defaults to JSON and marks the data format as
        implicit.
        """
        # pylint: disable=protected-access

        result, ns, cmd = invoke_cli(
            'cmd_load',
            'load',
            '--target-type',
            'file',
            '/path/to/out.json',
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'load'
        assert ns.source == '-'
        assert ns.target_type == 'file'
        assert ns.target == '/path/to/out.json'
        assert ns.format == 'json'
        assert ns._format_explicit is False

    def test_load_explicit_format_maps_namespace(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """
        Test that ``load`` marks the target data format as explicit when
        provided.
        """
        # pylint: disable=protected-access

        result, ns, cmd = invoke_cli(
            'cmd_load',
            'load',
            '--target-format',
            'csv',
            '/path/to/out.csv',
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        assert isinstance(ns, argparse.Namespace)
        assert ns.source == '-'
        assert ns.target_type == 'file'
        assert ns.format == 'csv'
        assert ns._format_explicit is True

    def test_load_to_option_defaults_source_to_stdin(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """
        Test that ``load`` defaults to stdin when only target options are
        provided.
        """

        result, ns, cmd = invoke_cli(
            'cmd_load',
            'load',
            '--target-type',
            'database',
            'postgres://db.example.org/app',
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

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
        invoke_cli: InvokeCli,
    ) -> None:
        """
        Test that ``pipeline`` maps list flags into the handler namespace.
        """
        result, ns, cmd = invoke_cli(
            'cmd_pipeline',
            'pipeline',
            '--config',
            'p.yml',
            '--jobs',
        )
        assert result.exit_code == 0
        cmd.assert_called_once()

        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'pipeline'
        assert ns.config == 'p.yml'
        assert ns.list is True
        assert ns.run is None

    def test_pipeline_run_sets_run_option(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """
        Test that ``pipeline --job`` wires run metadata into the namespace.
        """
        result, ns, cmd = invoke_cli(
            'cmd_pipeline',
            'pipeline',
            '--config',
            'p.yml',
            '--job',
            'job-2',
        )

        assert result.exit_code == 0
        cmd.assert_called_once()
        assert isinstance(ns, argparse.Namespace)
        assert ns.run == 'job-2'
        assert ns.list is False

    def test_render_maps_namespace(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """Test that ``render`` maps options into the handler namespace."""

        result, ns, cmd = invoke_cli(
            'cmd_render',
            'render',
            '--config',
            'pipeline.yml',
            '--table',
            'Customers',
            '--template',
            'ddl',
            '--output',
            'out.sql',
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'render'
        assert ns.config == 'pipeline.yml'
        assert ns.spec is None
        assert ns.table == 'Customers'
        assert ns.template == 'ddl'
        assert ns.template_path is None
        assert ns.output == 'out.sql'

    def test_run_maps_flags(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """
        Test that ``run`` maps job flags into the handler namespace.
        """
        result, ns, cmd = invoke_cli(
            'cmd_run',
            'run',
            '--config',
            'p.yml',
            '--job',
            'j1',
        )
        assert result.exit_code == 0
        cmd.assert_called_once()

        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'run'
        assert ns.config == 'p.yml'
        assert ns.job == 'j1'

    def test_transform_parses_operations_json(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """
        Test that ``transform`` parses JSON operations passed via
        ``--operations``.
        """
        result, ns, cmd = invoke_cli(
            'cmd_transform',
            'transform',
            '/path/to/file.json',
            '--operations',
            '{"select": ["id"]}',
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'transform'
        assert ns.source == '/path/to/file.json'
        assert isinstance(ns.operations, dict)
        assert ns.operations.get('select') == ['id']

    def test_transform_respects_source_format(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """
        Test that ``transform`` propagates ``--source-format`` into the
        namespace.
        """
        result, ns, cmd = invoke_cli(
            'cmd_transform',
            'transform',
            '--source-format',
            'csv',
        )

        assert result.exit_code == 0
        cmd.assert_called_once()
        assert isinstance(ns, argparse.Namespace)
        assert ns.source_format == 'csv'

    def test_validate_parses_rules_json(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """
        Test that ``validate`` parses JSON rules passed via ``--rules``.
        """
        result, ns, cmd = invoke_cli(
            'cmd_validate',
            'validate',
            '/path/to/file.json',
            '--rules',
            '{"required": ["id"]}',
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        assert isinstance(ns, argparse.Namespace)
        assert ns.command == 'validate'
        assert ns.source == '/path/to/file.json'
        assert isinstance(ns.rules, dict)
        assert ns.rules.get('required') == ['id']

    def test_validate_respects_source_format(
        self,
        invoke_cli: InvokeCli,
    ) -> None:
        """
        Test that ``validate`` propagates ``--source-format`` into the
        namespace.
        """
        result, ns, cmd = invoke_cli(
            'cmd_validate',
            'validate',
            '--source-format',
            'csv',
        )

        assert result.exit_code == 0
        cmd.assert_called_once()

        assert isinstance(ns, argparse.Namespace)
        assert ns.source_format == 'csv'

    def test_version_flag_exits_zero(self, runner: CliRunner) -> None:
        """Test that command option ``--version`` exits successfully."""
        result = runner.invoke(cli_app, ['--version'])
        assert result.exit_code == 0
        assert etlplus.__version__ in result.stdout
