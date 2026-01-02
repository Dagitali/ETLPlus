"""
:mod:`tests.unit.test_u_cli` module.

Unit tests for :mod:`etlplus.cli`.

Notes
-----
These tests are intended to be hermetic. They avoid real network I/O and keep
file I/O limited to pytest-managed temporary directories.
"""

from __future__ import annotations

import argparse
import types
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Final
from typing import cast
from unittest.mock import Mock

import pytest
from typer.testing import CliRunner

import etlplus
import etlplus.cli.app as cli_app_module
import etlplus.cli.handlers as handlers
from etlplus.cli.app import app as cli_app
from etlplus.cli.main import main as cli_main
from etlplus.config import PipelineConfig

# SECTION: HELPERS ========================================================== #

pytestmark = pytest.mark.unit

CSV_TEXT: Final[str] = 'a,b\n1,2\n3,4\n'


@dataclass(frozen=True, slots=True)
class DummyCfg:
    """Minimal stand-in pipeline config for CLI helper tests."""

    name: str = 'p1'
    version: str = 'v1'
    sources: list[object] = field(
        default_factory=lambda: [types.SimpleNamespace(name='s1')],
    )
    targets: list[object] = field(
        default_factory=lambda: [types.SimpleNamespace(name='t1')],
    )
    transforms: list[object] = field(
        default_factory=lambda: [types.SimpleNamespace(name='tr1')],
    )
    jobs: list[object] = field(
        default_factory=lambda: [types.SimpleNamespace(name='j1')],
    )


@pytest.fixture(name='runner')
def runner_fixture() -> CliRunner:
    """Provide a Typer test runner."""

    return CliRunner()


def _capture_cmd(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> tuple[dict[str, object], Mock]:
    """Patch a `cmd_*` handler and capture the received namespace."""

    captured: dict[str, object] = {}

    def _fake(ns: argparse.Namespace) -> int:
        captured['ns'] = ns
        return 0

    m = Mock(side_effect=_fake)
    monkeypatch.setattr(cli_app_module, name, m)
    return captured, m


# SECTION: TESTS ============================================================ #


class TestTyperCliWiring:
    """Tests for Typer parsing + Namespace adaptation."""

    def test_no_args_prints_help(self, runner: CliRunner) -> None:
        """Invoking with no args prints help and exits 0."""

        result = runner.invoke(cli_app, [])
        assert result.exit_code == 0
        assert 'ETLPlus' in result.stdout

    def test_version_flag_exits_zero(self, runner: CliRunner) -> None:
        """``--version`` exits successfully."""

        result = runner.invoke(cli_app, ['--version'])
        assert result.exit_code == 0
        assert etlplus.__version__ in result.stdout

    def test_extract_default_format_maps_namespace(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Extract defaults to json and marks format as implicit."""
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
        """Extract marks format as explicit when provided."""
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
        """Load defaults to json and marks format as implicit."""
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
        """Load marks format as explicit when provided."""
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
        """Validate parses `--rules` via `json_type`."""

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
        """Transform parses `--operations` via `json_type`."""

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
        """Pipeline maps `--list` and `--run` into the expected namespace."""

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
        """List maps section flags into the expected namespace."""

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
        """Run maps `--job`/`--pipeline` into the expected namespace."""

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


class TestCliInternalHelpers:
    """Unit tests for internal CLI helpers in :mod:`etlplus.cli`."""

    @pytest.mark.parametrize(
        ('behavior', 'expected_err', 'should_raise'),
        [
            pytest.param('ignore', '', False, id='ignore'),
            pytest.param('silent', '', False, id='silent'),
            pytest.param('warn', 'Warning:', False, id='warn'),
            pytest.param('error', '', True, id='error'),
        ],
    )
    def test_emit_behavioral_notice(
        self,
        behavior: str,
        expected_err: str,
        should_raise: bool,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Behavioral notice raises or emits stderr per configured behavior."""
        # pylint: disable=protected-access

        if should_raise:
            with pytest.raises(ValueError):
                handlers._emit_behavioral_notice('msg', behavior, quiet=False)
            return

        handlers._emit_behavioral_notice('msg', behavior, quiet=False)
        captured = capsys.readouterr()
        assert expected_err in captured.err

    def test_emit_behavioral_notice_quiet_suppresses(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Quiet mode suppresses warning emission."""
        # pylint: disable=protected-access

        handlers._emit_behavioral_notice('msg', 'warn', quiet=True)
        captured = capsys.readouterr()
        assert captured.err == ''

    def test_format_behavior_strict(self) -> None:
        """Strict mode maps to error behavior."""
        # pylint: disable=protected-access

        assert handlers._format_behavior(True) == 'error'

    def test_format_behavior_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Environment overrides behavior when not strict."""
        # pylint: disable=protected-access

        monkeypatch.setenv(handlers.FORMAT_ENV_KEY, 'fail')
        assert handlers._format_behavior(False) == 'fail'

        monkeypatch.delenv(handlers.FORMAT_ENV_KEY, raising=False)
        assert handlers._format_behavior(False) == 'warn'

    @pytest.mark.parametrize(
        ('resource_type', 'format_explicit', 'should_raise'),
        [
            pytest.param('file', True, True, id='file-explicit'),
            pytest.param('file', False, False, id='file-implicit'),
            pytest.param('database', True, False, id='nonfile-explicit'),
        ],
    )
    def test_handle_format_guard(
        self,
        monkeypatch: pytest.MonkeyPatch,
        resource_type: str,
        format_explicit: bool,
        should_raise: bool,
    ) -> None:
        """Guard raises only for explicit formats on file resources."""
        # pylint: disable=protected-access

        monkeypatch.setattr(
            handlers,
            '_format_behavior',
            lambda _strict: 'error',
        )

        def call() -> None:
            handlers._handle_format_guard(
                io_context='source',
                resource_type=resource_type,
                format_explicit=format_explicit,
                strict=False,
                quiet=False,
            )

        if should_raise:
            with pytest.raises(ValueError):
                call()
        else:
            call()

    def test_list_sections_all(self) -> None:
        """`_list_sections` includes all requested sections."""
        # pylint: disable=protected-access

        args = argparse.Namespace(
            pipelines=True,
            sources=True,
            targets=True,
            transforms=True,
        )
        cfg = cast(PipelineConfig, DummyCfg())
        result = handlers._list_sections(
            cfg,
            args,
        )
        assert set(result) >= {'pipelines', 'sources', 'targets', 'transforms'}

    def test_list_sections_default(self) -> None:
        """`_list_sections` defaults to jobs when no flags are set."""
        # pylint: disable=protected-access

        args = argparse.Namespace(
            pipelines=False,
            sources=False,
            targets=False,
            transforms=False,
        )
        cfg = cast(PipelineConfig, DummyCfg())
        result = handlers._list_sections(
            cfg,
            args,
        )
        assert 'jobs' in result

    def test_materialize_csv_payload_non_str(self) -> None:
        """Non-string payloads return unchanged."""
        # pylint: disable=protected-access

        payload: object = {'foo': 1}
        assert handlers._materialize_csv_payload(payload) is payload

    def test_materialize_csv_payload_non_csv(self, tmp_path: Path) -> None:
        """Non-CSV file paths are returned unchanged."""
        # pylint: disable=protected-access

        f = tmp_path / 'file.txt'
        f.write_text('abc')
        assert handlers._materialize_csv_payload(str(f)) == str(f)

    def test_materialize_csv_payload_csv(self, tmp_path: Path) -> None:
        """CSV file paths are loaded into row dictionaries."""
        # pylint: disable=protected-access

        f = tmp_path / 'file.csv'
        f.write_text(CSV_TEXT)
        rows = handlers._materialize_csv_payload(str(f))

        assert isinstance(rows, list)
        assert rows[0] == {'a': '1', 'b': '2'}

    def test_pipeline_summary(self) -> None:
        """`_pipeline_summary` returns a mapping for a pipeline config."""
        # pylint: disable=protected-access

        cfg = cast(PipelineConfig, DummyCfg())
        summary = handlers._pipeline_summary(cfg)
        result: Mapping[str, object] = summary
        assert result['name'] == 'p1'
        assert result['version'] == 'v1'
        assert set(result) >= {'sources', 'targets', 'jobs'}

    def test_read_csv_rows(self, tmp_path: Path) -> None:
        """`_read_csv_rows` reads a CSV into row dictionaries."""
        # pylint: disable=protected-access

        f = tmp_path / 'data.csv'
        f.write_text(CSV_TEXT)
        assert handlers._read_csv_rows(f) == [
            {'a': '1', 'b': '2'},
            {'a': '3', 'b': '4'},
        ]

    def test_write_json_output_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When a file path is provided, JSON is written via :class:`File`."""
        # pylint: disable=protected-access

        data = {'x': 1}

        dummy_file = Mock()
        monkeypatch.setattr(handlers, 'File', lambda _p, _f: dummy_file)

        handlers._write_json_output(data, 'out.json', success_message='msg')
        dummy_file.write_json.assert_called_once_with(data)


class TestMain:
    """Unit test suite for :func:`etlplus.cli.main`."""

    def test_no_args_exits_zero(self) -> None:
        """No args prints help and exits 0."""

        assert cli_main([]) == 0

    def test_handles_keyboard_interrupt(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """KeyboardInterrupt maps to the conventional exit code 130."""

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
        """`main` does not swallow `SystemExit` from the dispatched command."""

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
        """ValueError from a command maps to exit code 1."""

        monkeypatch.setattr(
            cli_app_module,
            'cmd_extract',
            Mock(side_effect=ValueError('fail')),
        )
        assert cli_main(['extract', 'file', 'foo']) == 1
        assert 'Error:' in capsys.readouterr().err
