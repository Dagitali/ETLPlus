"""
:mod:`tests.unit.test_u_cli_handlers` module.

Unit tests for :mod:`etlplus.cli.handlers`.
"""

from __future__ import annotations

import argparse
import io
import types
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Final
from typing import cast
from unittest.mock import Mock

import pytest

import etlplus.cli.handlers as handlers
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


# SECTION: TESTS ============================================================ #


class TestCliHandlersInternalHelpers:
    """Unit tests for internal CLI helpers in :mod:`etlplus.cli.handlers`."""

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
        """
        Test that behavioral notice raises or emits stderr per configured
        behavior.
        """
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
        """Test that quiet mode suppresses warning emission."""
        # pylint: disable=protected-access

        handlers._emit_behavioral_notice('msg', 'warn', quiet=True)
        captured = capsys.readouterr()
        assert captured.err == ''

    def test_emit_json_compact_prints_minified(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that compact mode writes JSON to stdout."""
        # pylint: disable=protected-access

        handlers._emit_json({'b': 2, 'a': 1}, pretty=False)
        captured = capsys.readouterr()
        assert captured.out.strip() == '{"b":2,"a":1}'

    def test_emit_json_pretty_uses_print_json(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that pretty-printing delegates to :func:`print_json`."""
        # pylint: disable=protected-access

        called_with: list[object] = []
        monkeypatch.setattr(handlers, 'print_json', called_with.append)

        payload = {'a': 1}
        handlers._emit_json(payload, pretty=True)
        assert called_with == [payload]

    def test_format_behavior_strict(self) -> None:
        """Test that strict mode maps to error behavior."""
        # pylint: disable=protected-access

        assert handlers._format_behavior(True) == 'error'

    def test_format_behavior_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that the environment overrides behavior when not strict."""
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
        """
        Test that the guard raises only for explicit formats on file resources.
        """
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

    def test_handle_format_guard_warns_when_non_quiet(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Warn when explicit format targets a file resource in warn mode."""
        # pylint: disable=protected-access

        monkeypatch.setattr(
            handlers,
            '_format_behavior',
            lambda _strict: 'warn',
        )

        handlers._handle_format_guard(
            io_context='target',
            resource_type='file',
            format_explicit=True,
            strict=False,
            quiet=False,
        )
        captured = capsys.readouterr()
        assert 'Warning:' in captured.err

    def test_list_sections_all(self) -> None:
        """
        Test that :func:`_list_sections` includes all requested sections."""
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

    def test_materialize_csv_payload_non_csv(self, tmp_path: Path) -> None:
        """Test that non-CSV file paths are returned unchanged."""
        # pylint: disable=protected-access

        file_path = tmp_path / 'file.txt'
        file_path.write_text('abc')
        assert handlers._materialize_csv_payload(str(file_path)) == str(
            file_path,
        )

    def test_materialize_csv_payload_non_str(self) -> None:
        """Test that non-string payloads return unchanged."""
        # pylint: disable=protected-access

        payload: object = {'foo': 1}
        assert handlers._materialize_csv_payload(payload) is payload

    def test_materialize_csv_payload_csv(self, tmp_path: Path) -> None:
        """Test that CSV file paths are loaded into row dictionaries."""
        # pylint: disable=protected-access

        file_path = tmp_path / 'file.csv'
        file_path.write_text(CSV_TEXT)
        rows = handlers._materialize_csv_payload(str(file_path))

        assert isinstance(rows, list)
        assert rows[0] == {'a': '1', 'b': '2'}

    def test_pipeline_summary(self) -> None:
        """
        Test that :func:`_pipeline_summary` returns a mapping for a pipeline
        config.
        """
        # pylint: disable=protected-access

        cfg = cast(PipelineConfig, DummyCfg())
        summary = handlers._pipeline_summary(cfg)
        result: Mapping[str, object] = summary
        assert result['name'] == 'p1'
        assert result['version'] == 'v1'
        assert set(result) >= {'sources', 'targets', 'jobs'}

    def test_read_csv_rows(self, tmp_path: Path) -> None:
        """
        Test that :func:`_read_csv_rows` reads a CSV into row dictionaries.
        """
        # pylint: disable=protected-access

        file_path = tmp_path / 'data.csv'
        file_path.write_text(CSV_TEXT)
        assert handlers._read_csv_rows(file_path) == [
            {'a': '1', 'b': '2'},
            {'a': '3', 'b': '4'},
        ]

    def test_write_json_output_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that, when a file path is provided, JSON is written via
        :class:`File`.
        """
        # pylint: disable=protected-access

        data = {'x': 1}

        dummy_file = Mock()
        monkeypatch.setattr(handlers, 'File', lambda _p, _f: dummy_file)

        handlers._write_json_output(data, 'out.json', success_message='msg')
        dummy_file.write_json.assert_called_once_with(data)

    def test_write_json_output_stdout_flag(self) -> None:
        """
        Test that returning False signals stdout emission when no output path.
        """
        # pylint: disable=protected-access

        assert (
            handlers._write_json_output(
                {'x': 1},
                None,
                success_message='msg',
            )
            is False
        )

    def test_infer_payload_format(self) -> None:
        """Test inferring JSON vs CSV using the first significant byte."""
        # pylint: disable=protected-access

        assert handlers._infer_payload_format(' {"a":1}') == 'json'
        assert handlers._infer_payload_format('  col1,col2') == 'csv'

    def test_parse_text_payload_json(self) -> None:
        """Test that parsing JSON text yields structured data."""
        # pylint: disable=protected-access

        result = handlers._parse_text_payload('{"a": 1}', fmt=None)
        assert result == {'a': 1}

    def test_parse_text_payload_csv(self) -> None:
        """Test that parsing CSV text returns row dictionaries."""
        # pylint: disable=protected-access

        result = handlers._parse_text_payload('a,b\n1,2\n', fmt='csv')
        assert result == [{'a': '1', 'b': '2'}]

    def test_parse_text_payload_passthrough(self) -> None:
        """Test that unknown formats return the original text."""
        # pylint: disable=protected-access

        text = 'payload'
        assert handlers._parse_text_payload(text, fmt='yaml') == text

    def test_presentation_flags_defaults(self) -> None:
        """
        Test that missing attributes fall back to pretty output and non-quiet.
        """
        # pylint: disable=protected-access

        pretty, quiet = handlers._presentation_flags(argparse.Namespace())
        assert (pretty, quiet) == (True, False)

    def test_presentation_flags_custom(self) -> None:
        """Test that explicit pretty/quiet flags are respected."""
        # pylint: disable=protected-access

        args = argparse.Namespace(pretty=False, quiet=True)
        assert handlers._presentation_flags(args) == (False, True)

    def test_read_stdin_text(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that reading stdin returns the buffered stream contents."""
        # pylint: disable=protected-access

        buffer = io.StringIO('stream-data')
        monkeypatch.setattr(
            handlers,
            'sys',
            types.SimpleNamespace(stdin=buffer),
        )
        assert handlers._read_stdin_text() == 'stream-data'


class TestCliHandlersCommands:
    """Unit tests that exercise the public CLI handler functions."""

    def test_cmd_extract_reads_stdin_and_emits_json(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that stdin source bypasses extract and emits parsed data."""
        args = argparse.Namespace(
            source='-',
            source_type='api',
            format=None,
            output=None,
            _format_explicit=False,
            strict_format=False,
            pretty=False,
            quiet=False,
        )
        monkeypatch.setattr(handlers, '_handle_format_guard', lambda **_: None)
        monkeypatch.setattr(handlers, '_read_stdin_text', lambda: 'raw-text')
        monkeypatch.setattr(
            handlers,
            '_parse_text_payload',
            lambda text, fmt: {'payload': text, 'fmt': fmt},
        )

        def fail_extract(*_args: object, **_kwargs: object) -> None:
            raise AssertionError('extract should not be called')

        monkeypatch.setattr(handlers, 'extract', fail_extract)
        monkeypatch.setattr(
            handlers,
            '_write_json_output',
            lambda *_a, **_k: False,
        )

        emitted: list[tuple[object, bool]] = []
        monkeypatch.setattr(
            handlers,
            '_emit_json',
            lambda data, pretty: emitted.append((data, pretty)),
        )

        assert handlers.cmd_extract(args) == 0
        assert emitted == [({'payload': 'raw-text', 'fmt': None}, False)]

    def test_cmd_extract_calls_extract_for_non_file_sources(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that non-stdin sources invoke extract and emit results."""
        args = argparse.Namespace(
            source='endpoint',
            source_type='api',
            format='json',
            output=None,
            _format_explicit=True,
            strict_format=False,
            pretty=True,
            quiet=False,
        )
        monkeypatch.setattr(handlers, '_handle_format_guard', lambda **_: None)
        monkeypatch.setattr(
            handlers,
            '_write_json_output',
            lambda *_a, **_k: False,
        )

        observed: dict[str, object] = {}

        def fake_extract(
            source_type: str,
            source: str,
            *,
            file_format: str | None,
        ) -> dict[str, object]:
            observed['params'] = (source_type, source, file_format)
            return {'status': 'ok'}

        monkeypatch.setattr(handlers, 'extract', fake_extract)
        emitted: list[tuple[object, bool]] = []
        monkeypatch.setattr(
            handlers,
            '_emit_json',
            lambda data, pretty: emitted.append((data, pretty)),
        )

        assert handlers.cmd_extract(args) == 0
        assert observed['params'] == ('api', 'endpoint', 'json')
        assert emitted == [({'status': 'ok'}, True)]

    def test_cmd_list_prints_sections(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that the ``pipeline --list`` command prints job listings."""
        cfg = cast(PipelineConfig, DummyCfg())
        monkeypatch.setattr(
            handlers,
            'load_pipeline_config',
            lambda path, substitute: cfg,
        )
        monkeypatch.setattr(
            handlers,
            '_list_sections',
            lambda _cfg, args: {'targets': ['t1']},
        )
        observed: list[object] = []
        monkeypatch.setattr(handlers, 'print_json', observed.append)

        args = argparse.Namespace(config='cfg.yml')
        assert handlers.cmd_list(args) == 0
        assert observed == [{'targets': ['t1']}]

    def test_cmd_load_file_target_streams_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that file target streams payload."""
        args = argparse.Namespace(
            source='data.csv',
            source_type='file',
            target_type='file',
            target='-',
            source_format=None,
            format=None,
            output=None,
            _format_explicit=False,
            strict_format=False,
            pretty=True,
            quiet=False,
            emitted=None,
        )
        monkeypatch.setattr(handlers, '_handle_format_guard', lambda **_: None)
        monkeypatch.setattr(
            handlers,
            '_materialize_csv_payload',
            lambda src: ['rows', src],
        )
        monkeypatch.setattr(
            handlers,
            '_emit_json',
            lambda data, pretty: setattr(args, 'emitted', (data, pretty)),
        )

        def fail_load(*_args: object, **_kwargs: object) -> None:
            raise AssertionError('load should not be called for stdout path')

        monkeypatch.setattr(handlers, 'load', fail_load)

        assert handlers.cmd_load(args) == 0
        assert args.emitted == (['rows', 'data.csv'], True)

    def test_cmd_load_non_file_target_uses_load_with_format(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that non-file targets invoke load with the correct format."""
        args = argparse.Namespace(
            source='-',
            source_type='api',
            target_type='database',
            target='dest',
            source_format='json',
            format='csv',
            output=None,
            _format_explicit=True,
            strict_format=False,
            pretty=False,
            quiet=False,
        )
        monkeypatch.setattr(handlers, '_handle_format_guard', lambda **_: None)
        monkeypatch.setattr(handlers, '_read_stdin_text', lambda: 'stream')
        monkeypatch.setattr(
            handlers,
            '_parse_text_payload',
            lambda text, fmt: {'parsed': text, 'fmt': fmt},
        )
        monkeypatch.setattr(
            handlers,
            '_write_json_output',
            lambda *_a, **_k: False,
        )

        captured: dict[str, object] = {}

        def fake_load(
            source_value: object,
            target_type: str,
            target: str,
            *,
            file_format: str | None,
        ) -> dict[str, object]:
            captured['params'] = (
                source_value,
                target_type,
                target,
                file_format,
            )
            return {'loaded': True}

        monkeypatch.setattr(handlers, 'load', fake_load)
        emitted: list[tuple[object, bool]] = []
        monkeypatch.setattr(
            handlers,
            '_emit_json',
            lambda data, pretty: emitted.append((data, pretty)),
        )

        assert handlers.cmd_load(args) == 0
        assert captured['params'] == (
            {'parsed': 'stream', 'fmt': 'json'},
            'database',
            'dest',
            'csv',
        )
        assert emitted == [({'loaded': True}, False)]

    def test_cmd_pipeline_list_prints_jobs(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that the ``pipeline --list`` command prints job listings."""
        cfg = cast(PipelineConfig, DummyCfg())
        monkeypatch.setattr(
            handlers,
            'load_pipeline_config',
            lambda _path, substitute: cfg,
        )
        observed: list[object] = []
        monkeypatch.setattr(handlers, 'print_json', observed.append)

        args = argparse.Namespace(config='cfg.yml', list=True, run=None)
        assert handlers.cmd_pipeline(args) == 0
        assert observed == [{'jobs': ['j1']}]

    def test_cmd_pipeline_run_invokes_run(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that the ``pipeline --run`` command invokes ``run``."""
        cfg = cast(PipelineConfig, DummyCfg())
        monkeypatch.setattr(
            handlers,
            'load_pipeline_config',
            lambda _path, substitute: cfg,
        )

        def fake_run(job: str, config_path: str) -> dict[str, str]:
            return {'job': job, 'source': config_path}

        monkeypatch.setattr(handlers, 'run', fake_run)
        observed: list[object] = []
        monkeypatch.setattr(handlers, 'print_json', observed.append)

        args = argparse.Namespace(config='cfg.yml', list=False, run='job-42')
        assert handlers.cmd_pipeline(args) == 0
        assert observed == [
            {'status': 'ok', 'result': {'job': 'job-42', 'source': 'cfg.yml'}},
        ]

    def test_cmd_run_invokes_run_when_job_present(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that the ``pipeline --run`` command invokes ``run``."""
        cfg = cast(PipelineConfig, DummyCfg())
        monkeypatch.setattr(
            handlers,
            'load_pipeline_config',
            lambda path, substitute: cfg,
        )

        def fake_run(job: str, config_path: str) -> dict[str, str]:
            return {'job': job, 'src': config_path}

        monkeypatch.setattr(handlers, 'run', fake_run)
        observed: list[object] = []
        monkeypatch.setattr(handlers, 'print_json', observed.append)

        args = argparse.Namespace(config='cfg.yml', job='job-7', pipeline=None)
        assert handlers.cmd_run(args) == 0
        assert observed == [
            {'status': 'ok', 'result': {'job': 'job-7', 'src': 'cfg.yml'}},
        ]

    def test_cmd_run_prints_summary_when_no_job(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that the ``pipeline --run`` command prints a summary when no job
        is specified.
        """
        cfg = cast(PipelineConfig, DummyCfg())
        monkeypatch.setattr(
            handlers,
            'load_pipeline_config',
            lambda path, substitute: cfg,
        )
        monkeypatch.setattr(
            handlers,
            '_pipeline_summary',
            lambda _cfg: {'name': 'p1'},
        )
        observed: list[object] = []
        monkeypatch.setattr(handlers, 'print_json', observed.append)

        args = argparse.Namespace(config='cfg.yml', job=None, pipeline=None)
        assert handlers.cmd_run(args) == 0
        assert observed == [{'name': 'p1'}]

    def test_cmd_transform_processes_payload_and_prints(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that the ``transform`` command processes payload and prints
        results.
        """
        args = argparse.Namespace(
            source='data.csv',
            operations=['clean'],
            source_format=None,
            output=None,
            pretty=False,
        )
        monkeypatch.setattr(
            handlers,
            '_materialize_csv_payload',
            lambda src: ['rows'],
        )
        monkeypatch.setattr(
            handlers,
            'transform',
            lambda payload, ops: {'ops': ops, 'payload': payload},
        )
        monkeypatch.setattr(
            handlers,
            '_write_json_output',
            lambda *_a, **_k: False,
        )
        emitted: list[tuple[object, bool]] = []
        monkeypatch.setattr(
            handlers,
            '_emit_json',
            lambda data, pretty: emitted.append((data, pretty)),
        )

        assert handlers.cmd_transform(args) == 0
        assert emitted == [({'ops': ['clean'], 'payload': ['rows']}, False)]

    def test_cmd_validate_writes_validated_data(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that the ``validate`` command writes validated data to file."""
        args = argparse.Namespace(
            source='data.csv',
            rules=['schema'],
            source_format=None,
            output='result.json',
            pretty=True,
        )
        monkeypatch.setattr(
            handlers,
            '_materialize_csv_payload',
            lambda src: ['rows'],
        )
        monkeypatch.setattr(
            handlers,
            'validate',
            lambda payload, rules: {
                'data': {'ok': True},
                'payload': payload,
                'rules': rules,
            },
        )

        recorded: dict[str, object] = {}

        def fake_write_json_output(
            data: object,
            output_path: str | None,
            *,
            success_message: str,
        ) -> bool:
            recorded['data'] = data
            recorded['output_path'] = output_path
            recorded['success_message'] = success_message
            return True

        monkeypatch.setattr(
            handlers,
            '_write_json_output',
            fake_write_json_output,
        )

        def fail_emit_json(*_args: object, **_kwargs: object) -> None:
            raise AssertionError(
                'validate should not print when writing output',
            )

        monkeypatch.setattr(handlers, '_emit_json', fail_emit_json)

        assert handlers.cmd_validate(args) == 0
        assert recorded == {
            'data': {'ok': True},
            'output_path': 'result.json',
            'success_message': 'Validation result saved to',
        }
