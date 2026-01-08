"""
:mod:`tests.unit.test_u_cli_handlers` module.

Unit tests for :mod:`etlplus.cli.handlers`.
"""

from __future__ import annotations

import argparse
import io
import json
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
from etlplus.enums import FileFormat

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

    def test_check_sections_all(self) -> None:
        """
        Test that :func:`_check_sections` includes all requested sections."""
        # pylint: disable=protected-access

        args = argparse.Namespace(
            pipelines=True,
            sources=True,
            targets=True,
            transforms=True,
        )
        cfg = cast(PipelineConfig, DummyCfg())
        result = handlers._check_sections(
            cfg,
            args,
        )
        assert set(result) >= {'pipelines', 'sources', 'targets', 'transforms'}

    def test_check_sections_default(self) -> None:
        """
        Test that :func:`_check_sections` defaults to jobs when no flags are
        set.
        """
        # pylint: disable=protected-access

        args = argparse.Namespace(
            pipelines=False,
            sources=False,
            targets=False,
            transforms=False,
        )
        cfg = cast(PipelineConfig, DummyCfg())
        result = handlers._check_sections(
            cfg,
            args,
        )
        assert 'jobs' in result

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

    def test_explicit_cli_format_requires_flag(self) -> None:
        """Test that explicit format hint is ignored unless the flag is set."""
        # pylint: disable=protected-access

        args = argparse.Namespace(format='csv', _format_explicit=False)
        assert handlers._explicit_cli_format(args) is None

    def test_explicit_cli_format_normalizes_hint(self) -> None:
        """Test that explicit format hints are normalized when returned."""
        # pylint: disable=protected-access

        args = argparse.Namespace(format='CSV', _format_explicit=True)
        assert handlers._explicit_cli_format(args) == 'csv'

    def test_materialize_file_payload_non_file(self) -> None:
        """Test that non-file payloads are returned unchanged."""
        # pylint: disable=protected-access

        payload: object = {'foo': 1}
        assert (
            handlers._materialize_file_payload(
                payload,
                format_hint=None,
                format_explicit=False,
            )
            is payload
        )

    def test_materialize_file_payload_infers_csv(self, tmp_path: Path) -> None:
        """Test that CSV files are parsed when no explicit hint is provided."""
        # pylint: disable=protected-access

        file_path = tmp_path / 'file.csv'
        file_path.write_text(CSV_TEXT)

        rows = handlers._materialize_file_payload(
            str(file_path),
            format_hint=None,
            format_explicit=False,
        )

        assert isinstance(rows, list)
        assert rows[0] == {'a': '1', 'b': '2'}

    def test_materialize_file_payload_infers_json(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that JSON files are parsed when no format hint is provided."""
        # pylint: disable=protected-access

        file_path = tmp_path / 'payload.json'
        file_path.write_text('{"alpha": 1}')

        payload = handlers._materialize_file_payload(
            str(file_path),
            format_hint=None,
            format_explicit=False,
        )

        assert payload == {'alpha': 1}

    def test_materialize_file_payload_infers_xml(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that XML files are materialized via :class:`File` when inferred.
        """
        # pylint: disable=protected-access

        file_path = tmp_path / 'payload.xml'
        file_path.write_text('<root><value>1</value></root>')

        sentinel = {'xml': True}
        captured: dict[str, object] = {}

        class DummyFile:
            def __init__(self, path_arg: Path, fmt_arg: FileFormat) -> None:
                captured['path'] = Path(path_arg)
                captured['fmt'] = fmt_arg

            def read(self) -> object:
                return sentinel

        monkeypatch.setattr(handlers, 'File', DummyFile)

        payload = handlers._materialize_file_payload(
            str(file_path),
            format_hint=None,
            format_explicit=False,
        )

        assert payload is sentinel
        assert captured['path'] == file_path
        assert captured['fmt'] == FileFormat.XML

    def test_materialize_file_payload_ignores_hint_without_flag(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that format hints are ignored when the explicit flag is not set.
        """
        # pylint: disable=protected-access

        file_path = tmp_path / 'payload.json'
        file_path.write_text('{"beta": 2}')

        payload = handlers._materialize_file_payload(
            str(file_path),
            format_hint='csv',
            format_explicit=False,
        )

        assert payload == {'beta': 2}

    def test_materialize_file_payload_missing_file_raises(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that missing input files propagate :class:`FileNotFoundError`.
        """
        # pylint: disable=protected-access

        file_path = tmp_path / 'missing.json'

        with pytest.raises(FileNotFoundError):
            handlers._materialize_file_payload(
                str(file_path),
                format_hint=None,
                format_explicit=False,
            )

    def test_materialize_file_payload_respects_hint(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that explicit format hints override filename inference."""
        # pylint: disable=protected-access

        file_path = tmp_path / 'data.txt'
        file_path.write_text(CSV_TEXT)

        rows = handlers._materialize_file_payload(
            str(file_path),
            format_hint='csv',
            format_explicit=True,
        )
        assert isinstance(rows, list)

        json_path = tmp_path / 'mislabeled.csv'
        json_path.write_text('[{"ok": true}]')
        payload = handlers._materialize_file_payload(
            str(json_path),
            format_hint='json',
            format_explicit=True,
        )
        assert payload == [{'ok': True}]

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

    @pytest.mark.parametrize(
        ('payload', 'fmt', 'expected'),
        (
            ('{"a": 1}', None, {'a': 1}),
            ('a,b\n1,2\n', 'csv', [{'a': '1', 'b': '2'}]),
            ('payload', 'yaml', 'payload'),
        ),
    )
    def test_parse_text_payload_variants(
        self,
        payload: str,
        fmt: str | None,
        expected: object,
    ) -> None:
        """
        Test that :func:`_parse_text_payload` handles JSON, CSV, and
        passthrough cases.
        """
        # pylint: disable=protected-access

        assert handlers._parse_text_payload(payload, fmt=fmt) == expected

    def test_parse_text_payload_infers_csv_when_unspecified(self) -> None:
        """
        Test that CSV payloads are parsed when no format hint is provided.
        """
        # pylint: disable=protected-access

        result = handlers._parse_text_payload(CSV_TEXT, fmt=None)
        assert result == [
            {'a': '1', 'b': '2'},
            {'a': '3', 'b': '4'},
        ]

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


class TestCmdRender:
    """Unit tests for the render command handler."""

    def test_cmd_render_writes_sql_from_spec(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test rendering a standalone spec to a file."""

        spec = {
            'schema': 'dbo',
            'table': 'Widget',
            'columns': [
                {'name': 'Id', 'type': 'int', 'nullable': False},
                {'name': 'Name', 'type': 'nvarchar(50)', 'nullable': True},
            ],
            'primary_key': {'columns': ['Id']},
        }

        spec_path = tmp_path / 'spec.json'
        spec_path.write_text(json.dumps(spec), encoding='utf-8')

        output_path = tmp_path / 'out.sql'
        args = argparse.Namespace(
            command='render',
            config=None,
            spec=str(spec_path),
            table=None,
            template='ddl',
            template_path=None,
            output=str(output_path),
            pretty=True,
            quiet=False,
        )

        assert handlers.cmd_render(args) == 0

        sql_text = output_path.read_text(encoding='utf-8')
        assert 'CREATE TABLE [dbo].[Widget]' in sql_text

        captured = capsys.readouterr()
        assert f'Rendered 1 schema(s) to {output_path}' in captured.out

    def test_cmd_render_errors_without_specs(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that missing configs/specs surfaces a helpful error."""

        args = argparse.Namespace(
            command='render',
            config=None,
            spec=None,
            table=None,
            template='ddl',
            template_path=None,
            output=None,
            pretty=True,
            quiet=False,
        )

        assert handlers.cmd_render(args) == 1
        assert 'No table schemas found' in capsys.readouterr().err


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
            pretty=False,
            quiet=False,
        )
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
            pretty=True,
            quiet=False,
        )
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

    def test_cmd_extract_file_respects_explicit_format(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that explicit format hints are forwarded for file extractions.
        """
        args = argparse.Namespace(
            source='table.dat',
            source_type='file',
            format='csv',
            output=None,
            _format_explicit=True,
            pretty=True,
            quiet=False,
        )
        captured: dict[str, object] = {}

        def fake_extract(
            source_type: str,
            source: str,
            *,
            file_format: str | None,
        ) -> dict[str, object]:
            captured['params'] = (source_type, source, file_format)
            return {'ok': True}

        monkeypatch.setattr(handlers, 'extract', fake_extract)
        monkeypatch.setattr(
            handlers,
            '_emit_json',
            lambda *_a, **_k: None,
        )

        assert handlers.cmd_extract(args) == 0
        assert captured['params'] == ('file', 'table.dat', 'csv')

    def test_cmd_extract_suppresses_emit_when_output_written(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that Extract skips stdout emission when output file is provided.
        """
        args = argparse.Namespace(
            source='endpoint',
            source_type='api',
            target_format='json',
            target='export.json',
            _format_explicit=True,
            pretty=True,
            quiet=False,
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
            raise AssertionError('emit_json should not be called')

        monkeypatch.setattr(handlers, '_emit_json', fail_emit_json)

        assert handlers.cmd_extract(args) == 0
        assert observed['params'] == ('api', 'endpoint', 'json')
        assert recorded['data'] == {'status': 'ok'}
        assert recorded['output_path'] == 'export.json'
        assert isinstance(recorded['success_message'], str)

    def test_cmd_check_prints_sections(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``check`` prints requested sections."""
        cfg = cast(PipelineConfig, DummyCfg())
        monkeypatch.setattr(
            handlers,
            'load_pipeline_config',
            lambda path, substitute: cfg,
        )
        monkeypatch.setattr(
            handlers,
            '_check_sections',
            lambda _cfg, args: {'targets': ['t1']},
        )
        observed: list[object] = []
        monkeypatch.setattr(handlers, 'print_json', observed.append)

        args = argparse.Namespace(config='cfg.yml')
        assert handlers.cmd_check(args) == 0
        assert observed == [{'targets': ['t1']}]

    def test_cmd_check_passes_substitute_flag(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`cmd_check` forwards the substitute flag to config
        loader.
        """
        cfg = cast(PipelineConfig, DummyCfg())
        recorded: dict[str, object] = {}

        def fake_load_pipeline_config(
            path: str,
            substitute: bool,
        ) -> PipelineConfig:
            recorded['params'] = (path, substitute)
            return cfg

        monkeypatch.setattr(
            handlers,
            'load_pipeline_config',
            fake_load_pipeline_config,
        )
        monkeypatch.setattr(
            handlers,
            '_check_sections',
            lambda _cfg, _args: {'pipelines': ['p1']},
        )
        captured: list[object] = []
        monkeypatch.setattr(handlers, 'print_json', captured.append)

        args = argparse.Namespace(config='cfg.yml', substitute=True)
        assert handlers.cmd_check(args) == 0
        assert recorded['params'] == ('cfg.yml', True)
        assert captured == [{'pipelines': ['p1']}]

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
            pretty=True,
            quiet=False,
            emitted=None,
        )
        monkeypatch.setattr(
            handlers,
            '_write_json_output',
            lambda *_a, **_k: False,
        )

        recorded: dict[str, object] = {}

        def fake_materialize(
            src: str,
            *,
            format_hint: str | None,
            format_explicit: bool,
        ) -> list[object]:
            recorded['call'] = (src, format_hint, format_explicit)
            return ['rows', src]

        monkeypatch.setattr(
            handlers,
            '_materialize_file_payload',
            fake_materialize,
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
        assert recorded['call'] == ('data.csv', None, False)
        assert args.emitted == (['rows', 'data.csv'], True)

    def test_cmd_load_reads_stdin_and_invokes_load(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that stdin payloads are parsed and routed through :func:`load`.
        """
        args = argparse.Namespace(
            source='-',
            source_type='stream',
            target_type='api',
            target='endpoint',
            source_format=None,
            format=None,
            output=None,
            _format_explicit=False,
            pretty=False,
            quiet=False,
        )

        read_calls = {'count': 0}

        def fake_read_stdin() -> str:
            read_calls['count'] += 1
            return 'stdin-payload'

        monkeypatch.setattr(handlers, '_read_stdin_text', fake_read_stdin)

        parsed_payload = {'payload': 'stdin-payload', 'fmt': None}
        parse_calls: dict[str, object] = {}

        def fake_parse(text: str, fmt: str | None) -> object:
            parse_calls['params'] = (text, fmt)
            return parsed_payload

        monkeypatch.setattr(handlers, '_parse_text_payload', fake_parse)

        def fail_materialize(*_args: object, **_kwargs: object) -> None:
            raise AssertionError(
                '_materialize_file_payload should not be called '
                'for stdin sources',
            )

        monkeypatch.setattr(
            handlers,
            '_materialize_file_payload',
            fail_materialize,
        )

        load_record: dict[str, object] = {}

        def fake_load(
            payload: object,
            target_type: str,
            target: str,
            *,
            file_format: str | None,
        ) -> dict[str, object]:
            load_record['params'] = (payload, target_type, target, file_format)
            return {'loaded': True}

        monkeypatch.setattr(handlers, 'load', fake_load)

        writes: list[tuple[object, str | None, str]] = []

        def fake_write_json(
            data: object,
            output_path: str | None,
            *,
            success_message: str,
        ) -> bool:
            writes.append((data, output_path, success_message))
            return False

        monkeypatch.setattr(handlers, '_write_json_output', fake_write_json)

        emissions: list[tuple[object, bool]] = []
        monkeypatch.setattr(
            handlers,
            '_emit_json',
            lambda data, pretty: emissions.append((data, pretty)),
        )

        assert handlers.cmd_load(args) == 0
        assert read_calls['count'] == 1
        assert parse_calls['params'] == ('stdin-payload', None)
        assert load_record['params'] == (
            parsed_payload,
            'api',
            'endpoint',
            None,
        )
        assert writes[0][0] == {'loaded': True}
        assert writes[0][1] is None
        assert isinstance(writes[0][2], str)
        assert emissions == [({'loaded': True}, False)]

    def test_cmd_load_writes_output_file_and_skips_emit(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that writing to a file skips stdout emission after :func:`load`.
        """
        args = argparse.Namespace(
            source='payload.json',
            source_type='file',
            target_type='db',
            target='warehouse',
            source_format='json',
            format='json',
            output='result.json',
            _format_explicit=True,
            pretty=True,
            quiet=False,
        )

        load_record: dict[str, object] = {}

        def fake_load(
            payload_obj: object,
            target_type: str,
            target: str,
            *,
            file_format: str | None,
        ) -> dict[str, object]:
            load_record['params'] = (
                payload_obj,
                target_type,
                target,
                file_format,
            )
            return {'status': 'queued'}

        monkeypatch.setattr(handlers, 'load', fake_load)

        writes: list[tuple[object, str | None, str]] = []

        def fake_write_json(
            data: object,
            output_path: str | None,
            *,
            success_message: str,
        ) -> bool:
            writes.append((data, output_path, success_message))
            return True

        monkeypatch.setattr(handlers, '_write_json_output', fake_write_json)

        def fail_emit(*_args: object, **_kwargs: object) -> None:
            raise AssertionError(
                '_emit_json should not be called when output is written',
            )

        monkeypatch.setattr(handlers, '_emit_json', fail_emit)

        assert handlers.cmd_load(args) == 0
        assert load_record['params'] == (
            'payload.json',
            'db',
            'warehouse',
            'json',
        )
        assert writes[0][0] == {'status': 'queued'}
        assert writes[0][1] == 'result.json'
        assert isinstance(writes[0][2], str)
