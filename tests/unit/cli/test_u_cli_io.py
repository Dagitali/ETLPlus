"""
:mod:`tests.unit.cli.test_u_cli_io` module.

Unit tests for :mod:`etlplus.cli._io`.
"""

from __future__ import annotations

import io
import types
from pathlib import Path
from unittest.mock import Mock

import pytest

import etlplus.cli._io as _io
from etlplus.file import FileFormat

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestEmitJson:
    """Unit tests for :func:`emit_json`."""

    def test_compact_prints_minified(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that compact mode writes JSON to STDOUT."""
        _io.emit_json({'b': 2, 'a': 1}, pretty=False)
        captured = capsys.readouterr()
        assert captured.out.strip() == '{"b":2,"a":1}'

    def test_pretty_uses_print_json(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that pretty-printing delegates to :func:`print_json`."""
        called_with: list[object] = []
        monkeypatch.setattr(_io, 'print_json', called_with.append)

        payload = {'a': 1}
        _io.emit_json(payload, pretty=True)
        assert called_with == [payload]


class TestEmitOrWrite:
    """Unit tests for :func:`emit_or_write`."""

    def test_falls_back_to_emit_when_write_is_skipped(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that, when writes are skipped, payload emits to STDOUT."""
        emitted: list[tuple[object, bool]] = []
        monkeypatch.setattr(
            _io,
            'write_json_output',
            lambda data, output_path, *, success_message: False,
        )
        monkeypatch.setattr(
            _io,
            'emit_json',
            lambda data, *, pretty: emitted.append((data, pretty)),
        )

        _io.emit_or_write(
            {'ok': True},
            None,
            pretty=True,
            success_message='written to',
        )

        assert emitted == [({'ok': True}, True)]

    def test_short_circuits_when_write_succeeds(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that successful writes skip STDOUT emission."""
        emitted: list[tuple[object, bool]] = []
        monkeypatch.setattr(
            _io,
            'write_json_output',
            lambda data, output_path, *, success_message: True,
        )
        monkeypatch.setattr(
            _io,
            'emit_json',
            lambda data, *, pretty: emitted.append((data, pretty)),
        )

        _io.emit_or_write(
            {'ok': True},
            'out.json',
            pretty=False,
            success_message='written to',
        )

        assert emitted == []


class TestInferPayloadFormat:
    """Unit tests for :func:`infer_payload_format`."""

    def test_inferring_payload_format(self) -> None:
        """Test that inferring JSON vs CSV using the first significant byte."""
        assert _io.infer_payload_format(' {"a":1}') == 'json'
        assert _io.infer_payload_format('  col1,col2') == 'csv'


class TestMaterializeFilePayload:
    """Unit tests for :func:`materialize_file_payload`."""

    def test_explicit_without_hint_skips_suffix_inference(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that explicit mode without hint does not infer from suffix."""
        file_path = tmp_path / 'payload.json'
        file_path.write_text('{"ok": true}', encoding='utf-8')

        payload = _io.materialize_file_payload(
            str(file_path),
            format_hint=None,
            format_explicit=True,
        )
        assert payload == str(file_path)

    def test_ignoring_hint_without_flag(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that format hints are ignored when the explicit flag is not set.
        """
        file_path = tmp_path / 'payload.json'
        file_path.write_text('{"beta": 2}')

        payload = _io.materialize_file_payload(
            str(file_path),
            format_hint='csv',
            format_explicit=False,
        )

        assert payload == {'beta': 2}

    def test_inferring_csv(
        self,
        tmp_path: Path,
        csv_text: str,
    ) -> None:
        """Test that CSV files are parsed when no explicit hint is provided."""
        file_path = tmp_path / 'file.csv'
        file_path.write_text(csv_text)

        rows = _io.materialize_file_payload(
            str(file_path),
            format_hint=None,
            format_explicit=False,
        )

        assert isinstance(rows, list)
        assert rows[0] == {'a': '1', 'b': '2'}

    def test_inferring_json(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that JSON files are parsed when no format hint is provided."""
        file_path = tmp_path / 'payload.json'
        file_path.write_text('{"alpha": 1}')

        payload = _io.materialize_file_payload(
            str(file_path),
            format_hint=None,
            format_explicit=False,
        )

        assert payload == {'alpha': 1}

    def test_inferring_xml(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that XML files are materialized via :class:`File` when inferred.
        """
        file_path = tmp_path / 'payload.xml'
        file_path.write_text('<root><value>1</value></root>')

        sentinel = {'xml': True}
        captured: dict[str, object] = {}

        class DummyFile:
            """
            Mock :class:`File` that captures init args and returns a sentinel
            on read.
            """

            file_format = FileFormat.XML

            def __init__(
                self,
                path_arg: object,
                fmt_arg: FileFormat | None = None,
            ) -> None:
                captured['path'] = path_arg
                captured['fmt'] = fmt_arg

            def exists(self) -> bool:
                """Report that the test file exists."""
                return True

            def read(self) -> object:
                """Return the sentinel object."""
                return sentinel

        monkeypatch.setattr(_io, 'File', DummyFile)

        payload = _io.materialize_file_payload(
            str(file_path),
            format_hint=None,
            format_explicit=False,
        )

        assert payload is sentinel
        assert captured['path'] == str(file_path)
        assert captured['fmt'] is None

    def test_inline_payload_with_hint(self) -> None:
        """
        Test that inline payloads parse when format hints are explicit.
        """
        payload = _io.materialize_file_payload(
            '[{"ok": true}]',
            format_hint='json',
            format_explicit=True,
        )
        assert payload == [{'ok': True}]

    def test_invalid_explicit_hint_keeps_source_as_is(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that invalid explicit hints do not force parsing."""
        file_path = tmp_path / 'payload.json'
        file_path.write_text('{"ok": true}', encoding='utf-8')

        payload = _io.materialize_file_payload(
            str(file_path),
            format_hint='invalid-format',
            format_explicit=True,
        )
        assert payload == str(file_path)

    def test_missing_file_raises(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that missing input files propagate :class:`FileNotFoundError`.
        """
        file_path = tmp_path / 'missing.json'

        with pytest.raises(FileNotFoundError):
            _io.materialize_file_payload(
                str(file_path),
                format_hint=None,
                format_explicit=False,
            )

    def test_missing_path_with_inline_json_is_parsed(self) -> None:
        """Test that inline JSON parses when treated as a missing file path."""
        payload = _io.materialize_file_payload(
            '{"inline": true}',
            format_hint='json',
            format_explicit=True,
        )
        assert payload == {'inline': True}

    def test_missing_pathlike_source_raises_file_not_found(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that path-like sources still raise when files are missing."""
        missing = tmp_path / 'missing.json'
        with pytest.raises(FileNotFoundError, match='File not found'):
            _io.materialize_file_payload(
                missing,
                format_hint=None,
                format_explicit=False,
            )

    def test_no_suffix_without_explicit_format_keeps_source_as_is(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that sources without suffix do not infer a file format."""
        file_path = tmp_path / 'payload'
        file_path.write_text('opaque', encoding='utf-8')

        payload = _io.materialize_file_payload(
            str(file_path),
            format_hint=None,
            format_explicit=False,
        )
        assert payload == str(file_path)

    def test_non_path_payload_returns_unchanged(self) -> None:
        """Test that non-pathlike payloads bypass file materialization."""
        payload: object = 123
        assert (
            _io.materialize_file_payload(
                payload,
                format_hint='json',
                format_explicit=True,
            )
            is payload
        )

    def test_remote_uri_uses_file_reader(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that remote URIs are hydrated via :class:`File`."""
        captured: dict[str, object] = {}

        class DummyFile:
            """Capture remote URI construction and reads."""

            file_format = FileFormat.JSON

            def __init__(
                self,
                path_arg: object,
                fmt_arg: FileFormat | None = None,
            ) -> None:
                captured['path'] = path_arg
                captured['fmt'] = fmt_arg

            def exists(self) -> bool:
                """Report that the remote object exists."""
                return True

            def read(self) -> object:
                """Return a sentinel JSON payload."""
                return {'remote': True}

        monkeypatch.setattr(_io, 'File', DummyFile)

        payload = _io.materialize_file_payload(
            's3://bucket/payload.json',
            format_hint=None,
            format_explicit=False,
        )

        assert payload == {'remote': True}
        assert captured['path'] == 's3://bucket/payload.json'
        assert captured['fmt'] is None

    def test_respects_hint(
        self,
        tmp_path: Path,
        csv_text: str,
    ) -> None:
        """Test that explicit format hints override filename inference."""
        file_path = tmp_path / 'data.txt'
        file_path.write_text(csv_text)

        rows = _io.materialize_file_payload(
            str(file_path),
            format_hint='csv',
            format_explicit=True,
        )
        assert isinstance(rows, list)

        json_path = tmp_path / 'mislabeled.csv'
        json_path.write_text('[{"ok": true}]')
        payload = _io.materialize_file_payload(
            str(json_path),
            format_hint='json',
            format_explicit=True,
        )
        assert payload == [{'ok': True}]

    def test_unknown_suffix_keeps_source_as_is(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that unknown file extensions keep raw source value."""
        file_path = tmp_path / 'payload.unknown'
        file_path.write_text('opaque', encoding='utf-8')

        payload = _io.materialize_file_payload(
            str(file_path),
            format_hint=None,
            format_explicit=False,
        )
        assert payload == str(file_path)

    def test_with_non_file(self) -> None:
        """Test that non-file payloads are returned unchanged."""
        payload: object = {'foo': 1}
        assert (
            _io.materialize_file_payload(
                payload,
                format_hint=None,
                format_explicit=False,
            )
            is payload
        )


class TestParseTextPayload:
    """Unit tests for :func:`parse_text_payload`."""

    @pytest.mark.parametrize(
        ('payload', 'fmt', 'expected'),
        [
            ('{"a": 1}', None, {'a': 1}),
            ('a,b\n1,2\n', 'csv', [{'a': '1', 'b': '2'}]),
            ('payload', 'yaml', 'payload'),
        ],
    )
    def test_parsing_text_payload_variants(
        self,
        payload: str,
        fmt: str | None,
        expected: object,
    ) -> None:
        """
        Test that :func:`parse_text_payload` handles JSON, CSV, and passthrough
        cases.
        """
        assert _io.parse_text_payload(payload, fmt=fmt) == expected

    def test_inferring_csv_when_unspecified(
        self,
        csv_text: str,
    ) -> None:
        """
        Test that CSV payloads are parsed when no format hint is provided.
        """
        result = _io.parse_text_payload(csv_text, fmt=None)
        assert result == [
            {'a': '1', 'b': '2'},
            {'a': '3', 'b': '4'},
        ]

    def test_parse_json_payload_reports_decode_errors(self) -> None:
        """Test that invalid JSON raises a normalized :class:`ValueError`."""
        with pytest.raises(ValueError, match='Invalid JSON payload'):
            _io.parse_json_payload('{broken')


class TestReadCsvRows:
    """Unit tests for :func:`read_csv_rows`."""

    def test_reading_csv_rows(
        self,
        tmp_path: Path,
        csv_text: str,
    ) -> None:
        """
        Test that :func:`read_csv_rows` reads a CSV into row dictionaries.
        """
        file_path = tmp_path / 'data.csv'
        file_path.write_text(csv_text)
        assert _io.read_csv_rows(file_path) == [
            {'a': '1', 'b': '2'},
            {'a': '3', 'b': '4'},
        ]


class TestReadStdinText:
    """Unit tests for :func:`read_stdin_text`."""

    def test_reading_stdin_text(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that reading STDIN returns the buffered stream contents."""
        buffer = io.StringIO('stream-data')
        monkeypatch.setattr(
            _io,
            'sys',
            types.SimpleNamespace(stdin=buffer),
        )
        assert _io.read_stdin_text() == 'stream-data'


class TestResolveCliPayload:
    """Unit tests for :func:`resolve_cli_payload`."""

    def test_hydrates_file_sources_by_default(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that default behavior delegates to ``materialize_file_payload``.
        """
        captured: list[tuple[object, str | None, bool]] = []

        def _materialize(
            source: object,
            *,
            format_hint: str | None,
            format_explicit: bool,
        ) -> object:
            captured.append((source, format_hint, format_explicit))
            return {'ok': True}

        monkeypatch.setattr(_io, 'materialize_file_payload', _materialize)

        result = _io.resolve_cli_payload(
            'payload.json',
            format_hint='json',
            format_explicit=True,
        )

        assert result == {'ok': True}
        assert captured == [('payload.json', 'json', True)]


class TestWriteJsonOutput:
    """Unit tests for :func:`write_json_output`."""

    def test_writing_to_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that, when a file path is provided, JSON is written via
        :class:`File`.
        """
        data = {'x': 1}

        dummy_file = Mock()
        monkeypatch.setattr(_io, 'File', lambda _p, _f: dummy_file)

        _io.write_json_output(data, 'out.json', success_message='msg')
        dummy_file.write.assert_called_once_with(data)

    def test_writing_to_stdout(self) -> None:
        """
        Test that returning ``False`` signals STDOUT emission when no output
        path.
        """
        assert (
            _io.write_json_output(
                {'x': 1},
                None,
                success_message='msg',
            )
            is False
        )

    def test_writing_to_remote_uri(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that remote JSON targets are written via :class:`File`."""
        captured: dict[str, object] = {}

        class DummyFile:
            """Capture the remote URI passed to :class:`File`."""

            def __init__(self, path_arg: object, fmt_arg: FileFormat) -> None:
                captured['path'] = path_arg
                captured['fmt'] = fmt_arg

            def write(self, data: object) -> None:
                """Capture the written JSON payload."""
                captured['data'] = data

        monkeypatch.setattr(_io, 'File', DummyFile)

        assert (
            _io.write_json_output(
                {'remote': True},
                's3://bucket/out.json',
                success_message='msg',
            )
            is True
        )
        assert captured == {
            'path': 's3://bucket/out.json',
            'fmt': FileFormat.JSON,
            'data': {'remote': True},
        }
