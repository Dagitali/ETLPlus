"""
:mod:`tests.unit.cli.test_u_cli_io` module.

Unit tests for :mod:`etlplus.cli.io`.
"""

from __future__ import annotations

import io
import types
from pathlib import Path
from unittest.mock import Mock

import pytest

import etlplus.cli.io as _io
from etlplus.file import FileFormat

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


class TestEmitJson:
    """Unit test suite for :func:`emit_json`."""

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


class TestInferPayloadFormat:
    """Unit test suite for :func:`infer_payload_format`."""

    def test_inferring_payload_format(self) -> None:
        """Test inferring JSON vs CSV using the first significant byte."""
        assert _io.infer_payload_format(' {"a":1}') == 'json'
        assert _io.infer_payload_format('  col1,col2') == 'csv'


class TestMaterializeFilePayload:
    """Unit test suite for :func:`materialize_file_payload`."""

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

            def __init__(self, path_arg: Path, fmt_arg: FileFormat) -> None:
                captured['path'] = Path(path_arg)
                captured['fmt'] = fmt_arg

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
        assert captured['path'] == file_path
        assert captured['fmt'] == FileFormat.XML

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
    """Unit test suite for :func:`parse_text_payload`."""

    @pytest.mark.parametrize(
        ('payload', 'fmt', 'expected'),
        (
            ('{"a": 1}', None, {'a': 1}),
            ('a,b\n1,2\n', 'csv', [{'a': '1', 'b': '2'}]),
            ('payload', 'yaml', 'payload'),
        ),
    )
    def test_parsing_text_payload_variants(
        self,
        payload: str,
        fmt: str | None,
        expected: object,
    ) -> None:
        """
        Test that :func:`parse_text_payload` handles JSON, CSV, and
        passthrough cases.
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


class TestReadCsvRows:
    """Unit test suite for :func:`read_csv_rows`."""

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
    """Unit test suite for :func:`read_stdin_text`."""

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


class TestWriteJsonOutput:
    """Unit test suite for :func:`write_json_output`."""

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
