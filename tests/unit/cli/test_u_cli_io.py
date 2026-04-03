"""
:mod:`tests.unit.cli.test_u_cli_io` module.

Unit tests for CLI parsing, input, and output helper modules.
"""

from __future__ import annotations

import io
import types
from pathlib import Path
from unittest.mock import Mock

import pytest

from etlplus.cli._handlers import _input as input_mod
from etlplus.cli._handlers import _output as output_mod
from etlplus.file import FileFormat
from etlplus.utils._data import parse_json

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
        output_mod.emit_json({'b': 2, 'a': 1}, pretty=False)
        captured = capsys.readouterr()
        assert captured.out.strip() == '{"b":2,"a":1}'

    def test_pretty_uses_print_json(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that pretty-printing delegates to :func:`print_json`."""
        called_with: list[object] = []
        monkeypatch.setattr(output_mod, 'print_json', called_with.append)

        payload = {'a': 1}
        output_mod.emit_json(payload, pretty=True)
        assert called_with == [payload]


class TestEmitOrWrite:
    """Unit tests for :func:`emit_or_write`."""

    @pytest.mark.parametrize(
        ('output_path', 'pretty', 'write_succeeds', 'expected_emitted'),
        [
            pytest.param(
                None,
                True,
                False,
                [({'ok': True}, True)],
                id='emit-when-write-skipped',
            ),
            pytest.param(
                'out.json',
                False,
                True,
                [],
                id='skip-emit-when-write-succeeds',
            ),
        ],
    )
    def test_respects_write_result(
        self,
        monkeypatch: pytest.MonkeyPatch,
        output_path: str | None,
        pretty: bool,
        write_succeeds: bool,
        expected_emitted: list[tuple[object, bool]],
    ) -> None:
        """JSON payloads should emit only when the write path does not handle them."""
        emitted: list[tuple[object, bool]] = []
        monkeypatch.setattr(
            output_mod,
            'write_json_output',
            lambda data, output_path, *, success_message: write_succeeds,
        )
        monkeypatch.setattr(
            output_mod,
            'emit_json',
            lambda data, *, pretty: emitted.append((data, pretty)),
        )

        output_mod.emit_or_write(
            {'ok': True},
            output_path,
            pretty=pretty,
            success_message='written to',
        )

        assert emitted == expected_emitted


class TestEmitMarkdownTable:
    """Unit tests for :func:`emit_markdown_table`."""

    def test_formats_none_mappings_sequences_and_escaped_text(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Markdown table emission should normalize and escape cell values."""
        output_mod.emit_markdown_table(
            [
                {
                    'none': None,
                    'mapping': {'a': 1},
                    'sequence': ['x', 'y'],
                    'text': 'a|b\nc',
                },
            ],
            columns=('none', 'mapping', 'sequence', 'text'),
        )

        captured = capsys.readouterr().out.splitlines()

        assert captured == [
            '| none | mapping | sequence | text |',
            '| --- | --- | --- | --- |',
            '|  | {"a":1} | ["x","y"] | a\\|b<br>c |',
        ]


class TestInferPayloadFormat:
    """Unit tests for :func:`infer_payload_format`."""

    @pytest.mark.parametrize(
        ('raw', 'expected'),
        [
            pytest.param(' {"a":1}', 'json', id='json'),
            pytest.param('  col1,col2', 'csv', id='csv'),
        ],
    )
    def test_inferring_payload_format(
        self,
        raw: str,
        expected: str,
    ) -> None:
        """The first meaningful byte should distinguish JSON from CSV."""
        assert input_mod.infer_payload_format(raw) == expected


class TestMaterializeFilePayload:
    """Unit tests for :func:`materialize_file_payload`."""

    @pytest.mark.parametrize(
        ('contents', 'expected', 'format_explicit'),
        [
            pytest.param(
                '{"ok": true}',
                {'ok': True},
                True,
                id='explicit-without-hint',
            ),
            pytest.param(
                '{"alpha": 1}',
                {'alpha': 1},
                False,
                id='implicit-json',
            ),
        ],
    )
    def test_parses_json_suffix_files_without_conflicting_hints(
        self,
        tmp_path: Path,
        contents: str,
        expected: object,
        format_explicit: bool,
    ) -> None:
        """JSON suffix files should still parse when no conflicting hint exists."""
        file_path = tmp_path / 'payload.json'
        file_path.write_text(contents, encoding='utf-8')

        payload = input_mod.materialize_file_payload(
            str(file_path),
            format_hint=None,
            format_explicit=format_explicit,
        )
        assert payload == expected

    def test_ignoring_hint_without_flag(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that format hints are ignored when the explicit flag is not set.
        """
        file_path = tmp_path / 'payload.json'
        file_path.write_text('{"beta": 2}')

        payload = input_mod.materialize_file_payload(
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

        rows = input_mod.materialize_file_payload(
            str(file_path),
            format_hint=None,
            format_explicit=False,
        )

        assert isinstance(rows, list)
        assert rows[0] == {'a': '1', 'b': '2'}

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

        monkeypatch.setattr(input_mod, 'File', DummyFile)

        payload = input_mod.materialize_file_payload(
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
        payload = input_mod.materialize_file_payload(
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
            input_mod.materialize_file_payload(
                str(file_path),
                format_hint=None,
                format_explicit=False,
            )

    def test_missing_path_with_inline_json_is_parsed(self) -> None:
        """Test that inline JSON parses when treated as a missing file path."""
        payload = input_mod.materialize_file_payload(
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
            input_mod.materialize_file_payload(
                missing,
                format_hint=None,
                format_explicit=False,
            )

    @pytest.mark.parametrize(
        ('filename', 'contents', 'format_hint', 'format_explicit'),
        [
            pytest.param(
                'payload.json',
                '{"ok": true}',
                'invalid-format',
                True,
                id='invalid-explicit-hint',
            ),
            pytest.param(
                'payload',
                'opaque',
                None,
                False,
                id='no-suffix',
            ),
            pytest.param(
                'payload.unknown',
                'opaque',
                None,
                False,
                id='unknown-suffix',
            ),
        ],
    )
    def test_keeps_unresolved_sources_as_original_path(
        self,
        tmp_path: Path,
        filename: str,
        contents: str,
        format_hint: str | None,
        format_explicit: bool,
    ) -> None:
        """Unresolved file sources should pass through unchanged."""
        file_path = tmp_path / filename
        file_path.write_text(contents, encoding='utf-8')

        payload = input_mod.materialize_file_payload(
            str(file_path),
            format_hint=format_hint,
            format_explicit=format_explicit,
        )
        assert payload == str(file_path)

    @pytest.mark.parametrize(
        ('payload', 'format_hint', 'format_explicit'),
        [
            pytest.param(123, 'json', True, id='explicit-scalar'),
            pytest.param({'foo': 1}, None, False, id='implicit-mapping'),
        ],
    )
    def test_non_path_payloads_return_unchanged(
        self,
        payload: object,
        format_hint: str | None,
        format_explicit: bool,
    ) -> None:
        """Non-pathlike payloads should bypass file materialization."""
        assert (
            input_mod.materialize_file_payload(
                payload,
                format_hint=format_hint,
                format_explicit=format_explicit,
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

        monkeypatch.setattr(input_mod, 'File', DummyFile)

        payload = input_mod.materialize_file_payload(
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

        rows = input_mod.materialize_file_payload(
            str(file_path),
            format_hint='csv',
            format_explicit=True,
        )
        assert isinstance(rows, list)

        json_path = tmp_path / 'mislabeled.csv'
        json_path.write_text('[{"ok": true}]')
        payload = input_mod.materialize_file_payload(
            str(json_path),
            format_hint='json',
            format_explicit=True,
        )
        assert payload == [{'ok': True}]


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
        assert input_mod.parse_text_payload(payload, fmt=fmt) == expected

    def test_inferring_csv_when_unspecified(
        self,
        csv_text: str,
    ) -> None:
        """
        Test that CSV payloads are parsed when no format hint is provided.
        """
        result = input_mod.parse_text_payload(csv_text, fmt=None)
        assert result == [
            {'a': '1', 'b': '2'},
            {'a': '3', 'b': '4'},
        ]

    def test_parse_json_payload_reports_decode_errors(self) -> None:
        """Test that invalid JSON raises a normalized :class:`ValueError`."""
        with pytest.raises(ValueError, match='Invalid JSON payload'):
            parse_json('{broken')


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
        assert input_mod.read_csv_rows(file_path) == [
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
            input_mod,
            'sys',
            types.SimpleNamespace(stdin=buffer),
        )
        assert input_mod.read_stdin_text() == 'stream-data'


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

        monkeypatch.setattr(input_mod, 'materialize_file_payload', _materialize)

        result = input_mod.resolve_cli_payload(
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
        monkeypatch.setattr(output_mod, 'File', lambda _p, _f: dummy_file)

        output_mod.write_json_output(data, 'out.json', success_message='msg')
        dummy_file.write.assert_called_once_with(data)

    def test_writing_to_stdout(self) -> None:
        """
        Test that returning ``False`` signals STDOUT emission when no output
        path.
        """
        assert (
            output_mod.write_json_output(
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

        monkeypatch.setattr(output_mod, 'File', DummyFile)

        assert (
            output_mod.write_json_output(
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
