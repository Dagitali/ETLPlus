"""
:mod:`tests.unit.cli.test_u_cli_io` module.

Unit tests for CLI parsing, input, and output helper modules.
"""

from __future__ import annotations

import io
import types
from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.cli._handlers import _input as input_mod
from etlplus.cli._handlers import _output as output_mod
from etlplus.file import FileFormat
from etlplus.utils import JsonCodec
from etlplus.utils import _payloads as payload_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


type SourceBuilder = Callable[[Path], str | Path]


def _string_missing_source(tmp_path: Path) -> str:
    """Build one missing string path for file-materialization tests."""
    return str(tmp_path / 'missing.json')


def _path_missing_source(tmp_path: Path) -> Path:
    """Build one missing pathlike source for file-materialization tests."""
    return tmp_path / 'missing.json'


def _build_readable_file_double(
    *,
    payload: object,
    resolved_format: FileFormat,
) -> tuple[type[object], dict[str, object]]:
    """Build a ``File`` double that captures reads and returns one payload."""
    captured: dict[str, object] = {}

    class DummyFile:
        """Capture file construction and return a fixed payload on read."""

        file_format = resolved_format

        def __init__(
            self,
            path_arg: object,
            fmt_arg: FileFormat | None = None,
        ) -> None:
            captured['path'] = path_arg
            captured['fmt'] = fmt_arg

        def exists(self) -> bool:
            """Report that the referenced file exists."""
            return True

        def read(self) -> object:
            """Return the configured sentinel payload."""
            return payload

    return DummyFile, captured


def _build_writable_file_double() -> tuple[type[object], dict[str, object]]:
    """Build a ``File`` double that captures write-target construction."""
    captured: dict[str, object] = {}

    class DummyFile:
        """Capture file construction and the payload written to it."""

        def __init__(self, path_arg: object, fmt_arg: FileFormat) -> None:
            captured['path'] = path_arg
            captured['fmt'] = fmt_arg

        def write(self, data: object) -> None:
            """Capture the written payload."""
            captured['data'] = data

    return DummyFile, captured


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
        """Test that pretty-printing delegates to :meth:`JsonCodec.print`."""
        called_with: list[object] = []
        monkeypatch.setattr(output_mod.JsonCodec, 'print', called_with.append)

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

    @pytest.mark.parametrize(
        ('source', 'resolved_format', 'expected'),
        [
            pytest.param(
                'payload.xml',
                FileFormat.XML,
                {'xml': True},
                id='local-xml',
            ),
            pytest.param(
                's3://bucket/payload.json',
                FileFormat.JSON,
                {'remote': True},
                id='remote-json-uri',
            ),
        ],
    )
    def test_reads_existing_structured_sources_via_file(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        source: str,
        resolved_format: FileFormat,
        expected: object,
    ) -> None:
        """Existing file-like sources should hydrate through :class:`File`."""
        actual_source = (
            str(tmp_path / source) if not source.startswith('s3://') else source
        )
        if source.endswith('.xml'):
            Path(actual_source).write_text(
                '<root><value>1</value></root>',
                encoding='utf-8',
            )
        dummy_file, captured = _build_readable_file_double(
            payload=expected,
            resolved_format=resolved_format,
        )
        monkeypatch.setattr(payload_mod, 'FILE', dummy_file)

        payload = input_mod.materialize_file_payload(
            actual_source,
            format_hint=None,
            format_explicit=False,
        )

        assert payload == expected
        assert captured['path'] == actual_source
        assert captured['fmt'] is None

    @pytest.mark.parametrize(
        ('source', 'expected'),
        [
            pytest.param('[{"ok": true}]', [{'ok': True}], id='list-json'),
            pytest.param('{"inline": true}', {'inline': True}, id='dict-json'),
        ],
    )
    def test_parses_inline_json_when_hint_is_explicit(
        self,
        source: str,
        expected: object,
    ) -> None:
        """Inline JSON payloads should parse when the hint is explicit."""
        payload = input_mod.materialize_file_payload(
            source,
            format_hint='json',
            format_explicit=True,
        )
        assert payload == expected

    @pytest.mark.parametrize(
        ('source_builder', 'match'),
        [
            pytest.param(
                _string_missing_source,
                'File not found: ',
                id='string-path',
            ),
            pytest.param(
                _path_missing_source,
                'File not found',
                id='pathlike',
            ),
        ],
    )
    def test_missing_file_sources_raise_file_not_found(
        self,
        tmp_path: Path,
        source_builder: SourceBuilder,
        match: str,
    ) -> None:
        """Missing file sources should propagate :class:`FileNotFoundError`."""
        missing_source = source_builder(tmp_path)

        with pytest.raises(FileNotFoundError, match=match):
            input_mod.materialize_file_payload(
                missing_source,
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

    @pytest.mark.parametrize(
        ('filename', 'contents', 'format_hint', 'expected'),
        [
            pytest.param(
                'data.txt',
                'a,b\n1,2\n3,4\n',
                'csv',
                [
                    {'a': '1', 'b': '2'},
                    {'a': '3', 'b': '4'},
                ],
                id='csv-hint-overrides-text-suffix',
            ),
            pytest.param(
                'mislabeled.csv',
                '[{"ok": true}]',
                'json',
                [{'ok': True}],
                id='json-hint-overrides-csv-suffix',
            ),
        ],
    )
    def test_respects_explicit_format_hints(
        self,
        tmp_path: Path,
        filename: str,
        contents: str,
        format_hint: str,
        expected: object,
    ) -> None:
        """Explicit format hints should override filename-based inference."""
        file_path = tmp_path / filename
        file_path.write_text(contents, encoding='utf-8')

        payload = input_mod.materialize_file_payload(
            str(file_path),
            format_hint=format_hint,
            format_explicit=True,
        )
        assert payload == expected


class TestParseTextPayload:
    """Unit tests for :func:`parse_text_payload`."""

    @pytest.mark.parametrize(
        ('payload', 'fmt_hint', 'expected'),
        [
            ('{"a": 1}', None, {'a': 1}),
            ('a,b\n1,2\n', 'csv', [{'a': '1', 'b': '2'}]),
            ('payload', 'yaml', 'payload'),
        ],
    )
    def test_parsing_text_payload_variants(
        self,
        payload: str,
        fmt_hint: str | None,
        expected: object,
    ) -> None:
        """
        Test that :func:`parse_text_payload` handles JSON, CSV, and passthrough
        cases.
        """
        assert input_mod.parse_text_payload(payload, fmt_hint=fmt_hint) == expected

    def test_inferring_csv_when_unspecified(
        self,
        csv_text: str,
    ) -> None:
        """
        Test that CSV payloads are parsed when no format hint is provided.
        """
        result = input_mod.parse_text_payload(csv_text, fmt_hint=None)
        assert result == [
            {'a': '1', 'b': '2'},
            {'a': '3', 'b': '4'},
        ]

    def test_parse_json_payload_reports_decode_errors(self) -> None:
        """Test that invalid JSON raises a normalized :class:`ValueError`."""
        with pytest.raises(ValueError, match='Invalid JSON payload'):
            JsonCodec.parse('{broken')


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

    @pytest.mark.parametrize(
        ('source', 'expected'),
        [
            pytest.param('-', True, id='dash'),
            pytest.param(' - ', True, id='spaced-dash'),
            pytest.param('', False, id='empty'),
            pytest.param(None, False, id='none'),
            pytest.param(Path('-'), False, id='pathlike-dash'),
        ],
    )
    def test_is_stdin_source(
        self,
        source: object,
        expected: bool,
    ) -> None:
        """Test source normalization for STDIN destinations."""
        assert input_mod.is_stdin_source(source) is expected

    @pytest.mark.parametrize(
        'source',
        [
            pytest.param('-', id='dash'),
            pytest.param(' - ', id='spaced-dash'),
        ],
    )
    def test_reads_stdin_source(
        self,
        monkeypatch: pytest.MonkeyPatch,
        source: str,
    ) -> None:
        """Test STDIN source sentinels read and parse STDIN."""
        monkeypatch.setattr(input_mod, 'read_stdin_text', lambda: '{"ok": true}')

        assert input_mod.resolve_cli_payload(
            source,
            format_hint='json',
            format_explicit=True,
        ) == {'ok': True}


class TestWriteJsonOutput:
    """Unit tests for :func:`write_json_output`."""

    @pytest.mark.parametrize(
        'output_path',
        [
            pytest.param('out.json', id='local-file'),
            pytest.param('s3://bucket/out.json', id='remote-uri'),
        ],
    )
    def test_writing_to_file_target(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        output_path: str,
    ) -> None:
        """File-like output paths should write JSON through :class:`File`."""
        data = {'x': 1}
        dummy_file, captured = _build_writable_file_double()
        monkeypatch.setattr(output_mod, 'File', dummy_file)

        assert (
            output_mod.write_json_output(data, output_path, success_message='msg')
            is True
        )
        assert captured == {
            'path': output_path,
            'fmt': FileFormat.JSON,
            'data': data,
        }
        assert capsys.readouterr().out.strip() == f'msg {output_path}'

    @pytest.mark.parametrize(
        'output_path',
        [
            pytest.param(None, id='none'),
            pytest.param('', id='empty'),
            pytest.param(' - ', id='spaced-dash'),
        ],
    )
    def test_writing_to_stdout(self, output_path: str | None) -> None:
        """
        Test that returning ``False`` signals STDOUT emission when no output
        path.
        """
        assert (
            output_mod.write_json_output(
                {'x': 1},
                output_path,
                success_message='msg',
            )
            is False
        )
