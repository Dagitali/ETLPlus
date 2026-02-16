"""
:mod:`tests.unit.file.test_u_file_core` module.

Unit tests for :mod:`etlplus.file.core`.
"""

from __future__ import annotations

import math
import numbers
import sqlite3
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

from etlplus.file import File
from etlplus.file import FileFormat
from etlplus.file import core as core_mod
from etlplus.file import csv as csv_file
from etlplus.file import json as json_file
from etlplus.file import xml as xml_file
from etlplus.file.base import WriteOptions
from etlplus.types import JSONData

from ...pytest_file_common import Operation
from ...pytest_file_common import skip_on_known_file_io_error
from .pytest_file_core_cases import EMBEDDED_DB_MULTI_TABLE_CASE_IDS
from .pytest_file_core_cases import EMBEDDED_DB_MULTI_TABLE_CASES
from .pytest_file_core_cases import EXPLICIT_STRING_FORMAT_CASE_IDS
from .pytest_file_core_cases import EXPLICIT_STRING_FORMAT_CASES
from .pytest_file_core_cases import FORMAT_CASES
from .pytest_file_core_cases import FORMAT_INFERENCE_CASE_IDS
from .pytest_file_core_cases import FORMAT_INFERENCE_CASES
from .pytest_file_core_cases import NUMERIC_ROUNDTRIP_NORMALIZED_FORMATS
from .pytest_file_core_cases import STUB_OPERATION_CASES
from .pytest_file_core_cases import STUBBED_FORMATS
from .pytest_file_core_cases import UNKNOWN_FORMAT_CASE_IDS
from .pytest_file_core_cases import UNKNOWN_FORMAT_CASES
from .pytest_file_core_cases import XML_ROUNDTRIP_NORMALIZED_FORMATS

# SECTION: HELPERS ========================================================== #


def _assert_core_write_dispatch(
    calls: dict[str, object],
    *,
    expected_format: FileFormat,
    expected_path: Path,
    expected_data: JSONData,
    expected_root_tag: str,
) -> None:
    """Assert core write dispatch metadata with routed XML root tag."""
    assert calls['format'] is expected_format
    assert calls['write_path'] == expected_path
    assert calls['write_data'] == expected_data

    options = calls['write_options']
    assert isinstance(options, WriteOptions)
    assert options.root_tag == expected_root_tag


def _require_optional_modules(
    requires: tuple[str, ...],
) -> None:
    """Import optional dependencies for one format case or skip."""
    for module_name in requires:
        pytest.importorskip(module_name)


def _coerce_numeric_value(value: object) -> object:
    """Coerce numeric scalars into stable Python numeric types."""
    if isinstance(value, numbers.Real):
        try:
            numeric = float(value)
            if math.isnan(numeric):
                return None
        except (TypeError, ValueError):
            return value
        if numeric.is_integer():
            return int(numeric)
        return float(numeric)
    return value


def _install_core_handler_stub(
    monkeypatch: pytest.MonkeyPatch,
    *,
    read_result: JSONData | None = None,
    write_result: int = 0,
) -> dict[str, object]:
    """Install a configurable core handler stub and return call metadata."""
    calls: dict[str, object] = {}

    def _get_handler(file_format: FileFormat) -> object:
        calls['format'] = file_format
        return handler

    def _read(path: Path) -> JSONData:
        calls['read_path'] = path
        return [] if read_result is None else read_result

    def _write(
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        calls['write_path'] = path
        calls['write_data'] = data
        calls['write_options'] = options
        return write_result

    handler = SimpleNamespace(read=_read, write=_write)

    monkeypatch.setattr(core_mod, 'get_handler', _get_handler)
    return calls


def normalize_numeric_records(records: JSONData) -> JSONData:
    """Normalize numeric values for deterministic record comparisons."""
    if not isinstance(records, list):
        return records
    return [
        (
            {key: _coerce_numeric_value(value) for key, value in row.items()}
            if isinstance(row, dict)
            else row
        )
        for row in records
    ]


def normalize_xml_payload(payload: JSONData) -> JSONData:
    """Normalize XML payloads to list-based item structures."""
    if not isinstance(payload, dict):
        return payload
    root = payload.get('root')
    if not isinstance(root, dict):
        return payload
    items = root.get('items')
    if isinstance(items, dict):
        root = {**root, 'items': [items]}
        return {**payload, 'root': root}
    return payload


def _normalize_roundtrip_values(
    *,
    file_format: FileFormat,
    result: JSONData,
    expected: JSONData,
) -> tuple[JSONData, JSONData]:
    """Normalize roundtrip values for format-specific assertion behavior."""
    normalized_result = result
    normalized_expected = expected
    if file_format in XML_ROUNDTRIP_NORMALIZED_FORMATS:
        normalized_result = normalize_xml_payload(normalized_result)
        normalized_expected = normalize_xml_payload(normalized_expected)
    if file_format in NUMERIC_ROUNDTRIP_NORMALIZED_FORMATS:
        normalized_result = normalize_numeric_records(normalized_result)
    return normalized_result, normalized_expected


def _read_with_known_io_skip(
    *,
    path: Path,
    file_format: FileFormat,
) -> JSONData:
    """Read one file while applying shared known I/O skip policy."""
    try:
        return File(path, file_format).read()
    except OSError as err:
        skip_on_known_file_io_error(
            error=err,
            file_format=file_format,
        )
        raise


# SECTION: TESTS ============================================================ #


class TestFile:
    """Unit tests for :class:`etlplus.file.File`."""

    @pytest.mark.parametrize(
        ('file_format', 'filename'),
        EMBEDDED_DB_MULTI_TABLE_CASES,
        ids=EMBEDDED_DB_MULTI_TABLE_CASE_IDS,
    )
    def test_embedded_db_read_fails_with_multiple_tables(
        self,
        tmp_path: Path,
        file_format: FileFormat,
        filename: str,
    ) -> None:
        """Test embedded DB readers rejecting multi-table files."""
        path = tmp_path / filename
        if file_format is FileFormat.DUCKDB:
            duckdb = pytest.importorskip('duckdb')
            conn = duckdb.connect(str(path))
        else:
            conn = sqlite3.connect(path)
        try:
            conn.execute('CREATE TABLE one (id INTEGER)')
            conn.execute('CREATE TABLE two (id INTEGER)')
            if file_format is FileFormat.SQLITE:
                conn.commit()
        finally:
            conn.close()

        with pytest.raises(ValueError, match='Multiple tables'):
            File(path, file_format).read()

    @pytest.mark.parametrize(
        ('raw_format', 'expected'),
        EXPLICIT_STRING_FORMAT_CASES,
        ids=EXPLICIT_STRING_FORMAT_CASE_IDS,
    )
    def test_explicit_string_file_format_validation(
        self,
        tmp_path: Path,
        raw_format: str,
        expected: FileFormat | None,
    ) -> None:
        """Test explicit string file-format coercion and validation."""
        path = tmp_path / 'data.json'
        if expected is None:
            with pytest.raises(ValueError):
                File(path, cast(Any, raw_format))
            return
        file = File(path, cast(Any, raw_format))
        assert file.file_format is expected

    def test_gz_roundtrip_json(
        self,
        tmp_path: Path,
    ) -> None:
        """Test JSON round trip inside a gzip archive."""
        path = tmp_path / 'data.json.gz'
        payload = [{'name': 'Ada'}]

        File(path, FileFormat.GZ).write(payload)
        result = File(path, FileFormat.GZ).read()

        assert result == payload

    @pytest.mark.parametrize(
        ('filename', 'expected_format'),
        FORMAT_INFERENCE_CASES,
        ids=FORMAT_INFERENCE_CASE_IDS,
    )
    def test_infers_format_from_extension_patterns(
        self,
        tmp_path: Path,
        filename: str,
        expected_format: FileFormat,
    ) -> None:
        """Test inference from standard and compressed filename patterns."""
        path = tmp_path / filename
        path.write_text('{}', encoding='utf-8')

        file = File(path)

        assert file.file_format == expected_format

    def test_path_support_for_module_helpers(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test module helpers accept ``Path`` inputs.
        """
        cases: tuple[
            tuple[Any, str, JSONData, JSONData, dict[str, object]],
            ...,
        ] = (
            (
                csv_file.CsvFile(),
                'data.csv',
                [{'name': 'Ada'}],
                [{'name': 'Ada'}],
                {},
            ),
            (
                json_file.JsonFile(),
                'data.json',
                {'name': 'Ada'},
                {'name': 'Ada'},
                {},
            ),
            (
                xml_file.XmlFile(),
                'data.xml',
                {'root': {'text': 'hello'}},
                {'root': {'text': 'hello'}},
                {'options': WriteOptions(root_tag='root')},
            ),
        )
        for handler, filename, payload, expected, write_kwargs in cases:
            path = tmp_path / filename
            handler.write(path, payload, **write_kwargs)
            assert handler.read(path) == expected

    def test_read_csv_skips_blank_rows(
        self,
        tmp_path: Path,
    ) -> None:
        """Test CSV reader ignoring empty rows."""
        payload = 'name,age\nJohn,30\n,\nJane,25\n'
        path = tmp_path / 'data.csv'
        path.write_text(payload, encoding='utf-8')

        rows = File(path, FileFormat.CSV).read()

        assert [
            row['name']
            for row in rows
            if isinstance(row, dict) and 'name' in row
        ] == ['John', 'Jane']

    def test_read_json_type_errors(self, tmp_path: Path) -> None:
        """Test list elements being dicts when reading JSON."""
        path = tmp_path / 'bad.json'
        path.write_text('[{"ok": 1}, 2]', encoding='utf-8')

        with pytest.raises(TypeError):
            File(path, FileFormat.JSON).read()

    def test_read_missing_file_raises_before_format_inference(
        self,
        tmp_path: Path,
    ) -> None:
        """Test missing path checks run before format inference on read."""
        missing = tmp_path / 'missing.unknown'
        file = File(missing)

        with pytest.raises(FileNotFoundError):
            file.read()

    @pytest.mark.parametrize(
        ('filename', 'contents', 'operation', 'error_pattern'),
        UNKNOWN_FORMAT_CASES,
        ids=UNKNOWN_FORMAT_CASE_IDS,
    )
    def test_unknown_formats_defer_error(
        self,
        tmp_path: Path,
        filename: str,
        contents: str | None,
        operation: Operation,
        error_pattern: str,
    ) -> None:
        """Test unresolved formats deferring failure until dispatch."""
        path = tmp_path / filename
        if contents is not None:
            path.write_text(contents, encoding='utf-8')
        file = File(path)

        assert file.file_format is None
        operation_kwargs = {
            'read': {},
            'write': {'data': {'ok': True}},
        }[operation]
        with pytest.raises(ValueError, match=error_pattern):
            getattr(file, operation)(**operation_kwargs)

    @pytest.mark.parametrize(
        'file_format,filename,payload,expected,requires',
        [
            pytest.param(
                file_format,
                filename,
                payload,
                expected,
                requires,
                id=file_format.value,
            )
            for (
                file_format,
                filename,
                payload,
                expected,
                requires,
            ) in FORMAT_CASES
        ],
    )
    def test_roundtrip_by_format(
        self,
        tmp_path: Path,
        file_format: FileFormat,
        filename: str,
        payload: JSONData,
        expected: JSONData,
        requires: tuple[str, ...],
    ) -> None:
        """Test round-trip reads and writes across file formats."""
        _require_optional_modules(requires)
        path = tmp_path / filename

        File(path, file_format).write(payload)
        result = _read_with_known_io_skip(
            path=path,
            file_format=file_format,
        )
        normalized_result, normalized_expected = _normalize_roundtrip_values(
            file_format=file_format,
            result=result,
            expected=expected,
        )
        assert normalized_result == normalized_expected

    @pytest.mark.parametrize(
        ('file_format', 'filename'),
        [
            pytest.param(
                file_format,
                filename,
                id=file_format.value,
            )
            for file_format, filename in STUBBED_FORMATS
        ],
    )
    @pytest.mark.parametrize('operation', STUB_OPERATION_CASES)
    def test_stub_formats_raise_on_operations(
        self,
        tmp_path: Path,
        file_format: FileFormat,
        filename: str,
        operation: Operation,
    ) -> None:
        """Test stub formats raising NotImplementedError on read/write."""
        path = tmp_path / filename
        seed_content = {'read': 'stub', 'write': None}[operation]
        if seed_content is not None:
            path.write_text(seed_content, encoding='utf-8')

        file = File(path, file_format)
        operation_kwargs = {
            'read': {},
            'write': {'data': {'stub': True}},
        }[operation]
        with pytest.raises(NotImplementedError):
            getattr(file, operation)(**operation_kwargs)

    def test_write_csv_rejects_non_dicts(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test non-dict entries raising a TypeError when writing CSV rows.
        """
        path = tmp_path / 'data.csv'
        invalid_entry = cast(dict[str, object], 'invalid')
        with pytest.raises(TypeError, match='CSV payloads'):
            File(path, FileFormat.CSV).write(
                [{'name': 'John'}, invalid_entry],
            )

    def test_write_json_returns_record_count(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test ``write_json`` returning the record count for lists.
        """
        path = tmp_path / 'data.json'
        records = [{'a': 1}, {'a': 2}]

        written = File(path, FileFormat.JSON).write(records)

        assert written == 2
        json_content = path.read_text(encoding='utf-8')
        assert json_content
        assert json_content.count('\n') >= 2

    def test_xls_write_not_supported(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`write` raises an error indicating lack of support.
        """
        pytest.importorskip('pandas')
        path = tmp_path / 'sample.xls'

        with pytest.raises(RuntimeError, match='read-only'):
            File(path, FileFormat.XLS).write([{'name': 'Ada', 'age': 36}])

    def test_xml_respects_root_tag(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test custom root_tag being used when data lacks a single root.
        """
        path = tmp_path / 'export.xml'
        records = [{'name': 'Ada'}, {'name': 'Linus'}]

        File(path, FileFormat.XML).write(records, root_tag='records')

        text = path.read_text(encoding='utf-8')
        assert text.startswith('<?xml')
        assert '<records>' in text

    def test_xml_roundtrip_with_attributes(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test XML read/write preserves attribute fields.
        """
        path = tmp_path / 'attrs.xml'
        payload: JSONData = {
            'root': {
                '@id': '42',
                'item': {'@lang': 'en', 'text': 'Hello'},
            },
        }

        File(path, FileFormat.XML).write(payload)
        result = File(path, FileFormat.XML).read()

        assert result == payload

    def test_zip_multi_file_read(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test ZIP files with multiple entries return a dict payload.
        """
        path = tmp_path / 'bundle.zip'
        with zipfile.ZipFile(path, 'w') as archive:
            archive.writestr('a.json', '{"a": 1}')
            archive.writestr('b.json', '{"b": 2}')

        result = File(path, FileFormat.ZIP).read()

        assert result == {'a.json': {'a': 1}, 'b.json': {'b': 2}}


class TestFileCoreDispatch:
    """Unit tests for class-based dispatch in :class:`etlplus.file.File`."""

    def test_read_uses_class_based_handler_dispatch(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test read dispatch through ``core.get_handler`` handlers."""
        path = tmp_path / 'sample.csv'
        path.write_text('name\nAda\n', encoding='utf-8')
        read_result: JSONData = {'ok': True}
        calls = _install_core_handler_stub(
            monkeypatch,
            read_result=read_result,
        )

        result = File(path, FileFormat.CSV).read()

        assert result == read_result
        assert calls['format'] is FileFormat.CSV
        assert calls['read_path'] == path

    @pytest.mark.parametrize(
        ('root_tag', 'expected_root_tag', 'write_result'),
        [
            pytest.param(
                None,
                xml_file.DEFAULT_XML_ROOT,
                1,
                id='default_root_tag',
            ),
            pytest.param(
                'records',
                'records',
                3,
                id='custom_root_tag',
            ),
        ],
    )
    def test_write_uses_class_based_handler_and_root_tag(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        root_tag: str | None,
        expected_root_tag: str,
        write_result: int,
    ) -> None:
        """Test write dispatch preserving XML ``root_tag`` in options."""
        path = tmp_path / 'export.xml'
        payload: JSONData = [{'name': 'Ada'}]
        calls = _install_core_handler_stub(
            monkeypatch,
            write_result=write_result,
        )

        file = File(path, FileFormat.XML)
        if root_tag is None:
            written = file.write(payload)
        else:
            written = file.write(payload, root_tag=root_tag)

        assert written == write_result
        _assert_core_write_dispatch(
            calls,
            expected_format=FileFormat.XML,
            expected_path=path,
            expected_data=payload,
            expected_root_tag=expected_root_tag,
        )
