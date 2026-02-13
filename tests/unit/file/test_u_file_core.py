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

# SECTION: HELPERS ========================================================== #


type FormatCase = tuple[FileFormat, str, JSONData, JSONData, tuple[str, ...]]


FORMAT_CASES: list[FormatCase] = [
    # Tabular & delimited text
    (
        FileFormat.CSV,
        'sample.csv',
        [{'name': 'Ada', 'age': '36'}],
        [{'name': 'Ada', 'age': '36'}],
        (),
    ),
    (
        FileFormat.DAT,
        'sample.dat',
        [{'name': 'Ada', 'age': '36'}],
        [{'name': 'Ada', 'age': '36'}],
        (),
    ),
    (
        FileFormat.FWF,
        'sample.fwf',
        [{'name': 'Ada', 'age': '36x'}],
        [{'name': 'Ada', 'age': '36x'}],
        ('pandas',),
    ),
    (
        FileFormat.PSV,
        'sample.psv',
        [{'name': 'Ada', 'age': '36'}],
        [{'name': 'Ada', 'age': '36'}],
        (),
    ),
    (
        FileFormat.TAB,
        'sample.tab',
        [{'name': 'Ada', 'age': '36'}],
        [{'name': 'Ada', 'age': '36'}],
        (),
    ),
    (
        FileFormat.TSV,
        'sample.tsv',
        [{'name': 'Ada', 'age': '36'}],
        [{'name': 'Ada', 'age': '36'}],
        (),
    ),
    # (
    #     FileFormat.TXT,
    #     'sample.txt',
    #     [{'name': 'Ada', 'age': '36'}],
    #     [{'name': 'Ada', 'age': '36'}],
    #     (),
    # ),
    (
        FileFormat.TXT,
        'sample.txt',
        [{'text': 'hello'}, {'text': 'world'}],
        [{'text': 'hello'}, {'text': 'world'}],
        (),
    ),
    (
        FileFormat.BSON,
        'sample.bson',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        ('bson',),
    ),
    (
        FileFormat.CBOR,
        'sample.cbor',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        ('cbor2',),
    ),
    (
        FileFormat.JSON,
        'sample.json',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        (),
    ),
    (
        FileFormat.MSGPACK,
        'sample.msgpack',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        ('msgpack',),
    ),
    (
        FileFormat.NDJSON,
        'sample.ndjson',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        (),
    ),
    (
        FileFormat.YAML,
        'sample.yaml',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        ('yaml',),
    ),
    (
        FileFormat.INI,
        'sample.ini',
        {'DEFAULT': {'name': 'Ada'}, 'main': {'age': '36'}},
        {'DEFAULT': {'name': 'Ada'}, 'main': {'age': '36'}},
        (),
    ),
    (
        FileFormat.TOML,
        'sample.toml',
        {'name': 'Ada', 'age': 36},
        {'name': 'Ada', 'age': 36},
        ('tomli_w',),
    ),
    (
        FileFormat.PROPERTIES,
        'sample.properties',
        {'name': 'Ada', 'age': '36'},
        {'name': 'Ada', 'age': '36'},
        (),
    ),
    (
        FileFormat.PROTO,
        'sample.proto',
        {'schema': 'syntax = "proto3";\nmessage Test { string name = 1; }\n'},
        {'schema': 'syntax = "proto3";\nmessage Test { string name = 1; }\n'},
        (),
    ),
    (
        FileFormat.PB,
        'sample.pb',
        {'payload_base64': 'aGVsbG8='},
        {'payload_base64': 'aGVsbG8='},
        (),
    ),
    (
        FileFormat.XML,
        'sample.xml',
        {'root': {'items': [{'text': 'one'}]}},
        {'root': {'items': [{'text': 'one'}]}},
        (),
    ),
    (
        FileFormat.GZ,
        'sample.json.gz',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        (),
    ),
    (
        FileFormat.ZIP,
        'sample.json.zip',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        (),
    ),
    (
        FileFormat.PARQUET,
        'sample.parquet',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        ('pandas', 'pyarrow'),
    ),
    (
        FileFormat.FEATHER,
        'sample.feather',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        ('pandas', 'pyarrow'),
    ),
    (
        FileFormat.ORC,
        'sample.orc',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        ('pandas', 'pyarrow'),
    ),
    (
        FileFormat.AVRO,
        'sample.avro',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        ('fastavro',),
    ),
    (
        FileFormat.ARROW,
        'sample.arrow',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        ('pyarrow',),
    ),
    (
        FileFormat.DUCKDB,
        'sample.duckdb',
        [{'name': 'Ada', 'age': '36'}],
        [{'name': 'Ada', 'age': '36'}],
        ('duckdb',),
    ),
    (
        FileFormat.ODS,
        'sample.ods',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        ('pandas', 'odf'),
    ),
    (
        FileFormat.XLSX,
        'sample.xlsx',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        ('pandas', 'openpyxl'),
    ),
    (
        FileFormat.SQLITE,
        'sample.sqlite',
        [{'name': 'Ada', 'age': '36'}],
        [{'name': 'Ada', 'age': '36'}],
        (),
    ),
    (
        FileFormat.DTA,
        'sample.dta',
        [{'name': 'Ada', 'age': '36'}],
        [{'name': 'Ada', 'age': '36'}],
        ('pandas', 'pyreadstat'),
    ),
    (
        FileFormat.SAV,
        'sample.sav',
        [{'name': 'Ada', 'age': '36'}],
        [{'name': 'Ada', 'age': '36'}],
        ('pandas', 'pyreadstat'),
    ),
    (
        FileFormat.XPT,
        'sample.xpt',
        [{'name': 'Ada', 'age': '36'}],
        [{'name': 'Ada', 'age': '36'}],
        ('pandas', 'pyreadstat'),
    ),
    (
        FileFormat.RDS,
        'sample.rds',
        [{'name': 'Ada', 'age': '36'}],
        [{'name': 'Ada', 'age': '36'}],
        ('pandas', 'pyreadr'),
    ),
    (
        FileFormat.RDA,
        'sample.rda',
        [{'name': 'Ada', 'age': '36'}],
        [{'name': 'Ada', 'age': '36'}],
        ('pandas', 'pyreadr'),
    ),
    (
        FileFormat.NC,
        'sample.nc',
        [{'name': 'Ada', 'age': '36'}],
        [{'name': 'Ada', 'age': '36'}],
        ('pandas', 'xarray', 'netCDF4'),
    ),
]

FORMAT_INFERENCE_CASES: tuple[tuple[str, FileFormat], ...] = (
    ('data.json', FileFormat.JSON),
    ('data.csv.gz', FileFormat.CSV),
    ('data.jsonl.gz', FileFormat.NDJSON),
)

STUBBED_FORMATS: tuple[tuple[FileFormat, str], ...] = (
    # Permanent stub as formality
    (FileFormat.STUB, 'data.stub'),
    # Temporary stubs until implemented
    (FileFormat.ACCDB, 'data.accdb'),
    (FileFormat.CFG, 'data.cfg'),
    (FileFormat.CONF, 'data.conf'),
    (FileFormat.ION, 'data.ion'),
    (FileFormat.MAT, 'data.mat'),
    (FileFormat.MDB, 'data.mdb'),
    # (FileFormat.MDF, 'data.mdf'),
    (FileFormat.PBF, 'data.pbf'),
    # (FileFormat.RAW, 'data.raw'),
    # (FileFormat.RTF, 'data.rtf'),
    # (FileFormat.SDF, 'data.sdf'),
    # (FileFormat.SLV, 'data.slv'),
    (FileFormat.SYLK, 'data.sylk'),
    # (FileFormat.VCF, 'data.vcf'),
    # (FileFormat.WSV, 'data.wsv'),
    (FileFormat.ZSAV, 'data.zsav'),
)


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
    options = cast(WriteOptions, calls['write_options'])
    assert options.root_tag == expected_root_tag


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


# SECTION: TESTS ============================================================ #


class TestFile:
    """Unit tests for :class:`etlplus.file.File`."""

    @pytest.mark.parametrize(
        ('file_format', 'filename'),
        [
            (FileFormat.DUCKDB, 'multi.duckdb'),
            (FileFormat.SQLITE, 'multi.sqlite'),
        ],
        ids=['duckdb', 'sqlite'],
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
        (('json', FileFormat.JSON), ('not-a-real-format', None)),
        ids=('valid_string_format', 'invalid_string_format'),
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

    def test_gz_round_trip_json(
        self,
        tmp_path: Path,
    ) -> None:
        """Test JSON round-trip inside a gzip archive."""
        path = tmp_path / 'data.json.gz'
        payload = [{'name': 'Ada'}]

        File(path, FileFormat.GZ).write(payload)
        result = File(path, FileFormat.GZ).read()

        assert result == payload

    @pytest.mark.parametrize(
        ('filename', 'expected_format'),
        FORMAT_INFERENCE_CASES,
        ids=('json', 'csv_gz', 'jsonl_gz'),
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
        csv_path = tmp_path / 'data.csv'
        json_path = tmp_path / 'data.json'
        xml_path = tmp_path / 'data.xml'

        csv_file.CsvFile().write(csv_path, [{'name': 'Ada'}])
        assert csv_file.CsvFile().read(csv_path) == [{'name': 'Ada'}]

        json_file.JsonFile().write(json_path, {'name': 'Ada'})
        assert json_file.JsonFile().read(json_path) == {'name': 'Ada'}

        xml_file.XmlFile().write(
            xml_path,
            {'root': {'text': 'hello'}},
            options=WriteOptions(root_tag='root'),
        )
        assert xml_file.XmlFile().read(xml_path) == {'root': {'text': 'hello'}}

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
        ('filename', 'contents', 'error_pattern'),
        [
            ('data.gz', 'compressed', 'compressed file'),
            ('weird.data', '{}', 'Cannot infer file format'),
        ],
        ids=['compression_only_suffix', 'unknown_extension'],
    )
    def test_read_unknown_formats_defer_error(
        self,
        tmp_path: Path,
        filename: str,
        contents: str,
        error_pattern: str,
    ) -> None:
        """Test unresolved formats deferring failure until read dispatch."""
        path = tmp_path / filename
        path.write_text(contents, encoding='utf-8')
        file = File(path)

        assert file.file_format is None
        with pytest.raises(ValueError, match=error_pattern):
            file.read()

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
    def test_round_trip_by_format(
        self,
        tmp_path: Path,
        file_format: FileFormat,
        filename: str,
        payload: JSONData,
        expected: JSONData,
        requires: tuple[str, ...],
    ) -> None:
        """Test round-trip reads and writes across file formats."""
        for module in requires:
            pytest.importorskip(module)
        path = tmp_path / filename

        File(path, file_format).write(payload)
        try:
            result = File(path, file_format).read()
        except OSError as err:
            if file_format is FileFormat.ORC and 'sysctlbyname' in str(err):
                pytest.skip('ORC read failed due to sysctl limitations')
            raise

        expected_result = expected
        if file_format is FileFormat.XML:
            result = normalize_xml_payload(result)
            expected_result = normalize_xml_payload(expected_result)
        if file_format is FileFormat.XLS:
            result = normalize_numeric_records(result)
        assert result == expected_result

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
    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_stub_formats_raise_on_operations(
        self,
        tmp_path: Path,
        file_format: FileFormat,
        filename: str,
        operation: str,
    ) -> None:
        """Test stub formats raising NotImplementedError on read/write."""
        path = tmp_path / filename
        if operation == 'read':
            path.write_text('stub', encoding='utf-8')

        with pytest.raises(NotImplementedError):
            if operation == 'read':
                File(path, file_format).read()
            else:
                File(path, file_format).write({'stub': True})

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

    def test_write_unknown_extension_without_format_raises(
        self,
        tmp_path: Path,
    ) -> None:
        """Test write fails when format cannot be inferred."""
        path = tmp_path / 'output.unknown'
        file = File(path)

        with pytest.raises(ValueError, match='Cannot infer file format'):
            file.write({'ok': True})

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

    def test_xml_round_trip_with_attributes(
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

    def test_xml_write_uses_default_root_tag_when_not_provided(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test default XML root tag routing in class-based dispatch."""
        path = tmp_path / 'export.xml'
        calls = _install_core_handler_stub(monkeypatch, write_result=1)

        written = File(path, FileFormat.XML).write([{'name': 'Ada'}])

        assert written == 1
        _assert_core_write_dispatch(
            calls,
            expected_format=FileFormat.XML,
            expected_path=path,
            expected_data=[{'name': 'Ada'}],
            expected_root_tag=xml_file.DEFAULT_XML_ROOT,
        )

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

    def test_write_uses_class_based_handler_and_root_tag(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test write dispatch preserving XML ``root_tag`` in options."""
        path = tmp_path / 'export.xml'
        payload: JSONData = [{'name': 'Ada'}]
        calls = _install_core_handler_stub(monkeypatch, write_result=3)

        written = File(path, FileFormat.XML).write(payload, root_tag='records')

        assert written == 3
        _assert_core_write_dispatch(
            calls,
            expected_format=FileFormat.XML,
            expected_path=path,
            expected_data=payload,
            expected_root_tag='records',
        )
