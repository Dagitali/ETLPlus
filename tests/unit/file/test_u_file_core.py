"""
:mod:`tests.unit.file.test_u_file_core` module.

Unit tests for :mod:`etlplus.file.core`.
"""

from __future__ import annotations

import math
import numbers
import sqlite3
import zipfile
from os import PathLike
from pathlib import Path
from pathlib import PurePath
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

STUBBED_FORMATS: tuple[tuple[FileFormat, str], ...] = (
    # Permanent stub as formality
    (FileFormat.STUB, 'data.stub'),
    # Temporary stubs until implemented
    (FileFormat.ACCDB, 'data.accdb'),
    (FileFormat.CFG, 'data.cfg'),
    (FileFormat.CONF, 'data.conf'),
    (FileFormat.ION, 'data.ion'),
    (FileFormat.JINJA2, 'data.jinja2'),
    (FileFormat.LOG, 'data.log'),
    (FileFormat.MAT, 'data.mat'),
    (FileFormat.MDB, 'data.mdb'),
    # (FileFormat.MDF, 'data.mdf'),
    (FileFormat.MUSTACHE, 'data.mustache'),
    (FileFormat.PBF, 'data.pbf'),
    # (FileFormat.RAW, 'data.raw'),
    # (FileFormat.RTF, 'data.rtf'),
    # (FileFormat.SDF, 'data.sdf'),
    # (FileFormat.SLV, 'data.slv'),
    (FileFormat.SYLK, 'data.sylk'),
    # (FileFormat.VCF, 'data.vcf'),
    (FileFormat.VM, 'data.vm'),
    # (FileFormat.WSV, 'data.wsv'),
    (FileFormat.ZSAV, 'data.zsav'),
)


type FormatCase = tuple[FileFormat, str, JSONData, JSONData, tuple[str, ...]]


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


def require_optional_modules(*modules: str) -> None:
    """Skip the test when optional dependencies are missing."""
    for module in modules:
        pytest.importorskip(module)


class FileCoreDispatchContract:
    """Reusable contract suite for class-based core dispatch in ``File``."""

    file_cls: type[Any]
    core_module: object
    read_format: FileFormat = FileFormat.CSV
    write_format: FileFormat = FileFormat.XML
    read_filename: str = 'sample.csv'
    write_filename: str = 'export.xml'
    read_result: JSONData = {'ok': True}
    write_payload: JSONData = [{'name': 'Ada'}]
    write_root_tag: str = 'records'
    write_result: int = 3

    def _install_handler_stub(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        read_result: JSONData | None = None,
        write_result: int = 0,
    ) -> dict[str, object]:
        """
        Install a configurable core handler stub and return call metadata.
        """
        calls: dict[str, object] = {}

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

        def _get_handler(file_format: FileFormat) -> object:
            calls['format'] = file_format
            return handler

        monkeypatch.setattr(self.core_module, 'get_handler', _get_handler)
        return calls

    def test_read_uses_class_based_handler_dispatch(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test read dispatch through ``core.get_handler`` handlers."""
        path = tmp_path / self.read_filename
        path.write_text('name\nAda\n', encoding='utf-8')
        calls = self._install_handler_stub(
            monkeypatch,
            read_result=self.read_result,
        )

        result = self.file_cls(path, self.read_format).read()

        assert result == self.read_result
        assert calls['format'] is self.read_format
        assert calls['read_path'] == path

    def test_write_uses_class_based_handler_and_root_tag(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test write dispatch preserving XML ``root_tag`` in options."""
        path = tmp_path / self.write_filename
        calls = self._install_handler_stub(
            monkeypatch,
            write_result=self.write_result,
        )

        written = self.file_cls(path, self.write_format).write(
            self.write_payload,
            root_tag=self.write_root_tag,
        )

        assert written == self.write_result
        assert calls['format'] is self.write_format
        assert calls['write_path'] == path
        assert calls['write_data'] == self.write_payload
        assert isinstance(calls['write_options'], WriteOptions)
        options = cast(WriteOptions, calls['write_options'])
        assert options.root_tag == self.write_root_tag


class _DummyPath(PathLike[str]):
    """Simple PathLike wrapper for StrPath tests."""

    def __init__(self, path: str) -> None:
        self._path = path

    def __fspath__(self) -> str:
        return self._path


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='stubbed_formats')
def stubbed_formats_fixture() -> list[tuple[FileFormat, str]]:
    """Return a list of stubbed file formats for testing."""
    return list(STUBBED_FORMATS)


# SECTION: TESTS ============================================================ #


class TestFile:
    """
    Unit tests for :class:`etlplus.file.File`.

    Notes
    -----
    - Exercises JSON detection and defers errors for unknown extensions.
    """

    def test_compression_only_extension_defers_error(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test compression-only file extension handling and error deferral.
        """
        p = tmp_path / 'data.gz'
        p.write_text('compressed', encoding='utf-8')

        f = File(p)

        assert f.file_format is None
        with pytest.raises(ValueError) as e:
            f.read()
        assert 'compressed file' in str(e.value)

    def test_duckdb_read_fails_with_multiple_tables(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test DuckDB reader rejects multiple table databases.
        """
        duckdb = pytest.importorskip('duckdb')
        path = tmp_path / 'multi.duckdb'
        conn = duckdb.connect(str(path))
        try:
            conn.execute('CREATE TABLE one (id INTEGER)')
            conn.execute('CREATE TABLE two (id INTEGER)')
        finally:
            conn.close()

        with pytest.raises(ValueError, match='Multiple tables'):
            File(path, FileFormat.DUCKDB).read()

    def test_explicit_string_file_format_is_coerced(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that string file-format arguments are normalized."""
        path = tmp_path / 'data.json'
        file = File(path, cast(Any, 'json'))

        assert file.file_format is FileFormat.JSON

    def test_gz_round_trip_json(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test JSON round-trip inside a gzip archive.
        """
        path = tmp_path / 'data.json.gz'
        payload = [{'name': 'Ada'}]

        File(path, FileFormat.GZ).write(payload)
        result = File(path, FileFormat.GZ).read()

        assert result == payload

    @pytest.mark.parametrize(
        'filename,expected_format',
        [
            ('data.csv.gz', FileFormat.CSV),
            ('data.jsonl.gz', FileFormat.NDJSON),
        ],
    )
    def test_infers_format_from_compressed_suffixes(
        self,
        tmp_path: Path,
        filename: str,
        expected_format: FileFormat,
    ) -> None:
        """
        Test format inference from multi-suffix compressed filenames.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory path.
        filename : str
            Name of the file to create.
        expected_format : FileFormat
            Expected file format.
        """
        p = tmp_path / filename
        p.write_text('{}', encoding='utf-8')

        f = File(p)

        assert f.file_format == expected_format

    def test_infers_json_from_extension(self, tmp_path: Path) -> None:
        """Test JSON file inference from extension and read behavior."""
        p = tmp_path / 'data.json'
        p.write_text('{}', encoding='utf-8')
        f = File(p)
        assert f.file_format == FileFormat.JSON
        assert f.read() == {}

    def test_invalid_explicit_string_file_format_raises(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that invalid explicit format strings fail fast."""
        path = tmp_path / 'data.json'

        with pytest.raises(ValueError):
            File(path, cast(Any, 'not-a-real-format'))

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
        require_optional_modules(*requires)
        path = tmp_path / filename

        File(path, file_format).write(payload)
        try:
            result = File(path, file_format).read()
        except OSError as err:
            if file_format is FileFormat.ORC and 'sysctlbyname' in str(err):
                pytest.skip('ORC read failed due to sysctl limitations')
            raise

        if file_format is FileFormat.XML:
            result = normalize_xml_payload(result)
            expected = normalize_xml_payload(expected)
        if file_format is FileFormat.XLS:
            result = normalize_numeric_records(result)
        assert result == expected

    def test_sqlite_read_fails_with_multiple_tables(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test SQLite reader rejects multiple table databases.
        """
        path = tmp_path / 'multi.sqlite'
        conn = sqlite3.connect(path)
        try:
            conn.execute('CREATE TABLE one (id INTEGER)')
            conn.execute('CREATE TABLE two (id INTEGER)')
            conn.commit()
        finally:
            conn.close()

        with pytest.raises(ValueError, match='Multiple tables'):
            File(path, FileFormat.SQLITE).read()

    def test_strpath_support_for_module_helpers(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test module helpers accept ``StrPath`` inputs.

        Uses ``str`` and ``PathLike`` inputs to validate normalization.
        """
        csv_path = tmp_path / 'data.csv'
        json_path = tmp_path / 'data.json'
        xml_path = tmp_path / 'data.xml'

        csv_file.write(str(csv_path), [{'name': 'Ada'}])
        assert csv_file.read(str(csv_path)) == [{'name': 'Ada'}]

        json_file.write(_DummyPath(str(json_path)), {'name': 'Ada'})
        assert json_file.read(_DummyPath(str(json_path))) == {'name': 'Ada'}

        xml_file.write(
            PurePath(xml_path),
            {'root': {'text': 'hello'}},
            root_tag='root',
        )
        assert xml_file.read(PurePath(xml_path)) == {'root': {'text': 'hello'}}

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_stub_formats_raise_on_operations(
        self,
        tmp_path: Path,
        stubbed_formats: list[tuple[FileFormat, str]],
        operation: str,
    ) -> None:
        """Test stub formats raising NotImplementedError on read/write."""
        if not stubbed_formats:
            pytest.skip('No stubbed formats to test')
        for file_format, filename in stubbed_formats:
            path = tmp_path / filename
            if operation == 'read':
                path.write_text('stub', encoding='utf-8')

            with pytest.raises(NotImplementedError):
                if operation == 'read':
                    File(path, file_format).read()
                else:
                    File(path, file_format).write({'stub': True})

    def test_unknown_extension_defers_error(self, tmp_path: Path) -> None:
        """Test unknown extension sets ``None`` format and defers error."""
        p = tmp_path / 'weird.data'
        p.write_text('{}', encoding='utf-8')
        f = File(p)
        assert f.file_format is None
        with pytest.raises(ValueError) as e:
            f.read()
        assert 'Cannot infer file format' in str(e.value)

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
        calls: dict[str, object] = {}

        class _StubHandler:
            def read(
                self,
                path: Path,
            ) -> JSONData:
                _ = path
                return []

            def write(
                self,
                path: Path,
                data: JSONData,
                *,
                options: WriteOptions | None = None,
            ) -> int:
                calls['path'] = path
                calls['data'] = data
                calls['options'] = options
                return 1

        def _get_handler(file_format: FileFormat) -> _StubHandler:
            assert file_format is FileFormat.XML
            return _StubHandler()

        monkeypatch.setattr(core_mod, 'get_handler', _get_handler)

        written = File(path, FileFormat.XML).write([{'name': 'Ada'}])

        assert written == 1
        options = cast(WriteOptions, calls['options'])
        assert options.root_tag == xml_file.DEFAULT_XML_ROOT

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


class TestFileCoreDispatch(FileCoreDispatchContract):
    """
    Contract tests for class-based dispatch in :class:`etlplus.file.File`.
    """

    file_cls = File
    core_module = core_mod
