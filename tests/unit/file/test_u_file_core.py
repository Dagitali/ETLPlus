"""
:mod:`tests.unit.test_u_file_core` module.

Unit tests for :mod:`etlplus.file.core`.

Notes
-----
- Uses ``tmp_path`` for filesystem isolation.
- Exercises JSON detection and defers errors for unknown extensions.
"""

from __future__ import annotations

import math
import numbers
from pathlib import Path
from typing import cast

import pytest

from etlplus.file import File
from etlplus.file import FileFormat
from etlplus.types import JSONData
from etlplus.types import JSONDict

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


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
    # (
    #     FileFormat.DAT,
    #     'sample.dat',
    #     [{'name': 'Ada', 'age': '36'}],
    #     [{'name': 'Ada', 'age': '36'}],
    #     (),
    # ),
    # (
    #     FileFormat.FWF,
    #     'sample.fwf',
    #     [{'name': 'Ada', 'age': '36'}],
    #     [{'name': 'Ada', 'age': '36'}],
    #     (),
    # ),
    # (
    #     FileFormat.PSV,
    #     'sample.psv',
    #     [{'name': 'Ada', 'age': '36'}],
    #     [{'name': 'Ada', 'age': '36'}],
    #     (),
    # ),
    # (
    #     FileFormat.TAB,
    #     'sample.tab',
    #     [{'name': 'Ada', 'age': '36'}],
    #     [{'name': 'Ada', 'age': '36'}],
    #     (),
    # ),
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
        FileFormat.JSON,
        'sample.json',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        (),
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
        FileFormat.XLSX,
        'sample.xlsx',
        [{'name': 'Ada', 'age': 36}],
        [{'name': 'Ada', 'age': 36}],
        ('pandas', 'openpyxl'),
    ),
]


def _coerce_numeric_value(
    value: object,
) -> object:
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


def _normalize_numeric_records(
    records: JSONData,
) -> JSONData:
    """Normalize numeric record values (e.g., floats to ints when integral)."""
    if isinstance(records, list):
        normalized: list[JSONDict] = []
        for row in records:
            if not isinstance(row, dict):
                normalized.append(row)
                continue
            cleaned: JSONDict = {}
            for key, value in row.items():
                cleaned[key] = _coerce_numeric_value(value)
            normalized.append(cleaned)
        return normalized
    return records


def _normalize_xml_payload(
    payload: JSONData,
) -> JSONData:
    """Normalize XML payloads to list-based item structures when possible."""
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


def _require_modules(
    modules: tuple[str, ...],
) -> None:
    """Skip the test when optional dependencies are missing."""
    for module in modules:
        pytest.importorskip(module)


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='stubbed_formats')
def stubbed_formats_fixture() -> list[tuple[FileFormat, str]]:
    """Return a list of stubbed file formats for testing."""
    return [
        # Permanent stub as formality
        (FileFormat.STUB, 'data.stub'),
        # Temporary stubs until implemented
        (FileFormat.ACCDB, 'data.accdb'),
        (FileFormat.BSON, 'data.bson'),
        (FileFormat.CFG, 'data.cfg'),
        (FileFormat.CBOR, 'data.cbor'),
        (FileFormat.CONF, 'data.conf'),
        (FileFormat.DAT, 'data.dat'),
        (FileFormat.DTA, 'data.dta'),
        (FileFormat.DUCKDB, 'data.duckdb'),
        (FileFormat.FWF, 'data.fwf'),
        (FileFormat.HDF5, 'data.hdf5'),
        (FileFormat.INI, 'data.ini'),
        (FileFormat.ION, 'data.ion'),
        (FileFormat.JINJA2, 'data.jinja2'),
        (FileFormat.LOG, 'data.log'),
        (FileFormat.MAT, 'data.mat'),
        (FileFormat.MDB, 'data.mdb'),
        # (FileFormat.MDF, 'data.mdf'),
        (FileFormat.MSGPACK, 'data.msgpack'),
        (FileFormat.MUSTACHE, 'data.mustache'),
        (FileFormat.NC, 'data.nc'),
        (FileFormat.PB, 'data.pb'),
        (FileFormat.PBF, 'data.pbf'),
        (FileFormat.PROPERTIES, 'data.properties'),
        (FileFormat.PSV, 'data.psv'),
        (FileFormat.PROTO, 'data.proto'),
        # (FileFormat.RAW, 'data.raw'),
        (FileFormat.RDA, 'data.rda'),
        (FileFormat.RDS, 'data.rds'),
        # (FileFormat.RTF, 'data.rtf'),
        # (FileFormat.SDF, 'data.sdf'),
        # (FileFormat.SLV, 'data.slv'),
        (FileFormat.SAS7BDAT, 'data.sas7bdat'),
        (FileFormat.SAV, 'data.sav'),
        (FileFormat.SYLK, 'data.sylk'),
        (FileFormat.SQLITE, 'data.sqlite'),
        (FileFormat.TAB, 'data.tab'),
        (FileFormat.TOML, 'data.toml'),
        # (FileFormat.VCF, 'data.vcf'),
        (FileFormat.VM, 'data.vm'),
        # (FileFormat.WSV, 'data.wsv'),
        (FileFormat.XPT, 'data.xpt'),
        (FileFormat.ZSAV, 'data.zsav'),
    ]


# SECTION: TESTS ============================================================ #


class TestFile:
    """
    Unit test suite for :class:`etlplus.file.File`.

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

    @pytest.mark.parametrize(
        'filename,expected_format,expected_content',
        [
            ('data.json', FileFormat.JSON, {}),
        ],
    )
    def test_infers_json_from_extension(
        self,
        tmp_path: Path,
        filename: str,
        expected_format: FileFormat,
        expected_content: dict[str, object],
    ) -> None:
        """
        Test JSON file inference from extension.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory path.
        filename : str
            Name of the file to create.
        expected_format : FileFormat
            Expected file format.
        expected_content : dict[str, object]
            Expected content after reading the file.
        """
        p = tmp_path / filename
        p.write_text('{}', encoding='utf-8')
        f = File(p)
        assert f.file_format == expected_format
        assert f.read() == expected_content

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
        _require_modules(requires)
        path = tmp_path / filename

        File(path, file_format).write(payload)
        result = File(path, file_format).read()

        if file_format is FileFormat.XML:
            result = _normalize_xml_payload(result)
            expected = _normalize_xml_payload(expected)
        if file_format is FileFormat.XLS:
            result = _normalize_numeric_records(result)
        assert result == expected

    def test_stub_formats_raise_on_read(
        self,
        tmp_path: Path,
        stubbed_formats: list[tuple[FileFormat, str]],
    ) -> None:
        """Test stub formats raising NotImplementedError on read."""
        if not stubbed_formats:
            pytest.skip('No stubbed formats to test')
        for file_format, filename in stubbed_formats:
            path = tmp_path / filename
            path.write_text('stub', encoding='utf-8')

            with pytest.raises(NotImplementedError):
                File(path, file_format).read()

    def test_stub_formats_raise_on_write(
        self,
        tmp_path: Path,
        stubbed_formats: list[tuple[FileFormat, str]],
    ) -> None:
        """Test stub formats raising NotImplementedError on write."""
        if not stubbed_formats:
            pytest.skip('No stubbed formats to test')
        for file_format, filename in stubbed_formats:
            path = tmp_path / filename

            with pytest.raises(NotImplementedError):
                File(path, file_format).write({'stub': True})

    @pytest.mark.parametrize(
        'filename,expected_format',
        [
            ('weird.data', None),
        ],
    )
    def test_unknown_extension_defers_error(
        self,
        tmp_path: Path,
        filename: str,
        expected_format: FileFormat | None,
    ) -> None:
        """
        Test unknown file extension handling and error deferral.

        Ensures :class:`FileFormat` is None and reading raises
        :class:`ValueError`.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory path.
        filename : str
            Name of the file to create.
        expected_format : FileFormat | None
            Expected file format (should be None).
        """
        p = tmp_path / filename
        p.write_text('{}', encoding='utf-8')
        f = File(p)
        assert f.file_format is expected_format
        with pytest.raises(ValueError) as e:
            f.read()
        assert 'Cannot infer file format' in str(e.value)

    def test_write_csv_filters_non_dicts(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test non-dict entries being ignored when writing CSV rows.
        """
        path = tmp_path / 'data.csv'
        invalid_entry = cast(dict[str, object], 'invalid')
        count = File(path, FileFormat.CSV).write(
            [{'name': 'John'}, invalid_entry],
        )

        assert count == 1
        assert 'name' in path.read_text(encoding='utf-8')

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
        """Test XLS writes raise a clear error."""
        pytest.importorskip('pandas')
        path = tmp_path / 'sample.xls'

        with pytest.raises(RuntimeError, match='XLS write is not supported'):
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
