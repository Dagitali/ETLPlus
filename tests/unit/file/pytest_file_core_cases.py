"""
:mod:`tests.unit.file.pytest_file_core_cases` module.

Shared case tables for :mod:`tests.unit.file.test_u_file_core`.
"""

from __future__ import annotations

from copy import deepcopy

from etlplus.file import FileFormat
from etlplus.types import JSONData

# SECTION: TYPES ============================================================ #


type FormatCase = tuple[FileFormat, str, JSONData, JSONData, tuple[str, ...]]


# SECTION: CONSTANTS ======================================================== #


PYARROW_DEPS = ('pandas', 'pyarrow')
PYREADR_DEPS = ('pandas', 'pyreadr')
PYREADSTAT_DEPS = ('pandas', 'pyreadstat')

COMMON_ROWS_STR: JSONData = [{'name': 'Ada', 'age': '36'}]
COMMON_ROWS_NUM: JSONData = [{'name': 'Ada', 'age': 36}]
COMMON_ROWS_TXT: JSONData = [{'text': 'hello'}, {'text': 'world'}]
COMMON_INI: JSONData = {'DEFAULT': {'name': 'Ada'}, 'main': {'age': '36'}}
COMMON_TOML: JSONData = {'name': 'Ada', 'age': 36}
COMMON_PROPERTIES: JSONData = {'name': 'Ada', 'age': '36'}
COMMON_PROTO: JSONData = {
    'schema': 'syntax = "proto3";\nmessage Test { string name = 1; }\n',
}
COMMON_PB: JSONData = {'payload_base64': 'aGVsbG8='}
COMMON_XML: JSONData = {'root': {'items': [{'text': 'one'}]}}
EMBEDDED_DB_MULTI_TABLE_CASES = (
    (FileFormat.DUCKDB, 'multi.duckdb'),
    (FileFormat.SQLITE, 'multi.sqlite'),
)
EMBEDDED_DB_MULTI_TABLE_CASE_IDS = ('duckdb', 'sqlite')
EXPLICIT_STRING_FORMAT_CASES = (
    ('json', FileFormat.JSON),
    ('not-a-real-format', None),
)
EXPLICIT_STRING_FORMAT_CASE_IDS = (
    'valid_string_format',
    'invalid_string_format',
)
FORMAT_INFERENCE_CASES = (
    ('data.json', FileFormat.JSON),
    ('data.csv.gz', FileFormat.CSV),
    ('data.jsonl.gz', FileFormat.NDJSON),
)
FORMAT_INFERENCE_CASE_IDS = ('json', 'csv_gz', 'jsonl_gz')
UNKNOWN_FORMAT_CASES = (
    ('data.gz', 'compressed', 'read', 'compressed file'),
    ('weird.data', '{}', 'read', 'Cannot infer file format'),
    ('output.unknown', None, 'write', 'Cannot infer file format'),
)
UNKNOWN_FORMAT_CASE_IDS = (
    'compression_only_suffix',
    'read_unknown_extension',
    'write_unknown',
)
STUB_OPERATION_CASES = ('read', 'write')
XML_ROUNDTRIP_NORMALIZED_FORMATS = frozenset({FileFormat.XML})
NUMERIC_ROUNDTRIP_NORMALIZED_FORMATS = frozenset({FileFormat.XLS})


# SECTION: FUNCTIONS ======================================================== #


def _format_case(
    file_format: FileFormat,
    filename: str,
    payload: JSONData,
    requires: tuple[str, ...] = (),
    *,
    expected: JSONData | None = None,
) -> FormatCase:
    """Create one roundtrip format case with defensive payload copies."""
    expected_payload = payload if expected is None else expected
    return (
        file_format,
        filename,
        deepcopy(payload),
        deepcopy(expected_payload),
        requires,
    )


# SECTION: CASE TABLES ====================================================== #


FORMAT_CASES: list[FormatCase] = [
    # Tabular & delimited text
    _format_case(FileFormat.CSV, 'sample.csv', COMMON_ROWS_STR),
    _format_case(FileFormat.DAT, 'sample.dat', COMMON_ROWS_STR),
    _format_case(
        FileFormat.FWF,
        'sample.fwf',
        [{'name': 'Ada', 'age': '36x'}],
        ('pandas',),
    ),
    _format_case(FileFormat.PSV, 'sample.psv', COMMON_ROWS_STR),
    _format_case(FileFormat.TAB, 'sample.tab', COMMON_ROWS_STR),
    _format_case(FileFormat.TSV, 'sample.tsv', COMMON_ROWS_STR),
    _format_case(FileFormat.TXT, 'sample.txt', COMMON_ROWS_TXT),
    # Semi-structured and interchange
    _format_case(FileFormat.BSON, 'sample.bson', COMMON_ROWS_NUM, ('bson',)),
    _format_case(FileFormat.CBOR, 'sample.cbor', COMMON_ROWS_NUM, ('cbor2',)),
    _format_case(FileFormat.JSON, 'sample.json', COMMON_ROWS_NUM),
    _format_case(
        FileFormat.MSGPACK,
        'sample.msgpack',
        COMMON_ROWS_NUM,
        ('msgpack',),
    ),
    _format_case(FileFormat.NDJSON, 'sample.ndjson', COMMON_ROWS_NUM),
    _format_case(FileFormat.YAML, 'sample.yaml', COMMON_ROWS_NUM, ('yaml',)),
    _format_case(FileFormat.INI, 'sample.ini', COMMON_INI),
    _format_case(FileFormat.TOML, 'sample.toml', COMMON_TOML, ('tomli_w',)),
    _format_case(
        FileFormat.PROPERTIES,
        'sample.properties',
        COMMON_PROPERTIES,
    ),
    _format_case(FileFormat.PROTO, 'sample.proto', COMMON_PROTO),
    _format_case(FileFormat.PB, 'sample.pb', COMMON_PB),
    _format_case(FileFormat.XML, 'sample.xml', COMMON_XML),
    # Compression wrappers
    _format_case(FileFormat.GZ, 'sample.json.gz', COMMON_ROWS_NUM),
    _format_case(FileFormat.ZIP, 'sample.json.zip', COMMON_ROWS_NUM),
    # Columnar
    _format_case(
        FileFormat.PARQUET,
        'sample.parquet',
        COMMON_ROWS_NUM,
        PYARROW_DEPS,
    ),
    _format_case(
        FileFormat.FEATHER,
        'sample.feather',
        COMMON_ROWS_NUM,
        PYARROW_DEPS,
    ),
    _format_case(
        FileFormat.ORC,
        'sample.orc',
        COMMON_ROWS_NUM,
        PYARROW_DEPS,
    ),
    _format_case(
        FileFormat.AVRO,
        'sample.avro',
        COMMON_ROWS_NUM,
        ('fastavro',),
    ),
    _format_case(
        FileFormat.ARROW,
        'sample.arrow',
        COMMON_ROWS_NUM,
        ('pyarrow',),
    ),
    # Databases / spreadsheets
    _format_case(
        FileFormat.DUCKDB,
        'sample.duckdb',
        COMMON_ROWS_STR,
        ('duckdb',),
    ),
    _format_case(
        FileFormat.ODS,
        'sample.ods',
        COMMON_ROWS_NUM,
        ('pandas', 'odf'),
    ),
    _format_case(
        FileFormat.XLSX,
        'sample.xlsx',
        COMMON_ROWS_NUM,
        ('pandas', 'openpyxl'),
    ),
    _format_case(FileFormat.SQLITE, 'sample.sqlite', COMMON_ROWS_STR),
    # Scientific/statistical
    _format_case(
        FileFormat.DTA,
        'sample.dta',
        COMMON_ROWS_STR,
        PYREADSTAT_DEPS,
    ),
    _format_case(
        FileFormat.SAV,
        'sample.sav',
        COMMON_ROWS_STR,
        PYREADSTAT_DEPS,
    ),
    _format_case(
        FileFormat.XPT,
        'sample.xpt',
        COMMON_ROWS_STR,
        PYREADSTAT_DEPS,
    ),
    _format_case(
        FileFormat.RDS,
        'sample.rds',
        COMMON_ROWS_STR,
        PYREADR_DEPS,
    ),
    _format_case(
        FileFormat.RDA,
        'sample.rda',
        COMMON_ROWS_STR,
        PYREADR_DEPS,
    ),
    _format_case(
        FileFormat.NC,
        'sample.nc',
        COMMON_ROWS_STR,
        ('pandas', 'xarray', 'netCDF4'),
    ),
]

STUBBED_FORMATS = (
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
