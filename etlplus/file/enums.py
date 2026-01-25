"""
:mod:`etlplus.file.enums` module.

File-specific enums and helpers.
"""

from __future__ import annotations

from pathlib import PurePath

from ..enums import CoercibleStrEnum
from ..types import StrStrMap

# SECTION: EXPORTS ========================================================= #

__all__ = [
    'CompressionFormat',
    'FileFormat',
    'infer_file_format_and_compression',
]


# SECTION: ENUMS ============================================================ #


class CompressionFormat(CoercibleStrEnum):
    """Supported compression formats."""

    # -- Constants -- #

    GZ = 'gz'
    ZIP = 'zip'

    # -- Class Methods -- #

    @classmethod
    def aliases(cls) -> StrStrMap:
        """
        Return a mapping of common aliases for each enum member.

        Returns
        -------
        StrStrMap
            A mapping of alias names to their corresponding enum member names.
        """
        return {
            # File extensions
            '.gz': 'gz',
            '.gzip': 'gz',
            '.zip': 'zip',
            # MIME types
            'application/gzip': 'gz',
            'application/x-gzip': 'gz',
            'application/zip': 'zip',
            'application/x-zip-compressed': 'zip',
        }


class FileFormat(CoercibleStrEnum):
    """Supported file formats for extraction."""

    # -- Constants -- #

    # Stubbed / placeholder
    STUB = 'stub'  # Placeholder format for tests & future connectors

    # Tabular & delimited text
    CSV = 'csv'  # Comma-Separated Values
    DAT = 'dat'  # Generic data file, often delimited or fixed-width
    FWF = 'fwf'  # Fixed-Width Formatted
    PSV = 'psv'  # Pipe-Separated Values
    TAB = 'tab'  # Often synonymous with TSV
    TSV = 'tsv'  # Tab-Separated Values
    TXT = 'txt'  # Plain text, often delimited or fixed-width

    # Semi-structured text
    CFG = 'cfg'  # Config-style key-value pairs
    CONF = 'conf'  # Config-style key-value pairs
    INI = 'ini'  # INI-style key-value pairs
    JSON = 'json'  # JavaScript Object Notation
    NDJSON = 'ndjson'  # Newline-Delimited JSON
    PROPERTIES = 'properties'  # Java-style key-value pairs
    TOML = 'toml'  # Tom's Obvious Minimal Language
    XML = 'xml'  # Extensible Markup Language
    YAML = 'yaml'  # YAML Ain't Markup Language

    # Columnar / analytics-friendly
    ARROW = 'arrow'  # Apache Arrow IPC
    FEATHER = 'feather'  # Apache Arrow Feather
    ORC = 'orc'  # Optimized Row Columnar; common in Hadoop
    PARQUET = 'parquet'  # Apache Parquet; common in Big Data

    # Binary serialization & interchange
    AVRO = 'avro'  # Apache Avro
    BSON = 'bson'  # Binary JSON; common with MongoDB exports/dumps
    CBOR = 'cbor'  # Concise Binary Object Representation
    ION = 'ion'  # Amazon Ion
    MSGPACK = 'msgpack'  # MessagePack
    PB = 'pb'  # Protocol Buffers (Google Protobuf)
    PBF = 'pbf'  # Protocolbuffer Binary Format; often for GIS data
    PROTO = 'proto'  # Protocol Buffers schema; often in .pb / .bin

    # Databases & embedded storage
    ACCDB = 'accdb'  # Microsoft Access database file (newer format)
    DUCKDB = 'duckdb'  # DuckDB database file
    MDB = 'mdb'  # Microsoft Access database file (older format)
    SQLITE = 'sqlite'  # SQLite database file

    # Spreadsheets
    NUMBERS = 'numbers'  # Apple Numbers spreadsheet
    ODS = 'ods'  # OpenDocument spreadsheet
    WKS = 'wks'  # Lotus 1-2-3 spreadsheet
    XLS = 'xls'  # Microsoft Excel (BIFF); read-only
    XLSM = 'xlsm'  # Microsoft Excel Macro-Enabled (Open XML)
    XLSX = 'xlsx'  # Microsoft Excel (Open XML)

    # Statistical / scientific / numeric computing
    DTA = 'dta'  # Stata data file
    HDF5 = 'hdf5'  # Hierarchical Data Format
    MAT = 'mat'  # MATLAB data file
    NC = 'nc'  # NetCDF data file
    RDA = 'rda'  # RData workspace/object bundle
    RDS = 'rds'  # R data file
    SAS7BDAT = 'sas7bdat'  # SAS data file
    SAV = 'sav'  # SPSS data file
    SYLK = 'sylk'  # Symbolic Link
    XPT = 'xpt'  # SAS Transport file
    ZSAV = 'zsav'  # Compressed SPSS data file

    # Time series and financial data
    CAMT = 'camt'  # ISO 20022 Cash Management messages
    FXT = 'fxt'  # Forex time series data
    MT940 = 'mt940'  # SWIFT MT940 bank statement format
    MT942 = 'mt942'  # SWIFT MT942 interim transaction report format
    OFX = 'ofx'  # Open Financial Exchange
    QFX = 'qfx'  # Quicken Financial Exchange
    QIF = 'qif'  # Quicken Interchange Format
    QQQ = 'qqq'  # QuantQuote historical data
    TRR = 'trr'  # Trade and transaction reports
    TSDB = 'tsdb'  # Time series database export

    # Geospatial data
    GEOJSON = 'geojson'  # GeoJSON
    GEOTIFF = 'geotiff'  # GeoTIFF
    GML = 'gml'  # Geography Markup Language
    GPKG = 'gpkg'  # GeoPackage
    GPX = 'gpx'  # GPS Exchange Format
    KML = 'kml'  # Keyhole Markup Language
    LAS = 'las'  # LiDAR Aerial Survey
    LAZ = 'laz'  # LASzip (compressed LAS)
    OSM = 'osm'  # OpenStreetMap XML Data
    SHP = 'shp'  # ESRI Shapefile
    WKB = 'wkb'  # Well-Known Binary
    WKT = 'wkt'  # Well-Known Text

    # Logs & event streams
    EVT = 'evt'  # Windows Event Trace Log (pre-Vista)
    EVTX = 'evtx'  # Windows Event Trace Log (Vista and later)
    LOG = 'log'  # Generic log file
    PCAP = 'pcap'  # Packet Capture file
    PCAPPNG = 'pcapng'  # Packet Capture Next Generation file
    SLOG = 'slog'  # Structured log file
    W3CLOG = 'w3clog'  # W3C Extended Log File Format

    # “Data archives” & packaging
    _7Z = '7z'  # 7-Zip archive
    GZ = 'gz'  # Gzip-compressed file
    JAR = 'jar'  # Java archive
    RAR = 'rar'  # RAR archive
    SIT = 'sit'  # StuffIt archive
    SITX = 'sitx'  # StuffIt X archive
    TAR = 'tar'  # TAR archive
    TGZ = 'tgz'  # Gzip-compressed TAR archive
    ZIP = 'zip'  # ZIP archive

    # Domain-specific & less common

    # Templates
    HBS = 'hbs'  # Handlebars
    JINJA2 = 'jinja2'  # Jinja2
    MUSTACHE = 'mustache'  # Mustache
    VM = 'vm'  # Apache Velocity

    # -- Class Methods -- #

    @classmethod
    def aliases(cls) -> StrStrMap:
        """
        Return a mapping of common aliases for each enum member.

        Returns
        -------
        StrStrMap
            A mapping of alias names to their corresponding enum member names.
        """
        return {
            # Common shorthand
            'parq': 'parquet',
            'yml': 'yaml',
            # File extensions
            '.avro': 'avro',
            '.csv': 'csv',
            '.feather': 'feather',
            '.gz': 'gz',
            '.json': 'json',
            '.jsonl': 'ndjson',
            '.ndjson': 'ndjson',
            '.orc': 'orc',
            '.parquet': 'parquet',
            '.pq': 'parquet',
            '.stub': 'stub',
            '.tsv': 'tsv',
            '.txt': 'txt',
            '.xls': 'xls',
            '.xlsx': 'xlsx',
            '.zip': 'zip',
            '.xml': 'xml',
            '.yaml': 'yaml',
            '.yml': 'yaml',
            # MIME types
            'application/avro': 'avro',
            'application/csv': 'csv',
            'application/feather': 'feather',
            'application/gzip': 'gz',
            'application/json': 'json',
            'application/jsonlines': 'ndjson',
            'application/ndjson': 'ndjson',
            'application/orc': 'orc',
            'application/parquet': 'parquet',
            'application/vnd.apache.avro': 'avro',
            'application/vnd.apache.parquet': 'parquet',
            'application/vnd.apache.arrow.file': 'feather',
            'application/vnd.apache.orc': 'orc',
            'application/vnd.ms-excel': 'xls',
            (
                'application/vnd.openxmlformats-'
                'officedocument.spreadsheetml.sheet'
            ): 'xlsx',
            'application/x-avro': 'avro',
            'application/x-csv': 'csv',
            'application/x-feather': 'feather',
            'application/x-orc': 'orc',
            'application/x-ndjson': 'ndjson',
            'application/x-parquet': 'parquet',
            'application/x-yaml': 'yaml',
            'application/xml': 'xml',
            'application/zip': 'zip',
            'text/csv': 'csv',
            'text/plain': 'txt',
            'text/tab-separated-values': 'tsv',
            'text/tsv': 'tsv',
            'text/xml': 'xml',
            'text/yaml': 'yaml',
        }


# SECTION: INTERNAL CONSTANTS =============================================== #


# Compression formats that are also file formats.
_COMPRESSION_FILE_FORMATS: set[FileFormat] = {
    FileFormat.GZ,
    FileFormat.ZIP,
}


# SECTION: FUNCTIONS ======================================================== #


# TODO: Convert to a method on FileFormat or CompressionFormat?
def infer_file_format_and_compression(
    value: object,
    filename: object | None = None,
) -> tuple[FileFormat | None, CompressionFormat | None]:
    """
    Infer data format and compression from a filename, extension, or MIME type.

    Parameters
    ----------
    value : object
        A filename, extension, MIME type, or existing enum member.
    filename : object | None, optional
        A filename to consult for extension-based inference (e.g. when
        *value* is ``application/octet-stream``).

    Returns
    -------
    tuple[FileFormat | None, CompressionFormat | None]
        The inferred data format and compression, if any.
    """
    if isinstance(value, FileFormat):
        if value in _COMPRESSION_FILE_FORMATS:
            return None, CompressionFormat.coerce(value.value)
        return value, None
    if isinstance(value, CompressionFormat):
        return None, value

    text = str(value).strip()
    if not text:
        return None, None

    normalized = text.casefold()
    mime = normalized.split(';', 1)[0].strip()

    is_octet_stream = mime == 'application/octet-stream'
    compression = CompressionFormat.try_coerce(mime)
    fmt = None if is_octet_stream else FileFormat.try_coerce(mime)

    is_mime = mime.startswith(
        (
            'application/',
            'text/',
            'audio/',
            'image/',
            'video/',
            'multipart/',
        ),
    )
    suffix_source: object | None = filename if filename is not None else text
    if is_mime and filename is None:
        suffix_source = None

    suffixes = (
        PurePath(str(suffix_source)).suffixes
        if suffix_source is not None
        else []
    )
    if suffixes:
        normalized_suffixes = [suffix.casefold() for suffix in suffixes]
        compression = (
            CompressionFormat.try_coerce(normalized_suffixes[-1])
            or compression
        )
        if compression is not None:
            normalized_suffixes = normalized_suffixes[:-1]
        if normalized_suffixes:
            fmt = FileFormat.try_coerce(normalized_suffixes[-1]) or fmt

    if fmt in _COMPRESSION_FILE_FORMATS:
        compression = compression or CompressionFormat.coerce(fmt.value)
        fmt = None

    return fmt, compression
