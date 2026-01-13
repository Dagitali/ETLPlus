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
    'coerce_compression_format',
    'coerce_file_format',
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

    AVRO = 'avro'
    CSV = 'csv'
    FEATHER = 'feather'
    GZ = 'gz'
    JSON = 'json'
    NDJSON = 'ndjson'
    ORC = 'orc'
    PARQUET = 'parquet'
    TSV = 'tsv'
    TXT = 'txt'
    XLS = 'xls'
    XLSX = 'xlsx'
    ZIP = 'zip'
    XML = 'xml'
    YAML = 'yaml'

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


# TODO: Deprecate in favor of using the enum methods directly.
def coerce_compression_format(
    compression_format: CompressionFormat | str,
) -> CompressionFormat:
    """
    Normalize textual compression format values to :class:`CompressionFormat`.

    This thin wrapper is kept for backward compatibility; prefer
    :meth:`CompressionFormat.coerce` going forward.
    """
    return CompressionFormat.coerce(compression_format)


# TODO: Deprecate in favor of using the enum methods directly.
def coerce_file_format(
    file_format: FileFormat | str,
) -> FileFormat:
    """
    Normalize textual file format values to :class:`FileFormat`.

    This thin wrapper is kept for backward compatibility; prefer
    :meth:`FileFormat.coerce` going forward.
    """
    return FileFormat.coerce(file_format)


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
        ``value`` is ``application/octet-stream``).

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
