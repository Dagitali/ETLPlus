"""
:mod:`etlplus.enums` module.

Shared enumeration types used across ETLPlus modules.
"""

from __future__ import annotations

import enum
import operator as _op
from pathlib import PurePath
from statistics import fmean
from typing import Self

from .types import AggregateFunc
from .types import OperatorFunc
from .types import StrStrMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Enums
    'AggregateName',
    'CoercibleStrEnum',
    'CompressionFormat',
    'DataConnectorType',
    'FileFormat',
    'HttpMethod',
    'OperatorName',
    'PipelineStep',
    # Functions
    'coerce_compression_format',
    'coerce_data_connector_type',
    'coerce_file_format',
    'coerce_http_method',
    'infer_file_format_and_compression',
]


# SECTION: CLASSES ========================================================== #


class CoercibleStrEnum(enum.StrEnum):
    """
    StrEnum with ergonomic helpers.

    Provides a DRY, class-level :meth:`coerce` that normalizes inputs and
    produces consistent, informative error messages. Also exposes
    :meth:`choices` for UI/validation and :meth:`try_coerce` for soft parsing.

    Notes
    -----
    - Values are normalized via ``str(value).strip().casefold()``.
    - Error messages enumerate allowed values for easier debugging.
    """

    # -- Class Methods -- #

    @classmethod
    def aliases(cls) -> StrStrMap:
        """
        Return a mapping of common aliases for each enum member.

        Subclasses may override this method to provide custom aliases.

        Returns
        -------
        StrStrMap
            A mapping of alias names to their corresponding enum member names.
        """
        return {}

    @classmethod
    def choices(cls) -> tuple[str, ...]:
        """
        Return the allowed string values for this enum.

        Returns
        -------
        tuple[str, ...]
            A tuple of allowed string values for this enum.
        """
        return tuple(member.value for member in cls)

    @classmethod
    def coerce(cls, value: Self | str | object) -> Self:
        """
        Convert an enum member or string-like input to a member of ``cls``.

        Parameters
        ----------
        value : Self | str | object
            An existing enum member or a text value to normalize.

        Returns
        -------
        Self
            The corresponding enum member.

        Raises
        ------
        ValueError
            If the value cannot be coerced into a valid member.
        """
        if isinstance(value, cls):
            return value
        try:
            normalized = str(value).strip().casefold()
            resolved = cls.aliases().get(normalized, normalized)
            return cls(resolved)  # type: ignore[arg-type]
        except (ValueError, TypeError) as e:
            allowed = ', '.join(cls.choices())
            raise ValueError(
                f'Invalid {cls.__name__} value: {value!r}. Allowed: {allowed}',
            ) from e

    @classmethod
    def try_coerce(
        cls,
        value: object,
    ) -> Self | None:
        """
        Best-effort parse; return ``None`` on failure instead of raising.

        Parameters
        ----------
        value : object
            An existing enum member or a text value to normalize.

        Returns
        -------
        Self | None
            The corresponding enum member, or ``None`` if coercion fails.
        """
        try:
            return cls.coerce(value)
        except ValueError:
            return None


# SECTION: ENUMS ============================================================ #


class AggregateName(CoercibleStrEnum):
    """Supported aggregations with helpers."""

    # -- Constants -- #

    AVG = 'avg'
    COUNT = 'count'
    MAX = 'max'
    MIN = 'min'
    SUM = 'sum'

    # -- Class Methods -- #

    @property
    def func(self) -> AggregateFunc:
        """
        Get the aggregation function for this aggregation type.

        Returns
        -------
        AggregateFunc
            The aggregation function corresponding to this aggregation type.
        """
        if self is AggregateName.COUNT:
            return lambda xs, n: n
        if self is AggregateName.MAX:
            return lambda xs, n: (max(xs) if xs else None)
        if self is AggregateName.MIN:
            return lambda xs, n: (min(xs) if xs else None)
        if self is AggregateName.SUM:
            return lambda xs, n: sum(xs)

        # AVG
        return lambda xs, n: (fmean(xs) if xs else 0.0)


class CompressionFormat(CoercibleStrEnum):
    """Supported compression formats for data files."""

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


class DataConnectorType(CoercibleStrEnum):
    """Supported data connector types."""

    # -- Constants -- #

    API = 'api'
    DATABASE = 'database'
    FILE = 'file'

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
            'http': 'api',
            'https': 'api',
            'rest': 'api',
            'db': 'database',
            'filesystem': 'file',
            'fs': 'file',
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
            'application/feather': 'feather',
            'application/gzip': 'gz',
            'application/json': 'json',
            'application/jsonlines': 'ndjson',
            'application/ndjson': 'ndjson',
            'application/orc': 'orc',
            'application/vnd.apache.arrow.file': 'feather',
            'application/vnd.apache.orc': 'orc',
            'application/vnd.ms-excel': 'xls',
            (
                'application/vnd.openxmlformats-'
                'officedocument.spreadsheetml.sheet'
            ): 'xlsx',
            'application/x-avro': 'avro',
            'application/x-ndjson': 'ndjson',
            'application/x-parquet': 'parquet',
            'application/xml': 'xml',
            'application/zip': 'zip',
            'text/csv': 'csv',
            'text/plain': 'txt',
            'text/tab-separated-values': 'tsv',
        }


class HttpMethod(CoercibleStrEnum):
    """Supported HTTP verbs that accept JSON payloads."""

    # -- Constants -- #

    CONNECT = 'connect'
    DELETE = 'delete'
    GET = 'get'
    HEAD = 'head'
    OPTIONS = 'options'
    PATCH = 'patch'
    POST = 'post'
    PUT = 'put'
    TRACE = 'trace'

    # -- Getters -- #

    @property
    def allows_body(self) -> bool:
        """
        Whether the method typically allows a request body.

        Notes
        -----
        - RFCs do not strictly forbid bodies on some other methods (e.g.,
          ``DELETE``), but many servers/clients do not expect them. We mark
          ``POST``, ``PUT``, and ``PATCH`` as True.
        """
        return self in {HttpMethod.POST, HttpMethod.PUT, HttpMethod.PATCH}


class OperatorName(CoercibleStrEnum):
    """Supported comparison operators with helpers."""

    # -- Constants -- #

    EQ = 'eq'
    NE = 'ne'
    GT = 'gt'
    GTE = 'gte'
    LT = 'lt'
    LTE = 'lte'
    IN = 'in'
    CONTAINS = 'contains'

    # -- Getters -- #

    @property
    def func(self) -> OperatorFunc:
        """
        Get the comparison function for this operator.

        Returns
        -------
        OperatorFunc
            The comparison function corresponding to this operator.
        """
        match self:
            case OperatorName.EQ:
                return _op.eq
            case OperatorName.NE:
                return _op.ne
            case OperatorName.GT:
                return _op.gt
            case OperatorName.GTE:
                return _op.ge
            case OperatorName.LT:
                return _op.lt
            case OperatorName.LTE:
                return _op.le
            case OperatorName.IN:
                return lambda a, b: a in b
            case OperatorName.CONTAINS:
                return lambda a, b: b in a

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
            '==': 'eq',
            '=': 'eq',
            '!=': 'ne',
            '<>': 'ne',
            '>=': 'gte',
            '≥': 'gte',
            '<=': 'lte',
            '≤': 'lte',
            '>': 'gt',
            '<': 'lt',
        }


class PipelineStep(CoercibleStrEnum):
    """Pipeline step names as an enum for internal orchestration."""

    # -- Constants -- #

    FILTER = 'filter'
    MAP = 'map'
    SELECT = 'select'
    SORT = 'sort'
    AGGREGATE = 'aggregate'

    # -- Getters -- #

    @property
    def order(self) -> int:
        """
        Get the execution order of this pipeline step.

        Returns
        -------
        int
            The execution order of this pipeline step.
        """
        return _PIPELINE_ORDER_INDEX[self]


# SECTION: INTERNAL CONSTANTS ============================================== #


# Compression formats that are also file formats.
_COMPRESSION_FILE_FORMATS: set[FileFormat] = {
    FileFormat.GZ,
    FileFormat.ZIP,
}


# Precomputed order index for PipelineStep; avoids recomputing on each access.
_PIPELINE_ORDER_INDEX: dict[PipelineStep, int] = {
    PipelineStep.FILTER: 0,
    PipelineStep.MAP: 1,
    PipelineStep.SELECT: 2,
    PipelineStep.SORT: 3,
    PipelineStep.AGGREGATE: 4,
}


# SECTION: FUNCTIONS ======================================================== #


def coerce_data_connector_type(
    connector: DataConnectorType | str,
) -> DataConnectorType:
    """
    Normalize textual data connector values to :class:`DataConnectorType`.

    This thin wrapper is kept for backward compatibility; prefer
    :meth:`DataConnectorType.coerce` going forward.
    """
    return DataConnectorType.coerce(connector)


def coerce_file_format(
    file_format: FileFormat | str,
) -> FileFormat:
    """
    Normalize textual file format values to :class:`FileFormat`.

    This thin wrapper is kept for backward compatibility; prefer
    :meth:`FileFormat.coerce` going forward.
    """
    return FileFormat.coerce(file_format)


def coerce_compression_format(
    compression_format: CompressionFormat | str,
) -> CompressionFormat:
    """
    Normalize textual compression format values to :class:`CompressionFormat`.

    This thin wrapper is kept for backward compatibility; prefer
    :meth:`CompressionFormat.coerce` going forward.
    """
    return CompressionFormat.coerce(compression_format)


def coerce_http_method(
    http_method: HttpMethod | str,
) -> HttpMethod:
    """
    Normalize textual HTTP method values to :class:`HttpMethod`.

    This thin wrapper is kept for backward compatibility; prefer
    :meth:`HttpMethod.coerce` going forward.
    """
    return HttpMethod.coerce(http_method)


def infer_file_format_and_compression(
    value: object,
) -> tuple[FileFormat | None, CompressionFormat | None]:
    """
    Infer data format and compression from a filename, extension, or MIME type.

    Parameters
    ----------
    value : object
        A filename, extension, MIME type, or existing enum member.

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

    compression = CompressionFormat.try_coerce(mime)
    fmt = FileFormat.try_coerce(mime)

    suffixes = PurePath(text).suffixes
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
