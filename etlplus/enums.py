"""
:mod:`etlplus.enums` module.

Shared enumeration types used across ETLPlus modules.
"""

from __future__ import annotations

import enum
import operator as _op
from statistics import fmean
from typing import Self

from .types import AggregateFunc
from .types import OperatorFunc
from .types import StrStrMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'AggregateName',
    'CoercibleStrEnum',
    'DataConnectorType',
    'FileFormat',
    'HttpMethod',
    'OperatorName',
    'PipelineStep',
    'coerce_data_connector_type',
    'coerce_file_format',
    'coerce_http_method',
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

    CSV = 'csv'
    JSON = 'json'
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
            'yml': 'yaml',
            # MIME types
            'text/csv': 'csv',
            'application/json': 'json',
            'application/xml': 'xml',
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


def coerce_http_method(
    http_method: HttpMethod | str,
) -> HttpMethod:
    """
    Normalize textual HTTP method values to :class:`HttpMethod`.

    This thin wrapper is kept for backward compatibility; prefer
    :meth:`HttpMethod.coerce` going forward.
    """
    return HttpMethod.coerce(http_method)
