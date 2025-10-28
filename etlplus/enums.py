"""
ETLPlus Enums
=======================

Shared enumeration types used across ETLPlus modules.
"""
from __future__ import annotations

import enum
import operator as _op
from statistics import fmean
from typing import Any
from typing import Callable
from typing import Mapping
from typing import Self


# SECTION: PUBLIC API ======================================================= #


__all__ = (
    'CoercibleStrEnum',
    'DataConnectorType',
    'FileFormat',
    'HttpMethod',
    'OperatorName',
    'AggregateName',
    'PipelineStep',
    'coerce_data_connector_type',
    'coerce_file_format',
    'coerce_http_method',
)


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
    def aliases(cls) -> Mapping[str, str]:
        """Return the alias map for this enum (subclasses may override)."""
        return {}

    @classmethod
    def choices(cls) -> tuple[str, ...]:
        """
        Return the allowed string values for this enum.
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
        except Exception as e:  # ValueError or TypeError
            allowed = ', '.join(cls.choices())
            raise ValueError(
                f'Invalid {cls.__name__} value: {value!r}. Allowed: {allowed}',
            ) from e

    @classmethod
    def try_coerce(cls, value: object) -> Self | None:
        """
        Best-effort parse; return ``None`` on failure instead of raising.
        """

        try:
            return cls.coerce(value)
        except ValueError:
            return None


# SECTION: ENUMS ============================================================ #


class AggregateName(CoercibleStrEnum):
    """
    Supported aggregations with helpers.
    """

    # -- Constants -- #

    AVG = 'avg'
    COUNT = 'count'
    MAX = 'max'
    MIN = 'min'
    SUM = 'sum'

    # -- Class Methods -- #

    @property
    def func(self) -> Callable[[list[float], int], Any]:
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
    """
    Supported data connector types.
    """

    # -- Constants -- #

    API = 'api'
    DATABASE = 'database'
    FILE = 'file'

    # -- Class Methods -- #

    @classmethod
    def aliases(cls) -> Mapping[str, str]:
        return {
            'http': 'api',
            'https': 'api',
            'rest': 'api',
            'db': 'database',
            'filesystem': 'file',
            'fs': 'file',
        }


class FileFormat(CoercibleStrEnum):
    """
    Supported file formats for extraction.

    Includes common aliases  (e.g., ``yml`` for ``yaml``) and select MIME
    types.
    """

    # -- Constants -- #

    CSV = 'csv'
    JSON = 'json'
    XML = 'xml'
    YAML = 'yaml'

    # -- Class Methods -- #

    @classmethod
    def aliases(cls) -> Mapping[str, str]:

        return {
            'text/csv': 'csv',
            'application/json': 'json',
            'application/xml': 'xml',
            'yml': 'yaml',
        }


class HttpMethod(CoercibleStrEnum):
    """
    Supported HTTP verbs that accept JSON payloads.
    """

    # -- Constants -- #

    PATCH = 'patch'
    POST = 'post'
    PUT = 'put'

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
    """
    Supported comparison operators with helpers.
    """

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
    def func(self) -> Callable[[Any, Any], bool]:
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
    def aliases(cls) -> Mapping[str, str]:
        return {
            '==': 'eq', '=': 'eq', '!=': 'ne', '<>': 'ne',
            '>=': 'gte', '≥': 'gte', '<=': 'lte', '≤': 'lte',
            '>': 'gt', '<': 'lt',
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
        sequence = ('filter', 'map', 'select', 'sort', 'aggregate')

        return sequence.index(self.value)


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
