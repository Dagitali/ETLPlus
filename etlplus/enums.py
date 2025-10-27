"""
ETLPlus Enums
=======================

Shared enumeration types used across ETLPlus modules.
"""
from __future__ import annotations

import enum
from typing import Mapping
from typing import Self


# SECTION: PUBLIC API ======================================================= #


__all__ = (
    'CoercibleStrEnum',
    'DataConnectorType',
    'FileFormat',
    'HttpMethod',
    'coerce_data_connector_type',
    'coerce_file_format',
    'coerce_http_method',
)


# SECTION: ENUMS ============================================================ #


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


class DataConnectorType(CoercibleStrEnum):
    """
    Supported data connector types.
    """

    # -- Constants -- #

    FILE = 'file'
    DATABASE = 'database'
    API = 'api'

    # -- Class Methods -- #

    @classmethod
    def aliases(cls) -> Mapping[str, str]:
        return {
            'db': 'database',
            'fs': 'file',
            'filesystem': 'file',
            'rest': 'api',
            'http': 'api',
            'https': 'api',
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
            'yml': 'yaml',
            'text/csv': 'csv',
            'application/json': 'json',
            'application/xml': 'xml',
        }


class HttpMethod(CoercibleStrEnum):
    """
    HTTP verbs that accept JSON payloads.
    """

    # -- Constants -- #

    POST = 'post'
    PUT = 'put'
    PATCH = 'patch'

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
