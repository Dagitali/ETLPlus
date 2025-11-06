"""
etlplus.config.connector
========================

A module defining configuration types for data source/target connectors in ETL
pipelines. A "connector" is any I/O endpoint:

- file (local/remote file systems)
- database
- REST API service/endpoint
- (future) queues, streams, etc.

Quick start
-----------
- Use ``ConnectorApi``/``ConnectorFile``/``ConnectorDb`` when you want the
  concrete dataclasses.
- Use the ``Connector`` union for typing a value that can be any connector.
- Use ``parse_connector(obj)`` to construct a connector instance from a generic
  mapping that includes a ``type`` key.

Notes
-----
- TypedDict shapes are editor hints; runtime parsing remains permissive
  (from_obj accepts Mapping[str, Any]).
- TypedDicts referenced in :mod:`etlplus.config.types` remain editor hints.
  Runtime parsing stays permissive and tolerant.

See also
--------
- TypedDict shapes for editor hints (not enforced at runtime):
    etlplus.config.types.ConnectorApiConfigMap,
    etlplus.config.types.ConnectorDbConfigMap,
    etlplus.config.types.ConnectorFileConfigMap
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import overload
from typing import Self
from typing import TYPE_CHECKING

from .pagination import PaginationConfig
from .rate_limit import RateLimitConfig
from .utils import cast_str_dict

if TYPE_CHECKING:  # Editor-only typing hints to avoid runtime imports
    from .types import (
        ConnectorApiConfigMap,
        ConnectorDbConfigMap,
        ConnectorFileConfigMap,
    )


# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ConnectorApi', 'ConnectorDb', 'ConnectorFile',

    # Type aliases
    'Connector',

    # Functions
    'parse_connector',
]


# SECTION: TYPED ALIASES ==================================================== #


# Type alias representing any supported connector
type Connector = ConnectorApi | ConnectorDb | ConnectorFile


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class ConnectorApi:
    """
    Configuration for an API-based data connector.
    """

    # -- Attributes -- #

    name: str
    type: str = 'api'

    # Direct form
    url: str | None = None
    # Optional HTTP method; typically omitted for sources (defaults to GET
    # at runtime) and used for targets (e.g., 'post', 'put').
    method: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    query_params: dict[str, Any] = field(default_factory=dict)
    pagination: PaginationConfig | None = None
    rate_limit: RateLimitConfig | None = None

    # Reference form (to top-level APIs/endpoints)
    api: str | None = None
    endpoint: str | None = None

    # -- Class Methods -- #

    @classmethod
    @overload
    def from_obj(cls, obj: ConnectorApiConfigMap) -> Self: ...

    @classmethod
    @overload
    def from_obj(cls, obj: Mapping[str, Any]) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: Mapping[str, Any],
    ) -> Self:
        """
        Create a ConnectorApi from a mapping (tolerant to missing optional
        keys).

        Parameters
        ----------
        obj : Mapping[str, Any]
            The mapping to create the ConnectorApi from.

        Returns
        -------
        Self
            The created ConnectorApi instance.

        Raises
        ------
        TypeError
            If the input mapping is invalid.
        """

        name = obj.get('name')
        if not isinstance(name, str):
            raise TypeError('ConnectorApi requires a "name" (str)')
        headers = cast_str_dict(obj.get('headers'))

        return cls(
            name=name,
            type='api',
            url=obj.get('url'),
            method=obj.get('method'),
            headers=headers,
            query_params=dict(obj.get('query_params', {}) or {}),
            pagination=PaginationConfig.from_obj(obj.get('pagination')),
            rate_limit=RateLimitConfig.from_obj(obj.get('rate_limit')),
            api=obj.get('api') or obj.get('service'),
            endpoint=obj.get('endpoint'),
        )


@dataclass(slots=True)
class ConnectorDb:
    """
    Configuration for a database-based data connector.
    """

    # -- Attributes -- #

    name: str
    type: str = 'database'
    connection_string: str | None = None
    query: str | None = None
    table: str | None = None
    mode: str | None = None  # append|replace|upsert (future)

    # -- Class Methods -- #

    @classmethod
    @overload
    def from_obj(cls, obj: ConnectorDbConfigMap) -> Self: ...

    @classmethod
    @overload
    def from_obj(cls, obj: Mapping[str, Any]) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: Mapping[str, Any],
    ) -> Self:
        """
        Create a ConnectorDb from a mapping (tolerant to missing optional
        keys).

        Parameters
        ----------
        obj : Mapping[str, Any]
            The mapping to create the ConnectorDb from.

        Returns
        -------
        Self
            The created ConnectorDb instance.

        Raises
        ------
        TypeError
            If the input mapping is invalid.
        """

        name = obj.get('name')
        if not isinstance(name, str):
            raise TypeError('ConnectorDb requires a "name" (str)')

        return cls(
            name=name,
            type='database',
            connection_string=obj.get('connection_string'),
            query=obj.get('query'),
            table=obj.get('table'),
            mode=obj.get('mode'),
        )


@dataclass(slots=True)
class ConnectorFile:
    """
    Configuration for a file-based data connector.
    """

    # -- Attributes -- #

    name: str
    type: str = 'file'
    format: str | None = None
    path: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    # -- Class Methods -- #

    @classmethod
    @overload
    def from_obj(cls, obj: ConnectorFileConfigMap) -> Self: ...

    @classmethod
    @overload
    def from_obj(cls, obj: Mapping[str, Any]) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: Mapping[str, Any],
    ) -> Self:
        """
        Create a ConnectorFile from a mapping (tolerant to missing optional
        keys).

        Parameters
        ----------
        obj : Mapping[str, Any]
            The mapping to create the ConnectorFile from.

        Returns
        -------
        Self
            The created ConnectorFile instance.

        Raises
        ------
        TypeError
            If the input mapping is invalid.
        """

        name = obj.get('name')
        if not isinstance(name, str):
            raise TypeError('ConnectorFile requires a "name" (str)')

        return cls(
            name=name,
            type='file',
            format=obj.get('format'),
            path=obj.get('path'),
            options=dict(obj.get('options', {}) or {}),
        )


# SECTION: FUNCTIONS ======================================================== #


def parse_connector(obj: Mapping[str, Any]) -> Connector:
    """
    Construct a connector instance from a generic mapping.

    Parameters
    ----------
    obj : Mapping[str, Any]
        Mapping with at least ``name`` and ``type`` keys.

    Returns
    -------
    Connector
        A concrete connector dataclass instance.

    Notes
    -----
    - The mapping is parsed permissively by delegating to the underlying
      ``from_obj`` constructors, which tolerate missing optional keys.
    """

    t = str(obj.get('type', '')).casefold()
    if t == 'file':
        return ConnectorFile.from_obj(obj)
    if t == 'database':
        return ConnectorDb.from_obj(obj)
    if t == 'api':
        return ConnectorApi.from_obj(obj)

    raise TypeError(
        'Unsupported connector type; expected one of {file, database, api}',
    )
