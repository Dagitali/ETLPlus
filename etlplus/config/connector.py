"""
:mod:`etlplus.config.connector` module.

A module defining configuration types for data source/target connectors in ETL
pipelines. A "connector" is any I/O endpoint:

- file (local/remote file systems)
- database
- REST API service/endpoint
- (future) queues, streams, etc.

Examples
--------
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

See Also
--------
- TypedDict shapes for editor hints (not enforced at runtime):
    :mod:`etlplus.config.types.ConnectorApiConfigMap`,
    :mod:`etlplus.config.types.ConnectorDbConfigMap`,
    :mod:`etlplus.config.types.ConnectorFileConfigMap`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import Any
from typing import Self
from typing import overload

from ..api import PaginationConfig
from ..api import RateLimitConfig
from ..types import StrAnyMap
from ..utils import cast_str_dict
from ..utils import coerce_dict

if TYPE_CHECKING:  # Editor-only typing hints to avoid runtime imports
    from .types import ConnectorApiConfigMap
    from .types import ConnectorDbConfigMap
    from .types import ConnectorFileConfigMap
    from .types import ConnectorType


# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ConnectorApi',
    'ConnectorDb',
    'ConnectorFile',
    # Functions
    'parse_connector',
    # Type aliases
    'Connector',
]


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class ConnectorApi:
    """
    Configuration for an API-based data connector.

    Attributes
    ----------
    name : str
        Unique connector name.
    type : ConnectorType
        Connector kind literal, always ``"api"``.
    url : str | None
        Direct absolute URL (when not using ``service``/``endpoint`` refs).
    method : str | None
        Optional HTTP method; typically omitted for sources (defaults to
        GET) and used for targets (e.g., ``"post"``).
    headers : dict[str, str]
        Additional request headers.
    query_params : dict[str, Any]
        Default query parameters.
    pagination : PaginationConfig | None
        Pagination settings (optional).
    rate_limit : RateLimitConfig | None
        Rate limiting settings (optional).
    api : str | None
        Service reference into the pipeline ``apis`` block (a.k.a.
        ``service``).
    endpoint : str | None
        Endpoint name within the referenced service.
    """

    # -- Attributes -- #

    name: str
    type: ConnectorType = 'api'

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
    def from_obj(cls, obj: StrAnyMap) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into a ``ConnectorApi`` instance.

        Parameters
        ----------
        obj : StrAnyMap
            Mapping with at least ``name``.

        Returns
        -------
        Self
            Parsed connector instance.

        Raises
        ------
        TypeError
            If ``name`` is missing or invalid.
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
            query_params=coerce_dict(obj.get('query_params')),
            pagination=PaginationConfig.from_obj(obj.get('pagination')),
            rate_limit=RateLimitConfig.from_obj(obj.get('rate_limit')),
            api=obj.get('api') or obj.get('service'),
            endpoint=obj.get('endpoint'),
        )


@dataclass(kw_only=True, slots=True)
class ConnectorDb:
    """
    Configuration for a database-based data connector.

    Attributes
    ----------
    name : str
        Unique connector name.
    type : ConnectorType
        Connector kind literal, always ``"database"``.
    connection_string : str | None
        Connection string/DSN for the database.
    query : str | None
        Query to execute for extraction (optional).
    table : str | None
        Target/source table name (optional).
    mode : str | None
        Load mode hint (e.g., ``"append"``, ``"replace"``) — future use.
    """

    # -- Attributes -- #

    name: str
    type: ConnectorType = 'database'
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
    def from_obj(cls, obj: StrAnyMap) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into a ``ConnectorDb`` instance.

        Parameters
        ----------
        obj : StrAnyMap
            Mapping with at least ``name``.

        Returns
        -------
        Self
            Parsed connector instance.

        Raises
        ------
        TypeError
            If ``name`` is missing or invalid.
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


@dataclass(kw_only=True, slots=True)
class ConnectorFile:
    """
    Configuration for a file-based data connector.

    Attributes
    ----------
    name : str
        Unique connector name.
    type : ConnectorType
        Connector kind literal, always ``"file"``.
    format : str | None
        File format (e.g., ``"json"``, ``"csv"``).
    path : str | None
        File path or URI.
    options : dict[str, Any]
        Reader/writer format options.
    """

    # -- Attributes -- #

    name: str
    type: ConnectorType = 'file'
    format: str | None = None
    path: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    # -- Class Methods -- #

    @classmethod
    @overload
    def from_obj(cls, obj: ConnectorFileConfigMap) -> Self: ...

    @classmethod
    @overload
    def from_obj(cls, obj: StrAnyMap) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into a ``ConnectorFile`` instance.

        Parameters
        ----------
        obj : StrAnyMap
            Mapping with at least ``name``.

        Returns
        -------
        Self
            Parsed connector instance.

        Raises
        ------
        TypeError
            If ``name`` is missing or invalid.
        """
        name = obj.get('name')
        if not isinstance(name, str):
            raise TypeError('ConnectorFile requires a "name" (str)')

        return cls(
            name=name,
            type='file',
            format=obj.get('format'),
            path=obj.get('path'),
            options=coerce_dict(obj.get('options')),
        )


# SECTION: FUNCTIONS ======================================================== #


def parse_connector(obj: Mapping[str, Any]) -> Connector:
    """
    Dispatch to a concrete connector constructor based on ``type``.

    Parameters
    ----------
    obj : Mapping[str, Any]
        Mapping with at least ``name`` and ``type``.

    Returns
    -------
    Connector
        Concrete connector instance.

    Raises
    ------
    TypeError
        If ``type`` is unsupported or missing.

    Notes
    -----
    Delegates to the tolerant ``from_obj`` constructors for each connector
    kind.
    """
    match str(obj.get('type', '')).casefold():
        case 'file':
            return ConnectorFile.from_obj(obj)
        case 'database':
            return ConnectorDb.from_obj(obj)
        case 'api':
            return ConnectorApi.from_obj(obj)
        case _:
            raise TypeError(
                'Unsupported connector type; '
                'expected one of {file, database, api}',
            )


# SECTION: TYPED ALIASES (post-class definitions) ========================= #

# Type alias representing any supported connector
type Connector = ConnectorApi | ConnectorDb | ConnectorFile
