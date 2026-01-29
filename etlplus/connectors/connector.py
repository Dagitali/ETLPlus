"""
:mod:`etlplus.connectors.connector` module.

A module defining configuration types for data source/target connectors in ETL
pipelines. A "connector" is any I/O endpoint:

- File (local/remote file systems)
- Database
- REST API service/endpoint
- (Future) queues, streams, etc.

Examples
--------
- Use :class:`ConnectorApi`/:class:`ConnectorFile`/:class:`ConnectorDb` when
    you want the concrete dataclasses.
- Use the :class:`Connector` union for typing a value that can be any
    connector.
- Use :func:`parse_connector(obj)` to construct a connector instance from a
    generic mapping that includes a *type* key.

Notes
-----
- TypedDict shapes are editor hints; runtime parsing remains permissive
    (from_obj accepts Mapping[str, Any]).
- TypedDicts referenced in :mod:`etlplus.connectors.types` remain editor hints.
    Runtime parsing stays permissive and tolerant.

See Also
--------
- TypedDict shapes for editor hints (not enforced at runtime):
    :mod:`etlplus.connectors.types.ConnectorApiConfigMap`,
    :mod:`etlplus.connectors.types.ConnectorDbConfigMap`,
    :mod:`etlplus.connectors.types.ConnectorFileConfigMap`.
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
from ..utils import maybe_mapping
from .enums import DataConnectorType

if TYPE_CHECKING:  # Editor-only typing hints to avoid runtime imports
    from .types import ConnectorApiConfigMap
    from .types import ConnectorDbConfigMap
    from .types import ConnectorFileConfigMap
    from .types import ConnectorType


# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'ConnectorApi',
    'ConnectorDb',
    'ConnectorFile',
    # Functions
    'parse_connector',
    # Type aliases
    'Connector',
]


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _coerce_connector_type(
    obj: Mapping[str, Any],
) -> DataConnectorType:
    """
    Normalize and validate the connector ``type`` field.

    Parameters
    ----------
    obj : Mapping[str, Any]
        Mapping with a ``type`` entry.

    Returns
    -------
    DataConnectorType
        Normalized connector type enum.

    Raises
    ------
    TypeError
        If ``type`` is missing or unsupported.
    """
    if 'type' not in obj:
        raise TypeError('Connector requires a "type"')
    try:
        return DataConnectorType.coerce(obj.get('type'))
    except ValueError as exc:
        allowed = ', '.join(DataConnectorType.choices())
        raise TypeError(
            f'Unsupported connector type: {obj.get("type")!r}. '
            f'Expected one of {allowed}.',
        ) from exc


def _require_name(
    obj: StrAnyMap,
    *,
    kind: str,
) -> str:
    """
    Extract and validate the ``name`` field from connector mappings.

    Parameters
    ----------
    obj : StrAnyMap
        Connector mapping with a ``name`` entry.
    kind : str
        Connector kind used in the error message.

    Returns
    -------
    str
        Valid connector name.

    Raises
    ------
    TypeError
        If ``name`` is missing or not a string.
    """
    name = obj.get('name')
    if not isinstance(name, str):
        raise TypeError(f'Connector{kind} requires a "name" (str)')
    return name


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
        Connector kind, always ``'api'``.
    url : str | None
        Direct absolute URL (when not using ``service``/``endpoint`` refs).
    method : str | None
        Optional HTTP method; typically omitted for sources (defaults to
        GET) and used for targets (e.g., ``'post'``).
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
    type: ConnectorType = DataConnectorType.API

    # Direct form
    url: str | None = None
    # Optional HTTP method; typically omitted for sources (defaults to GET)
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
        """
        name = _require_name(obj, kind='Api')
        headers = cast_str_dict(maybe_mapping(obj.get('headers')))

        return cls(
            name=name,
            type=DataConnectorType.API,
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
        Connector kind, always ``'database'``.
    connection_string : str | None
        Connection string/DSN for the database.
    query : str | None
        Query to execute for extraction (optional).
    table : str | None
        Target/source table name (optional).
    mode : str | None
        Load mode hint (e.g., ``'append'``, ``'replace'``) — future use.
    """

    # -- Attributes -- #

    name: str
    type: ConnectorType = DataConnectorType.DATABASE
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
        """
        name = _require_name(obj, kind='Db')

        return cls(
            name=name,
            type=DataConnectorType.DATABASE,
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
        Connector kind, always ``'file'``.
    format : str | None
        File format (e.g., ``'json'``, ``'csv'``).
    path : str | None
        File path or URI.
    options : dict[str, Any]
        Reader/writer format options.
    """

    # -- Attributes -- #

    name: str
    type: ConnectorType = DataConnectorType.FILE
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
        """
        name = _require_name(obj, kind='File')

        return cls(
            name=name,
            type=DataConnectorType.FILE,
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
        If ``type`` is missing/unsupported or if ``obj`` is not a mapping.

    Notes
    -----
    Delegates to the tolerant ``from_obj`` constructors for each connector
    kind. Connector types are normalized via
    :class:`etlplus.connectors.enums.DataConnectorType`, so common aliases
    (e.g., ``'db'`` or ``'http'``) are accepted.
    """
    if not isinstance(obj, Mapping):
        raise TypeError('Connector configuration must be a mapping.')
    match _coerce_connector_type(obj):
        case DataConnectorType.FILE:
            return ConnectorFile.from_obj(obj)
        case DataConnectorType.DATABASE:
            return ConnectorDb.from_obj(obj)
        case DataConnectorType.API:
            return ConnectorApi.from_obj(obj)


# SECTION: TYPED ALIASES (post-class definitions) ========================= #

# Type alias representing any supported connector
type Connector = ConnectorApi | ConnectorDb | ConnectorFile
