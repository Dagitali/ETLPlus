"""
etlplus.config.types
====================

A module centralizing type aliases used in the ``:mod:etlplus.config`` package.

Contents
--------
- Type aliases: ``Source``, ``Target``

Notes
-----
- TypedDicts in this module are editor/type-checking hints. They are
    intentionally ``total=False`` (all keys optional) and are not enforced at
    runtime.
- Constructors like ``*.from_obj`` accept ``Mapping[str, Any]`` and perform
    tolerant parsing and light casting. This keeps the runtime permissive while
    improving autocomplete and static analysis for contributors.

Examples
--------
>>> from etlplus.config import Source
>>> src: Source = {
>>>     "type": "file",
>>>     "path": "/data/input.csv",
>>> }
>>> from etlplus.config import Target
>>> tgt: Target = {
>>>     "type": "database",
>>>     "connection_string": "postgresql://user:pass@localhost/db",
>>> }
>>> from etlplus.api import RetryPolicy
>>> rp: RetryPolicy = {"max_attempts": 3, "backoff": 0.5}
"""
from __future__ import annotations

from typing import Any
from typing import Mapping
from typing import NotRequired
from typing import TypedDict

from .connector import ConnectorApi
from .connector import ConnectorDb
from .connector import ConnectorFile


# SECTION: EXPORTS  ========================================================= #


__all__ = [
    # Type aliases
    'Connector',

    # TypedDicts
    'ApiProfileDefaultsMap', 'ApiProfileConfigMap', 'ApiConfigMap',
    'EndpointConfigMap', 'PaginationConfigMap', 'RateLimitConfigMap',
    'ConnectorApiConfigMap', 'ConnectorDbConfigMap', 'ConnectorFileConfigMap',
]


# SECTION: TYPE ALIASES ===================================================== #


type Connector = ConnectorApi | ConnectorDb | ConnectorFile


# SECTION: TYPED DICTS ====================================================== #


class ApiConfigMap(TypedDict, total=False):
    """
    Top-level API config shape parsed by ApiConfig.from_obj.

    Either provide a 'base_url' with optional 'headers' and 'endpoints', or
    provide 'profiles' with at least one profile having a 'base_url'.

    See also
    --------
    - etlplus.config.api.ApiConfig.from_obj: parses this mapping
    """

    base_url: str
    headers: Mapping[str, Any]
    endpoints: Mapping[str, EndpointConfigMap | str]
    profiles: Mapping[str, ApiProfileConfigMap]


class ApiProfileConfigMap(TypedDict, total=False):
    """
    Shape accepted for a profile entry under ApiConfigMap.profiles.

    Notes
    -----
    `base_url` is required at runtime when profiles are provided.

    See also
    --------
    - etlplus.config.api.ApiProfileConfig.from_obj: parses this mapping
    """

    base_url: str
    headers: Mapping[str, Any]
    base_path: str
    auth: Mapping[str, Any]
    defaults: ApiProfileDefaultsMap


class ApiProfileDefaultsMap(TypedDict, total=False):
    """
    Defaults block available under a profile (all keys optional).

    Notes
    -----
    Runtime expects header values to be str; typing remains permissive.

    See also
    --------
    - etlplus.config.api.ApiProfileConfig.from_obj: consumes this block
    - etlplus.config.pagination.PaginationConfig.from_obj: parses pagination
    - etlplus.config.rate_limit.RateLimitConfig.from_obj: parses rate_limit
    """

    headers: Mapping[str, Any]
    pagination: PaginationConfigMap | Mapping[str, Any]
    rate_limit: RateLimitConfigMap | Mapping[str, Any]


class ConnectorApiConfigMap(TypedDict, total=False):
    """
    Shape accepted by ConnectorApi.from_obj (all keys optional).

    See also
    --------
    - etlplus.config.connector.ConnectorApi.from_obj
    """

    name: str
    type: str
    url: str
    method: str
    headers: Mapping[str, Any]
    query_params: Mapping[str, Any]
    pagination: PaginationConfigMap
    rate_limit: RateLimitConfigMap
    api: str
    endpoint: str


class ConnectorDbConfigMap(TypedDict, total=False):
    """
    Shape accepted by ConnectorDb.from_obj (all keys optional).

    See also
    --------
    - etlplus.config.connector.ConnectorDb.from_obj
    """

    name: str
    type: str
    connection_string: str
    query: str
    table: str
    mode: str


class ConnectorFileConfigMap(TypedDict, total=False):
    """
    Shape accepted by ConnectorFile.from_obj (all keys optional).

    See also
    --------
    - etlplus.config.connector.ConnectorFile.from_obj
    """

    name: str
    type: str
    format: str
    path: str
    options: Mapping[str, Any]


class EndpointConfigMap(TypedDict, total=False):
    """
    Shape accepted by EndpointConfig.from_obj.

    One of 'path' or 'url' should be provided.

    See also
    --------
    - etlplus.config.api.EndpointConfig.from_obj: parses this mapping
    """

    path: NotRequired[str]
    url: NotRequired[str]
    method: NotRequired[str]
    path_params: NotRequired[Mapping[str, Any]]
    query_params: NotRequired[Mapping[str, Any]]
    body: NotRequired[Any]
    pagination: NotRequired[PaginationConfigMap]
    rate_limit: NotRequired[RateLimitConfigMap]


class PaginationConfigMap(TypedDict, total=False):
    """
    Shape accepted by PaginationConfig.from_obj (all keys optional).

    See also
    --------
    - etlplus.config.pagination.PaginationConfig.from_obj
    """

    type: str
    page_param: str
    size_param: str
    start_page: int
    page_size: int
    cursor_param: str
    cursor_path: str
    start_cursor: str | int
    records_path: str
    max_pages: int
    max_records: int


class RateLimitConfigMap(TypedDict, total=False):
    """
    Shape accepted by RateLimitConfig.from_obj (all keys optional).

    See also
    --------
    - etlplus.config.rate_limit.RateLimitConfig.from_obj
    """

    sleep_seconds: float
    max_per_sec: float
