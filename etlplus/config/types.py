"""
:mod:`etlplus.config.types` module.

Type aliases and editor-only TypedDicts for :mod:`etlplus.config`.

These types improve IDE autocomplete and static analysis while the runtime
parsers remain permissive.

Notes
-----
- TypedDicts in this module are intentionally ``total=False`` and are not
    enforced at runtime.
- ``*.from_obj`` constructors accept ``Mapping[str, Any]`` and perform
    tolerant parsing and light casting. This keeps the runtime permissive while
    improving autocomplete and static analysis for contributors.

Examples
--------
>>> from etlplus.config import Connector
>>> src: Connector = {
>>>     "type": "file",
>>>     "path": "/data/input.csv",
>>> }
>>> tgt: Connector = {
>>>     "type": "database",
>>>     "connection_string": "postgresql://user:pass@localhost/db",
>>> }
>>> from etlplus.api import RetryPolicy
>>> rp: RetryPolicy = {"max_attempts": 3, "backoff": 0.5}
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import Literal
from typing import TypedDict

from ..api import PaginationConfigMap
from ..api import RateLimitConfigMap
from ..types import StrAnyMap

# SECTION: EXPORTS  ========================================================= #


__all__ = [
    # Type aliases
    'ConnectorType',
    # 'PaginationType',
    # TypedDicts
    'ApiProfileDefaultsMap',
    'ApiProfileConfigMap',
    'ApiConfigMap',
    'EndpointMap',
    'ConnectorApiConfigMap',
    'ConnectorDbConfigMap',
    'ConnectorFileConfigMap',
]


# SECTION: TYPE ALIASES ===================================================== #


# Literal type for supported connector kinds
type ConnectorType = Literal['api', 'database', 'file']

# Literal type for supported pagination kinds
# type PaginationType = Literal['page', 'offset', 'cursor']


# SECTION: TYPED DICTS ====================================================== #


class ApiConfigMap(TypedDict, total=False):
    """
    Top-level API config shape parsed by ApiConfig.from_obj.

    Either provide a 'base_url' with optional 'headers' and 'endpoints', or
    provide 'profiles' with at least one profile having a 'base_url'.

    See Also
    --------
    - etlplus.config.api.ApiConfig.from_obj: parses this mapping
    """

    base_url: str
    headers: StrAnyMap
    endpoints: Mapping[str, EndpointMap | str]
    profiles: Mapping[str, ApiProfileConfigMap]


class ApiProfileConfigMap(TypedDict, total=False):
    """
    Shape accepted for a profile entry under ApiConfigMap.profiles.

    Notes
    -----
    `base_url` is required at runtime when profiles are provided.

    See Also
    --------
    - etlplus.config.api.ApiProfileConfig.from_obj: parses this mapping
    """

    base_url: str
    headers: StrAnyMap
    base_path: str
    auth: StrAnyMap
    defaults: ApiProfileDefaultsMap


class ApiProfileDefaultsMap(TypedDict, total=False):
    """
    Defaults block available under a profile (all keys optional).

    Notes
    -----
    Runtime expects header values to be str; typing remains permissive.

    See Also
    --------
    - etlplus.config.api.ApiProfileConfig.from_obj: consumes this block
    - etlplus.config.pagination.PaginationConfig.from_obj: parses pagination
    - etlplus.api.rate_limiting.RateLimitConfig.from_obj: parses rate_limit
    """

    headers: StrAnyMap
    pagination: PaginationConfigMap | StrAnyMap
    rate_limit: RateLimitConfigMap | StrAnyMap


class ConnectorApiConfigMap(TypedDict, total=False):
    """
    Shape accepted by ConnectorApi.from_obj (all keys optional).

    See Also
    --------
    - etlplus.config.connector.ConnectorApi.from_obj
    """

    name: str
    type: ConnectorType
    url: str
    method: str
    headers: StrAnyMap
    query_params: StrAnyMap
    pagination: PaginationConfigMap
    rate_limit: RateLimitConfigMap
    api: str
    endpoint: str


class ConnectorDbConfigMap(TypedDict, total=False):
    """
    Shape accepted by ConnectorDb.from_obj (all keys optional).

    See Also
    --------
    - etlplus.config.connector.ConnectorDb.from_obj
    """

    name: str
    type: ConnectorType
    connection_string: str
    query: str
    table: str
    mode: str


class ConnectorFileConfigMap(TypedDict, total=False):
    """
    Shape accepted by ConnectorFile.from_obj (all keys optional).

    See Also
    --------
    - etlplus.config.connector.ConnectorFile.from_obj
    """

    name: str
    type: ConnectorType
    format: str
    path: str
    options: StrAnyMap


class EndpointMap(TypedDict, total=False):
    """
    Shape accepted by EndpointConfig.from_obj.

    One of 'path' or 'url' should be provided.

    See Also
    --------
    - etlplus.config.api.EndpointConfig.from_obj: parses this mapping
    """

    path: str
    url: str
    method: str
    path_params: StrAnyMap
    query_params: StrAnyMap
    body: Any
    pagination: PaginationConfigMap
    rate_limit: RateLimitConfigMap
