"""
etlplus.config.types
====================

A module centralizing type aliases used in the ``:mod:etlplus.config`` package.

Contents
--------
- Type aliases: ``Source``, ``Target``

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

from .sources import SourceApi
from .sources import SourceDb
from .sources import SourceFile
from .targets import TargetApi
from .targets import TargetDb
from .targets import TargetFile


# SECTION: EXPORTS  ========================================================= #


__all__ = [
    # Type aliases
    'Source', 'Target',

    # TypedDicts
    'ApiProfileDefaultsMap', 'ApiProfileConfigMap', 'ApiConfigMap',
    'EndpointConfigMap', 'PaginationConfigMap', 'RateLimitConfigMap',
]


# SECTION: TYPE ALIASES ===================================================== #


type Source = SourceFile | SourceDb | SourceApi
type Target = TargetFile | TargetApi | TargetDb


# SECTION: TYPED DICTS ====================================================== #


class ApiConfigMap(TypedDict, total=False):
    """
    Top-level API config shape parsed by ApiConfig.from_obj.

    Either provide a 'base_url' with optional 'headers' and 'endpoints', or
    provide 'profiles' with at least one profile having a 'base_url'.
    """

    base_url: str
    headers: Mapping[str, Any]
    endpoints: Mapping[str, EndpointConfigMap | str]
    profiles: Mapping[str, ApiProfileConfigMap]


class ApiProfileConfigMap(TypedDict, total=False):
    """
    Shape accepted for a profile entry under ApiConfigMap.profiles.

    Note: `base_url` is required at runtime when profiles are provided.
    """

    base_url: str
    headers: Mapping[str, Any]
    base_path: str
    auth: Mapping[str, Any]
    defaults: ApiProfileDefaultsMap


class ApiProfileDefaultsMap(TypedDict, total=False):
    """
    Defaults block available under a profile (all keys optional).

    Note: Runtime expects header values to be str; typing remains permissive.
    """

    headers: Mapping[str, Any]
    pagination: PaginationConfigMap | Mapping[str, Any]
    rate_limit: RateLimitConfigMap | Mapping[str, Any]


class EndpointConfigMap(TypedDict, total=False):
    """
    Shape accepted by EndpointConfig.from_obj.

    One of 'path' or 'url' should be provided.
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
    """

    sleep_seconds: float
    max_per_sec: float
