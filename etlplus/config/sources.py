"""
etlplus.config.sources
======================

A module defining configuration types for data sources in ETL pipelines.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Self

from .pagination import PaginationConfig
from .rate_limit import RateLimitConfig


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class SourceApi:
    """
    Configuration for an API-based data source.
    """

    # -- Attributes -- #

    name: str
    type: str = 'api'

    # Direct form
    url: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    query_params: dict[str, Any] = field(default_factory=dict)
    pagination: PaginationConfig | None = None
    rate_limit: RateLimitConfig | None = None

    # Reference form (to top-level APIs/endpoints)
    api: str | None = None
    endpoint: str | None = None

    # -- Class Methods -- #

    @classmethod
    def from_obj(cls, obj: Mapping[str, Any]) -> Self:
        """
        Create a SourceApi from a mapping (tolerant to missing optional keys).
        """
        name = obj.get('name')
        if not isinstance(name, str):
            raise TypeError('SourceApi requires a "name" (str)')
        headers = {
            k: str(v)
            for k, v in (obj.get('headers', {}) or {}).items()
        }
        return cls(
            name=name,
            type='api',
            url=obj.get('url'),
            headers=headers,
            query_params=dict(obj.get('query_params', {}) or {}),
            pagination=PaginationConfig.from_obj(obj.get('pagination')),
            rate_limit=RateLimitConfig.from_obj(obj.get('rate_limit')),
            api=obj.get('api') or obj.get('service'),
            endpoint=obj.get('endpoint'),
        )


@dataclass(slots=True)
class SourceDb:
    """
    Configuration for a database-based data source.
    """

    # -- Attributes -- #

    name: str
    type: str = 'database'
    connection_string: str | None = None
    query: str | None = None

    # -- Class Methods -- #

    @classmethod
    def from_obj(cls, obj: Mapping[str, Any]) -> Self:
        name = obj.get('name')
        if not isinstance(name, str):
            raise TypeError('SourceDb requires a "name" (str)')
        return cls(
            name=name,
            type='database',
            connection_string=obj.get('connection_string'),
            query=obj.get('query'),
        )


@dataclass(slots=True)
class SourceFile:
    """
    Configuration for a file-based data source.
    """

    name: str
    type: str = 'file'
    format: str | None = None
    path: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    # -- Class Methods -- #

    @classmethod
    def from_obj(cls, obj: Mapping[str, Any]) -> Self:
        name = obj.get('name')
        if not isinstance(name, str):
            raise TypeError('SourceFile requires a "name" (str)')
        return cls(
            name=name,
            type='file',
            format=obj.get('format'),
            path=obj.get('path'),
            options=dict(obj.get('options', {}) or {}),
        )
