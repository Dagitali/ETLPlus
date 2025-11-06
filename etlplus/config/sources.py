"""
etlplus.config.sources
======================

A module defining configuration types for data sources in ETL pipelines.

TypedDicts are editor hints; runtime remains permissive.

See also
--------
- TypedDict shapes for editor hints (not enforced at runtime):
    etlplus.config.types.SourceApiConfigMap,
    etlplus.config.types.SourceDbConfigMap,
    etlplus.config.types.SourceFileConfigMap
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
        SourceApiConfigMap,
        SourceDbConfigMap,
        SourceFileConfigMap,
    )


# SECTION: EXPORTS ========================================================== #


__all__ = ['SourceApi', 'SourceDb', 'SourceFile']


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
    @overload
    def from_obj(cls, obj: SourceApiConfigMap) -> Self: ...

    @classmethod
    @overload
    def from_obj(cls, obj: Mapping[str, Any]) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: Mapping[str, Any],
    ) -> Self:
        """
        Create a SourceApi from a mapping (tolerant to missing optional keys).

        Parameters
        ----------
        obj : Mapping[str, Any]
            The mapping to create the SourceApi from.

        Returns
        -------
        Self
            The created SourceApi instance.

        Raises
        ------
        TypeError
            If the input mapping is invalid.
        """

        name = obj.get('name')
        if not isinstance(name, str):
            raise TypeError('SourceApi requires a "name" (str)')
        headers = cast_str_dict(obj.get('headers'))

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
    @overload
    def from_obj(cls, obj: SourceDbConfigMap) -> Self: ...

    @classmethod
    @overload
    def from_obj(cls, obj: Mapping[str, Any]) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: Mapping[str, Any],
    ) -> Self:
        """
        Create a SourceDb from a mapping (tolerant to missing optional keys).

        Parameters
        ----------
        obj : Mapping[str, Any]
            The mapping to create the SourceDb from.

        Returns
        -------
        Self
            The created SourceDb instance.

        Raises
        ------
        TypeError
            If the input mapping is invalid.
        """

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

    # -- Attributes -- #

    name: str
    type: str = 'file'
    format: str | None = None
    path: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    # -- Class Methods -- #

    @classmethod
    @overload
    def from_obj(cls, obj: SourceFileConfigMap) -> Self: ...

    @classmethod
    @overload
    def from_obj(cls, obj: Mapping[str, Any]) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: Mapping[str, Any],
    ) -> Self:
        """
        Create a SourceFile from a mapping (tolerant to missing optional keys).

        Parameters
        ----------
        obj : Mapping[str, Any]
            The mapping to create the SourceFile from.

        Returns
        -------
        Self
            The created SourceFile instance.

        Raises
        ------
        TypeError
            If the input mapping is invalid.
        """

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
