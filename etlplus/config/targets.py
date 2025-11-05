"""
etlplus.config.targets
======================

A module defining configuration types for data targets in ETL pipelines.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Self


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class TargetApi:
    """
    Configuration for an API-based data target.
    """

    # -- Attributes -- #

    name: str
    type: str = 'api'

    # Direct form
    url: str | None = None
    method: str | None = None
    headers: dict[str, str] = field(default_factory=dict)

    # Reference form (to top-level APIs/endpoints)
    api: str | None = None
    endpoint: str | None = None

    # -- Class Methods -- #

    @classmethod
    def from_obj(cls, obj: Mapping[str, Any]) -> Self:
        name = obj.get('name')
        if not isinstance(name, str):
            raise TypeError('TargetApi requires a "name" (str)')
        headers = {
            k: str(v)
            for k, v in (obj.get('headers', {}) or {}).items()
        }
        return cls(
            name=name,
            type='api',
            url=obj.get('url'),
            method=obj.get('method'),
            headers=headers,
            api=obj.get('api') or obj.get('service'),
            endpoint=obj.get('endpoint'),
        )


@dataclass(slots=True)
class TargetDb:
    """
    Configuration for a database-based data target.
    """

    # -- Attributes -- #

    name: str
    type: str = 'database'
    connection_string: str | None = None
    table: str | None = None
    mode: str | None = None  # append|replace|upsert (future)

    # -- Class Methods -- #

    @classmethod
    def from_obj(cls, obj: Mapping[str, Any]) -> Self:
        name = obj.get('name')
        if not isinstance(name, str):
            raise TypeError('TargetDb requires a "name" (str)')
        return cls(
            name=name,
            type='database',
            connection_string=obj.get('connection_string'),
            table=obj.get('table'),
            mode=obj.get('mode'),
        )


@dataclass(slots=True)
class TargetFile:
    """
    Configuration for a file-based data target.
    """

    # -- Attributes -- #

    name: str
    type: str = 'file'
    format: str | None = None
    path: str | None = None

    # -- Class Methods -- #

    @classmethod
    def from_obj(cls, obj: Mapping[str, Any]) -> Self:
        name = obj.get('name')
        if not isinstance(name, str):
            raise TypeError('TargetFile requires a "name" (str)')
        return cls(
            name=name,
            type='file',
            format=obj.get('format'),
            path=obj.get('path'),
        )
