"""
etlplus.config.sources
======================

A module defining configuration types for data sources in ETL pipelines.
"""
from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any

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
