"""
etlplus.config.targets
======================

A module defining configuration types for data targets in ETL pipelines.
"""
from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field


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
