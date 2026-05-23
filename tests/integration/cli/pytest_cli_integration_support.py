"""
:mod:`tests.integration.cli.pytest_cli_integration_support` module.

Shared support types for CLI integration tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Protocol

from etlplus.storage import StorageLocation

# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True)
class PipelineConfig:
    """Container for generated pipeline configuration paths."""

    config_path: Path
    source_path: Path
    output_path: Path
    job_name: str


@dataclass(slots=True)
class PipelineSchema:
    """Container for generated pipeline configs with table_schemas."""

    config_path: Path
    schema_name: str
    table_name: str


@dataclass(slots=True)
class RealRemoteSource:
    """Real cloud-backed source object seeded for one integration test."""

    uri: str
    location: StorageLocation
    backend: Any


@dataclass(slots=True)
class RealRemoteTarget:
    """Real cloud-backed target allocated for one integration test."""

    uri: str
    location: StorageLocation
    backend: Any


@dataclass(slots=True)
class TableSpec:
    """Container for generated table spec paths."""

    spec_path: Path
    schema_name: str
    table_name: str


# SECTION: PROTOCOLS ======================================================== #


class PipelineConfigFactory(Protocol):
    """Protocol for pipeline config factory fixtures."""

    def __call__(
        self,
        data: list[dict[str, Any]] | list[Any],
    ) -> PipelineConfig: ...


class RealRemoteSourceFactory(Protocol):
    """Create and seed a real cloud-backed source URI for one test."""

    def __call__(
        self,
        env_name: str,
        *,
        payload: Any,
        suffix: str,
        file_format: str = 'json',
    ) -> RealRemoteSource: ...


class RealRemoteTargetFactory(Protocol):
    """Create a real cloud-backed target URI for one test."""

    def __call__(
        self,
        env_name: str,
        *,
        suffix: str,
        extension: str = 'json',
    ) -> RealRemoteTarget: ...
