"""
:mod:`tests.pytest_shared_support` module.

Shared typing protocols and helpers for top-level test fixtures.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import Protocol

from requests import PreparedRequest  # type: ignore[import]

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: PROTOCOLS ======================================================== #


class CliInvoke(Protocol):
    """Protocol describing the :func:`cli_invoke` fixture."""

    def __call__(
        self,
        *cli_args: str | Sequence[str],
    ) -> tuple[int, str, str]: ...


class CliRunner(Protocol):
    """Protocol describing the ``cli_runner`` fixture."""

    def __call__(self, *cli_args: str | Sequence[str]) -> int: ...


class JsonFactory(Protocol):
    """Protocol describing the :func:`json_file_factory` fixture."""

    def __call__(
        self,
        payload: Any,
        *,
        filename: str | None = None,
        ensure_ascii: bool = False,
    ) -> Path: ...


class JsonOutputParser(Protocol):
    """Protocol for JSON parsing helpers."""

    def __call__(self, output: str | Path) -> Any: ...


class JsonFileParser(Protocol):
    """Protocol for JSON file parsing helpers."""

    def __call__(self, path: Path) -> Any: ...


class RequestFactory(Protocol):
    """Protocol describing prepared-request factories."""

    def __call__(
        self,
        url: str | None = None,
    ) -> PreparedRequest: ...


# SECTION: TYPE ALIASES ===================================================== #


type CaptureHandler = Callable[[object, str], dict[str, object]]


# SECTION: INTERNAL CONSTANTS =============================================== #


_CLOUD_DATABASE_PROVIDER_FIELDS: tuple[str, ...] = (
    'project',
    'dataset',
    'location',
    'account',
    'database',
    'schema',
    'warehouse',
)


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class CloudDatabaseProviderCase:
    """Canonical shared test data for one cloud database provider."""

    provider: str
    provider_alias: str
    connector_name: str
    display_name: str
    extra: str
    metadata: dict[str, str]
    missing_package: str
    table_name: str = 'events'

    def connector_payload(
        self,
        *,
        connection_string: object | None = None,
        include_provider: bool = True,
        mode: object | None = None,
        name: str | None = None,
        omit_fields: Sequence[str] = (),
        query: object | None = None,
        table: object | None = None,
        use_alias: bool = False,
    ) -> dict[str, object]:
        """Return one canonical connector payload for this provider."""
        omitted = set(omit_fields)
        payload: dict[str, object] = {
            'name': self.connector_name if name is None else name,
            'type': 'database',
        }
        if include_provider:
            payload['provider'] = (
                self.provider_alias if use_alias else self.provider
            )
        for field, value in self.metadata.items():
            if field not in omitted:
                payload[field] = value
        if connection_string is not None:
            payload['connection_string'] = connection_string
        if query is not None:
            payload['query'] = query
        if table is not None:
            payload['table'] = table
        if mode is not None:
            payload['mode'] = mode
        return payload

    def expected_connector_attrs(
        self,
        *,
        connection_string: object | None = None,
        expected_provider: str | None = None,
        mode: object | None = None,
        name: str | None = None,
        omit_fields: Sequence[str] = (),
        query: object | None = None,
        table: object | None = None,
    ) -> dict[str, object]:
        """Return the normalized connector attributes for this provider."""
        omitted = set(omit_fields)
        expected: dict[str, object] = {
            'type': 'database',
            'name': self.connector_name if name is None else name,
            'connection_string': connection_string,
            'provider': (
                self.provider if expected_provider is None else expected_provider
            ),
            'query': query,
            'table': table,
            'mode': mode,
        }
        for field in _CLOUD_DATABASE_PROVIDER_FIELDS:
            expected[field] = None
        for field, value in self.metadata.items():
            if field not in omitted:
                expected[field] = value
        return expected

    def runtime_connector(
        self,
        *,
        connection_string: object | None = None,
        name: str | None = None,
        omit_fields: Sequence[str] = (),
        table: object | None = None,
    ) -> object:
        """Return one normalized runtime connector object for this provider."""
        fields = self.expected_connector_attrs(
            connection_string=connection_string,
            name=name,
            omit_fields=omit_fields,
            table=table,
        )
        return SimpleNamespace(**fields)


_CLOUD_DATABASE_PROVIDER_CASES: dict[str, CloudDatabaseProviderCase] = {
    'bigquery': CloudDatabaseProviderCase(
        provider='bigquery',
        provider_alias='gcp-bigquery',
        connector_name='warehouse_bigquery',
        display_name='BigQuery',
        extra='database-bigquery',
        metadata={
            'project': 'analytics-project',
            'dataset': 'warehouse',
            'location': 'US',
        },
        missing_package='google-cloud-bigquery/sqlalchemy-bigquery',
    ),
    'snowflake': CloudDatabaseProviderCase(
        provider='snowflake',
        provider_alias='snowflake-db',
        connector_name='warehouse_snowflake',
        display_name='Snowflake',
        extra='database-snowflake',
        metadata={
            'account': 'acme.us-east-1',
            'database': 'analytics',
            'schema': 'public',
            'warehouse': 'transforming',
        },
        missing_package='snowflake-connector-python/snowflake-sqlalchemy',
    ),
}


# SECTIONS: FUNCTIONS ======================================================= #


def coerce_cli_args(
    cli_args: tuple[str | Sequence[str], ...],
) -> tuple[str, ...]:
    """Normalize CLI arguments into ``tuple[str, ...]``."""
    if (
        len(cli_args) == 1
        and isinstance(cli_args[0], Sequence)
        and not isinstance(cli_args[0], (str, bytes))
    ):
        return tuple(str(part) for part in cli_args[0])
    return tuple(str(part) for part in cli_args)


def parse_json(
    output: str | Path,
) -> Any:
    """Parse JSON from a string or file path."""
    raw = output.read_text(encoding='utf-8') if isinstance(output, Path) else output
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AssertionError(f'Expected JSON output, got: {raw!r}') from exc


def get_cloud_database_provider_case(
    provider: str,
) -> CloudDatabaseProviderCase:
    """Return one canonical shared test-data case for *provider*."""
    return _CLOUD_DATABASE_PROVIDER_CASES[provider]
