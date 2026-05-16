"""
:mod:`tests.unit.runtime.test_u_runtime_readiness_connectors` module.

Connector readiness unit tests for :mod:`etlplus.runtime.readiness._builder`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

import etlplus.runtime.readiness._builder as readiness_builder_mod
import etlplus.runtime.readiness._connectors as readiness_connectors_mod
from etlplus.runtime.readiness._support import RequirementSpec
from tests.pytest_shared_support import get_cloud_database_provider_case

from .pytest_runtime_readiness import build_connector_gap_row as _connector_gap
from .pytest_runtime_readiness import (
    build_missing_requirement_row as _missing_requirement,
)
from .pytest_runtime_readiness import build_runtime_cfg as _cfg

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


def _connector_checks(cfg: object) -> list[dict[str, object]]:
    """Return connector checks using the production policy seams."""
    return readiness_connectors_mod.ConnectorReadinessPolicy.readiness_checks(
        cast(Any, cfg),
        connector_gap_rows_fn=(
            readiness_connectors_mod.ConnectorReadinessPolicy.gap_rows
        ),
        make_check=(readiness_builder_mod.ReadinessReportBuilder.make_check),
        package_available=(
            readiness_builder_mod.ReadinessReportBuilder.package_available
        ),
    )


BIGQUERY_CASE = get_cloud_database_provider_case('bigquery')
SNOWFLAKE_CASE = get_cloud_database_provider_case('snowflake')


# SECTION: TESTS ============================================================ #


class TestReadinessReportBuilderConnectors:
    """Connector readiness tests for :class:`ReadinessReportBuilder`."""

    def test_connector_gap_rows_continue_after_complete_database_connector(
        self,
    ) -> None:
        """
        A complete database connector should not prevent later gaps from being
        reported.
        """
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    connection_string='sqlite:///:memory:',
                    name='db-source',
                    type='database',
                ),
                SimpleNamespace(
                    name='file-source',
                    path=None,
                    type='file',
                ),
            ],
        )

        rows = readiness_connectors_mod.ConnectorReadinessPolicy.gap_rows(
            cast(Any, cfg),
        )

        assert rows == [
            _connector_gap(
                connector='file-source',
                guidance=(
                    'Set "path" to a local path or storage URI for this file connector.'
                ),
                issue='missing path',
                role='source',
                connector_type='file',
            ),
        ]

    @pytest.mark.parametrize(
        ('connector', 'expected'),
        [
            pytest.param(
                BIGQUERY_CASE.runtime_connector(
                    connection_string=None,
                    name='warehouse_bigquery',
                    omit_fields=('dataset',),
                ),
                _connector_gap(
                    connector='warehouse_bigquery',
                    guidance=(
                        'Set "connection_string" to a database DSN or SQLAlchemy-style '
                        'URL, or define both "project" and "dataset" for this '
                        'BigQuery connector.'
                    ),
                    issue='missing connection_string or bigquery project/dataset',
                    role='target',
                    connector_type='database',
                ),
                id='bigquery',
            ),
            pytest.param(
                SNOWFLAKE_CASE.runtime_connector(
                    connection_string=None,
                    name='warehouse_snowflake',
                    omit_fields=('schema',),
                ),
                _connector_gap(
                    connector='warehouse_snowflake',
                    guidance=(
                        'Set "connection_string" to a database DSN or SQLAlchemy-style '
                        'URL, or define "account", "database", and "schema" for '
                        'this Snowflake connector.'
                    ),
                    issue=(
                        'missing connection_string or snowflake account/database/schema'
                    ),
                    role='target',
                    connector_type='database',
                ),
                id='snowflake',
            ),
        ],
    )
    def test_connector_gap_rows_cover_cloud_provider_specific_fields(
        self,
        connector: object,
        expected: dict[str, object],
    ) -> None:
        """Test provider-specific DB gap rows for cloud database connectors."""
        cfg = _cfg(
            targets=[connector],
        )

        rows = readiness_connectors_mod.ConnectorReadinessPolicy.gap_rows(
            cast(Any, cfg),
        )

        assert rows == [expected]

    def test_connector_gap_rows_cover_missing_required_connector_fields(
        self,
    ) -> None:
        """Test gap rows for missing path, API linkage, and generic DB data."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(name='file-source', path=None, type='file'),
                SimpleNamespace(name='api-source', api=None, type='api', url=None),
                SimpleNamespace(
                    name='api-ref-source',
                    api='missing-api',
                    type='api',
                    url=None,
                ),
            ],
            targets=[
                SimpleNamespace(
                    connection_string=None,
                    name='db-target',
                    type='database',
                ),
            ],
            apis={},
        )

        rows = readiness_connectors_mod.ConnectorReadinessPolicy.gap_rows(
            cast(Any, cfg),
        )

        assert rows == [
            _connector_gap(
                connector='file-source',
                guidance=(
                    'Set "path" to a local path or storage URI for this file connector.'
                ),
                issue='missing path',
                role='source',
                connector_type='file',
            ),
            _connector_gap(
                connector='api-source',
                guidance=(
                    'Set "url" to a reachable endpoint or "api" to a configured '
                    'top-level API name.'
                ),
                issue='missing url or api reference',
                role='source',
                connector_type='api',
            ),
            _connector_gap(
                connector='api-ref-source',
                guidance=(
                    'Define "missing-api" under top-level "apis" or update the '
                    'connector "api" reference.'
                ),
                issue='unknown api reference: missing-api',
                role='source',
                connector_type='api',
            ),
            _connector_gap(
                connector='db-target',
                guidance=(
                    'Set "connection_string" to a database DSN or SQLAlchemy-style URL.'
                ),
                issue='missing connection_string',
                role='target',
                connector_type='database',
            ),
        ]

    def test_connector_gap_rows_report_actionable_unsupported_type_details(
        self,
    ) -> None:
        """Test that unsupported connector types include actionable guidance."""
        cfg = SimpleNamespace(
            sources=[
                SimpleNamespace(
                    name='remote-source',
                    type='s3',
                ),
            ],
            targets=[],
            apis={},
        )

        rows = readiness_connectors_mod.ConnectorReadinessPolicy.gap_rows(
            cast(Any, cfg),
        )

        assert rows == [
            _connector_gap(
                connector='remote-source',
                guidance=(
                    '"s3" is a storage scheme, not a connector type. '
                    'Use connector type "file" and keep the provider in '
                    'the path or URI scheme.'
                ),
                issue='unsupported type',
                role='source',
                supported_types=['api', 'database', 'file', 'queue'],
                connector_type='s3',
            ),
        ]

    def test_connector_gap_rows_return_empty_for_complete_connectors(
        self,
    ) -> None:
        """Test that complete connector definitions produce no gap rows."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    name='file-source',
                    path='input.csv',
                    type='file',
                ),
                SimpleNamespace(
                    name='api-url-source',
                    api=None,
                    type='api',
                    url='https://example.test/data',
                ),
                SimpleNamespace(
                    name='api-ref-source',
                    api='known-api',
                    type='api',
                    url=None,
                ),
            ],
            targets=[
                SimpleNamespace(
                    connection_string='sqlite:///:memory:',
                    name='db-target',
                    type='database',
                ),
                SimpleNamespace(
                    connection_string='sqlite:///other.db',
                    name='db-target-2',
                    type='database',
                ),
            ],
            apis={'known-api': object()},
        )

        rows = readiness_connectors_mod.ConnectorReadinessPolicy.gap_rows(
            cast(Any, cfg),
        )

        assert not rows

    def test_connector_gap_rows_coerce_supported_connector_type_text(
        self,
    ) -> None:
        """Test soft connector-type coercion without a private wrapper seam."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    name='file-source',
                    path=None,
                    type=' FILE ',
                ),
                SimpleNamespace(
                    name='queue-source',
                    path='input.csv',
                    type='QUEUE',
                ),
            ],
        )

        rows = readiness_connectors_mod.ConnectorReadinessPolicy.gap_rows(
            cast(Any, cfg),
        )

        assert rows == [
            _connector_gap(
                connector='file-source',
                guidance=(
                    'Set "path" to a local path or storage URI for this file connector.'
                ),
                issue='missing path',
                role='source',
                connector_type=' FILE ',
            ),
        ]

    @pytest.mark.parametrize(
        ('connector', 'expected'),
        [
            pytest.param(
                BIGQUERY_CASE.runtime_connector(
                    connection_string=None,
                    name='warehouse_bigquery',
                ),
                _missing_requirement(
                    connector='warehouse_bigquery',
                    detected_database_provider='bigquery',
                    extra='database-bigquery',
                    guidance=(
                        'Install google-cloud-bigquery/sqlalchemy-bigquery directly '
                        'or install the ETLPlus "database-bigquery" extra. Required '
                        'for "bigquery" database connectors.'
                    ),
                    missing_package=BIGQUERY_CASE.missing_package,
                    reason=(
                        'bigquery database connector requires '
                        'google-cloud-bigquery/sqlalchemy-bigquery'
                    ),
                    role='target',
                ),
                id='bigquery',
            ),
            pytest.param(
                SNOWFLAKE_CASE.runtime_connector(
                    connection_string=None,
                    name='warehouse_snowflake',
                ),
                _missing_requirement(
                    connector='warehouse_snowflake',
                    detected_database_provider='snowflake',
                    extra='database-snowflake',
                    guidance=(
                        'Install snowflake-connector-python/snowflake-sqlalchemy '
                        'directly or install the ETLPlus "database-snowflake" '
                        'extra. Required for "snowflake" database connectors.'
                    ),
                    missing_package=SNOWFLAKE_CASE.missing_package,
                    reason=(
                        'snowflake database connector requires '
                        'snowflake-connector-python/snowflake-sqlalchemy'
                    ),
                    role='target',
                ),
                id='snowflake',
            ),
        ],
    )
    def test_connector_missing_requirement_rows_cover_database_provider_extras(
        self,
        connector: object,
        expected: dict[str, object],
    ) -> None:
        """Test requirement rows for cloud-database provider extras."""
        cfg = _cfg(
            targets=[connector],
        )

        rows = (
            readiness_connectors_mod.ConnectorReadinessPolicy.missing_requirement_rows(
                cfg=cast(Any, cfg),
                package_available=lambda _module: False,
            )
        )

        assert rows == [expected]

    def test_connector_readiness_checks_report_all_ok_states(self) -> None:
        """Test readiness rows when gaps and optional dependency gaps are absent."""
        cfg = _cfg()

        checks = _connector_checks(cfg)

        assert checks == [
            {
                'message': 'Configured connectors include the required runtime fields.',
                'name': 'connector-readiness',
                'status': 'ok',
            },
            {
                'message': (
                    'No missing optional dependencies were detected for '
                    'configured connectors.'
                ),
                'name': 'optional-dependencies',
                'status': 'ok',
            },
        ]

    def test_connector_readiness_checks_report_gap_and_dependency_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test readiness rows when connector and dependency errors exist."""
        cfg = _cfg()
        monkeypatch.setattr(
            readiness_connectors_mod.ConnectorReadinessPolicy,
            'gap_rows',
            lambda _cfg: [{'connector': 'bad-source'}],
        )
        monkeypatch.setattr(
            readiness_connectors_mod.ConnectorReadinessPolicy,
            'missing_requirement_rows',
            lambda *, cfg, package_available: [
                {'connector': 'bad-source', 'missing_package': 'boto3'},
            ],
        )

        checks = _connector_checks(cfg)

        assert checks == [
            {
                'gaps': [{'connector': 'bad-source'}],
                'message': (
                    'One or more configured connectors are missing required '
                    'runtime fields or use unsupported connector types.'
                ),
                'name': 'connector-readiness',
                'status': 'error',
            },
            {
                'message': (
                    'Configured connectors require optional dependencies that are '
                    'not installed.'
                ),
                'missing_requirements': [
                    {'connector': 'bad-source', 'missing_package': 'boto3'},
                ],
                'name': 'optional-dependencies',
                'status': 'error',
            },
        ]

    def test_connector_type_guidance_covers_blank_and_generic_invalid_values(
        self,
    ) -> None:
        """Test actionable guidance for blank and non-storage invalid types."""
        assert readiness_connectors_mod.connector_type_guidance('') == (
            'Set type to one of: api, database, file, queue.'
        )
        assert readiness_connectors_mod.connector_type_guidance('weird') == (
            'Use one of the supported connector types: api, database, file, queue.'
        )

    def test_dedupe_rows_preserves_first_occurrence(self) -> None:
        """Test duplicate requirement rows are removed while keeping order."""
        row = {
            'connector': 'source-a',
            'role': 'source',
            'missing_package': 'boto3',
            'reason': 's3 storage path requires boto3',
            'extra': 'storage',
        }

        rows = readiness_connectors_mod.ReadinessSupportPolicy.dedupe_rows(
            [row, dict(row), {**row, 'connector': 'source-b'}],
        )

        assert rows == [row, {**row, 'connector': 'source-b'}]

    def test_missing_requirement_guidance_returns_plain_install_hint_without_context(
        self,
    ) -> None:
        """Missing-dependency guidance should fall back to one plain install hint."""
        assert (
            readiness_connectors_mod.ReadinessSupportPolicy.missing_requirement_guidance(
                package='tables',
                extra=None,
            )
            == 'Install tables.'
        )

    def test_missing_requirement_rows_cover_netcdf_and_format_specific_branches(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test requirement rows for netCDF and format-specific extras."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    format='csv',
                    name='s3-source',
                    path='s3://bucket/input.csv',
                    type='file',
                ),
                SimpleNamespace(
                    format='nc',
                    name='nc-source',
                    path='input.nc',
                    type='file',
                ),
                SimpleNamespace(
                    format='rda',
                    name='rda-source',
                    path='input.rda',
                    type='file',
                ),
            ],
        )

        rows = (
            readiness_connectors_mod.ConnectorReadinessPolicy.missing_requirement_rows(
                cfg=cast(Any, cfg),
                package_available=lambda module_name: module_name == 'boto3',
            )
        )

        assert rows == [
            _missing_requirement(
                connector='nc-source',
                detected_format='nc',
                extra='file',
                guidance=(
                    'Install xarray plus one of netCDF4 or h5netcdf, or install '
                    'the ETLPlus "file" extra.'
                ),
                missing_package='xarray/netCDF4',
                reason='nc format requires xarray and netCDF4 or h5netcdf',
                role='source',
            ),
            _missing_requirement(
                connector='rda-source',
                detected_format='rda',
                extra='file',
                guidance=(
                    'Install pyreadr directly or install the ETLPlus "file" '
                    'extra. Required for "rda" file format.'
                ),
                missing_package='pyreadr',
                reason='rda format requires pyreadr',
                role='source',
            ),
        ]

    def test_missing_requirement_rows_ignore_storage_schemes_without_extras(
        self,
    ) -> None:
        """Unknown storage schemes should not emit optional dependency rows."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    name='custom-source',
                    path='custom://bucket/input.csv',
                    type='file',
                ),
            ],
        )

        rows = (
            readiness_connectors_mod.ConnectorReadinessPolicy.missing_requirement_rows(
                cfg=cast(Any, cfg),
                package_available=lambda _name: False,
            )
        )

        assert rows == []

    @pytest.mark.parametrize(
        ('service', 'expected_service', 'missing_module', 'extra', 'package'),
        [
            pytest.param(
                ' AWS-SQS ',
                'aws-sqs',
                'boto3',
                'queue-aws',
                'boto3',
                id='aws-sqs-normalized',
            ),
            pytest.param(
                'sqs',
                'aws-sqs',
                'boto3',
                'queue-aws',
                'boto3',
                id='sqs-alias',
            ),
            pytest.param('amqp', 'amqp', 'pika', 'queue-amqp', 'pika', id='amqp'),
            pytest.param(
                'azure-service-bus',
                'azure-service-bus',
                'azure.servicebus',
                'queue-azure',
                'azure-servicebus',
                id='azure-service-bus',
            ),
            pytest.param(
                'gcp-pubsub',
                'gcp-pubsub',
                'google.cloud.pubsub',
                'queue-gcp',
                'google-cloud-pubsub',
                id='gcp-pubsub',
            ),
            pytest.param(
                'redis',
                'redis',
                'redis',
                'queue-redis',
                'redis',
                id='redis',
            ),
        ],
    )
    def test_missing_requirement_rows_report_queue_service_dependencies(
        self,
        service: str,
        expected_service: str,
        missing_module: str,
        extra: str,
        package: str,
    ) -> None:
        """Test that queue connectors require provider-specific dependencies."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    name='events',
                    queue_name='events',
                    service=service,
                    type='queue',
                ),
            ],
        )

        rows = (
            readiness_connectors_mod.ConnectorReadinessPolicy.missing_requirement_rows(
                cfg=cast(Any, cfg),
                package_available=lambda module_name: module_name != missing_module,
            )
        )

        assert rows == [
            _missing_requirement(
                connector='events',
                detected_queue_service=expected_service,
                extra=extra,
                guidance=(
                    f'Install {package} directly or install the ETLPlus '
                    f'"{extra}" extra. Required for "{expected_service}" '
                    'queue connectors.'
                ),
                missing_package=package,
                reason=f'{expected_service} queue connector requires {package}',
                role='source',
            ),
        ]

    def test_missing_requirement_rows_respects_package_available_seam(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that class-based dependency checks still honor wrapper patches."""
        monkeypatch.setattr(
            readiness_builder_mod.ReadinessReportBuilder,
            'package_available',
            lambda module_name: False if module_name == 'boto3' else True,
        )
        cfg = SimpleNamespace(
            sources=[
                SimpleNamespace(
                    format='csv',
                    name='s3-source',
                    path='s3://bucket/input.csv',
                    type='file',
                ),
            ],
            targets=[],
            apis={},
        )

        rows = (
            readiness_connectors_mod.ConnectorReadinessPolicy.missing_requirement_rows(
                cfg=cast(Any, cfg),
                package_available=(
                    readiness_builder_mod.ReadinessReportBuilder.package_available
                ),
            )
        )

        assert rows == [
            _missing_requirement(
                connector='s3-source',
                detected_scheme='s3',
                extra='storage',
                guidance=(
                    'Install boto3 directly or install the ETLPlus "storage" '
                    'extra. Required for "s3" storage paths.'
                ),
                missing_package='boto3',
                reason='s3 storage path requires boto3',
                role='source',
            ),
        ]

    def test_missing_requirement_rows_return_empty_when_requirements_are_satisfied(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test requirement rows when connectors either omit paths or have deps."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    format='csv',
                    name='pathless-source',
                    path=None,
                    type='file',
                ),
                SimpleNamespace(
                    format='nc',
                    name='nc-source',
                    path='input.nc',
                    type='file',
                ),
                SimpleNamespace(
                    format='sav',
                    name='sav-source',
                    path='input.sav',
                    type='file',
                ),
                SimpleNamespace(
                    name='queue-source',
                    queue_name='events',
                    service='redis',
                    type='queue',
                ),
            ],
        )

        rows = (
            readiness_connectors_mod.ConnectorReadinessPolicy.missing_requirement_rows(
                cfg=cast(Any, cfg),
                package_available=lambda _module_name: True,
            )
        )

        assert not rows

    def test_missing_requirement_rows_skip_database_provider_when_available(
        self,
    ) -> None:
        """Installed database-provider extras should not emit dependency rows."""
        cfg = _cfg(
            targets=[
                BIGQUERY_CASE.runtime_connector(
                    connection_string=None,
                    name='warehouse_bigquery',
                ),
            ],
        )

        rows = (
            readiness_connectors_mod.ConnectorReadinessPolicy.missing_requirement_rows(
                cfg=cast(Any, cfg),
                package_available=lambda _name: True,
            )
        )

        assert rows == []

    def test_requirement_row_adds_optional_format_and_scheme_context(
        self,
    ) -> None:
        """Requirement rows should keep both detected format and scheme fields."""
        requirement = RequirementSpec(
            ('pyarrow',),
            'pyarrow',
            'file',
        )

        row = readiness_connectors_mod.ConnectorReadinessPolicy.requirement_row(
            connector='out',
            detected_format='csv',
            detected_scheme='s3',
            reason='csv format requires pyarrow',
            requirement=requirement,
            role='target',
        )

        assert row == {
            'connector': 'out',
            'detected_format': 'csv',
            'detected_scheme': 's3',
            'extra': 'file',
            'guidance': (
                'Install pyarrow directly or install the ETLPlus "file" extra. '
                'Required for "csv" file format.'
            ),
            'missing_package': 'pyarrow',
            'reason': 'csv format requires pyarrow',
            'role': 'target',
        }

    def test_requirement_row_keeps_database_provider_context(
        self,
    ) -> None:
        """Requirement rows should preserve provider-specific DB context."""
        requirement = RequirementSpec(
            ('google.cloud.bigquery', 'sqlalchemy_bigquery'),
            'google-cloud-bigquery/sqlalchemy-bigquery',
            'database-bigquery',
        )

        row = readiness_connectors_mod.ConnectorReadinessPolicy.requirement_row(
            connector='warehouse',
            detected_database_provider='bigquery',
            reason=(
                'bigquery database connector requires '
                'google-cloud-bigquery/sqlalchemy-bigquery'
            ),
            requirement=requirement,
            role='source',
        )

        assert row == {
            'connector': 'warehouse',
            'detected_database_provider': 'bigquery',
            'extra': 'database-bigquery',
            'guidance': (
                'Install google-cloud-bigquery/sqlalchemy-bigquery directly or '
                'install the ETLPlus "database-bigquery" extra. Required for '
                '"bigquery" database connectors.'
            ),
            'missing_package': 'google-cloud-bigquery/sqlalchemy-bigquery',
            'reason': (
                'bigquery database connector requires '
                'google-cloud-bigquery/sqlalchemy-bigquery'
            ),
            'role': 'source',
        }

    def test_requirement_row_keeps_snowflake_provider_context(
        self,
    ) -> None:
        """Requirement rows should preserve Snowflake provider context."""
        requirement = RequirementSpec(
            ('snowflake.connector', 'snowflake.sqlalchemy'),
            'snowflake-connector-python/snowflake-sqlalchemy',
            'database-snowflake',
        )

        row = readiness_connectors_mod.ConnectorReadinessPolicy.requirement_row(
            connector='snowflake_wh',
            detected_database_provider='snowflake',
            reason=(
                'snowflake database connector requires '
                'snowflake-connector-python/snowflake-sqlalchemy'
            ),
            requirement=requirement,
            role='source',
        )

        assert row == {
            'connector': 'snowflake_wh',
            'detected_database_provider': 'snowflake',
            'extra': 'database-snowflake',
            'guidance': (
                'Install snowflake-connector-python/snowflake-sqlalchemy '
                'directly or install the ETLPlus "database-snowflake" extra. '
                'Required for "snowflake" database connectors.'
            ),
            'missing_package': 'snowflake-connector-python/snowflake-sqlalchemy',
            'reason': (
                'snowflake database connector requires '
                'snowflake-connector-python/snowflake-sqlalchemy'
            ),
            'role': 'source',
        }

    def test_requirement_row_keeps_format_without_scheme(
        self,
    ) -> None:
        """Requirement rows should omit scheme when only format context exists."""
        requirement = RequirementSpec(
            ('pyarrow',),
            'pyarrow',
            'file',
        )

        row = readiness_connectors_mod.ConnectorReadinessPolicy.requirement_row(
            connector='out',
            detected_format='csv',
            reason='csv format requires pyarrow',
            requirement=requirement,
            role='target',
        )

        assert row == {
            'connector': 'out',
            'detected_format': 'csv',
            'extra': 'file',
            'guidance': (
                'Install pyarrow directly or install the ETLPlus "file" extra. '
                'Required for "csv" file format.'
            ),
            'missing_package': 'pyarrow',
            'reason': 'csv format requires pyarrow',
            'role': 'target',
        }

    @pytest.mark.parametrize(
        ('available_modules', 'expected'),
        [
            ({'xarray', 'netCDF4'}, True),
            ({'xarray', 'h5netcdf'}, True),
            ({'h5netcdf'}, False),
        ],
    )
    def test_netcdf_available_requires_xarray_and_one_backend(
        self,
        monkeypatch: pytest.MonkeyPatch,
        available_modules: set[str],
        expected: bool,
    ) -> None:
        """Test netCDF availability resolution across supported backend combos."""
        assert (
            readiness_connectors_mod.ConnectorReadinessPolicy.netcdf_available(
                package_available=lambda module_name: module_name in available_modules,
            )
            is expected
        )
