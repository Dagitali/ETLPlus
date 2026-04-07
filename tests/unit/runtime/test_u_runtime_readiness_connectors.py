"""
:mod:`tests.unit.runtime.test_u_runtime_readiness_connectors` module.

Connector readiness unit tests for :mod:`etlplus.runtime._readiness`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

import etlplus.runtime._readiness as readiness_mod
import etlplus.runtime._readiness_connectors as readiness_connectors_mod

from .pytest_runtime_readiness import build_connector_gap_row as _connector_gap
from .pytest_runtime_readiness import (
    build_missing_requirement_row as _missing_requirement,
)
from .pytest_runtime_readiness import build_runtime_cfg as _cfg

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

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

        rows = readiness_connectors_mod.connector_gap_rows(cast(Any, cfg))

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

    def test_connector_gap_rows_cover_missing_required_connector_fields(
        self,
    ) -> None:
        """Test gap rows for missing path, API linkage, and DB connection data."""
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

        rows = readiness_connectors_mod.connector_gap_rows(cast(Any, cfg))

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

        rows = readiness_connectors_mod.connector_gap_rows(
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
                supported_types=['api', 'database', 'file'],
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

        rows = readiness_connectors_mod.connector_gap_rows(cast(Any, cfg))

        assert not rows

    def test_connector_gap_rows_tolerate_unexpected_coerced_type_values(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test defensive fallthrough when the connector-type seam misbehaves."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    name='weird-source',
                    path='input.csv',
                    type='file',
                ),
                SimpleNamespace(
                    name='normal-source',
                    path='input.csv',
                    type='file',
                ),
            ],
        )
        calls = {'count': 0}

        def _connector_type(_connector_type: str) -> object:
            calls['count'] += 1
            if calls['count'] == 1:
                return object()
            return readiness_mod.DataConnectorType.FILE

        monkeypatch.setattr(
            readiness_connectors_mod,
            '_connector_type',
            _connector_type,
        )

        rows = readiness_connectors_mod.connector_gap_rows(cast(Any, cfg))

        assert not rows

    def test_connector_readiness_checks_report_all_ok_states(self) -> None:
        """Test readiness rows when gaps and optional dependency gaps are absent."""
        cfg = _cfg()

        checks = readiness_mod.ReadinessReportBuilder.connector_readiness_checks(
            cast(Any, cfg),
        )

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
            readiness_connectors_mod,
            'connector_gap_rows',
            lambda _cfg: [{'connector': 'bad-source'}],
        )
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'missing_requirement_rows',
            lambda *, cfg: [{'connector': 'bad-source', 'missing_package': 'boto3'}],
        )

        checks = readiness_mod.ReadinessReportBuilder.connector_readiness_checks(
            cast(Any, cfg),
        )

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
            'Set type to one of: api, database, file.'
        )
        assert readiness_connectors_mod.connector_type_guidance('weird') == (
            'Use one of the supported connector types: api, database, file.'
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

        rows = readiness_mod.ReadinessReportBuilder.dedupe_rows(
            [row, dict(row), {**row, 'connector': 'source-b'}],
        )

        assert rows == [row, {**row, 'connector': 'source-b'}]

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param('', None, id='blank'),
            pytest.param('s3', 's3', id='known-scheme'),
            pytest.param('not-a-real-scheme', None, id='unknown-scheme'),
        ],
    )
    def test_connector_helper_module_coerces_connector_storage_scheme(
        self,
        value: str,
        expected: str | None,
    ) -> None:
        """Connector helper module should normalize connector storage schemes."""
        assert (
            readiness_connectors_mod.coerce_connector_storage_scheme(value) == expected
        )

    @pytest.mark.parametrize(
        ('path', 'expected'),
        [
            pytest.param('local/path.csv', None, id='local-path'),
            pytest.param('://missing', None, id='missing-scheme'),
            pytest.param('custom://bucket/input.csv', 'custom', id='custom-scheme'),
        ],
    )
    def test_connector_helper_module_coerces_storage_scheme(
        self,
        path: str,
        expected: str | None,
    ) -> None:
        """Connector helper module should normalize storage schemes from paths."""
        assert readiness_connectors_mod.coerce_storage_scheme(path) == expected

    @pytest.mark.parametrize(
        ('issue', 'api_reference', 'expected'),
        [
            pytest.param(
                'unknown api reference: missing-api',
                None,
                'Define the referenced API under top-level "apis".',
                id='unknown-api-without-reference',
            ),
            pytest.param('unhandled', None, None, id='fallback-none'),
        ],
    )
    def test_connector_helper_module_private_guidance_covers_fallbacks(
        self,
        issue: str,
        api_reference: str | None,
        expected: str | None,
    ) -> None:
        """Private connector guidance should still handle internal fallback cases."""
        assert (
            readiness_connectors_mod._connector_gap_guidance(
                api_reference=api_reference,
                issue=issue,
            )
            == expected
        )

    def test_connector_helper_module_private_dedupe_preserves_first_occurrence(
        self,
    ) -> None:
        """Private connector dedupe helper should drop later duplicate rows."""
        row = {
            'connector': 'source-a',
            'role': 'source',
            'missing_package': 'boto3',
            'reason': 's3 storage path requires boto3',
            'extra': 'storage',
        }

        rows = readiness_connectors_mod._dedupe_rows(
            [row, dict(row), {**row, 'connector': 'source-b'}],
        )

        assert rows == [row, {**row, 'connector': 'source-b'}]

    def test_connector_helper_module_private_missing_requirement_guidance_plain_hint(
        self,
    ) -> None:
        """Private dependency guidance should fall back to a plain install hint."""
        assert (
            readiness_connectors_mod._missing_requirement_guidance(
                package='tables',
                extra=None,
            )
            == 'Install tables.'
        )

    def test_missing_requirement_guidance_returns_plain_install_hint_without_context(
        self,
    ) -> None:
        """Missing-dependency guidance should fall back to one plain install hint."""
        assert (
            readiness_mod.ReadinessReportBuilder.missing_requirement_guidance(
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

        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'requirement_available',
            lambda requirement: requirement.package == 'boto3',
        )
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'netcdf_available',
            lambda: False,
        )

        rows = readiness_mod.ReadinessReportBuilder.missing_requirement_rows(
            cfg=cast(Any, cfg),
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

    def test_missing_requirement_rows_respects_package_available_seam(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that class-based dependency checks still honor wrapper patches."""
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
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

        rows = readiness_mod.ReadinessReportBuilder.missing_requirement_rows(
            cfg=cast(Any, cfg),
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
            ],
        )

        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'netcdf_available',
            lambda: True,
        )
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'requirement_available',
            lambda requirement: True,
        )

        rows = readiness_mod.ReadinessReportBuilder.missing_requirement_rows(
            cfg=cast(Any, cfg),
        )

        assert not rows

    def test_requirement_row_adds_optional_format_and_scheme_context(
        self,
    ) -> None:
        """Requirement rows should keep both detected format and scheme fields."""
        requirement = readiness_mod._RequirementSpec(('pyarrow',), 'pyarrow', 'file')

        row = readiness_mod.ReadinessReportBuilder.requirement_row(
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

    def test_requirement_row_keeps_format_without_scheme(
        self,
    ) -> None:
        """Requirement rows should omit scheme when only format context exists."""
        requirement = readiness_mod._RequirementSpec(('pyarrow',), 'pyarrow', 'file')

        row = readiness_mod.ReadinessReportBuilder.requirement_row(
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
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'package_available',
            lambda module_name: module_name in available_modules,
        )

        assert readiness_mod.ReadinessReportBuilder.netcdf_available() is expected
