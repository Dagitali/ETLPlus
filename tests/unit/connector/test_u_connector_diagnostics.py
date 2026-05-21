"""
:mod:`tests.unit.connector.test_u_connector_diagnostics` module.

Unit tests for :mod:`etlplus.connector._diagnostics`.
"""

from __future__ import annotations

import pytest

from etlplus.connector import ConnectorDiagnosticPolicy
from etlplus.connector import DataConnectorType

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorDiagnosticPolicy:
    """Unit tests for shared connector diagnostic wording."""

    def test_connector_type_choices_returns_supported_connector_types(self) -> None:
        """Connector type choices should match the connector enum surface."""
        assert ConnectorDiagnosticPolicy.connector_type_choices() == (
            DataConnectorType.choices()
        )

    @pytest.mark.parametrize(
        ('connector_type', 'expected'),
        [
            pytest.param(
                '',
                'Set type to one of: api, database, file, queue.',
                id='blank-type',
            ),
            pytest.param(
                'stream',
                'Use one of the supported connector types: api, database, file, queue.',
                id='unsupported-type',
            ),
            pytest.param(
                's3',
                (
                    '"s3" is a storage scheme, not a connector type. '
                    'Use connector type "file" and keep the provider in the path '
                    'or URI scheme.'
                ),
                id='storage-scheme',
            ),
        ],
    )
    def test_connector_type_guidance_covers_supported_branches(
        self,
        connector_type: str,
        expected: str,
    ) -> None:
        """Unsupported connector type guidance should be actionable."""
        assert ConnectorDiagnosticPolicy.connector_type_guidance(connector_type) == (
            expected
        )

    @pytest.mark.parametrize(
        ('issue', 'api_reference', 'expected'),
        [
            pytest.param(
                'missing path',
                None,
                'Set "path" to a local path or storage URI for this file connector.',
                id='static-file-guidance',
            ),
            pytest.param(
                'missing connection_string or bigquery project/dataset',
                None,
                (
                    'Set "connection_string" to a database DSN or SQLAlchemy-style '
                    'URL, or define both "project" and "dataset" for this '
                    'BigQuery connector.'
                ),
                id='provider-specific-database-guidance',
            ),
            pytest.param(
                'unknown api reference: people',
                'people',
                (
                    'Define "people" under top-level "apis" or update the '
                    'connector "api" reference.'
                ),
                id='unknown-api-reference-with-name',
            ),
            pytest.param(
                'unknown api reference: people',
                None,
                'Define the referenced API under top-level "apis".',
                id='unknown-api-reference-without-name',
            ),
            pytest.param('unmapped issue', None, None, id='unmapped-issue'),
        ],
    )
    def test_gap_guidance_covers_supported_branches(
        self,
        issue: str,
        api_reference: str | None,
        expected: str | None,
    ) -> None:
        """Connector gap guidance should cover static and dynamic policies."""
        assert (
            ConnectorDiagnosticPolicy.gap_guidance(
                issue=issue,
                api_reference=api_reference,
            )
            == expected
        )

    def test_static_entry_shape_guidance_helpers(self) -> None:
        """Static entry-shape helpers should keep concise remediation wording."""
        assert ConnectorDiagnosticPolicy.invalid_entry_guidance() == (
            'Define each connector as a mapping with at least "name" and "type" '
            'fields.'
        )
        assert (
            ConnectorDiagnosticPolicy.missing_name_guidance()
            == 'Set "name" to a non-empty string.'
        )
