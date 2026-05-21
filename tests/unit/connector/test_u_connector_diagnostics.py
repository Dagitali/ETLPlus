"""
:mod:`tests.unit.connector.test_u_connector_diagnostics` module.

Unit tests for :mod:`etlplus.connector._diagnostics`.
"""

from __future__ import annotations

from etlplus.connector import ConnectorDiagnosticPolicy

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorDiagnosticPolicy:
    """Unit tests for shared connector diagnostic wording."""

    def test_connector_type_guidance_treats_storage_scheme_as_file_path_hint(
        self,
    ) -> None:
        """Storage schemes used as connector types should get file guidance."""
        assert ConnectorDiagnosticPolicy.connector_type_guidance('s3') == (
            '"s3" is a storage scheme, not a connector type. '
            'Use connector type "file" and keep the provider in the path '
            'or URI scheme.'
        )

    def test_gap_guidance_reuses_provider_specific_database_wording(self) -> None:
        """Provider-specific DB issues should resolve through one policy seam."""
        assert ConnectorDiagnosticPolicy.gap_guidance(
            issue='missing connection_string or bigquery project/dataset',
        ) == (
            'Set "connection_string" to a database DSN or SQLAlchemy-style URL, '
            'or define both "project" and "dataset" for this BigQuery connector.'
        )
