"""
:mod:`tests.unit.connector.test_u_connector_init` module.

Unit tests for :mod:`etlplus.connector` package exports.
"""

from __future__ import annotations

import etlplus.connector as connector_pkg

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorPackageExports:
    """Unit tests for package-level exports."""

    def test_expected_symbols_are_exported(self) -> None:
        """Connector package should expose its documented public API."""
        expected = {
            'ConnectorApi',
            'ConnectorApiConfigDict',
            'ConnectorDb',
            'ConnectorDbConfigDict',
            'ConnectorFile',
            'ConnectorFileConfigDict',
            'Connector',
            'ConnectorBase',
            'ConnectorProtocol',
            'ConnectorType',
            'DataConnectorType',
            'parse_connector',
        }
        assert expected.issubset(set(connector_pkg.__all__))

    def test_exported_symbols_are_present(self) -> None:
        """Every exported package symbol should resolve as an attribute."""
        for name in connector_pkg.__all__:
            assert hasattr(connector_pkg, name)
