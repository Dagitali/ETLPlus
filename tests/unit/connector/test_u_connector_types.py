"""
:mod:`tests.unit.connector.test_u_connector_types` module.

Unit tests for :mod:`etlplus.connector.types`.
"""

from __future__ import annotations

from etlplus.connector import types as connector_types
from etlplus.connector.enums import DataConnectorType

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorTypes:
    """Unit tests for connector type alias module."""

    def test_alias_accepts_enum_and_literal_values(self) -> None:
        """ConnectorType alias should support enum members and literals."""

        def identity(value: connector_types.ConnectorType) -> str:
            return str(value)

        assert identity(DataConnectorType.API) == 'api'
        assert identity('database') == 'database'
        assert identity('file') == 'file'

    def test_exports_include_connector_type(self) -> None:
        """Module should export ConnectorType alias."""
        assert connector_types.__all__ == ['ConnectorType']
