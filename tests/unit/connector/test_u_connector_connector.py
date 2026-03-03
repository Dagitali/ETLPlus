"""
:mod:`tests.unit.connector.test_u_connector_connector` module.

Unit tests for :mod:`etlplus.connector.connector`.
"""

from __future__ import annotations

from etlplus.connector.api import ConnectorApi
from etlplus.connector.connector import Connector
from etlplus.connector.database import ConnectorDb
from etlplus.connector.file import ConnectorFile

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorAlias:
    """Unit tests for connector union alias module."""

    def test_connector_alias_supports_all_variants(self) -> None:
        """Connector alias should accept API, DB, and file connector types."""
        connectors: list[Connector] = [
            ConnectorApi(name='a'),
            ConnectorDb(name='b'),
            ConnectorFile(name='c'),
        ]

        assert isinstance(connectors[0], ConnectorApi)
        assert isinstance(connectors[1], ConnectorDb)
        assert isinstance(connectors[2], ConnectorFile)

    def test_module_exports_connector_alias(self) -> None:
        """Connector module should only export the Connector alias."""
        from etlplus.connector import connector as connector_mod

        assert connector_mod.__all__ == ['Connector']
