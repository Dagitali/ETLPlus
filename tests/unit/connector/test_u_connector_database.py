"""
:mod:`tests.unit.connector.test_u_connector_database` module.

Unit tests for :mod:`etlplus.connector.database`.
"""

from __future__ import annotations

import pytest

from etlplus.connector.database import ConnectorDb
from etlplus.connector.enums import DataConnectorType

# SECTION: TESTS ============================================================ #


class TestConnectorDb:
    """Unit tests for :class:`ConnectorDb`."""

    def test_from_obj_parses_database_fields(self) -> None:
        """from_obj should parse standard database connector fields."""
        connector = ConnectorDb.from_obj(
            {
                'name': 'warehouse',
                'type': 'database',
                'connection_string': 'sqlite:///warehouse.db',
                'query': 'select * from events',
                'table': 'events',
                'mode': 'append',
            },
        )

        assert connector.type is DataConnectorType.DATABASE
        assert connector.name == 'warehouse'
        assert connector.connection_string == 'sqlite:///warehouse.db'
        assert connector.query == 'select * from events'
        assert connector.table == 'events'
        assert connector.mode == 'append'

    def test_from_obj_requires_name(self) -> None:
        """from_obj should reject mappings without a valid name."""
        with pytest.raises(TypeError, match='ConnectorDb requires a "name"'):
            ConnectorDb.from_obj({'type': 'database'})
