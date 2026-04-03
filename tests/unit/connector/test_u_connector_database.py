"""
:mod:`tests.unit.connector.test_u_connector_database` module.

Unit tests for :mod:`etlplus.connector._database`.
"""

from __future__ import annotations

import pytest

from etlplus.connector._database import ConnectorDb
from etlplus.connector._enums import DataConnectorType

from .pytest_connector_support import assert_connector_fields

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorDb:
    """Unit tests for :class:`ConnectorDb`."""

    def test_from_obj_normalizes_database_fields(self) -> None:
        """
        Test that :meth:`from_obj` preserves standard database connector
        fields.
        """
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

        assert_connector_fields(
            connector,
            {
                'type': DataConnectorType.DATABASE,
                'name': 'warehouse',
                'connection_string': 'sqlite:///warehouse.db',
                'query': 'select * from events',
                'table': 'events',
                'mode': 'append',
            },
        )

    @pytest.mark.parametrize(
        'payload',
        [
            pytest.param({'type': 'database'}, id='missing-name'),
            pytest.param({'name': None, 'type': 'database'}, id='non-string-name'),
        ],
    )
    def test_from_obj_requires_name(
        self,
        payload: dict[str, object],
    ) -> None:
        """
        Test that :meth:`from_obj` rejects mappings with missing or invalid names.
        """
        with pytest.raises(TypeError, match='ConnectorDb requires a "name"'):
            ConnectorDb.from_obj(payload)
