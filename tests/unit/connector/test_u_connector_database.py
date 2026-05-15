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

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            pytest.param(
                {
                    'name': 'warehouse',
                    'type': 'database',
                    'provider': 'gcp-bigquery',
                    'project': 'analytics-project',
                    'dataset': 'warehouse',
                    'location': 'US',
                    'connection_string': 'sqlite:///warehouse.db',
                    'query': 'select * from events',
                    'table': 'events',
                    'mode': 'append',
                },
                {
                    'type': DataConnectorType.DATABASE,
                    'name': 'warehouse',
                    'provider': 'bigquery',
                    'project': 'analytics-project',
                    'dataset': 'warehouse',
                    'location': 'US',
                    'connection_string': 'sqlite:///warehouse.db',
                    'query': 'select * from events',
                    'table': 'events',
                    'mode': 'append',
                },
                id='provider-normalization',
            ),
            pytest.param(
                {
                    'name': 'warehouse',
                    'type': 'database',
                    'connection_string': 123,
                    'query': False,
                    'table': 456,
                    'mode': None,
                },
                {
                    'type': DataConnectorType.DATABASE,
                    'name': 'warehouse',
                    'connection_string': '123',
                    'provider': None,
                    'project': None,
                    'dataset': None,
                    'location': None,
                    'query': 'False',
                    'table': '456',
                    'mode': None,
                },
                id='coerces-optional-strings',
            ),
            pytest.param(
                {
                    'name': 'warehouse',
                    'type': 'database',
                    'project': 'analytics-project',
                    'dataset': 'warehouse',
                },
                {
                    'type': DataConnectorType.DATABASE,
                    'name': 'warehouse',
                    'connection_string': None,
                    'provider': 'bigquery',
                    'project': 'analytics-project',
                    'dataset': 'warehouse',
                    'location': None,
                    'query': None,
                    'table': None,
                    'mode': None,
                },
                id='infers-bigquery-provider',
            ),
        ],
    )
    def test_from_obj_normalizes_database_fields(
        self,
        payload: dict[str, object],
        expected: dict[str, object],
    ) -> None:
        """
        Test that :meth:`from_obj` preserves standard database connector
        fields.
        """
        connector = ConnectorDb.from_obj(payload)

        assert_connector_fields(connector, expected)

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
