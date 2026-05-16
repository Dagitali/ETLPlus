"""
:mod:`tests.unit.connector.test_u_connector_database` module.

Unit tests for :mod:`etlplus.connector._database`.
"""

from __future__ import annotations

import pytest

from etlplus.connector._database import ConnectorDb
from etlplus.connector._enums import DataConnectorType
from tests.pytest_shared_support import get_cloud_database_provider_case

from .pytest_connector_support import assert_connector_fields

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


BIGQUERY_CASE = get_cloud_database_provider_case('bigquery')
SNOWFLAKE_CASE = get_cloud_database_provider_case('snowflake')


# SECTION: TESTS ============================================================ #


class TestConnectorDb:
    """Unit tests for :class:`ConnectorDb`."""

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            pytest.param(
                BIGQUERY_CASE.connector_payload(
                    use_alias=True,
                    name='warehouse',
                    connection_string='sqlite:///warehouse.db',
                    query='select * from events',
                    table='events',
                    mode='append',
                ),
                BIGQUERY_CASE.expected_connector_attrs(
                    name='warehouse',
                    connection_string='sqlite:///warehouse.db',
                    query='select * from events',
                    table='events',
                    mode='append',
                )
                | {'type': DataConnectorType.DATABASE},
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
                    'account': None,
                    'database': None,
                    'schema': None,
                    'warehouse': None,
                    'query': 'False',
                    'table': '456',
                    'mode': None,
                },
                id='coerces-optional-strings',
            ),
            pytest.param(
                BIGQUERY_CASE.connector_payload(
                    include_provider=False,
                    omit_fields=('location',),
                    name='warehouse',
                ),
                BIGQUERY_CASE.expected_connector_attrs(
                    name='warehouse',
                    omit_fields=('location',),
                )
                | {'type': DataConnectorType.DATABASE},
                id='infers-bigquery-provider',
            ),
            pytest.param(
                BIGQUERY_CASE.connector_payload(
                    include_provider=False,
                    omit_fields=('location',),
                    name='warehouse',
                )
                | {'provider': None, 'engine': 'gcp-bigquery'},
                BIGQUERY_CASE.expected_connector_attrs(
                    name='warehouse',
                    omit_fields=('location',),
                )
                | {'type': DataConnectorType.DATABASE},
                id='engine-used-when-provider-empty',
            ),
            pytest.param(
                SNOWFLAKE_CASE.connector_payload(
                    use_alias=True,
                    name='snowflake_wh',
                    table='events',
                ),
                SNOWFLAKE_CASE.expected_connector_attrs(
                    name='snowflake_wh',
                    table='events',
                )
                | {'type': DataConnectorType.DATABASE},
                id='snowflake-provider-normalization',
            ),
            pytest.param(
                SNOWFLAKE_CASE.connector_payload(
                    include_provider=False,
                    omit_fields=('warehouse',),
                    name='snowflake_wh',
                ),
                SNOWFLAKE_CASE.expected_connector_attrs(
                    name='snowflake_wh',
                    omit_fields=('warehouse',),
                )
                | {'type': DataConnectorType.DATABASE},
                id='infers-snowflake-provider',
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

    @pytest.mark.parametrize(
        ('payload', 'expected_missing_fields'),
        [
            pytest.param(
                {
                    'provider': None,
                    'engine': 'gcp-bigquery',
                    'project': 'analytics-project',
                },
                ('dataset',),
                id='mapping-uses-engine-when-provider-empty',
            ),
            pytest.param(
                {
                    'provider': None,
                    'account': 'acme.us-east-1',
                    'database': 'analytics',
                },
                ('schema',),
                id='mapping-uses-hint-fields-when-provider-empty',
            ),
        ],
    )
    def test_missing_provider_fields_infers_provider_for_raw_mapping(
        self,
        payload: dict[str, object],
        expected_missing_fields: tuple[str, ...],
    ) -> None:
        """Provider field detection should match raw mapping inference rules."""
        assert ConnectorDb.missing_provider_fields(payload) == expected_missing_fields
