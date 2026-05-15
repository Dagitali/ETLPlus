"""
:mod:`tests.unit.connector.test_u_connector_utils` module.

Unit tests for :mod:`etlplus.connector._utils`.

Notes
-----
- Uses minimal ``dict`` payloads.
"""

from __future__ import annotations

from typing import Any
from typing import cast

import pytest

import etlplus.connector._utils as connector_utils
from etlplus.connector import ConnectorApi
from etlplus.connector import ConnectorDb
from etlplus.connector import ConnectorFile
from etlplus.connector import ConnectorQueue
from tests.pytest_shared_support import get_cloud_database_provider_case

from .pytest_connector_support import assert_connector_fields
from .pytest_connector_support import get_queue_connector_provider_case

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


AWS_SQS_CASE = get_queue_connector_provider_case('aws-sqs')
BIGQUERY_CASE = get_cloud_database_provider_case('bigquery')
SNOWFLAKE_CASE = get_cloud_database_provider_case('snowflake')


# SECTION: TESTS ============================================================ #


class TestParseConnector:
    """
    Unit tests for :func:`parse_connector`.

    Notes
    -----
    Tests error handling for unsupported connector types and missing fields.
    """

    @pytest.mark.parametrize(
        ('payload', 'match'),
        [
            pytest.param(
                {'name': 'missing_type'},
                'requires a "type"',
                id='missing-type',
            ),
            pytest.param(None, 'must be a mapping', id='none'),
            pytest.param(123, 'must be a mapping', id='integer'),
            pytest.param('not a mapping', 'must be a mapping', id='string'),
            pytest.param(
                {'name': 'x', 'type': 'unknown'},
                'Unsupported connector type',
                id='unsupported-type-with-name',
            ),
            pytest.param(
                {'type': 'unknown'},
                'Unsupported connector type',
                id='unsupported-type-without-name',
            ),
        ],
    )
    def test_invalid_payloads_raise_type_error(
        self,
        payload: object,
        match: str,
    ) -> None:
        """Invalid connector payloads should raise :class:`TypeError`."""
        with pytest.raises(TypeError, match=match):
            connector_utils.parse_connector(
                cast(dict[str, Any], payload),
            )

    @pytest.mark.parametrize(
        ('payload', 'expected_cls', 'expected_attrs'),
        [
            pytest.param(
                {
                    'name': 'input_json',
                    'type': 'file',
                    'path': '/tmp/in.json',
                    'format': 'json',
                },
                ConnectorFile,
                {
                    'name': 'input_json',
                    'path': '/tmp/in.json',
                    'format': 'json',
                },
                id='file',
            ),
            pytest.param(
                BIGQUERY_CASE.connector_payload(
                    name='warehouse',
                    table='events',
                )
                | {'engine': 'sqlite'},
                ConnectorDb,
                {
                    'name': 'warehouse',
                    'provider': 'bigquery',
                    'project': BIGQUERY_CASE.metadata['project'],
                    'dataset': BIGQUERY_CASE.metadata['dataset'],
                    'table': 'events',
                },
                id='database-provider',
            ),
            pytest.param(
                SNOWFLAKE_CASE.connector_payload(
                    use_alias=True,
                    name='snowflake-events',
                    table='events',
                ),
                ConnectorDb,
                {
                    'name': 'snowflake-events',
                    'provider': 'snowflake',
                    'account': SNOWFLAKE_CASE.metadata['account'],
                    'database': SNOWFLAKE_CASE.metadata['database'],
                    'schema': SNOWFLAKE_CASE.metadata['schema'],
                    'warehouse': SNOWFLAKE_CASE.metadata['warehouse'],
                    'table': 'events',
                },
                id='database-snowflake-provider',
            ),
            pytest.param(
                {
                    'name': 'github',
                    'type': 'api',
                    'api': 'gh',
                    'endpoint': 'issues',
                },
                ConnectorApi,
                {'name': 'github', 'api': 'gh', 'endpoint': 'issues'},
                id='api',
            ),
            pytest.param(
                AWS_SQS_CASE.connector_payload(type='sqs'),
                ConnectorQueue,
                AWS_SQS_CASE.expected_connector_attrs(),
                id='queue',
            ),
        ],
    )
    def test_supported_connector_types_parse_successfully(
        self,
        payload: dict[str, object],
        expected_cls: type,
        expected_attrs: dict[str, object],
    ) -> None:
        """
        Test that ``parse_connector`` dispatches to supported connector types.
        """

        connector = connector_utils.parse_connector(payload)
        assert isinstance(connector, expected_cls)
        assert_connector_fields(connector, expected_attrs)
