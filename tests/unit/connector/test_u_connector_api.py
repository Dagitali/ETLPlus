"""
:mod:`tests.unit.connector.test_u_connector_api` module.

Unit tests for :mod:`etlplus.connector._api`.
"""

from __future__ import annotations

import pytest

from etlplus.connector._api import ConnectorApi
from etlplus.connector._enums import DataConnectorType

from .pytest_connector_support import assert_connector_fields

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorApi:
    """Unit tests for :class:`ConnectorApi`."""

    @pytest.mark.parametrize(
        ('payload', 'expected', 'pagination_start_page', 'rate_limit_sleep_seconds'),
        [
            pytest.param(
                {
                    'name': 'users_api',
                    'type': 'api',
                    'url': '  https://example.test/users  ',
                    'method': '  GET  ',
                    'headers': {1: True},
                    'query_params': [('bad', 'shape')],
                    'pagination': {'type': 'page', 'start_page': '2'},
                    'rate_limit': {'sleep_seconds': '0.2'},
                    'service': '  people  ',
                    'endpoint': '  users  ',
                },
                {
                    'type': DataConnectorType.API,
                    'name': 'users_api',
                    'url': 'https://example.test/users',
                    'method': 'GET',
                    'headers': {'1': 'True'},
                    'query_params': {},
                    'api': 'people',
                    'endpoint': 'users',
                },
                2,
                0.2,
                id='service-alias-and-nested-configs',
            ),
            pytest.param(
                {
                    'name': 'users_api',
                    'type': 'api',
                    'api': 'canonical',
                    'service': 'fallback',
                },
                {
                    'type': DataConnectorType.API,
                    'name': 'users_api',
                    'api': 'canonical',
                    'url': None,
                    'method': None,
                    'headers': {},
                    'query_params': {},
                    'endpoint': None,
                },
                None,
                None,
                id='api-field-precedes-service-alias',
            ),
            pytest.param(
                {
                    'name': 'users_api',
                    'type': 'api',
                    'api': None,
                    'service': 'fallback',
                },
                {
                    'type': DataConnectorType.API,
                    'name': 'users_api',
                    'api': 'fallback',
                    'url': None,
                    'method': None,
                    'headers': {},
                    'query_params': {},
                    'endpoint': None,
                },
                None,
                None,
                id='service-used-when-api-empty',
            ),
            pytest.param(
                {
                    'name': 'users_api',
                    'type': 'api',
                    'api': '   ',
                    'service': 'fallback',
                },
                {
                    'type': DataConnectorType.API,
                    'name': 'users_api',
                    'api': 'fallback',
                    'url': None,
                    'method': None,
                    'headers': {},
                    'query_params': {},
                    'endpoint': None,
                },
                None,
                None,
                id='service-used-when-api-blank',
            ),
            pytest.param(
                {
                    'name': 'users_api',
                    'type': 'api',
                    'url': 123,
                    'method': False,
                    'api': 456,
                    'endpoint': 789,
                },
                {
                    'type': DataConnectorType.API,
                    'name': 'users_api',
                    'url': '123',
                    'method': 'False',
                    'headers': {},
                    'query_params': {},
                    'api': '456',
                    'endpoint': '789',
                },
                None,
                None,
                id='coerces-optional-strings',
            ),
        ],
    )
    def test_from_obj_normalizes_api_connector_fields(
        self,
        payload: dict[str, object],
        expected: dict[str, object],
        pagination_start_page: int | None,
        rate_limit_sleep_seconds: float | None,
    ) -> None:
        """
        Test that :meth:`from_obj` normalizes connector fields, aliases, and
        nested config payloads.
        """
        connector = ConnectorApi.from_obj(payload)

        assert_connector_fields(connector, expected)
        assert (
            getattr(connector.pagination, 'start_page', None) == pagination_start_page
        )
        assert (
            getattr(connector.rate_limit, 'sleep_seconds', None)
            == rate_limit_sleep_seconds
        )
