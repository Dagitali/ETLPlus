"""
:mod:`tests.unit.connector.test_u_connector_api` module.

Unit tests for :mod:`etlplus.connector.api`.
"""

from __future__ import annotations

import pytest

from etlplus.connector.api import ConnectorApi
from etlplus.connector.enums import DataConnectorType

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorApi:
    """Unit tests for :class:`ConnectorApi`."""

    def test_from_obj_parses_service_alias_and_nested_configs(self) -> None:
        """
        Test that :meth:`from_obj` supports service alias and nested API
        settings.
        """
        connector = ConnectorApi.from_obj(
            {
                'name': 'users_api',
                'type': 'api',
                'url': 'https://example.test/users',
                'method': 'GET',
                'headers': {1: True},
                'query_params': [('bad', 'shape')],
                'pagination': {'type': 'page', 'start_page': '2'},
                'rate_limit': {'sleep_seconds': '0.2'},
                'service': 'people',
                'endpoint': 'users',
            },
        )

        assert connector.type is DataConnectorType.API
        assert connector.name == 'users_api'
        assert connector.url == 'https://example.test/users'
        assert connector.method == 'GET'
        assert connector.headers == {'1': 'True'}
        assert not connector.query_params
        assert connector.api == 'people'
        assert connector.endpoint == 'users'
        assert connector.pagination is not None
        assert connector.pagination.start_page == 2
        assert connector.rate_limit is not None
        assert connector.rate_limit.sleep_seconds == 0.2

    def test_from_obj_requires_name(self) -> None:
        """
        Test that :meth:`from_obj` rejects mappings without a valid name.
        """
        with pytest.raises(TypeError, match='ConnectorApi requires a "name"'):
            ConnectorApi.from_obj({'type': 'api'})
