"""
:mod:`tests.unit.connector.test_u_connector_utils` module.

Unit tests for :mod:`etlplus.connector.utils`.

Notes
-----
- Uses minimal ``dict`` payloads.
"""

from __future__ import annotations

import pytest

from etlplus.connector import ConnectorApi
from etlplus.connector import ConnectorDb
from etlplus.connector import ConnectorFile
from etlplus.connector import parse_connector

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestParseConnector:
    """
    Unit test suite for :func:`parse_connector`.

    Notes
    -----
    Tests error handling for unsupported connector types and missing fields.
    """

    @pytest.mark.parametrize(
        'payload,expected_exception',
        [
            ({'name': 'x', 'type': 'unknown'}, TypeError),
            ({'type': 'unknown'}, TypeError),
        ],
        ids=['unsupported_type', 'missing_name'],
    )
    def test_unsupported_type_raises(
        self,
        payload: dict[str, object],
        expected_exception: type[Exception],
    ) -> None:
        """
        Test that unsupported connector types raise the expected exception.

        Parameters
        ----------
        payload : dict[str, object]
            Connector payload to test.
        expected_exception : type[Exception]
            Expected exception type.
        """
        with pytest.raises(expected_exception):
            parse_connector(payload)

    @pytest.mark.parametrize(
        'payload,expected_cls,expected_attrs',
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
                {
                    'name': 'warehouse',
                    'type': 'database',
                    'table': 'events',
                    'engine': 'sqlite',
                },
                ConnectorDb,
                {'name': 'warehouse', 'table': 'events'},
                id='database',
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
        ],
    )
    def test_supported_connector_types_parse_successfully(
        self,
        payload: dict[str, object],
        expected_cls: type,
        expected_attrs: dict[str, object],
    ) -> None:
        """
        Test that ``parse_connector`` instantiates supported connector types.
        """

        connector = parse_connector(payload)
        assert isinstance(connector, expected_cls)
        for field, value in expected_attrs.items():
            assert getattr(connector, field) == value
