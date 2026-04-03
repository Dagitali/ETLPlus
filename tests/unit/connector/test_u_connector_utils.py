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
from etlplus.connector._enums import DataConnectorType

from .pytest_connector_support import assert_connector_fields

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

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
        Test that ``parse_connector`` dispatches to supported connector types.
        """

        connector = connector_utils.parse_connector(payload)
        assert isinstance(connector, expected_cls)
        assert_connector_fields(connector, expected_attrs)


class TestInternalLoadConnector:
    """Unit tests for :func:`etlplus.connector._utils._load_connector`."""

    @pytest.mark.parametrize(
        ('kind', 'expected'),
        [
            (DataConnectorType.API, ConnectorApi),
            (DataConnectorType.DATABASE, ConnectorDb),
            (DataConnectorType.FILE, ConnectorFile),
        ],
    )
    def test_load_connector_for_known_kinds(
        self,
        kind: DataConnectorType,
        expected: type[object],
    ) -> None:
        """Test that known connector kinds resolve to concrete classes."""
        assert connector_utils._load_connector(kind) is expected

    def test_load_connector_rejects_unknown_kind(self) -> None:
        """Test that unknown enum-like values raise :class:`TypeError`."""
        with pytest.raises(TypeError, match='Unsupported connector type'):
            connector_utils._load_connector(
                cast(DataConnectorType, 'unknown'),
            )
