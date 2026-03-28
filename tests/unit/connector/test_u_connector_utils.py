"""
:mod:`tests.unit.connector.test_u_connector_utils` module.

Unit tests for :mod:`etlplus.connector._utils`.

Notes
-----
- Uses minimal ``dict`` payloads.
"""

from __future__ import annotations

from typing import cast

import pytest

import etlplus.connector._utils as connector_utils
from etlplus.connector import ConnectorApi
from etlplus.connector import ConnectorDb
from etlplus.connector import ConnectorFile
from etlplus.connector import parse_connector
from etlplus.connector._enums import DataConnectorType

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

    def test_missing_type_raises(self) -> None:
        """
        Test that connector payloads without ``type`` raise :class:`TypeError`.
        """
        with pytest.raises(TypeError, match='requires a "type"'):
            parse_connector({'name': 'missing_type'})

    @pytest.mark.parametrize(
        'payload',
        [None, 123, 'not a mapping'],
    )
    def test_non_mapping_raises(
        self,
        payload: object,
    ) -> None:
        """Test that non-mapping payloads raise :class:`TypeError`."""
        with pytest.raises(TypeError, match='must be a mapping'):
            parse_connector(payload)  # type: ignore[arg-type]

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
        Test that ``parse_connector`` instantiates supported connector types.
        """

        connector = parse_connector(payload)
        assert isinstance(connector, expected_cls)
        for field, value in expected_attrs.items():
            assert getattr(connector, field) == value

    @pytest.mark.parametrize(
        ('payload', 'expected_exception'),
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
