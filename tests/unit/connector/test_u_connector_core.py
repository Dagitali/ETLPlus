"""
:mod:`tests.unit.connector.test_u_connector_core` module.

Unit tests for :mod:`etlplus.connector._core`.
"""

from __future__ import annotations

import pytest

from etlplus.connector import ConnectorApi
from etlplus.connector import ConnectorDb
from etlplus.connector import ConnectorFile
from etlplus.connector import ConnectorQueue
from etlplus.connector._core import ConnectorProtocol

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


type ConnectorClass = (
    type[ConnectorApi] | type[ConnectorDb] | type[ConnectorFile] | type[ConnectorQueue]
)

CONNECTOR_CASES = (
    pytest.param(ConnectorApi, 'api', id='api'),
    pytest.param(ConnectorDb, 'database', id='database'),
    pytest.param(ConnectorFile, 'file', id='file'),
    pytest.param(ConnectorQueue, 'queue', id='queue'),
)


# SECTION: TESTS ============================================================ #


class TestConnectorProtocol:
    """Unit tests for connector protocol behavior."""

    @pytest.mark.parametrize(('connector_cls', 'connector_type'), CONNECTOR_CASES)
    def test_concrete_connector_satisfies_runtime_protocol(
        self,
        connector_cls: ConnectorClass,
        connector_type: str,
    ) -> None:
        """
        Test that concrete connector dataclasses satisfy the runtime protocol.
        """
        connector = connector_cls.from_obj(
            {'name': f'{connector_type}_connector', 'type': connector_type},
        )

        assert isinstance(connector, ConnectorProtocol)

    def test_protocol_placeholder_from_obj_raises_not_implemented(self) -> None:
        """The protocol placeholder should fail closed when called directly."""
        with pytest.raises(NotImplementedError):
            ConnectorProtocol.from_obj(  # pyright: ignore[reportAbstractUsage]
                {'name': 'payload'},
            )


class TestConnectorBaseContracts:
    """Shared contract tests for concrete connector base subclasses."""

    @pytest.mark.parametrize(('connector_cls', 'connector_type'), CONNECTOR_CASES)
    @pytest.mark.parametrize(
        'name_fields',
        [
            pytest.param({}, id='missing-name'),
            pytest.param({'name': None}, id='non-string-name'),
        ],
    )
    def test_requires_name(
        self,
        connector_cls: ConnectorClass,
        connector_type: str,
        name_fields: dict[str, object],
    ) -> None:
        """Connector constructors should reject missing or invalid names."""
        with pytest.raises(
            TypeError,
            match=f'{connector_cls.__name__} requires a "name"',
        ):
            connector_cls.from_obj({'type': connector_type} | name_fields)
