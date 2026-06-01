"""
:mod:`tests.unit.connector.test_u_connector_core` module.

Unit tests for :mod:`etlplus.connector._core`.
"""

from __future__ import annotations

import pytest

from etlplus.connector._core import ConnectorProtocol

from .pytest_connector_support import CONNECTOR_CLASS_PARAMS
from .pytest_connector_support import ConnectorClass

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorProtocol:
    """Unit tests for connector protocol behavior."""

    @pytest.mark.parametrize(
        'connector_cls',
        CONNECTOR_CLASS_PARAMS,
    )
    def test_concrete_connector_satisfies_runtime_protocol(
        self,
        connector_cls: ConnectorClass,
    ) -> None:
        """
        Test that concrete connector dataclasses satisfy the runtime protocol.
        """
        connector = connector_cls.from_obj({'name': 'connector'})

        assert isinstance(connector, ConnectorProtocol)

    def test_protocol_placeholder_from_obj_raises_not_implemented(self) -> None:
        """The protocol placeholder should fail closed when called directly."""
        with pytest.raises(NotImplementedError):
            ConnectorProtocol.from_obj(  # pyright: ignore[reportAbstractUsage]
                {'name': 'payload'},
            )


class TestConnectorBaseContracts:
    """Shared contract tests for concrete connector base subclasses."""

    @pytest.mark.parametrize(
        'connector_cls',
        CONNECTOR_CLASS_PARAMS,
    )
    @pytest.mark.parametrize(
        'name_fields',
        [
            pytest.param({}, id='missing-name'),
            pytest.param({'name': None}, id='non-string-name'),
            pytest.param({'name': ''}, id='empty-name'),
            pytest.param({'name': '   '}, id='whitespace-name'),
        ],
    )
    def test_requires_name(
        self,
        connector_cls: ConnectorClass,
        name_fields: dict[str, object],
    ) -> None:
        """Connector constructors should reject missing or invalid names."""
        with pytest.raises(
            TypeError,
            match=f'{connector_cls.__name__} requires a "name"',
        ):
            connector_cls.from_obj(name_fields)

    @pytest.mark.parametrize(
        'connector_cls',
        CONNECTOR_CLASS_PARAMS,
    )
    def test_strips_name(
        self,
        connector_cls: ConnectorClass,
    ) -> None:
        """Connector constructors should strip accidental name whitespace."""
        connector = connector_cls.from_obj({'name': '  connector  '})

        assert connector.name == 'connector'
