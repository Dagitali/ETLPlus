"""
:mod:`tests.unit.connector.test_u_connector_connector` module.

Unit tests for :mod:`etlplus.connector._connector`.
"""

from __future__ import annotations

import pytest

import etlplus.connector._connector as connector_mod
from etlplus.connector._connector import Connector

from .pytest_connector_support import CONNECTOR_CLASS_PARAMS
from .pytest_connector_support import ConnectorClass

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorAlias:
    """Unit tests for connector union alias exports."""

    @pytest.mark.parametrize(
        'connector_cls',
        CONNECTOR_CLASS_PARAMS,
    )
    def test_connector_alias_supports_all_variants(
        self,
        connector_cls: ConnectorClass,
    ) -> None:
        """
        Test that the :class:`Connector` alias accepts all supported variants.
        """
        connector: Connector = connector_cls(name='connector')

        assert isinstance(connector, connector_cls)

    def test_module_exports_connector_alias(self) -> None:
        """
        Test that the module only exports the :class:`Connector` alias.
        """
        assert tuple(connector_mod.__all__) == ('Connector',)
