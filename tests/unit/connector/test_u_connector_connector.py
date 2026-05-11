"""
:mod:`tests.unit.connector.test_u_connector_connector` module.

Unit tests for :mod:`etlplus.connector._connector`.
"""

from __future__ import annotations

import pytest

import etlplus.connector._connector as connector_mod
from etlplus.connector import ConnectorApi
from etlplus.connector import ConnectorDb
from etlplus.connector import ConnectorFile
from etlplus.connector import ConnectorQueue
from etlplus.connector._connector import Connector

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorAlias:
    """Unit tests for connector union alias exports."""

    @pytest.mark.parametrize(
        ('connector', 'expected_cls'),
        [
            pytest.param(ConnectorApi(name='a'), ConnectorApi, id='api'),
            pytest.param(ConnectorDb(name='b'), ConnectorDb, id='database'),
            pytest.param(ConnectorFile(name='c'), ConnectorFile, id='file'),
            pytest.param(ConnectorQueue(name='q'), ConnectorQueue, id='queue'),
        ],
    )
    def test_connector_alias_supports_all_variants(
        self,
        connector: Connector,
        expected_cls: type[object],
    ) -> None:
        """
        Test that the :class:`Connector` alias accepts all supported variants.
        """
        assert isinstance(connector, expected_cls)

    def test_module_exports_connector_alias(self) -> None:
        """
        Test that the module only exports the :class:`Connector` alias.
        """
        assert connector_mod.__all__ == ['Connector']
