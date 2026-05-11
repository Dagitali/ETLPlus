"""
:mod:`tests.unit.connector.test_u_connector_init` module.

Unit tests for :mod:`etlplus.connector` package facade exports.
"""

from __future__ import annotations

import pytest

import etlplus.connector as connector_pkg
from etlplus.connector._api import ConnectorApi
from etlplus.connector._api import ConnectorApiConfigDict
from etlplus.connector._connector import Connector
from etlplus.connector._core import ConnectorBase
from etlplus.connector._core import ConnectorProtocol
from etlplus.connector._database import ConnectorDb
from etlplus.connector._database import ConnectorDbConfigDict
from etlplus.connector._enums import DataConnectorType
from etlplus.connector._file import ConnectorFile
from etlplus.connector._file import ConnectorFileConfigDict
from etlplus.connector._queue import ConnectorQueue
from etlplus.connector._queue import ConnectorQueueConfigDict
from etlplus.connector._types import ConnectorType
from etlplus.connector._utils import parse_connector

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #

CONNECTOR_EXPORTS = [
    ('ConnectorApi', ConnectorApi),
    ('ConnectorDb', ConnectorDb),
    ('ConnectorFile', ConnectorFile),
    ('ConnectorQueue', ConnectorQueue),
    ('DataConnectorType', DataConnectorType),
    ('parse_connector', parse_connector),
    ('Connector', Connector),
    ('ConnectorBase', ConnectorBase),
    ('ConnectorProtocol', ConnectorProtocol),
    ('ConnectorType', ConnectorType),
    ('ConnectorApiConfigDict', ConnectorApiConfigDict),
    ('ConnectorDbConfigDict', ConnectorDbConfigDict),
    ('ConnectorFileConfigDict', ConnectorFileConfigDict),
    ('ConnectorQueueConfigDict', ConnectorQueueConfigDict),
]

# SECTION: TESTS ============================================================ #


class TestConnectorPackageExports:
    """Unit tests for package-level exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert connector_pkg.__all__ == [name for name, _value in CONNECTOR_EXPORTS]

    @pytest.mark.parametrize(('name', 'expected'), CONNECTOR_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(connector_pkg, name) == expected
