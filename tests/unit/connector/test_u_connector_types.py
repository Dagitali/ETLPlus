"""
:mod:`tests.unit.connector.test_u_connector_types` module.

Unit tests for :mod:`etlplus.connector._types`.
"""

from __future__ import annotations

import pytest

import etlplus.connector._types as connector_types
from etlplus.connector._enums import DataConnectorType

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorTypes:
    """Unit tests for connector kind type alias exports."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param(DataConnectorType.API, 'api', id='enum-member'),
            pytest.param('database', 'database', id='database-literal'),
            pytest.param('file', 'file', id='file-literal'),
        ],
    )
    def test_alias_accepts_enum_and_literal_values(
        self,
        value: connector_types.ConnectorType,
        expected: str,
    ) -> None:
        """Test that :class:`ConnectorType` accepts enum members and literals."""

        def identity(value: connector_types.ConnectorType) -> str:
            return str(value)

        assert identity(value) == expected

    def test_exports_include_connector_type(self) -> None:
        """Test that the module exports the :class:`ConnectorType` alias."""
        assert connector_types.__all__ == ['ConnectorType']
