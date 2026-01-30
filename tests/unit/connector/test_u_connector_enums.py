"""
:mod:`tests.unit.test_u_connector_enums` module.

Unit tests for :mod:`etlplus.connector.enums` coercion helpers and behaviors.
"""

from __future__ import annotations

import pytest

from etlplus.connector.enums import DataConnectorType

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


class TestDataConnectorType:
    """
    Unit test suite for :class:`etlplus.connector.enums.DataConnectorType`.
    """

    @pytest.mark.parametrize(
        'value,expected',
        [
            ('API', DataConnectorType.API),
            (' rest ', DataConnectorType.API),
            ('db', DataConnectorType.DATABASE),
            ('Fs', DataConnectorType.FILE),
        ],
    )
    def test_coerce_aliases(
        self,
        value: str,
        expected: DataConnectorType,
    ) -> None:
        """Test alias coercions."""
        assert DataConnectorType.coerce(value) is expected

    def test_invalid_value(self) -> None:
        """Test that invalid values raise ValueError."""
        with pytest.raises(ValueError, match='Invalid DataConnectorType'):
            DataConnectorType.coerce('queue')
