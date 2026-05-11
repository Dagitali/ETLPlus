"""
:mod:`tests.unit.connector.test_u_connector_enums` module.

Unit tests for :mod:`etlplus.connector._enums` coercion helpers and behaviors.
"""

from __future__ import annotations

import pytest

from etlplus.connector._enums import DataConnectorType

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestDataConnectorType:
    """Unit tests for :class:`etlplus.connector._enums.DataConnectorType`."""

    def test_aliases_returns_expected_mapping(self) -> None:
        """Test that :meth:`aliases` returns the documented alias map."""
        assert DataConnectorType.aliases() == {
            'http': 'api',
            'https': 'api',
            'rest': 'api',
            'db': 'database',
            'filesystem': 'file',
            'fs': 'file',
            'message-queue': 'queue',
            'mq': 'queue',
            'sqs': 'queue',
        }

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            ('API', DataConnectorType.API),
            (' rest ', DataConnectorType.API),
            ('db', DataConnectorType.DATABASE),
            ('Fs', DataConnectorType.FILE),
            ('sqs', DataConnectorType.QUEUE),
        ],
    )
    def test_coerce_aliases(
        self,
        value: str,
        expected: DataConnectorType,
    ) -> None:
        """Test that alias values coerce to the expected enum members."""
        assert DataConnectorType.coerce(value) is expected

    def test_invalid_value_raises_value_error(self) -> None:
        """Test that invalid values raise :class:`ValueError`."""
        with pytest.raises(ValueError, match='Invalid DataConnectorType'):
            DataConnectorType.coerce('stream')
