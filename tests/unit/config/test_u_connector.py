"""
:mod:`tests.unit.config.test_u_connector` module.

Unit tests for ``etlplus.config.connector``.

Notes
-----
- Uses minimal ``dict`` payloads.
"""

from __future__ import annotations

import pytest

from etlplus.config import parse_connector

# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestParseConnector:
    """
    Unit test suite for :func:`parse_connector`.

    Notes
    -----
    Tests error handling for unsupported connector types and missing fields.
    """

    @pytest.mark.parametrize(
        'payload,expected_exception',
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
