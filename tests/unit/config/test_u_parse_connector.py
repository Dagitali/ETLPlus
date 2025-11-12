"""
``tests.unit.config.test_u_parse_connector`` module.

Unit tests for ``etlplus.config.connector``.

Notes
-----
- Uses minimal ``dict`` payloads.
"""
from __future__ import annotations

import pytest

from etlplus.config import parse_connector


# SECTION: TESTS ============================================================ #


class TestParseConnector:
    """
    Unit test suite for the :func:`parse_connector` function.
    """

    def test_unsupported_type_raises(self):  # noqa: D401
        with pytest.raises(TypeError):
            parse_connector({'name': 'x', 'type': 'unknown'})
