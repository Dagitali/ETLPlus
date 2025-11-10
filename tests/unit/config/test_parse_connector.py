"""
tests.unit.config.test_parse_connector unit tests module.


Unit tests for the ETLPlus connector error handling.
"""
from __future__ import annotations

import pytest

from etlplus.config import parse_connector


# SECTION: TESTS ============================================================ #


class TestParseConnector:
    def test_unsupported_type_raises(self):  # noqa: D401
        with pytest.raises(TypeError):
            parse_connector({'name': 'x', 'type': 'unknown'})
