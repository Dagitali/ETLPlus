"""
ETLPlus Connector Error Tests
==============================

Unit tests for the ETLPlus connector error handling.
"""
from __future__ import annotations

import pytest

from etlplus.config.connector import parse_connector


def test_parse_connector_unsupported_type_raises():
    with pytest.raises(TypeError):
        parse_connector({'name': 'x', 'type': 'unknown'})
