"""Smoke tests for etlplus.file.properties."""

from __future__ import annotations

from etlplus.file import properties as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestProperties(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.properties."""

    module = mod
    file_name = 'data.properties'
    payload = {'id': '99', 'name': 'Grace'}
