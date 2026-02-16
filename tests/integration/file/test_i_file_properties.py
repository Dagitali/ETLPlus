"""
:mod:`tests.integration.file.test_i_file_properties` module.

Integration smoke tests for :mod:`etlplus.file.properties`.
"""

from __future__ import annotations

from etlplus.file import properties as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestProperties(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.properties`."""

    module = mod
    payload = {'id': '99', 'name': 'Grace'}
