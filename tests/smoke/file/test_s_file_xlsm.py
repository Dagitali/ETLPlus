"""
Smoke tests for etlplus.file.xlsm.
"""

from __future__ import annotations

from etlplus.file import xlsm as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestXlsm(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.xlsm."""

    module = mod
    file_name = 'data.xlsm'
