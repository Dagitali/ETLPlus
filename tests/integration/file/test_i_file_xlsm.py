"""
:mod:`tests.integration.file.test_i_file_xlsm` module.

Integration tests for :mod:`etlplus.file.xlsm`.
"""

from __future__ import annotations

from etlplus.file import xlsm as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestXlsm(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.xlsm`."""

    module = mod
    file_name = 'data.xlsm'
