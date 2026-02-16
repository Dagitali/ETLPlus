"""
:mod:`tests.integration.file.test_i_file_xlsx` module.

Integration tests for :mod:`etlplus.file.xlsx`.
"""

from __future__ import annotations

from etlplus.file import xlsx as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestXlsx(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.xlsx`."""

    module = mod
    file_name = 'data.xlsx'
