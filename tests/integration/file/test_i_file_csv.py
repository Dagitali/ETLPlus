"""
:mod:`tests.integration.file.test_i_file_csv` module.

Integration tests for :mod:`etlplus.file.csv`.
"""

from __future__ import annotations

from etlplus.file import csv as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestCsv(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.csv`."""

    module = mod
    file_name = 'data.csv'
