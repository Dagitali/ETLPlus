"""
:mod:`tests.unit.file.test_u_file_csv` module.

Unit tests for :mod:`etlplus.file.csv`.
"""

from __future__ import annotations

from etlplus.file import csv as mod
from tests.unit.file.conftest import DelimitedModuleContract

# SECTION: TESTS ============================================================ #


class TestCsv(DelimitedModuleContract):
    """Unit tests for :mod:`etlplus.file.csv`."""

    module = mod
    format_name = 'csv'
    delimiter = ','
