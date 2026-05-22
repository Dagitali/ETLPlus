"""
:mod:`tests.unit.file.test_u_file_csv` module.

Unit tests for :mod:`etlplus.file.csv`.
"""

from __future__ import annotations

from etlplus.file import csv as mod

from .pytest_file_contracts import DelimitedRoundtripModuleContract

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestCsv(DelimitedRoundtripModuleContract):
    """Unit tests for :mod:`etlplus.file.csv`."""

    module = mod
    format_name = 'csv'
    delimiter = ','
