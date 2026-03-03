"""
:mod:`tests.integration.file.test_i_file_csv` module.

Integration smoke tests for :mod:`etlplus.file.csv`.
"""

from __future__ import annotations

from etlplus.file import csv as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestCsv(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.csv`."""

    module = mod
