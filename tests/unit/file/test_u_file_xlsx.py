"""
:mod:`tests.unit.file.test_u_file_xlsx` module.

Unit tests for :mod:`etlplus.file.xlsx`.
"""

from __future__ import annotations

from etlplus.file import xlsx as mod

from .pytest_file_contracts import WritableSpreadsheetModuleContract

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestXlsx(WritableSpreadsheetModuleContract):
    """Unit tests for :mod:`etlplus.file.xlsx`."""

    module = mod
    format_name = 'xlsx'
    read_engine = 'openpyxl'
    write_engine = 'openpyxl'
