"""
:mod:`tests.unit.file.test_u_file_xlsm` module.

Unit tests for :mod:`etlplus.file.xlsm`.
"""

from __future__ import annotations

from etlplus.file import xlsm as mod

from .pytest_file_contracts import WritableSpreadsheetModuleContract

# SECTION: TESTS ============================================================ #


class TestXlsm(WritableSpreadsheetModuleContract):
    """Unit tests for :mod:`etlplus.file.xlsm`."""

    module = mod
    format_name = 'xlsm'
    dependency_hint = 'openpyxl'
