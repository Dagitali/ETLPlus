"""
:mod:`tests.unit.file.test_u_file_xlsx` module.

Unit tests for :mod:`etlplus.file.xlsx`.
"""

from __future__ import annotations

from etlplus.file import xlsx as mod
from tests.unit.file.conftest import WritableSpreadsheetModuleContract

# SECTION: TESTS ============================================================ #


class TestXlsx(WritableSpreadsheetModuleContract):
    """Unit tests for :mod:`etlplus.file.xlsx`."""

    module = mod
    format_name = 'xlsx'
    dependency_hint = 'openpyxl'
