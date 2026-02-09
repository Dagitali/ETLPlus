"""
:mod:`tests.unit.file.test_u_file_xls` module.

Unit tests for :mod:`etlplus.file.xls`.
"""

from __future__ import annotations

from etlplus.file import xls as mod
from tests.unit.file.conftest import ReadOnlyWriteGuardMixin
from tests.unit.file.conftest import SpreadsheetCategoryContractBase
from tests.unit.file.conftest import SpreadsheetReadImportErrorMixin

# SECTION: CONTRACTS ======================================================== #


class ReadOnlySpreadsheetModuleContract(
    SpreadsheetCategoryContractBase,
    SpreadsheetReadImportErrorMixin,
    ReadOnlyWriteGuardMixin,
):
    """Reusable contract suite for read-only spreadsheet wrapper modules."""

# SECTION: TESTS ============================================================ #


class TestXls(ReadOnlySpreadsheetModuleContract):
    """Unit tests for :mod:`etlplus.file.xls`."""

    module = mod
    format_name = 'xls'
    dependency_hint = 'xlrd'
