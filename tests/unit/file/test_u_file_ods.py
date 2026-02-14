"""
:mod:`tests.unit.file.test_u_file_ods` module.

Unit tests for :mod:`etlplus.file.ods`.
"""

from __future__ import annotations

from etlplus.file import ods as mod

from .pytest_file_contract_contracts import WritableSpreadsheetModuleContract

# SECTION: TESTS ============================================================ #


class TestOds(WritableSpreadsheetModuleContract):
    """Unit tests for :mod:`etlplus.file.ods`."""

    module = mod
    format_name = 'ods'
    dependency_hint = 'odfpy'
    read_engine = 'odf'
    write_engine = 'odf'
