"""
:mod:`tests.unit.file.test_u_file_wks` module.

Unit tests for :mod:`etlplus.file.wks`.
"""

from __future__ import annotations

from etlplus.file import wks as mod

from .pytest_file_contract_contracts import StubModuleContract

# SECTION: TESTS ============================================================ #


class TestWks(StubModuleContract):
    """Unit tests for :mod:`etlplus.file.wks`."""

    module = mod
    handler_cls = mod.WksFile
    format_name = 'wks'
