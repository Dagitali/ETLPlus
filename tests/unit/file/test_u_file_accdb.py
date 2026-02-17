"""
:mod:`tests.unit.file.test_u_file_accdb` module.

Unit tests for :mod:`etlplus.file.accdb`.
"""

from __future__ import annotations

from etlplus.file import accdb as mod

from .pytest_file_contracts import StubModuleContract

# SECTION: TESTS ============================================================ #


class TestAccdb(StubModuleContract):
    """Unit tests for :mod:`etlplus.file.accdb`."""

    module = mod
    handler_cls = mod.AccdbFile
    format_name = 'accdb'
