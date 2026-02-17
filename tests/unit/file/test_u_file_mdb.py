"""
:mod:`tests.unit.file.test_u_file_mdb` module.

Unit tests for :mod:`etlplus.file.mdb`.
"""

from __future__ import annotations

from etlplus.file import mdb as mod

from .pytest_file_contracts import StubModuleContract

# SECTION: TESTS ============================================================ #


class TestMdb(StubModuleContract):
    """Unit tests for :mod:`etlplus.file.mdb`."""

    module = mod
    handler_cls = mod.MdbFile
    format_name = 'mdb'
