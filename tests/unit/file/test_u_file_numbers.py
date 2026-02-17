"""
:mod:`tests.unit.file.test_u_file_numbers` module.

Unit tests for :mod:`etlplus.file.numbers`.
"""

from __future__ import annotations

from etlplus.file import numbers as mod

from .pytest_file_contracts import StubModuleContract

# SECTION: TESTS ============================================================ #


class TestNumbers(StubModuleContract):
    """Unit tests for :mod:`etlplus.file.numbers`."""

    module = mod
    handler_cls = mod.NumbersFile
    format_name = 'numbers'
