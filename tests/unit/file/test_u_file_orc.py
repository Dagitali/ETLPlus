"""
:mod:`tests.unit.file.test_u_file_orc` module.

Unit tests for :mod:`etlplus.file.orc`.
"""

from __future__ import annotations

from etlplus.file import orc as mod

from .pytest_file_contracts import PyarrowGatedPandasColumnarModuleContract

# SECTION: TESTS ============================================================ #


class TestOrc(PyarrowGatedPandasColumnarModuleContract):
    """Unit tests for :mod:`etlplus.file.orc`."""

    module = mod
    format_name = 'orc'
    read_method_name = 'read_orc'
    write_calls_attr = 'to_orc_calls'
    write_uses_index = True
