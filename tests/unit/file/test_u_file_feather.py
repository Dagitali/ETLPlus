"""
:mod:`tests.unit.file.test_u_file_feather` module.

Unit tests for :mod:`etlplus.file.feather`.
"""

from __future__ import annotations

from etlplus.file import feather as mod
from tests.unit.file.conftest import PyarrowGatedPandasColumnarModuleContract

# SECTION: TESTS ============================================================ #


class TestFeather(PyarrowGatedPandasColumnarModuleContract):
    """Unit tests for :mod:`etlplus.file.feather`."""

    module = mod
    format_name = 'feather'
    read_method_name = 'read_feather'
    write_calls_attr = 'to_feather_calls'
