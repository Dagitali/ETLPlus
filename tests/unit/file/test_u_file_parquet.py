"""
:mod:`tests.unit.file.test_u_file_parquet` module.

Unit tests for :mod:`etlplus.file.parquet`.
"""

from __future__ import annotations

from etlplus.file import parquet as mod

from .pytest_file_contracts import PyarrowGatedPandasColumnarModuleContract

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestParquet(PyarrowGatedPandasColumnarModuleContract):
    """Unit tests for :mod:`etlplus.file.parquet`."""

    module = mod
    format_name = 'parquet'
    read_method_name = 'read_parquet'
    write_calls_attr = 'to_parquet_calls'
    write_uses_index = True
