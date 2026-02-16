"""
:mod:`tests.integration.file.test_i_file_duckdb` module.

Integration tests for :mod:`etlplus.file.duckdb`.
"""

from __future__ import annotations

from etlplus.file import duckdb as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestDuckdb(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.duckdb`."""

    module = mod
    file_name = 'data.duckdb'
