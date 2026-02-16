"""
:mod:`tests.integration.file.test_i_file_duckdb` module.

Integration smoke tests for :mod:`etlplus.file.duckdb`.
"""

from __future__ import annotations

from etlplus.file import duckdb as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestDuckdb(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.duckdb`."""

    module = mod
