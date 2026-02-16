"""
:mod:`tests.integration.file.test_i_file_sqlite` module.

Integration tests for :mod:`etlplus.file.sqlite`.
"""

from __future__ import annotations

from etlplus.file import sqlite as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestSqlite(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.sqlite`."""

    module = mod
    file_name = 'data.sqlite'
