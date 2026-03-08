"""
:mod:`tests.integration.file.test_i_file_sqlite` module.

Integration smoke tests for :mod:`etlplus.file.sqlite`.
"""

from __future__ import annotations

from etlplus.file import sqlite as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestSqlite(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.sqlite`."""

    module = mod
