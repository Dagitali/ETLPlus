"""
Smoke tests for etlplus.file.sqlite.
"""

from __future__ import annotations

from etlplus.file import sqlite as mod
from tests.smoke.file.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestSqlite(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.sqlite."""

    module = mod
    file_name = 'data.sqlite'
