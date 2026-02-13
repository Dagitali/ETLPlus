"""
Smoke tests for etlplus.file.duckdb.
"""

from __future__ import annotations

from etlplus.file import duckdb as mod
from tests.smoke.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestDuckdb(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.duckdb."""

    module = mod
    file_name = 'data.duckdb'
