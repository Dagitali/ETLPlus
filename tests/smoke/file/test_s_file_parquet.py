"""
Smoke tests for etlplus.file.parquet.
"""

from __future__ import annotations

from etlplus.file import parquet as mod
from tests.smoke.file.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestParquet(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.parquet."""

    module = mod
    file_name = 'data.parquet'
