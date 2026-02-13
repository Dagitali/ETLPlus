"""
Smoke tests for etlplus.file.csv.
"""

from __future__ import annotations

from etlplus.file import csv as mod
from tests.smoke.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestCsv(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.csv."""

    module = mod
    file_name = 'data.csv'
