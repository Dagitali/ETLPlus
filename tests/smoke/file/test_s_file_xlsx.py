"""
Smoke tests for etlplus.file.xlsx.
"""

from __future__ import annotations

from etlplus.file import xlsx as mod
from tests.smoke.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestXlsx(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.xlsx."""

    module = mod
    file_name = 'data.xlsx'
