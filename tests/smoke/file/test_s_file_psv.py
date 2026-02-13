"""
Smoke tests for etlplus.file.psv.
"""

from __future__ import annotations

from etlplus.file import psv as mod
from tests.smoke.file.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestPsv(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.psv."""

    module = mod
    file_name = 'data.psv'
