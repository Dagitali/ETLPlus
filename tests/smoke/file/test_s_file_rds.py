"""
Smoke tests for etlplus.file.rds.
"""

from __future__ import annotations

from etlplus.file import rds as mod
from tests.smoke.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestRds(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.rds."""

    module = mod
    file_name = 'data.rds'
