"""
Smoke tests for etlplus.file.nc.
"""

from __future__ import annotations

from etlplus.file import nc as mod
from tests.smoke.file.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestNc(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.nc."""

    module = mod
    file_name = 'data.nc'
