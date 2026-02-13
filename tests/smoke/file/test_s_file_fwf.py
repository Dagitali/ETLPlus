"""
Smoke tests for etlplus.file.fwf.
"""

from __future__ import annotations

from etlplus.file import fwf as mod
from tests.smoke.file.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestFwf(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.fwf."""

    module = mod
    file_name = 'data.fwf'
