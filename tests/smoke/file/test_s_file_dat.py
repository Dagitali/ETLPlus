"""
Smoke tests for etlplus.file.dat.
"""

from __future__ import annotations

from etlplus.file import dat as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestDat(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.dat."""

    module = mod
    file_name = 'data.dat'
