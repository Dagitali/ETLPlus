"""
Smoke tests for etlplus.file.sav.
"""

from __future__ import annotations

from etlplus.file import sav as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestSav(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.sav."""

    module = mod
    file_name = 'data.sav'
