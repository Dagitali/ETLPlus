"""
Smoke tests for etlplus.file.dta.
"""

from __future__ import annotations

from etlplus.file import dta as mod
from tests.smoke.file.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestDta(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.dta."""

    module = mod
    file_name = 'data.dta'
