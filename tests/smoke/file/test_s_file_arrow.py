"""
Smoke tests for etlplus.file.arrow.
"""

from __future__ import annotations

from etlplus.file import arrow as mod
from tests.smoke.file.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestArrow(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.arrow."""

    module = mod
    file_name = 'data.arrow'
