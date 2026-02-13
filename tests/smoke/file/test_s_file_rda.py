"""
Smoke tests for etlplus.file.rda.
"""

from __future__ import annotations

from etlplus.file import rda as mod
from tests.smoke.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestRda(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.rda."""

    module = mod
    file_name = 'data.rda'
