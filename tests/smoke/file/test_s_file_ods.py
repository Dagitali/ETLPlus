"""
Smoke tests for etlplus.file.ods.
"""

from __future__ import annotations

from etlplus.file import ods as mod
from tests.smoke.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestOds(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.ods."""

    module = mod
    file_name = 'data.ods'
