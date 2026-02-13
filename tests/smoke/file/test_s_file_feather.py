"""
Smoke tests for etlplus.file.feather.
"""

from __future__ import annotations

from etlplus.file import feather as mod
from tests.smoke.file.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestFeather(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.feather."""

    module = mod
    file_name = 'data.feather'
