"""
Smoke tests for etlplus.file.feather.
"""

from __future__ import annotations

from etlplus.file import feather as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestFeather(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.feather."""

    module = mod
    file_name = 'data.feather'
