"""
Smoke tests for etlplus.file.orc.
"""

from __future__ import annotations

from etlplus.file import orc as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestOrc(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.orc."""

    module = mod
    file_name = 'data.orc'
