"""
Smoke tests for etlplus.file.gz.
"""

from __future__ import annotations

from etlplus.file import gz as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestGz(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.gz."""

    module = mod
    file_name = 'data.json.gz'
