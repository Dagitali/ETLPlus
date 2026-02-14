"""
Smoke tests for etlplus.file.zip.
"""

from __future__ import annotations

from etlplus.file import zip as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestZip(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.zip."""

    module = mod
    file_name = 'data.json.zip'
