"""
Smoke tests for etlplus.file.json.
"""

from __future__ import annotations

from etlplus.file import json as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestJson(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.json."""

    module = mod
    file_name = 'data.json'
