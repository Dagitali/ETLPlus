"""
Smoke tests for etlplus.file.bson.
"""

from __future__ import annotations

from etlplus.file import bson as mod
from tests.smoke.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestBson(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.bson."""

    module = mod
    file_name = 'data.bson'
