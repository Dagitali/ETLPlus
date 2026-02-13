"""
Smoke tests for etlplus.file.ndjson.
"""

from __future__ import annotations

from etlplus.file import ndjson as mod
from tests.smoke.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestNdjson(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.ndjson."""

    module = mod
    file_name = 'data.ndjson'
