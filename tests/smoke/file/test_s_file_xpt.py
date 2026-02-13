"""
Smoke tests for etlplus.file.xpt.
"""

from __future__ import annotations

from etlplus.file import xpt as mod
from tests.smoke.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestXpt(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.xpt."""

    module = mod
    file_name = 'data.xpt'
