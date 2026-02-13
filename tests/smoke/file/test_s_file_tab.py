"""
Smoke tests for etlplus.file.tab.
"""

from __future__ import annotations

from etlplus.file import tab as mod
from tests.smoke.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestTab(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.tab."""

    module = mod
    file_name = 'data.tab'
