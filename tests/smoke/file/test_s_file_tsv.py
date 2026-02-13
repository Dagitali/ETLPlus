"""
Smoke tests for etlplus.file.tsv.
"""

from __future__ import annotations

from etlplus.file import tsv as mod
from tests.smoke.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestTsv(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.tsv."""

    module = mod
    file_name = 'data.tsv'
