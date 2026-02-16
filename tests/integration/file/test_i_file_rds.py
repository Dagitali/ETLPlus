"""
:mod:`tests.integration.file.test_i_file_rds` module.

Integration tests for :mod:`etlplus.file.rds`.
"""

from __future__ import annotations

from etlplus.file import rds as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestRds(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.rds`."""

    module = mod
    file_name = 'data.rds'
