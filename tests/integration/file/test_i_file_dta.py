"""
:mod:`tests.integration.file.test_i_file_dta` module.

Integration tests for :mod:`etlplus.file.dta`.
"""

from __future__ import annotations

from etlplus.file import dta as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestDta(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.dta`."""

    module = mod
    file_name = 'data.dta'
