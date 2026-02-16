"""
:mod:`tests.integration.file.test_i_file_sav` module.

Integration tests for :mod:`etlplus.file.sav`.
"""

from __future__ import annotations

from etlplus.file import sav as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestSav(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.sav`."""

    module = mod
    file_name = 'data.sav'
