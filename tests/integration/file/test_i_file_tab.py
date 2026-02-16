"""
:mod:`tests.integration.file.test_i_file_tab` module.

Integration smoke tests for :mod:`etlplus.file.tab`.
"""

from __future__ import annotations

from etlplus.file import tab as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestTab(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.tab`."""

    module = mod
