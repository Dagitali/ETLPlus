"""
:mod:`tests.integration.file.test_i_file_fwf` module.

Integration tests for :mod:`etlplus.file.fwf`.
"""

from __future__ import annotations

from etlplus.file import fwf as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestFwf(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.fwf`."""

    module = mod
    file_name = 'data.fwf'
