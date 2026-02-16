"""
:mod:`tests.integration.file.test_i_file_nc` module.

Integration smoke tests for :mod:`etlplus.file.nc`.
"""

from __future__ import annotations

from etlplus.file import nc as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestNc(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.nc`."""

    module = mod
