"""
:mod:`tests.integration.file.test_i_file_psv` module.

Integration smoke tests for :mod:`etlplus.file.psv`.
"""

from __future__ import annotations

from etlplus.file import psv as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestPsv(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.psv`."""

    module = mod
