"""
:mod:`tests.integration.file.test_i_file_rda` module.

Integration tests for :mod:`etlplus.file.rda`.
"""

from __future__ import annotations

from etlplus.file import rda as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestRda(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.rda`."""

    module = mod
    file_name = 'data.rda'
