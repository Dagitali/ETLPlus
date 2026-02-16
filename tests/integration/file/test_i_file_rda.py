"""
:mod:`tests.integration.file.test_i_file_rda` module.

Integration smoke tests for :mod:`etlplus.file.rda`.
"""

from __future__ import annotations

from etlplus.file import rda as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestRda(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.rda`."""

    module = mod
