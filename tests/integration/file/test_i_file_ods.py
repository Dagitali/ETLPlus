"""
:mod:`tests.integration.file.test_i_file_ods` module.

Integration smoke tests for :mod:`etlplus.file.ods`.
"""

from __future__ import annotations

from etlplus.file import ods as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestOds(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.ods`."""

    module = mod
