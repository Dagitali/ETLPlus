"""
:mod:`tests.integration.file.test_i_file_ods` module.

Integration tests for :mod:`etlplus.file.ods`.
"""

from __future__ import annotations

from etlplus.file import ods as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestOds(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.ods`."""

    module = mod
    file_name = 'data.ods'
