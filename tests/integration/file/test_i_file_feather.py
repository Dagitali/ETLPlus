"""
:mod:`tests.integration.file.test_i_file_feather` module.

Integration smoke tests for :mod:`etlplus.file.feather`.
"""

from __future__ import annotations

from etlplus.file import feather as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestFeather(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.feather`."""

    module = mod
