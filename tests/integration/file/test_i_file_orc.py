"""
:mod:`tests.integration.file.test_i_file_orc` module.

Integration smoke tests for :mod:`etlplus.file.orc`.
"""

from __future__ import annotations

from etlplus.file import orc as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestOrc(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.orc`."""

    module = mod
