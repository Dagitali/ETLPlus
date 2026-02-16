"""
:mod:`tests.integration.file.test_i_file_arrow` module.

Integration smoke tests for :mod:`etlplus.file.arrow`.
"""

from __future__ import annotations

from etlplus.file import arrow as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestArrow(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.arrow`."""

    module = mod
