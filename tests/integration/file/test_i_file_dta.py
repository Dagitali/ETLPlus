"""
:mod:`tests.integration.file.test_i_file_dta` module.

Integration smoke tests for :mod:`etlplus.file.dta`.
"""

from __future__ import annotations

from etlplus.file import dta as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestDta(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.dta`."""

    module = mod
