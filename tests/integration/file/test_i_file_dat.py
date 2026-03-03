"""
:mod:`tests.integration.file.test_i_file_dat` module.

Integration smoke tests for :mod:`etlplus.file.dat`.
"""

from __future__ import annotations

from etlplus.file import dat as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestDat(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.dat`."""

    module = mod
