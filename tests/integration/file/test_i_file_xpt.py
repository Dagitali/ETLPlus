"""
:mod:`tests.integration.file.test_i_file_xpt` module.

Integration smoke tests for :mod:`etlplus.file.xpt`.
"""

from __future__ import annotations

from etlplus.file import xpt as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestXpt(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.xpt`."""

    module = mod
