"""
:mod:`tests.unit.file.test_u_file_tab` module.

Unit tests for :mod:`etlplus.file.tab`.
"""

from __future__ import annotations

from etlplus.file import tab as mod

from .pytest_file_contracts import DelimitedRoundtripModuleContract

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestTab(DelimitedRoundtripModuleContract):
    """Unit tests for :mod:`etlplus.file.tab`."""

    module = mod
    format_name = 'tab'
    delimiter = '\t'
