"""
:mod:`tests.unit.file.test_u_file_psv` module.

Unit tests for :mod:`etlplus.file.psv`.
"""

from __future__ import annotations

from etlplus.file import psv as mod

from .pytest_file_contracts import DelimitedRoundtripModuleContract

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestPsv(DelimitedRoundtripModuleContract):
    """Unit tests for :mod:`etlplus.file.psv`."""

    module = mod
    format_name = 'psv'
    delimiter = '|'
