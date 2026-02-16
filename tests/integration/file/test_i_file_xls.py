"""
:mod:`tests.integration.file.test_i_file_xls` module.

Integration tests for :mod:`etlplus.file.xls`.
"""

from __future__ import annotations

from etlplus.file import xls as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestXls(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.xls`."""

    module = mod
    file_name = 'data.xls'
    expect_write_error = RuntimeError
    error_match = 'read-only'
