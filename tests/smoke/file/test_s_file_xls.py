"""Smoke tests for etlplus.file.xls."""

from __future__ import annotations

from etlplus.file import xls as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestXls(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.xls."""

    module = mod
    file_name = 'data.xls'
    expect_write_error = RuntimeError
    error_match = 'read-only'
