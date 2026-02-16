"""
:mod:`tests.integration.file.test_i_file_parquet` module.

Integration smoke tests for :mod:`etlplus.file.parquet`.
"""

from __future__ import annotations

from etlplus.file import parquet as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestParquet(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.parquet`."""

    module = mod
