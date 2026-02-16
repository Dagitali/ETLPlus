"""
:mod:`tests.integration.file.test_i_file_tsv` module.

Integration tests for :mod:`etlplus.file.tsv`.
"""

from __future__ import annotations

from etlplus.file import tsv as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestTsv(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.tsv`."""

    module = mod
    file_name = 'data.tsv'
