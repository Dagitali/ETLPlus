"""
:mod:`tests.integration.file.test_i_file_gz` module.

Integration tests for :mod:`etlplus.file.gz`.
"""

from __future__ import annotations

from etlplus.file import gz as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestGz(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.gz`."""

    module = mod
    file_name = 'data.json.gz'
