"""
:mod:`tests.integration.file.test_i_file_zip` module.

Integration tests for :mod:`etlplus.file.zip`.
"""

from __future__ import annotations

from etlplus.file import zip as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestZip(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.zip`."""

    module = mod
    file_name = 'data.json.zip'
