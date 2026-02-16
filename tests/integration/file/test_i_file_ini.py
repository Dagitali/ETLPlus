"""
:mod:`tests.integration.file.test_i_file_ini` module.

Integration tests for :mod:`etlplus.file.ini`.
"""

from __future__ import annotations

from etlplus.file import ini as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestIni(SmokeRoundtripModuleContract):
    """Integration tests for :mod:`etlplus.file.ini`."""

    module = mod
    file_name = 'data.ini'
    payload = {'DEFAULT': {'name': 'Ada'}, 'main': {'age': '36'}}
