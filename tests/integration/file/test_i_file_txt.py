"""
:mod:`tests.integration.file.test_i_file_txt` module.

Integration smoke tests for :mod:`etlplus.file.txt`.
"""

from __future__ import annotations

from etlplus.file import txt as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestTxt(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.txt`."""

    module = mod
    payload = {'text': '99\nGrace'}
