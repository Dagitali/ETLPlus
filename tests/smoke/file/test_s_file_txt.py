"""Smoke tests for etlplus.file.txt."""

from __future__ import annotations

from etlplus.file import txt as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestTxt(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.txt."""

    module = mod
    file_name = 'data.txt'
    payload = {'text': '99\nGrace'}
