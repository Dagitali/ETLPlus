"""Smoke tests for etlplus.file.ini."""

from __future__ import annotations

from etlplus.file import ini as mod
from tests.smoke.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestIni(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.ini."""

    module = mod
    file_name = 'data.ini'
    payload = {'DEFAULT': {'name': 'Ada'}, 'main': {'age': '36'}}
