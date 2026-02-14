"""Smoke tests for etlplus.file.pb."""

from __future__ import annotations

from etlplus.file import pb as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestPb(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.pb."""

    module = mod
    file_name = 'data.pb'
    payload = {'payload_base64': 'aGVsbG8='}
