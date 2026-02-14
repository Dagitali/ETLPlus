"""
Smoke tests for etlplus.file.cbor.
"""

from __future__ import annotations

from etlplus.file import cbor as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestCbor(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.cbor."""

    module = mod
    file_name = 'data.cbor'
