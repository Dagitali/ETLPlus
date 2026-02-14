"""
Smoke tests for etlplus.file.msgpack.
"""

from __future__ import annotations

from etlplus.file import msgpack as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestMsgpack(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.msgpack."""

    module = mod
    file_name = 'data.msgpack'
